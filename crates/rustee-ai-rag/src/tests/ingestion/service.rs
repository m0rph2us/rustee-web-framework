use std::sync::{Arc, Mutex};

use crate::{
    Citation, Embedding, EmbeddingBatchLimits, IngestionChunk, RagIngestionError, RagIngestor,
    VectorIndex, VectorStoreCapability,
};

use super::support::{
    BatchLimitedEmbedder, CapturingIndex, FakeChunker, FakeEmbedder, FakeLoader,
    document_reference, ingestion_chunk, ingestion_document, ingestion_request,
};

#[tokio::test]
async fn ingestor_preserves_revision_metadata_through_embedding_and_upsert() {
    let inputs = Arc::new(Mutex::new(Vec::new()));
    let indexed = Arc::new(Mutex::new(Vec::new()));
    let index = CapturingIndex {
        chunks: Arc::clone(&indexed),
    };
    assert!(
        index
            .capabilities()
            .supports(VectorStoreCapability::MetadataFilter)
    );
    assert!(
        index
            .capabilities()
            .supports(VectorStoreCapability::DeleteByDocument)
    );
    assert!(
        !index
            .capabilities()
            .supports(VectorStoreCapability::HybridSearch)
    );
    let ingestor = RagIngestor::new(
        FakeLoader {
            document: ingestion_document(),
        },
        FakeChunker {
            chunks: vec![ingestion_chunk()],
        },
        FakeEmbedder {
            embeddings: vec![Embedding::new(vec![0.25, -0.5]).expect("test embedding is valid")],
            inputs: Arc::clone(&inputs),
        },
        index,
    );

    let request = ingestion_request();
    let report = ingestor.ingest(request.clone()).await.unwrap();

    assert_eq!(report.document(), request.document());
    assert_eq!(report.embedding_model(), "embedding.default");
    assert_eq!(report.chunk_count(), 1);
    let inputs = inputs.lock().expect("test embedding lock is available");
    assert_eq!(inputs.len(), 1);
    assert_eq!(inputs[0].chunk_id(), "chunk-1");
    assert!(!format!("{:?}", inputs[0]).contains("internal source document text"));
    let indexed = indexed.lock().expect("test index lock is available");
    assert_eq!(indexed.len(), 1);
    assert_eq!(indexed[0].chunk().document(), request.document());
    assert_eq!(indexed[0].embedding_model(), "embedding.default");
    assert_eq!(indexed[0].embedding().values(), &[0.25, -0.5]);
    let serialized = serde_json::to_string(&request).expect("request serializes");
    assert!(!serialized.contains("internal source document text"));
    assert!(!format!("{request:?}").contains("sha256:abc"));
}

#[tokio::test]
async fn embedding_count_mismatch_blocks_the_vector_upsert() {
    let indexed = Arc::new(Mutex::new(Vec::new()));
    let ingestor = RagIngestor::new(
        FakeLoader {
            document: ingestion_document(),
        },
        FakeChunker {
            chunks: vec![ingestion_chunk()],
        },
        FakeEmbedder {
            embeddings: Vec::new(),
            inputs: Arc::new(Mutex::new(Vec::new())),
        },
        CapturingIndex {
            chunks: Arc::clone(&indexed),
        },
    );

    let error = ingestor.ingest(ingestion_request()).await.unwrap_err();

    assert!(matches!(
        error,
        RagIngestionError::EmbeddingCountMismatch {
            chunks: 1,
            embeddings: 0,
        }
    ));
    assert!(
        indexed
            .lock()
            .expect("test index lock is available")
            .is_empty()
    );
}

#[tokio::test]
async fn ingestor_batches_embedding_inputs_in_order_before_one_atomic_upsert() {
    let batches = Arc::new(Mutex::new(Vec::new()));
    let indexed = Arc::new(Mutex::new(Vec::new()));
    let second_chunk = IngestionChunk::new(
        "chunk-2",
        document_reference(),
        "another internal source document text",
        Citation::new("kb://policy", "Account policy").expect("test citation is valid"),
    )
    .expect("test ingestion chunk is valid");
    let ingestor = RagIngestor::new(
        FakeLoader {
            document: ingestion_document(),
        },
        FakeChunker {
            chunks: vec![ingestion_chunk(), second_chunk],
        },
        BatchLimitedEmbedder {
            limits: EmbeddingBatchLimits::new(10, 63).expect("test limits are valid"),
            batches: Arc::clone(&batches),
            dimensions: vec![2],
        },
        CapturingIndex {
            chunks: Arc::clone(&indexed),
        },
    );

    let report = ingestor
        .ingest(ingestion_request())
        .await
        .expect("batching ingestion succeeds");

    assert_eq!(report.chunk_count(), 2);
    let batches = batches.lock().expect("test batch lock is available");
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0][0].chunk_id(), "chunk-1");
    assert_eq!(batches[1][0].chunk_id(), "chunk-2");
    assert_eq!(
        indexed.lock().expect("test index lock is available").len(),
        2
    );
}

#[tokio::test]
async fn oversize_provider_batch_input_and_dimension_mismatch_block_upsert() {
    let indexed = Arc::new(Mutex::new(Vec::new()));
    let oversized = RagIngestor::new(
        FakeLoader {
            document: ingestion_document(),
        },
        FakeChunker {
            chunks: vec![ingestion_chunk()],
        },
        BatchLimitedEmbedder {
            limits: EmbeddingBatchLimits::new(1, 1).expect("test limits are valid"),
            batches: Arc::new(Mutex::new(Vec::new())),
            dimensions: vec![2],
        },
        CapturingIndex {
            chunks: Arc::clone(&indexed),
        },
    );
    assert!(matches!(
        oversized.ingest(ingestion_request()).await.unwrap_err(),
        RagIngestionError::EmbeddingInputTooLarge {
            content_bytes: _,
            max_content_bytes: 1,
        }
    ));
    assert!(
        indexed
            .lock()
            .expect("test index lock is available")
            .is_empty()
    );

    let dimension_indexed = Arc::new(Mutex::new(Vec::new()));
    let second_chunk = IngestionChunk::new(
        "chunk-2",
        document_reference(),
        "another internal source document text",
        Citation::new("kb://policy", "Account policy").expect("test citation is valid"),
    )
    .expect("test ingestion chunk is valid");
    let dimension_mismatch = RagIngestor::new(
        FakeLoader {
            document: ingestion_document(),
        },
        FakeChunker {
            chunks: vec![ingestion_chunk(), second_chunk],
        },
        BatchLimitedEmbedder {
            limits: EmbeddingBatchLimits::new(2, 1024).expect("test limits are valid"),
            batches: Arc::new(Mutex::new(Vec::new())),
            dimensions: vec![2, 3],
        },
        CapturingIndex {
            chunks: Arc::clone(&dimension_indexed),
        },
    );
    assert!(matches!(
        dimension_mismatch
            .ingest(ingestion_request())
            .await
            .unwrap_err(),
        RagIngestionError::EmbeddingDimensionMismatch {
            expected_dimensions: 2,
            received_dimensions: 3,
        }
    ));
    assert!(
        dimension_indexed
            .lock()
            .expect("test index lock is available")
            .is_empty()
    );
}
