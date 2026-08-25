use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
};

use futures_util::{future, future::BoxFuture};

use crate::{
    Citation, DocumentChunker, DocumentLoader, DocumentReference, EmbeddedChunk, Embedding,
    EmbeddingBatchLimits, EmbeddingInput, EmbeddingProvider, IngestionChunk, IngestionDocument,
    RagIngestionRequest, VectorIndex, VectorStoreCapabilities,
};

pub(super) fn document_reference() -> DocumentReference {
    DocumentReference::new("acme", "doc-1", "v3", "sha256:abc")
        .expect("test document reference is valid")
}

pub(super) fn ingestion_document() -> IngestionDocument {
    IngestionDocument::new(
        document_reference(),
        "internal source document text",
        Citation::new("kb://policy", "Account policy").expect("test citation is valid"),
    )
    .expect("test ingestion document is valid")
}

pub(super) fn ingestion_chunk() -> IngestionChunk {
    IngestionChunk::new(
        "chunk-1",
        document_reference(),
        "internal source document text",
        Citation::new("kb://policy", "Account policy").expect("test citation is valid"),
    )
    .expect("test ingestion chunk is valid")
}

pub(super) fn ingestion_request() -> RagIngestionRequest {
    RagIngestionRequest::new(document_reference(), "embedding.default")
        .expect("test ingestion request is valid")
}

#[derive(Clone)]
pub(super) struct FakeLoader {
    pub(super) document: IngestionDocument,
}

impl DocumentLoader for FakeLoader {
    type Error = Infallible;

    fn load(
        &self,
        _: RagIngestionRequest,
    ) -> BoxFuture<'static, Result<IngestionDocument, Self::Error>> {
        Box::pin(future::ready(Ok(self.document.clone())))
    }
}

#[derive(Clone)]
pub(super) struct FakeChunker {
    pub(super) chunks: Vec<IngestionChunk>,
}

impl DocumentChunker for FakeChunker {
    type Error = Infallible;

    fn chunk(
        &self,
        _: IngestionDocument,
    ) -> BoxFuture<'static, Result<Vec<IngestionChunk>, Self::Error>> {
        Box::pin(future::ready(Ok(self.chunks.clone())))
    }
}

#[derive(Clone)]
pub(super) struct FakeEmbedder {
    pub(super) embeddings: Vec<Embedding>,
    pub(super) inputs: Arc<Mutex<Vec<EmbeddingInput>>>,
}

impl EmbeddingProvider for FakeEmbedder {
    type Error = Infallible;

    fn embed(
        &self,
        _: String,
        inputs: Vec<EmbeddingInput>,
    ) -> BoxFuture<'static, Result<Vec<Embedding>, Self::Error>> {
        let captured = Arc::clone(&self.inputs);
        let embeddings = self.embeddings.clone();
        Box::pin(async move {
            *captured.lock().expect("test embedding lock is available") = inputs;
            Ok(embeddings)
        })
    }
}

#[derive(Clone)]
pub(super) struct BatchLimitedEmbedder {
    pub(super) limits: EmbeddingBatchLimits,
    pub(super) batches: Arc<Mutex<Vec<Vec<EmbeddingInput>>>>,
    pub(super) dimensions: Vec<usize>,
}

impl EmbeddingProvider for BatchLimitedEmbedder {
    type Error = Infallible;

    fn batch_limits(&self) -> EmbeddingBatchLimits {
        self.limits
    }

    fn embed(
        &self,
        _: String,
        inputs: Vec<EmbeddingInput>,
    ) -> BoxFuture<'static, Result<Vec<Embedding>, Self::Error>> {
        let captured = Arc::clone(&self.batches);
        let dimensions = self.dimensions.clone();
        Box::pin(async move {
            let embeddings = inputs
                .iter()
                .enumerate()
                .map(|(index, _)| {
                    Embedding::new(vec![0.0; dimensions[index % dimensions.len()]])
                        .expect("test embedding dimensions are positive")
                })
                .collect();
            captured
                .lock()
                .expect("test batch lock is available")
                .push(inputs);
            Ok(embeddings)
        })
    }
}

#[derive(Clone)]
pub(super) struct CapturingIndex {
    pub(super) chunks: Arc<Mutex<Vec<EmbeddedChunk>>>,
}

impl VectorIndex for CapturingIndex {
    type Error = Infallible;

    fn capabilities(&self) -> VectorStoreCapabilities {
        VectorStoreCapabilities::new(true, true, false)
    }

    fn upsert(&self, chunks: Vec<EmbeddedChunk>) -> BoxFuture<'static, Result<(), Self::Error>> {
        let captured = Arc::clone(&self.chunks);
        Box::pin(async move {
            *captured.lock().expect("test index lock is available") = chunks;
            Ok(())
        })
    }

    fn delete_document(&self, _: DocumentReference) -> BoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(future::ready(Ok(())))
    }
}
