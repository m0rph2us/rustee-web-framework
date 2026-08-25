use std::fmt;

use crate::{
    Citation, DocumentReference, EmbeddingInput, IngestionChunk, IngestionDocument, RagError,
    RagIngestionError, RagIngestor, RagRetriever, RetrievalQuery, RetrievalScope, RetrievedChunk,
};

struct LeakyAdapterError;

impl fmt::Debug for LeakyAdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LeakyAdapterError(private-document-content)")
    }
}

impl fmt::Display for LeakyAdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("private-document-content")
    }
}

impl std::error::Error for LeakyAdapterError {}

#[test]
fn rag_error_debug_output_redacts_adapter_diagnostics() {
    let retrieval = RagError::Store(LeakyAdapterError);
    let ingestion = RagIngestionError::<
        LeakyAdapterError,
        LeakyAdapterError,
        LeakyAdapterError,
        LeakyAdapterError,
    >::Embed(LeakyAdapterError);

    assert_eq!(format!("{retrieval:?}"), "RagError::Store");
    assert_eq!(format!("{ingestion:?}"), "RagIngestionError::Embed");

    for error in [&retrieval as &dyn std::error::Error, &ingestion] {
        assert!(std::error::Error::source(error).is_some());
        assert!(!format!("{error:?}").contains("private-document-content"));
        assert!(!error.to_string().contains("private-document-content"));
    }
}

#[test]
fn rag_input_and_service_debug_output_redacts_content_and_adapter_config() {
    let query = RetrievalQuery::new(
        "private search text",
        RetrievalScope::new("private-tenant", ["private-document".to_owned()]).unwrap(),
        3,
    )
    .unwrap();
    let retriever = RagRetriever::new(LeakyAdapterError);
    let ingestor = RagIngestor::new(
        LeakyAdapterError,
        LeakyAdapterError,
        LeakyAdapterError,
        LeakyAdapterError,
    );

    let output = format!("{query:?} {retriever:?} {ingestor:?}");

    for sensitive in [
        "private search text",
        "private-tenant",
        "private-document",
        "private-document-content",
    ] {
        assert!(!output.contains(sensitive));
    }
    assert!(output.contains("allowed_document_count: 1"));
    assert!(output.contains("max_chunks: 3"));
}

#[test]
fn rag_result_and_ingestion_debug_output_redacts_private_metadata() {
    let citation = Citation::new("private-source-uri", "private-citation-title").unwrap();
    let document = DocumentReference::new(
        "private-tenant",
        "private-document-id",
        "private-version",
        "private-checksum",
    )
    .unwrap();
    let retrieved = RetrievedChunk::new(
        "private-retrieved-chunk-id",
        "private-document-id",
        "private-tenant",
        "private-retrieved-content",
        citation.clone(),
    )
    .unwrap();
    let ingestion_document = IngestionDocument::new(
        document.clone(),
        "private-document-content",
        citation.clone(),
    )
    .unwrap();
    let ingestion_chunk = IngestionChunk::new(
        "private-ingestion-chunk-id",
        document,
        "private-ingestion-content",
        citation.clone(),
    )
    .unwrap();
    let embedding =
        EmbeddingInput::new("private-embedding-chunk-id", "private-embedding-content").unwrap();

    let output = format!(
        "{citation:?} {retrieved:?} {ingestion_document:?} {ingestion_chunk:?} {embedding:?}"
    );

    for sensitive in [
        "private-source-uri",
        "private-citation-title",
        "private-tenant",
        "private-document-id",
        "private-version",
        "private-checksum",
        "private-retrieved-chunk-id",
        "private-retrieved-content",
        "private-document-content",
        "private-ingestion-chunk-id",
        "private-ingestion-content",
        "private-embedding-chunk-id",
        "private-embedding-content",
    ] {
        assert!(!output.contains(sensitive));
    }
    assert!(output.contains("[REDACTED]"));
}
