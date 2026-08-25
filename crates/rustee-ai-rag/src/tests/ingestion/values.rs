use rustee_ai::MAX_MODEL_ALIAS_BYTES;

use crate::{
    Citation, DocumentReference, DocumentReferenceError, EmbeddingBatchLimits,
    EmbeddingBatchLimitsError, EmbeddingInput, EmbeddingInputError, IngestionChunk,
    IngestionChunkError, IngestionDocument, IngestionDocumentError,
    MAX_DOCUMENT_REFERENCE_FIELD_BYTES, MAX_INGESTION_CHUNK_CONTENT_BYTES,
    MAX_INGESTION_CHUNK_ID_BYTES, RagIngestionRequest, RagIngestionRequestError,
};

use super::support::{document_reference, ingestion_request};

#[test]
fn document_reference_bounds_durable_metadata() {
    assert!(
        DocumentReference::new(
            "t".repeat(MAX_DOCUMENT_REFERENCE_FIELD_BYTES),
            "doc-1",
            "v3",
            "sha256:abc",
        )
        .is_ok()
    );
    assert_eq!(
        DocumentReference::new(
            "t".repeat(MAX_DOCUMENT_REFERENCE_FIELD_BYTES + 1),
            "doc-1",
            "v3",
            "sha256:abc",
        )
        .unwrap_err(),
        DocumentReferenceError::FieldTooLong
    );
    assert_eq!(
        DocumentReference::new("acme\0", "doc-1", "v3", "sha256:abc").unwrap_err(),
        DocumentReferenceError::FieldContainsNul
    );
}

#[test]
fn durable_ingestion_request_deserialization_revalidates_metadata() {
    assert!(
        serde_json::from_value::<DocumentReference>(serde_json::json!({
            "tenant":" ",
            "document_id":"doc-1",
            "version":"v3",
            "checksum":"sha256:abc",
        }))
        .is_err()
    );
    for tenant in [
        "t".repeat(MAX_DOCUMENT_REFERENCE_FIELD_BYTES + 1),
        "tenant\0".to_owned(),
    ] {
        assert!(
            serde_json::from_value::<DocumentReference>(serde_json::json!({
                "tenant": tenant,
                "document_id":"doc-1",
                "version":"v3",
                "checksum":"sha256:abc",
            }))
            .is_err()
        );
    }
    assert!(
        serde_json::from_value::<RagIngestionRequest>(serde_json::json!({
            "document":{
                "tenant":"acme",
                "document_id":"doc-1",
                "version":"v3",
                "checksum":"sha256:abc",
            },
            "embedding_model":" ",
        }))
        .is_err()
    );
    for embedding_model in [
        "m".repeat(MAX_MODEL_ALIAS_BYTES + 1),
        "embedding\0alias".to_owned(),
    ] {
        assert!(
            serde_json::from_value::<RagIngestionRequest>(serde_json::json!({
                "document":{
                    "tenant":"acme",
                    "document_id":"doc-1",
                    "version":"v3",
                    "checksum":"sha256:abc",
                },
                "embedding_model": embedding_model,
            }))
            .is_err()
        );
    }

    let request = ingestion_request();
    let restored = serde_json::from_value::<RagIngestionRequest>(
        serde_json::to_value(&request).expect("valid request serializes"),
    )
    .expect("serialized valid request restores");
    assert_eq!(restored, request);
}

#[test]
fn durable_ingestion_request_uses_the_shared_model_alias_contract() {
    assert_eq!(
        RagIngestionRequest::new(document_reference(), "m".repeat(MAX_MODEL_ALIAS_BYTES + 1),)
            .unwrap_err(),
        RagIngestionRequestError::EmbeddingModelTooLong
    );
    assert_eq!(
        RagIngestionRequest::new(document_reference(), "embedding\0alias").unwrap_err(),
        RagIngestionRequestError::EmbeddingModelContainsNul
    );
}

#[test]
fn ingestion_values_bound_provider_input_without_exposing_content() {
    assert_eq!(
        IngestionDocument::new(
            document_reference(),
            "document\0content",
            Citation::new("kb://policy", "Account policy").expect("test citation is valid"),
        )
        .unwrap_err(),
        IngestionDocumentError::ContentContainsNul
    );
    for (chunk_id, content, expected) in [
        (
            "c".repeat(MAX_INGESTION_CHUNK_ID_BYTES + 1),
            "content".to_owned(),
            IngestionChunkError::ChunkIdTooLong,
        ),
        (
            "chunk".to_owned(),
            "c".repeat(MAX_INGESTION_CHUNK_CONTENT_BYTES + 1),
            IngestionChunkError::ContentTooLong,
        ),
        (
            "chunk\0id".to_owned(),
            "content".to_owned(),
            IngestionChunkError::FieldContainsNul,
        ),
    ] {
        assert_eq!(
            IngestionChunk::new(
                chunk_id,
                document_reference(),
                content,
                Citation::new("kb://policy", "Account policy").expect("test citation is valid"),
            )
            .unwrap_err(),
            expected
        );
    }
    assert_eq!(
        EmbeddingInput::new("chunk\0id", "content").unwrap_err(),
        EmbeddingInputError::FieldContainsNul
    );
    assert_eq!(
        EmbeddingBatchLimits::new(0, 1).unwrap_err(),
        EmbeddingBatchLimitsError::ZeroMaxInputs
    );
    assert_eq!(
        EmbeddingBatchLimits::new(1, 0).unwrap_err(),
        EmbeddingBatchLimitsError::ZeroMaxContentBytes
    );
}
