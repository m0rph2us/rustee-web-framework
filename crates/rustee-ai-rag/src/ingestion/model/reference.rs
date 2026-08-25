//! Durable content-free ingestion references and request admission.

use std::fmt;

use rustee_ai::{ModelAliasError, validate_model_alias};
use serde::{Deserialize, Serialize};

/// Maximum UTF-8 byte length accepted for each durable document-reference field.
pub const MAX_DOCUMENT_REFERENCE_FIELD_BYTES: usize = 255;

/// Versioned, content-free identity for one document selected for asynchronous ingestion.
///
/// The request is safe to serialize into a durable job. It intentionally contains stable document
/// identity and checksum rather than text or source URI; a worker resolves content and citation
/// from an application-owned document source when it runs.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct DocumentReference {
    tenant: String,
    document_id: String,
    version: String,
    checksum: String,
}

#[derive(Deserialize)]
struct SerializedDocumentReference {
    tenant: String,
    document_id: String,
    version: String,
    checksum: String,
}

impl<'de> Deserialize<'de> for DocumentReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serialized = SerializedDocumentReference::deserialize(deserializer)?;
        Self::new(
            serialized.tenant,
            serialized.document_id,
            serialized.version,
            serialized.checksum,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl DocumentReference {
    /// Creates bounded identity and revision metadata for one document revision.
    ///
    /// # Errors
    ///
    /// Returns [`DocumentReferenceError`] when required metadata is blank, contains a NUL byte,
    /// or exceeds [`MAX_DOCUMENT_REFERENCE_FIELD_BYTES`].
    pub fn new(
        tenant: impl Into<String>,
        document_id: impl Into<String>,
        version: impl Into<String>,
        checksum: impl Into<String>,
    ) -> Result<Self, DocumentReferenceError> {
        let tenant = tenant.into();
        let document_id = document_id.into();
        let version = version.into();
        let checksum = checksum.into();
        let fields = [
            tenant.as_str(),
            document_id.as_str(),
            version.as_str(),
            checksum.as_str(),
        ];
        if fields.iter().any(|value| value.trim().is_empty()) {
            return Err(DocumentReferenceError::BlankField);
        }
        if fields
            .iter()
            .any(|value| value.len() > MAX_DOCUMENT_REFERENCE_FIELD_BYTES)
        {
            return Err(DocumentReferenceError::FieldTooLong);
        }
        if fields.iter().any(|value| value.contains('\0')) {
            return Err(DocumentReferenceError::FieldContainsNul);
        }
        Ok(Self {
            tenant,
            document_id,
            version,
            checksum,
        })
    }

    /// Returns the tenant that owns this document revision.
    #[must_use]
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// Returns the stable application document identifier.
    #[must_use]
    pub fn document_id(&self) -> &str {
        &self.document_id
    }

    /// Returns the application document revision.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Returns the application checksum for stale-work detection.
    #[must_use]
    pub fn checksum(&self) -> &str {
        &self.checksum
    }
}

impl fmt::Debug for DocumentReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentReference")
            .field("tenant", &"[REDACTED]")
            .field("document_id", &"[REDACTED]")
            .field("version", &"[REDACTED]")
            .field("checksum", &"[REDACTED]")
            .finish()
    }
}

/// Invalid document metadata supplied to an ingestion workflow.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DocumentReferenceError {
    /// A durable ingestion request needs complete identity and revision metadata.
    #[error("RAG document reference fields must not be blank")]
    BlankField,
    /// A durable reference must fit fixed application storage and provider metadata bounds.
    #[error("RAG document reference fields exceeded the supported length")]
    FieldTooLong,
    /// NUL bytes cannot be represented safely across durable storage and provider adapters.
    #[error("RAG document reference fields must not contain a NUL byte")]
    FieldContainsNul,
}

/// Content-free request to index one document revision with an explicit embedding model alias.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct RagIngestionRequest {
    document: DocumentReference,
    embedding_model: String,
}

#[derive(Deserialize)]
struct SerializedRagIngestionRequest {
    document: DocumentReference,
    embedding_model: String,
}

impl<'de> Deserialize<'de> for RagIngestionRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serialized = SerializedRagIngestionRequest::deserialize(deserializer)?;
        Self::new(serialized.document, serialized.embedding_model).map_err(serde::de::Error::custom)
    }
}

impl RagIngestionRequest {
    /// Creates an ingestion request for one immutable document revision.
    ///
    /// # Errors
    ///
    /// Returns [`RagIngestionRequestError`] when the model alias is invalid for durable provider
    /// metadata.
    pub fn new(
        document: DocumentReference,
        embedding_model: impl Into<String>,
    ) -> Result<Self, RagIngestionRequestError> {
        let embedding_model = embedding_model.into();
        validate_model_alias(&embedding_model).map_err(embedding_model_error)?;
        Ok(Self {
            document,
            embedding_model,
        })
    }

    /// Returns the durable document reference to load when the worker starts.
    #[must_use]
    pub fn document(&self) -> &DocumentReference {
        &self.document
    }

    /// Returns the deployment-owned embedding model alias.
    #[must_use]
    pub fn embedding_model(&self) -> &str {
        &self.embedding_model
    }
}

impl fmt::Debug for RagIngestionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RagIngestionRequest")
            .field("document", &self.document)
            .field("embedding_model", &self.embedding_model)
            .finish()
    }
}

/// Invalid ingestion-request metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RagIngestionRequestError {
    /// Embedding deployment aliases must be selected by application configuration.
    #[error("RAG embedding model alias must not be blank")]
    BlankEmbeddingModel,
    /// Embedding deployment aliases must fit the durable provider metadata limit.
    #[error("RAG embedding model alias exceeded the supported length")]
    EmbeddingModelTooLong,
    /// Embedding deployment aliases cannot contain a NUL byte.
    #[error("RAG embedding model alias must not contain a NUL byte")]
    EmbeddingModelContainsNul,
}

fn embedding_model_error(error: ModelAliasError) -> RagIngestionRequestError {
    match error {
        ModelAliasError::Blank => RagIngestionRequestError::BlankEmbeddingModel,
        ModelAliasError::TooLong => RagIngestionRequestError::EmbeddingModelTooLong,
        ModelAliasError::ContainsNul => RagIngestionRequestError::EmbeddingModelContainsNul,
    }
}
