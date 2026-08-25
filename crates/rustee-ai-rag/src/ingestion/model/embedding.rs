//! Provider embedding input, vector, and index-record values.

use std::fmt;

use super::{IngestionChunk, MAX_INGESTION_CHUNK_CONTENT_BYTES, MAX_INGESTION_CHUNK_ID_BYTES};

/// Default maximum number of inputs in one provider embedding request.
pub const DEFAULT_EMBEDDING_BATCH_INPUTS: usize = 100;
/// Default maximum combined content bytes in one provider embedding request.
pub const DEFAULT_EMBEDDING_BATCH_CONTENT_BYTES: usize = 512 * 1024;

/// Provider request limits used by [`super::EmbeddingProvider`] batching.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EmbeddingBatchLimits {
    max_inputs: usize,
    max_content_bytes: usize,
}

impl EmbeddingBatchLimits {
    /// Creates positive limits for one provider embedding request.
    ///
    /// # Errors
    ///
    /// Returns [`EmbeddingBatchLimitsError::ZeroMaxInputs`] or
    /// [`EmbeddingBatchLimitsError::ZeroMaxContentBytes`] when either limit is zero.
    pub const fn new(
        max_inputs: usize,
        max_content_bytes: usize,
    ) -> Result<Self, EmbeddingBatchLimitsError> {
        if max_inputs == 0 {
            return Err(EmbeddingBatchLimitsError::ZeroMaxInputs);
        }
        if max_content_bytes == 0 {
            return Err(EmbeddingBatchLimitsError::ZeroMaxContentBytes);
        }
        Ok(Self {
            max_inputs,
            max_content_bytes,
        })
    }

    /// Returns the maximum number of inputs for one provider request.
    #[must_use]
    pub const fn max_inputs(self) -> usize {
        self.max_inputs
    }

    /// Returns the maximum combined UTF-8 content bytes for one provider request.
    #[must_use]
    pub const fn max_content_bytes(self) -> usize {
        self.max_content_bytes
    }
}

impl Default for EmbeddingBatchLimits {
    fn default() -> Self {
        Self {
            max_inputs: DEFAULT_EMBEDDING_BATCH_INPUTS,
            max_content_bytes: DEFAULT_EMBEDDING_BATCH_CONTENT_BYTES,
        }
    }
}

/// Invalid provider embedding batch limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EmbeddingBatchLimitsError {
    /// Each provider call needs at least one input slot.
    #[error("RAG embedding batch input limit must be non-zero")]
    ZeroMaxInputs,
    /// Each provider call needs a finite non-zero content budget.
    #[error("RAG embedding batch content byte limit must be non-zero")]
    ZeroMaxContentBytes,
}

/// Content-bearing input supplied to an embedding adapter for one chunk.
#[derive(Clone, Eq, PartialEq)]
pub struct EmbeddingInput {
    chunk_id: String,
    content: String,
}

impl EmbeddingInput {
    /// Creates one non-blank chunk input for an embedding provider.
    ///
    /// # Errors
    ///
    /// Returns [`EmbeddingInputError`] when the chunk ID or text is not safe for embedding.
    pub fn new(
        chunk_id: impl Into<String>,
        content: impl Into<String>,
    ) -> Result<Self, EmbeddingInputError> {
        let chunk_id = chunk_id.into();
        let content = content.into();
        if chunk_id.trim().is_empty() || content.trim().is_empty() {
            return Err(EmbeddingInputError::BlankField);
        }
        if chunk_id.len() > MAX_INGESTION_CHUNK_ID_BYTES {
            return Err(EmbeddingInputError::ChunkIdTooLong);
        }
        if content.len() > MAX_INGESTION_CHUNK_CONTENT_BYTES {
            return Err(EmbeddingInputError::ContentTooLong);
        }
        if chunk_id.contains('\0') || content.contains('\0') {
            return Err(EmbeddingInputError::FieldContainsNul);
        }
        Ok(Self { chunk_id, content })
    }

    pub(in crate::ingestion) fn from_chunk(chunk: &IngestionChunk) -> Self {
        Self {
            chunk_id: chunk.chunk_id().to_owned(),
            content: chunk.content().to_owned(),
        }
    }

    /// Returns the application chunk identifier for result-order reconciliation.
    #[must_use]
    pub fn chunk_id(&self) -> &str {
        &self.chunk_id
    }

    /// Returns chunk text sent to the provider. Do not log it by default.
    #[must_use]
    pub fn content(&self) -> &str {
        &self.content
    }
}

/// Invalid direct input for an embedding provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EmbeddingInputError {
    /// Provider batch order can only be reconciled for non-blank chunk IDs and text.
    #[error("RAG embedding input chunk ID and content must not be blank")]
    BlankField,
    /// A provider input ID must remain representable by every adapter.
    #[error("RAG embedding input chunk ID exceeded the supported length")]
    ChunkIdTooLong,
    /// A provider input must fit the framework content budget.
    #[error("RAG embedding input content exceeded the framework byte limit")]
    ContentTooLong,
    /// Provider-bound input data must remain representable by every adapter.
    #[error("RAG embedding input chunk ID and content must not contain a NUL byte")]
    FieldContainsNul,
}

impl fmt::Debug for EmbeddingInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EmbeddingInput")
            .field("chunk_id", &"[REDACTED]")
            .field("content", &"[REDACTED]")
            .finish()
    }
}

/// One validated embedding vector in provider response order.
#[derive(Clone, PartialEq)]
pub struct Embedding {
    values: Vec<f32>,
}

impl Embedding {
    /// Creates a non-empty finite embedding vector.
    ///
    /// # Errors
    ///
    /// Returns [`EmbeddingError`] when the vector is empty or contains a non-finite value.
    pub fn new(values: impl Into<Vec<f32>>) -> Result<Self, EmbeddingError> {
        let values = values.into();
        if values.is_empty() {
            return Err(EmbeddingError::Empty);
        }
        if values.iter().any(|value| !value.is_finite()) {
            return Err(EmbeddingError::NonFinite);
        }
        Ok(Self { values })
    }

    /// Returns the vector dimensions for a store adapter. Do not emit full vectors by default.
    #[must_use]
    pub fn values(&self) -> &[f32] {
        &self.values
    }
}

impl fmt::Debug for Embedding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Embedding")
            .field("dimensions", &self.values.len())
            .finish()
    }
}

/// Invalid provider embedding data.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EmbeddingError {
    /// A vector store cannot index a zero-dimensional value.
    #[error("RAG embedding vector must not be empty")]
    Empty,
    /// NaN and infinity make similarity ordering undefined.
    #[error("RAG embedding vector must contain only finite values")]
    NonFinite,
}

/// Embedded data for one upsert into a vector index.
#[derive(Clone, Debug, PartialEq)]
pub struct EmbeddedChunk {
    chunk: IngestionChunk,
    embedding_model: String,
    embedding: Embedding,
}

impl EmbeddedChunk {
    pub(in crate::ingestion) fn new(
        chunk: IngestionChunk,
        embedding_model: String,
        embedding: Embedding,
    ) -> Self {
        Self {
            chunk,
            embedding_model,
            embedding,
        }
    }

    /// Returns the application chunk and source metadata retained by the vector adapter.
    #[must_use]
    pub fn chunk(&self) -> &IngestionChunk {
        &self.chunk
    }

    /// Returns the configured embedding model alias for this vector.
    #[must_use]
    pub fn embedding_model(&self) -> &str {
        &self.embedding_model
    }

    /// Returns the validated embedding values for a vector adapter.
    #[must_use]
    pub fn embedding(&self) -> &Embedding {
        &self.embedding
    }
}
