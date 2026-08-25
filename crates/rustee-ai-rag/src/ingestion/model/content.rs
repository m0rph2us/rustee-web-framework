//! Content-bearing document and chunk values with redacted diagnostics.

use std::fmt;

use crate::Citation;

use super::DocumentReference;

/// Largest UTF-8 byte length accepted for one chunk identifier.
pub const MAX_INGESTION_CHUNK_ID_BYTES: usize = 255;
/// Largest UTF-8 byte length accepted for one chunk supplied to an embedding provider.
pub const MAX_INGESTION_CHUNK_CONTENT_BYTES: usize = 512 * 1024;

/// Document text loaded by an application-owned source only after a worker starts.
#[derive(Clone, Eq, PartialEq)]
pub struct IngestionDocument {
    reference: DocumentReference,
    content: String,
    citation: Citation,
}

impl IngestionDocument {
    /// Creates loaded document content for a validated document reference.
    ///
    /// # Errors
    ///
    /// Returns [`IngestionDocumentError`] when the source returned blank text or a NUL byte.
    pub fn new(
        reference: DocumentReference,
        content: impl Into<String>,
        citation: Citation,
    ) -> Result<Self, IngestionDocumentError> {
        let content = content.into();
        if content.trim().is_empty() {
            return Err(IngestionDocumentError::BlankContent);
        }
        if content.contains('\0') {
            return Err(IngestionDocumentError::ContentContainsNul);
        }
        Ok(Self {
            reference,
            content,
            citation,
        })
    }

    /// Returns the loaded document revision metadata.
    #[must_use]
    pub fn reference(&self) -> &DocumentReference {
        &self.reference
    }

    /// Returns document text to a chunker. Do not log it by default.
    #[must_use]
    pub fn content(&self) -> &str {
        &self.content
    }

    /// Returns the default source citation for chunks created from this document.
    #[must_use]
    pub fn citation(&self) -> &Citation {
        &self.citation
    }
}

impl fmt::Debug for IngestionDocument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IngestionDocument")
            .field("reference", &self.reference)
            .field("content", &"[REDACTED]")
            .field("citation", &self.citation)
            .finish()
    }
}

/// Invalid loaded document data.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum IngestionDocumentError {
    /// Empty source documents cannot produce auditable vector records.
    #[error("RAG ingestion document content must not be blank")]
    BlankContent,
    /// Text sources must remain representable by every chunker and embedding adapter.
    #[error("RAG ingestion document content must not contain a NUL byte")]
    ContentContainsNul,
}

/// One text chunk created by an application chunker before embedding.
#[derive(Clone, Eq, PartialEq)]
pub struct IngestionChunk {
    chunk_id: String,
    document: DocumentReference,
    content: String,
    citation: Citation,
}

impl IngestionChunk {
    /// Creates a bounded non-blank chunk tied to exactly one document revision.
    ///
    /// # Errors
    ///
    /// Returns [`IngestionChunkError`] when its ID or content is not safe for embedding.
    pub fn new(
        chunk_id: impl Into<String>,
        document: DocumentReference,
        content: impl Into<String>,
        citation: Citation,
    ) -> Result<Self, IngestionChunkError> {
        let chunk_id = chunk_id.into();
        let content = content.into();
        if chunk_id.trim().is_empty() || content.trim().is_empty() {
            return Err(IngestionChunkError::BlankField);
        }
        if chunk_id.len() > MAX_INGESTION_CHUNK_ID_BYTES {
            return Err(IngestionChunkError::ChunkIdTooLong);
        }
        if content.len() > MAX_INGESTION_CHUNK_CONTENT_BYTES {
            return Err(IngestionChunkError::ContentTooLong);
        }
        if chunk_id.contains('\0') || content.contains('\0') {
            return Err(IngestionChunkError::FieldContainsNul);
        }
        Ok(Self {
            chunk_id,
            document,
            content,
            citation,
        })
    }

    /// Returns the stable chunk identifier within its document revision.
    #[must_use]
    pub fn chunk_id(&self) -> &str {
        &self.chunk_id
    }

    /// Returns the source document revision for this chunk.
    #[must_use]
    pub fn document(&self) -> &DocumentReference {
        &self.document
    }

    /// Returns the text sent to an embedding provider. Do not log it by default.
    #[must_use]
    pub fn content(&self) -> &str {
        &self.content
    }

    /// Returns source metadata retained with the vector record.
    #[must_use]
    pub fn citation(&self) -> &Citation {
        &self.citation
    }
}

impl fmt::Debug for IngestionChunk {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IngestionChunk")
            .field("chunk_id", &"[REDACTED]")
            .field("document", &self.document)
            .field("content", &"[REDACTED]")
            .field("citation", &self.citation)
            .finish()
    }
}

/// Invalid chunk data returned by an application chunker.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum IngestionChunkError {
    /// Chunk identifiers and content must be retained for vector replacement and citation.
    #[error("RAG ingestion chunk ID and content must not be blank")]
    BlankField,
    /// A chunk ID must remain representable by vector and embedding adapters.
    #[error("RAG ingestion chunk ID exceeded the supported length")]
    ChunkIdTooLong,
    /// One embedding input must fit the framework content budget.
    #[error("RAG ingestion chunk content exceeded the framework byte limit")]
    ContentTooLong,
    /// Provider-bound chunk data must remain representable by every adapter.
    #[error("RAG ingestion chunk ID and content must not contain a NUL byte")]
    FieldContainsNul,
}
