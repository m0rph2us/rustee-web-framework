//! Redacted retrieved-content and citation values.

use std::fmt;

use rustee_ai::MAX_TENANT_BYTES;

use super::MAX_RETRIEVAL_IDENTIFIER_BYTES;

/// Largest UTF-8 byte length accepted for one source URI or display-title field.
pub const MAX_RETRIEVAL_CITATION_FIELD_BYTES: usize = 4 * 1024;
/// Largest UTF-8 byte length accepted for one retrieved chunk before context assembly.
pub const MAX_RETRIEVED_CHUNK_CONTENT_BYTES: usize = super::query::MAX_RETRIEVAL_CONTEXT_BYTES;

/// Citation metadata for a retrieved document chunk.
#[derive(Clone, Eq, PartialEq)]
pub struct Citation {
    source_uri: String,
    title: String,
}

impl fmt::Debug for Citation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Citation")
            .field("source_uri", &"[REDACTED]")
            .field("title", &"[REDACTED]")
            .finish()
    }
}

impl Citation {
    /// Creates source metadata that applications can render alongside an answer.
    ///
    /// # Errors
    ///
    /// Returns [`CitationError`] when a required citation field is blank, contains a NUL byte, or
    /// exceeds [`MAX_RETRIEVAL_CITATION_FIELD_BYTES`].
    pub fn new(
        source_uri: impl Into<String>,
        title: impl Into<String>,
    ) -> Result<Self, CitationError> {
        let source_uri = source_uri.into();
        let title = title.into();
        if source_uri.trim().is_empty() || title.trim().is_empty() {
            return Err(CitationError::BlankField);
        }
        if source_uri.len() > MAX_RETRIEVAL_CITATION_FIELD_BYTES
            || title.len() > MAX_RETRIEVAL_CITATION_FIELD_BYTES
        {
            return Err(CitationError::FieldTooLong);
        }
        if source_uri.contains('\0') || title.contains('\0') {
            return Err(CitationError::FieldContainsNul);
        }
        Ok(Self { source_uri, title })
    }

    /// Returns the stable source URI or application-owned source reference.
    #[must_use]
    pub fn source_uri(&self) -> &str {
        &self.source_uri
    }

    /// Returns the display title.
    #[must_use]
    pub fn title(&self) -> &str {
        &self.title
    }
}

/// Invalid citation metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CitationError {
    /// Citations must retain non-blank source and title metadata.
    #[error("RAG citation fields must not be blank")]
    BlankField,
    /// Citation metadata must remain within the bounded prompt-context budget.
    #[error("RAG citation fields exceeded the supported length")]
    FieldTooLong,
    /// Citation metadata must remain representable by prompt and store adapters.
    #[error("RAG citation fields must not contain a NUL byte")]
    FieldContainsNul,
}

/// One retrieved chunk, including the source tenant and document identity used for revalidation.
#[derive(Clone, Eq, PartialEq)]
pub struct RetrievedChunk {
    chunk_id: String,
    document_id: String,
    tenant: String,
    content: String,
    citation: Citation,
}

impl RetrievedChunk {
    /// Creates a chunk returned by a vector-store adapter.
    ///
    /// # Errors
    ///
    /// Returns [`RetrievedChunkError`] when identity or content fields are blank, contain a NUL
    /// byte, or exceed their bounded retrieval-model limits.
    pub fn new(
        chunk_id: impl Into<String>,
        document_id: impl Into<String>,
        tenant: impl Into<String>,
        content: impl Into<String>,
        citation: Citation,
    ) -> Result<Self, RetrievedChunkError> {
        let chunk_id = chunk_id.into();
        let document_id = document_id.into();
        let tenant = tenant.into();
        let content = content.into();
        if [
            chunk_id.as_str(),
            document_id.as_str(),
            tenant.as_str(),
            content.as_str(),
        ]
        .iter()
        .any(|value| value.trim().is_empty())
        {
            return Err(RetrievedChunkError::BlankField);
        }
        if chunk_id.len() > MAX_RETRIEVAL_IDENTIFIER_BYTES
            || document_id.len() > MAX_RETRIEVAL_IDENTIFIER_BYTES
            || tenant.len() > MAX_TENANT_BYTES
            || content.len() > MAX_RETRIEVED_CHUNK_CONTENT_BYTES
        {
            return Err(RetrievedChunkError::FieldTooLong);
        }
        if [
            chunk_id.as_str(),
            document_id.as_str(),
            tenant.as_str(),
            content.as_str(),
        ]
        .iter()
        .any(|value| value.contains('\0'))
        {
            return Err(RetrievedChunkError::FieldContainsNul);
        }
        Ok(Self {
            chunk_id,
            document_id,
            tenant,
            content,
            citation,
        })
    }

    /// Returns the stable chunk identifier.
    #[must_use]
    pub fn chunk_id(&self) -> &str {
        &self.chunk_id
    }

    /// Returns the source document identifier.
    #[must_use]
    pub fn document_id(&self) -> &str {
        &self.document_id
    }

    /// Returns the tenant persisted with this chunk.
    #[must_use]
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// Returns retrieved text after the retriever has revalidated its ACL scope.
    #[must_use]
    pub fn content(&self) -> &str {
        &self.content
    }

    /// Returns source metadata for this chunk.
    #[must_use]
    pub fn citation(&self) -> &Citation {
        &self.citation
    }
}

impl fmt::Debug for RetrievedChunk {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetrievedChunk")
            .field("chunk_id", &"[REDACTED]")
            .field("document_id", &"[REDACTED]")
            .field("tenant", &"[REDACTED]")
            .field("content", &"[REDACTED]")
            .field("citation", &self.citation)
            .finish()
    }
}

/// Invalid chunk metadata returned by an adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RetrievedChunkError {
    /// Identity and content fields must stay non-blank for later audit and citation.
    #[error("RAG chunk fields must not be blank")]
    BlankField,
    /// Chunk fields exceeded their bounded retrieval-model limits.
    #[error("RAG chunk fields exceeded the supported length")]
    FieldTooLong,
    /// Chunk fields must remain representable by vector-store and prompt adapters.
    #[error("RAG chunk fields must not contain a NUL byte")]
    FieldContainsNul,
}
