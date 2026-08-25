//! Bounded provider-bound retrieval query values.

use std::fmt;

use super::RetrievalScope;

/// Default maximum retrieved content bytes retained for explicit prompt construction.
pub const DEFAULT_RETRIEVAL_CONTEXT_BYTES: usize = 512 * 1024;
/// Largest retrieved content byte limit accepted for one query.
pub const MAX_RETRIEVAL_CONTEXT_BYTES: usize = 4 * 1024 * 1024;
/// Largest UTF-8 byte length accepted for one provider-bound retrieval query.
pub const MAX_RETRIEVAL_QUERY_BYTES: usize = 64 * 1024;
/// Largest number of ranked chunks an application may request from one retrieval store call.
pub const MAX_RETRIEVAL_CHUNKS: usize = 1_000;

/// One vector-store query with tenant and document permissions already attached.
#[derive(Clone, Eq, PartialEq)]
pub struct RetrievalQuery {
    text: String,
    scope: RetrievalScope,
    max_chunks: usize,
    max_context_bytes: usize,
}

impl RetrievalQuery {
    /// Creates a bounded retrieval query.
    ///
    /// # Errors
    ///
    /// Returns [`RetrievalQueryError`] when the text is blank or too large, or when
    /// `max_chunks` is zero or exceeds [`MAX_RETRIEVAL_CHUNKS`].
    pub fn new(
        text: impl Into<String>,
        scope: RetrievalScope,
        max_chunks: usize,
    ) -> Result<Self, RetrievalQueryError> {
        let text = text.into();
        if text.trim().is_empty() {
            return Err(RetrievalQueryError::BlankText);
        }
        if text.len() > MAX_RETRIEVAL_QUERY_BYTES {
            return Err(RetrievalQueryError::TextTooLong);
        }
        if text.contains('\0') {
            return Err(RetrievalQueryError::TextContainsNul);
        }
        if max_chunks == 0 {
            return Err(RetrievalQueryError::ZeroMaxChunks);
        }
        if max_chunks > MAX_RETRIEVAL_CHUNKS {
            return Err(RetrievalQueryError::MaxChunksLimit);
        }
        Ok(Self {
            text,
            scope,
            max_chunks,
            max_context_bytes: DEFAULT_RETRIEVAL_CONTEXT_BYTES,
        })
    }

    /// Returns the untrusted search text. Do not log it by default.
    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Returns the mandatory authorization scope for the search.
    #[must_use]
    pub fn scope(&self) -> &RetrievalScope {
        &self.scope
    }

    /// Returns the maximum number of chunks retained in the result context.
    #[must_use]
    pub const fn max_chunks(&self) -> usize {
        self.max_chunks
    }

    /// Sets the maximum retrieved content bytes retained for explicit prompt construction.
    ///
    /// # Errors
    ///
    /// Returns an error when the limit is zero or exceeds the finite framework maximum.
    pub fn with_max_context_bytes(
        mut self,
        max_context_bytes: usize,
    ) -> Result<Self, RetrievalQueryError> {
        if max_context_bytes == 0 {
            return Err(RetrievalQueryError::ZeroMaxContextBytes);
        }
        if max_context_bytes > MAX_RETRIEVAL_CONTEXT_BYTES {
            return Err(RetrievalQueryError::MaxContextBytesLimit);
        }
        self.max_context_bytes = max_context_bytes;
        Ok(self)
    }

    /// Returns the maximum retrieved content bytes retained in the result context.
    #[must_use]
    pub const fn max_context_bytes(&self) -> usize {
        self.max_context_bytes
    }
}

impl fmt::Debug for RetrievalQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetrievalQuery")
            .field("text", &"[REDACTED]")
            .field("scope", &self.scope)
            .field("max_chunks", &self.max_chunks)
            .field("max_context_bytes", &self.max_context_bytes)
            .finish()
    }
}

/// Invalid retrieval-query content.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RetrievalQueryError {
    /// Empty queries do not produce useful, auditable retrieval behavior.
    #[error("RAG retrieval text must not be blank")]
    BlankText,
    /// Provider-bound query text needs a finite memory and transport budget.
    #[error("RAG retrieval text exceeded the framework byte limit")]
    TextTooLong,
    /// Provider-bound query text must remain representable by every retrieval adapter.
    #[error("RAG retrieval text must not contain a NUL byte")]
    TextContainsNul,
    /// The application must bound retrieval context before prompt construction.
    #[error("RAG retrieval max chunks must be non-zero")]
    ZeroMaxChunks,
    /// A store call must not request an unbounded number of candidates.
    #[error("RAG retrieval max chunks exceeds the framework maximum")]
    MaxChunksLimit,
    /// Retrieval context must have a finite non-zero byte limit.
    #[error("RAG retrieval context byte limit must be non-zero")]
    ZeroMaxContextBytes,
    /// Retrieval context byte limit exceeded the fixed framework maximum.
    #[error("RAG retrieval context byte limit exceeds the framework maximum")]
    MaxContextBytesLimit,
}
