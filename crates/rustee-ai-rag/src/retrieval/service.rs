//! Fail-closed retrieval orchestration and returned prompt context.

use std::{error::Error as StdError, fmt};

use super::model::{Citation, RetrievalQuery, RetrievalStore, RetrievedChunk};

/// Revalidated context returned to an application for explicit prompt construction.
#[derive(Clone, Eq, PartialEq)]
pub struct RetrievalContext {
    chunks: Vec<RetrievedChunk>,
}

impl RetrievalContext {
    /// Returns ACL-revalidated chunks in the store's ranking order.
    #[must_use]
    pub fn chunks(&self) -> &[RetrievedChunk] {
        &self.chunks
    }

    /// Returns one citation for every chunk retained in the context.
    pub fn citations(&self) -> impl ExactSizeIterator<Item = &Citation> {
        self.chunks.iter().map(RetrievedChunk::citation)
    }
}

impl fmt::Debug for RetrievalContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetrievalContext")
            .field("chunk_count", &self.chunks.len())
            .finish()
    }
}

/// Retrieval service that verifies every store result against its original scope.
#[derive(Clone)]
pub struct RagRetriever<S> {
    store: S,
}

impl<S> RagRetriever<S> {
    /// Creates a retriever from one vector-store adapter.
    #[must_use]
    pub fn new(store: S) -> Self {
        Self { store }
    }
}

impl<S> fmt::Debug for RagRetriever<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RagRetriever")
            .finish_non_exhaustive()
    }
}

impl<S> RagRetriever<S>
where
    S: RetrievalStore,
{
    /// Retrieves bounded, ACL-revalidated chunks for explicit application prompt construction.
    ///
    /// # Errors
    ///
    /// Returns a store failure or [`RagError::ScopeViolation`] without returning any chunks when
    /// a store result belongs to a different tenant or document.
    pub async fn retrieve(
        &self,
        query: RetrievalQuery,
    ) -> Result<RetrievalContext, RagError<S::Error>> {
        let scope = query.scope().clone();
        let max_chunks = query.max_chunks();
        let max_context_bytes = query.max_context_bytes();
        let chunks = self.store.search(query).await.map_err(RagError::Store)?;
        for chunk in &chunks {
            if chunk.tenant() != scope.tenant() || !scope.permits_document(chunk.document_id()) {
                return Err(RagError::ScopeViolation);
            }
        }
        let mut context_bytes = 0_usize;
        let mut retained = Vec::with_capacity(chunks.len().min(max_chunks));
        for chunk in chunks.into_iter().take(max_chunks) {
            context_bytes = context_bytes.saturating_add(chunk.content().len());
            if context_bytes > max_context_bytes {
                return Err(RagError::ContextLimit);
            }
            retained.push(chunk);
        }
        Ok(RetrievalContext { chunks: retained })
    }
}

/// Retrieval failure with a fail-closed ACL result and content-free diagnostics.
#[derive(thiserror::Error)]
pub enum RagError<StoreError>
where
    StoreError: StdError + Send + Sync + 'static,
{
    /// The vector-store adapter could not complete the search.
    #[error("RAG retrieval store failed")]
    Store(#[source] StoreError),
    /// A store returned a chunk outside the authorization scope; no partial context is returned.
    #[error("RAG retrieval returned a chunk outside its authorization scope")]
    ScopeViolation,
    /// Returned content exceeded the query context byte limit; no partial context is returned.
    #[error("RAG retrieval exceeded the configured context byte limit")]
    ContextLimit,
}

impl<StoreError> fmt::Debug for RagError<StoreError>
where
    StoreError: StdError + Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(_) => formatter.write_str("RagError::Store"),
            Self::ScopeViolation => formatter.write_str("RagError::ScopeViolation"),
            Self::ContextLimit => formatter.write_str("RagError::ContextLimit"),
        }
    }
}
