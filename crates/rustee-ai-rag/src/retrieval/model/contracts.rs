//! Application-owned vector-store contract.

use std::error::Error as StdError;

use futures_util::future::BoxFuture;

use super::{RetrievalQuery, RetrievedChunk};

/// Vector-store boundary that receives an already-authorized query.
pub trait RetrievalStore: Clone + Send + Sync + 'static {
    /// Store-specific failure type.
    type Error: StdError + Send + Sync + 'static;

    /// Searches only within the query's tenant and document allowlist.
    fn search(
        &self,
        query: RetrievalQuery,
    ) -> BoxFuture<'static, Result<Vec<RetrievedChunk>, Self::Error>>;
}
