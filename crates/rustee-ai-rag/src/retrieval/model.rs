//! Stable retrieval-model facade and vector-store contracts.

mod content;
mod contracts;
mod query;
mod scope;

pub use content::{
    Citation, CitationError, MAX_RETRIEVAL_CITATION_FIELD_BYTES, MAX_RETRIEVED_CHUNK_CONTENT_BYTES,
    RetrievedChunk, RetrievedChunkError,
};
pub use contracts::RetrievalStore;
pub use query::{
    DEFAULT_RETRIEVAL_CONTEXT_BYTES, MAX_RETRIEVAL_CHUNKS, MAX_RETRIEVAL_CONTEXT_BYTES,
    MAX_RETRIEVAL_QUERY_BYTES, RetrievalQuery, RetrievalQueryError,
};
pub use scope::{
    MAX_RETRIEVAL_IDENTIFIER_BYTES, MAX_RETRIEVAL_SCOPE_DOCUMENTS, RetrievalScope,
    RetrievalScopeError,
};
