//! ACL-scoped retrieval public facade.

mod model;
mod service;

pub use model::{
    Citation, CitationError, DEFAULT_RETRIEVAL_CONTEXT_BYTES, MAX_RETRIEVAL_CHUNKS,
    MAX_RETRIEVAL_CITATION_FIELD_BYTES, MAX_RETRIEVAL_CONTEXT_BYTES,
    MAX_RETRIEVAL_IDENTIFIER_BYTES, MAX_RETRIEVAL_QUERY_BYTES, MAX_RETRIEVAL_SCOPE_DOCUMENTS,
    MAX_RETRIEVED_CHUNK_CONTENT_BYTES, RetrievalQuery, RetrievalQueryError, RetrievalScope,
    RetrievalScopeError, RetrievalStore, RetrievedChunk, RetrievedChunkError,
};
pub use service::{RagError, RagRetriever, RetrievalContext};
