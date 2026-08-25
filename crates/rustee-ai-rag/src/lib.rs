//! Tenant- and ACL-scoped retrieval plus document-ingestion contracts for Rustee AI applications.
//!
//! The application derives [`RetrievalScope`] from validated identity and authorization before a
//! vector store is called. Rustee verifies every returned chunk again before it can reach prompt
//! construction, so store misconfiguration does not silently cross a tenant or document boundary.

mod ingestion;
mod retrieval;

pub use ingestion::{
    DEFAULT_EMBEDDING_BATCH_CONTENT_BYTES, DEFAULT_EMBEDDING_BATCH_INPUTS, DocumentChunker,
    DocumentLoader, DocumentReference, DocumentReferenceError, EmbeddedChunk, Embedding,
    EmbeddingBatchLimits, EmbeddingBatchLimitsError, EmbeddingError, EmbeddingInput,
    EmbeddingInputError, EmbeddingProvider, IngestionChunk, IngestionChunkError, IngestionDocument,
    IngestionDocumentError, IngestionReport, MAX_DOCUMENT_REFERENCE_FIELD_BYTES,
    MAX_INGESTION_CHUNK_CONTENT_BYTES, MAX_INGESTION_CHUNK_ID_BYTES, RagIngestionError,
    RagIngestionRequest, RagIngestionRequestError, RagIngestor, VectorIndex,
    VectorStoreCapabilities, VectorStoreCapability,
};
pub use retrieval::{
    Citation, CitationError, DEFAULT_RETRIEVAL_CONTEXT_BYTES, MAX_RETRIEVAL_CHUNKS,
    MAX_RETRIEVAL_CITATION_FIELD_BYTES, MAX_RETRIEVAL_CONTEXT_BYTES,
    MAX_RETRIEVAL_IDENTIFIER_BYTES, MAX_RETRIEVAL_QUERY_BYTES, MAX_RETRIEVAL_SCOPE_DOCUMENTS,
    MAX_RETRIEVED_CHUNK_CONTENT_BYTES, RagError, RagRetriever, RetrievalContext, RetrievalQuery,
    RetrievalQueryError, RetrievalScope, RetrievalScopeError, RetrievalStore, RetrievedChunk,
    RetrievedChunkError,
};

#[cfg(test)]
mod tests;
