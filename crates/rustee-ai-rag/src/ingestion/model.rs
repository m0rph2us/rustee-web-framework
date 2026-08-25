//! Stable RAG ingestion-model facade and adapter contracts.

mod content;
mod contracts;
mod embedding;
mod reference;

pub use content::{
    IngestionChunk, IngestionChunkError, IngestionDocument, IngestionDocumentError,
    MAX_INGESTION_CHUNK_CONTENT_BYTES, MAX_INGESTION_CHUNK_ID_BYTES,
};
pub use contracts::{
    DocumentChunker, DocumentLoader, EmbeddingProvider, VectorIndex, VectorStoreCapabilities,
    VectorStoreCapability,
};
pub use embedding::{
    DEFAULT_EMBEDDING_BATCH_CONTENT_BYTES, DEFAULT_EMBEDDING_BATCH_INPUTS, EmbeddedChunk,
    Embedding, EmbeddingBatchLimits, EmbeddingBatchLimitsError, EmbeddingError, EmbeddingInput,
    EmbeddingInputError,
};
pub use reference::{
    DocumentReference, DocumentReferenceError, MAX_DOCUMENT_REFERENCE_FIELD_BYTES,
    RagIngestionRequest, RagIngestionRequestError,
};
