//! Application-owned ingestion adapter contracts.

use std::error::Error as StdError;

use futures_util::future::BoxFuture;

use super::{
    DocumentReference, EmbeddedChunk, Embedding, EmbeddingBatchLimits, EmbeddingInput,
    IngestionChunk, IngestionDocument, RagIngestionRequest,
};

/// Optional vector-index behavior that an adapter must expose instead of silently emulating.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VectorStoreCapability {
    /// Store-side tenant/document metadata filtering during similarity search.
    MetadataFilter,
    /// Removal of every vector for one document revision.
    DeleteByDocument,
    /// A combined keyword and vector ranking query.
    HybridSearch,
}

/// Capability declaration for one vector-index adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VectorStoreCapabilities {
    metadata_filter: bool,
    delete_by_document: bool,
    hybrid_search: bool,
}

impl VectorStoreCapabilities {
    /// Creates an explicit capability declaration for one adapter.
    #[must_use]
    pub const fn new(metadata_filter: bool, delete_by_document: bool, hybrid_search: bool) -> Self {
        Self {
            metadata_filter,
            delete_by_document,
            hybrid_search,
        }
    }

    /// Returns whether the adapter supports one optional operation natively.
    #[must_use]
    pub const fn supports(self, capability: VectorStoreCapability) -> bool {
        match capability {
            VectorStoreCapability::MetadataFilter => self.metadata_filter,
            VectorStoreCapability::DeleteByDocument => self.delete_by_document,
            VectorStoreCapability::HybridSearch => self.hybrid_search,
        }
    }
}

/// Application loader that resolves a content-free document reference after a job starts.
pub trait DocumentLoader: Clone + Send + Sync + 'static {
    /// Source-specific failure.
    type Error: StdError + Send + Sync + 'static;

    /// Loads document text and citation metadata for the requested immutable revision.
    ///
    /// Implementations own source-read limits because the framework receives the document only
    /// after it has been materialized.
    fn load(
        &self,
        request: RagIngestionRequest,
    ) -> BoxFuture<'static, Result<IngestionDocument, Self::Error>>;
}

/// Application chunker used before embedding a loaded document.
pub trait DocumentChunker: Clone + Send + Sync + 'static {
    /// Chunker-specific failure.
    type Error: StdError + Send + Sync + 'static;

    /// Splits a loaded document into chunks bounded by the ingestion value contract.
    fn chunk(
        &self,
        document: IngestionDocument,
    ) -> BoxFuture<'static, Result<Vec<IngestionChunk>, Self::Error>>;
}

/// Provider-neutral embedding boundary for a batch of redacted-debug chunk inputs.
pub trait EmbeddingProvider: Clone + Send + Sync + 'static {
    /// Provider-specific failure.
    type Error: StdError + Send + Sync + 'static;

    /// Declares the provider request limits that the ingestor must honor.
    ///
    /// The ingestor preserves chunk order while splitting calls to fit these limits.
    fn batch_limits(&self) -> EmbeddingBatchLimits {
        EmbeddingBatchLimits::default()
    }

    /// Returns one validated embedding in exactly the same order as `inputs`.
    fn embed(
        &self,
        model: String,
        inputs: Vec<EmbeddingInput>,
    ) -> BoxFuture<'static, Result<Vec<Embedding>, Self::Error>>;
}

/// Vector-index boundary for durable document replacement and capability declaration.
pub trait VectorIndex: Clone + Send + Sync + 'static {
    /// Index-specific failure.
    type Error: StdError + Send + Sync + 'static;

    /// Declares optional behavior that this adapter supports natively.
    fn capabilities(&self) -> VectorStoreCapabilities;

    /// Atomically inserts or replaces the complete immutable chunk batch.
    ///
    /// [`crate::RagIngestor`] calls this once only after every chunk and embedding has passed its
    /// validation boundary. Implementations must make the whole batch visible together, or return
    /// an error without presenting a partial replacement as a successful ingestion. Backends that
    /// cannot provide this guarantee need an application-owned reconciliation workflow rather than
    /// silently decomposing this call into independent visible writes.
    fn upsert(&self, chunks: Vec<EmbeddedChunk>) -> BoxFuture<'static, Result<(), Self::Error>>;

    /// Removes vectors for one document revision when [`VectorStoreCapability::DeleteByDocument`]
    /// is declared. Callers must check [`Self::capabilities`] before invoking it.
    fn delete_document(
        &self,
        document: DocumentReference,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}
