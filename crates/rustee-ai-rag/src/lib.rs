//! Tenant- and ACL-scoped retrieval contracts for Rustee AI applications.
//!
//! The application derives [`RetrievalScope`] from validated identity and authorization before a
//! vector store is called. Rustee verifies every returned chunk again before it can reach prompt
//! construction, so store misconfiguration does not silently cross a tenant or document boundary.

use std::{collections::BTreeSet, error::Error as StdError, fmt};

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};

/// Document permission scope derived by application authorization before retrieval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetrievalScope {
    tenant: String,
    allowed_document_ids: BTreeSet<String>,
}

impl RetrievalScope {
    /// Creates a tenant scope with an explicit non-empty allowlist of document IDs.
    ///
    /// # Errors
    ///
    /// Returns [`RetrievalScopeError`] when the tenant or allowlist is invalid.
    pub fn new(
        tenant: impl Into<String>,
        allowed_document_ids: impl IntoIterator<Item = String>,
    ) -> Result<Self, RetrievalScopeError> {
        let tenant = tenant.into();
        if tenant.trim().is_empty() {
            return Err(RetrievalScopeError::BlankTenant);
        }
        let allowed_document_ids = allowed_document_ids
            .into_iter()
            .map(|id| {
                if id.trim().is_empty() {
                    Err(RetrievalScopeError::BlankDocumentId)
                } else {
                    Ok(id)
                }
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        if allowed_document_ids.is_empty() {
            return Err(RetrievalScopeError::EmptyAllowlist);
        }
        Ok(Self {
            tenant,
            allowed_document_ids,
        })
    }

    /// Returns the tenant constraint sent to a retrieval store.
    #[must_use]
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// Returns whether the scope permits this document identifier.
    #[must_use]
    pub fn permits_document(&self, document_id: &str) -> bool {
        self.allowed_document_ids.contains(document_id)
    }
}

/// Invalid retrieval-scope content.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RetrievalScopeError {
    /// Tenant context must originate in validated application identity.
    #[error("RAG retrieval tenant must not be blank")]
    BlankTenant,
    /// The application must make document authorization explicit.
    #[error("RAG retrieval allowlist must not be empty")]
    EmptyAllowlist,
    /// An allowed document must have a stable identifier.
    #[error("RAG retrieval document ID must not be blank")]
    BlankDocumentId,
}

/// One vector-store query with tenant and document permissions already attached.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetrievalQuery {
    text: String,
    scope: RetrievalScope,
    max_chunks: usize,
}

impl RetrievalQuery {
    /// Creates a bounded retrieval query.
    ///
    /// # Errors
    ///
    /// Returns [`RetrievalQueryError`] when the text is blank or `max_chunks` is zero.
    pub fn new(
        text: impl Into<String>,
        scope: RetrievalScope,
        max_chunks: usize,
    ) -> Result<Self, RetrievalQueryError> {
        let text = text.into();
        if text.trim().is_empty() {
            return Err(RetrievalQueryError::BlankText);
        }
        if max_chunks == 0 {
            return Err(RetrievalQueryError::ZeroMaxChunks);
        }
        Ok(Self {
            text,
            scope,
            max_chunks,
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
}

/// Invalid retrieval-query content.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RetrievalQueryError {
    /// Empty queries do not produce useful, auditable retrieval behavior.
    #[error("RAG retrieval text must not be blank")]
    BlankText,
    /// The application must bound retrieval context before prompt construction.
    #[error("RAG retrieval max chunks must be non-zero")]
    ZeroMaxChunks,
}

/// Citation metadata for a retrieved document chunk.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Citation {
    source_uri: String,
    title: String,
}

impl Citation {
    /// Creates source metadata that applications can render alongside an answer.
    ///
    /// # Errors
    ///
    /// Returns [`CitationError`] when a required citation field is blank.
    pub fn new(
        source_uri: impl Into<String>,
        title: impl Into<String>,
    ) -> Result<Self, CitationError> {
        let source_uri = source_uri.into();
        let title = title.into();
        if source_uri.trim().is_empty() || title.trim().is_empty() {
            return Err(CitationError::BlankField);
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
    /// Returns [`RetrievedChunkError`] when identity or content fields are blank.
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
            .field("chunk_id", &self.chunk_id)
            .field("document_id", &self.document_id)
            .field("tenant", &self.tenant)
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
}

/// Versioned, content-free identity for one document selected for asynchronous ingestion.
///
/// The request is safe to serialize into a durable job. It intentionally contains stable document
/// identity and checksum rather than text or source URI; a worker resolves content and citation
/// from an application-owned document source when it runs.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct DocumentReference {
    tenant: String,
    document_id: String,
    version: String,
    checksum: String,
}

impl DocumentReference {
    /// Creates non-blank identity and revision metadata for one document revision.
    ///
    /// # Errors
    ///
    /// Returns [`DocumentReferenceError::BlankField`] when required metadata is blank.
    pub fn new(
        tenant: impl Into<String>,
        document_id: impl Into<String>,
        version: impl Into<String>,
        checksum: impl Into<String>,
    ) -> Result<Self, DocumentReferenceError> {
        let tenant = tenant.into();
        let document_id = document_id.into();
        let version = version.into();
        let checksum = checksum.into();
        if [
            tenant.as_str(),
            document_id.as_str(),
            version.as_str(),
            checksum.as_str(),
        ]
        .iter()
        .any(|value| value.trim().is_empty())
        {
            return Err(DocumentReferenceError::BlankField);
        }
        Ok(Self {
            tenant,
            document_id,
            version,
            checksum,
        })
    }

    /// Returns the tenant that owns this document revision.
    #[must_use]
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// Returns the stable application document identifier.
    #[must_use]
    pub fn document_id(&self) -> &str {
        &self.document_id
    }

    /// Returns the application document revision.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Returns the application checksum for stale-work detection.
    #[must_use]
    pub fn checksum(&self) -> &str {
        &self.checksum
    }
}

impl fmt::Debug for DocumentReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentReference")
            .field("tenant", &"[REDACTED]")
            .field("document_id", &"[REDACTED]")
            .field("version", &self.version)
            .field("checksum", &"[REDACTED]")
            .finish()
    }
}

/// Invalid document metadata supplied to an ingestion workflow.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DocumentReferenceError {
    /// A durable ingestion request needs complete identity and revision metadata.
    #[error("RAG document reference fields must not be blank")]
    BlankField,
}

/// Content-free request to index one document revision with an explicit embedding model alias.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct RagIngestionRequest {
    document: DocumentReference,
    embedding_model: String,
}

impl RagIngestionRequest {
    /// Creates an ingestion request for one immutable document revision.
    ///
    /// # Errors
    ///
    /// Returns [`RagIngestionRequestError::BlankEmbeddingModel`] when the model alias is blank.
    pub fn new(
        document: DocumentReference,
        embedding_model: impl Into<String>,
    ) -> Result<Self, RagIngestionRequestError> {
        let embedding_model = embedding_model.into();
        if embedding_model.trim().is_empty() {
            return Err(RagIngestionRequestError::BlankEmbeddingModel);
        }
        Ok(Self {
            document,
            embedding_model,
        })
    }

    /// Returns the durable document reference to load when the worker starts.
    #[must_use]
    pub fn document(&self) -> &DocumentReference {
        &self.document
    }

    /// Returns the deployment-owned embedding model alias.
    #[must_use]
    pub fn embedding_model(&self) -> &str {
        &self.embedding_model
    }
}

impl fmt::Debug for RagIngestionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RagIngestionRequest")
            .field("document", &self.document)
            .field("embedding_model", &self.embedding_model)
            .finish()
    }
}

/// Invalid ingestion-request metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RagIngestionRequestError {
    /// Embedding deployment aliases must be selected by application configuration.
    #[error("RAG embedding model alias must not be blank")]
    BlankEmbeddingModel,
}

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
    /// Returns [`IngestionDocumentError::BlankContent`] when the source returned blank text.
    pub fn new(
        reference: DocumentReference,
        content: impl Into<String>,
        citation: Citation,
    ) -> Result<Self, IngestionDocumentError> {
        let content = content.into();
        if content.trim().is_empty() {
            return Err(IngestionDocumentError::BlankContent);
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
    /// Creates a non-blank chunk tied to exactly one document revision.
    ///
    /// # Errors
    ///
    /// Returns [`IngestionChunkError::BlankField`] when its ID or content is blank.
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
            .field("chunk_id", &self.chunk_id)
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
}

/// Content-bearing input supplied to an embedding adapter for one chunk.
#[derive(Clone, Eq, PartialEq)]
pub struct EmbeddingInput {
    chunk_id: String,
    content: String,
}

impl EmbeddingInput {
    /// Creates one non-blank chunk input for an embedding provider.
    ///
    /// # Errors
    ///
    /// Returns [`EmbeddingInputError::BlankField`] when the chunk ID or text is blank.
    pub fn new(
        chunk_id: impl Into<String>,
        content: impl Into<String>,
    ) -> Result<Self, EmbeddingInputError> {
        let chunk_id = chunk_id.into();
        let content = content.into();
        if chunk_id.trim().is_empty() || content.trim().is_empty() {
            return Err(EmbeddingInputError::BlankField);
        }
        Ok(Self { chunk_id, content })
    }

    fn from_chunk(chunk: &IngestionChunk) -> Self {
        Self {
            chunk_id: chunk.chunk_id.clone(),
            content: chunk.content.clone(),
        }
    }

    /// Returns the application chunk identifier for result-order reconciliation.
    #[must_use]
    pub fn chunk_id(&self) -> &str {
        &self.chunk_id
    }

    /// Returns chunk text sent to the provider. Do not log it by default.
    #[must_use]
    pub fn content(&self) -> &str {
        &self.content
    }
}

/// Invalid direct input for an embedding provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EmbeddingInputError {
    /// Provider batch order can only be reconciled for non-blank chunk IDs and text.
    #[error("RAG embedding input chunk ID and content must not be blank")]
    BlankField,
}

impl fmt::Debug for EmbeddingInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EmbeddingInput")
            .field("chunk_id", &self.chunk_id)
            .field("content", &"[REDACTED]")
            .finish()
    }
}

/// One validated embedding vector in provider response order.
#[derive(Clone, PartialEq)]
pub struct Embedding {
    values: Vec<f32>,
}

impl Embedding {
    /// Creates a non-empty finite embedding vector.
    ///
    /// # Errors
    ///
    /// Returns [`EmbeddingError`] when the vector is empty or contains a non-finite value.
    pub fn new(values: impl Into<Vec<f32>>) -> Result<Self, EmbeddingError> {
        let values = values.into();
        if values.is_empty() {
            return Err(EmbeddingError::Empty);
        }
        if values.iter().any(|value| !value.is_finite()) {
            return Err(EmbeddingError::NonFinite);
        }
        Ok(Self { values })
    }

    /// Returns the vector dimensions for a store adapter. Do not emit full vectors by default.
    #[must_use]
    pub fn values(&self) -> &[f32] {
        &self.values
    }
}

impl fmt::Debug for Embedding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Embedding")
            .field("dimensions", &self.values.len())
            .finish()
    }
}

/// Invalid provider embedding data.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EmbeddingError {
    /// A vector store cannot index a zero-dimensional value.
    #[error("RAG embedding vector must not be empty")]
    Empty,
    /// NaN and infinity make similarity ordering undefined.
    #[error("RAG embedding vector must contain only finite values")]
    NonFinite,
}

/// Embedded data for one upsert into a vector index.
#[derive(Clone, Debug, PartialEq)]
pub struct EmbeddedChunk {
    chunk: IngestionChunk,
    embedding_model: String,
    embedding: Embedding,
}

impl EmbeddedChunk {
    fn new(chunk: IngestionChunk, embedding_model: String, embedding: Embedding) -> Self {
        Self {
            chunk,
            embedding_model,
            embedding,
        }
    }

    /// Returns the application chunk and source metadata retained by the vector adapter.
    #[must_use]
    pub fn chunk(&self) -> &IngestionChunk {
        &self.chunk
    }

    /// Returns the configured embedding model alias for this vector.
    #[must_use]
    pub fn embedding_model(&self) -> &str {
        &self.embedding_model
    }

    /// Returns the validated embedding values for a vector adapter.
    #[must_use]
    pub fn embedding(&self) -> &Embedding {
        &self.embedding
    }
}

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
    fn load(
        &self,
        request: RagIngestionRequest,
    ) -> BoxFuture<'static, Result<IngestionDocument, Self::Error>>;
}

/// Application chunker used before embedding a loaded document.
pub trait DocumentChunker: Clone + Send + Sync + 'static {
    /// Chunker-specific failure.
    type Error: StdError + Send + Sync + 'static;

    /// Splits a loaded document into bounded application chunks.
    fn chunk(
        &self,
        document: IngestionDocument,
    ) -> BoxFuture<'static, Result<Vec<IngestionChunk>, Self::Error>>;
}

/// Provider-neutral embedding boundary for a batch of redacted-debug chunk inputs.
pub trait EmbeddingProvider: Clone + Send + Sync + 'static {
    /// Provider-specific failure.
    type Error: StdError + Send + Sync + 'static;

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

    /// Inserts or replaces vectors for immutable chunk identifiers.
    fn upsert(&self, chunks: Vec<EmbeddedChunk>) -> BoxFuture<'static, Result<(), Self::Error>>;

    /// Removes vectors for one document revision when [`VectorStoreCapability::DeleteByDocument`]
    /// is declared. Callers must check [`Self::capabilities`] before invoking it.
    fn delete_document(
        &self,
        document: DocumentReference,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// Successful ingestion result without exposing loaded document or chunk content.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IngestionReport {
    document: DocumentReference,
    embedding_model: String,
    chunk_count: usize,
}

impl IngestionReport {
    /// Returns the document revision that was sent to the index.
    #[must_use]
    pub fn document(&self) -> &DocumentReference {
        &self.document
    }

    /// Returns the configured embedding model alias used for the vectors.
    #[must_use]
    pub fn embedding_model(&self) -> &str {
        &self.embedding_model
    }

    /// Returns the number of chunks written to the vector index.
    #[must_use]
    pub const fn chunk_count(&self) -> usize {
        self.chunk_count
    }
}

/// Ingestion service that preserves revision metadata from durable reference through vector upsert.
#[derive(Clone, Debug)]
pub struct RagIngestor<L, C, E, I> {
    loader: L,
    chunker: C,
    embeddings: E,
    index: I,
}

impl<L, C, E, I> RagIngestor<L, C, E, I> {
    /// Creates an ingestion service from application-owned source, chunker, provider, and index.
    #[must_use]
    pub fn new(loader: L, chunker: C, embeddings: E, index: I) -> Self {
        Self {
            loader,
            chunker,
            embeddings,
            index,
        }
    }
}

impl<L, C, E, I> RagIngestor<L, C, E, I>
where
    L: DocumentLoader,
    C: DocumentChunker,
    E: EmbeddingProvider,
    I: VectorIndex,
{
    /// Loads, chunks, embeds, and upserts one immutable document revision.
    ///
    /// # Errors
    ///
    /// Returns a component failure or rejects empty, mismatched, duplicate, or count-divergent
    /// data before the vector index receives a partial upsert.
    pub async fn ingest(
        &self,
        request: RagIngestionRequest,
    ) -> Result<IngestionReport, RagIngestionError<L::Error, C::Error, E::Error, I::Error>> {
        let expected_document = request.document().clone();
        let embedding_model = request.embedding_model().to_owned();
        let document = self
            .loader
            .load(request)
            .await
            .map_err(RagIngestionError::Load)?;
        if document.reference() != &expected_document {
            return Err(RagIngestionError::DocumentMismatch);
        }

        let chunks = self
            .chunker
            .chunk(document)
            .await
            .map_err(RagIngestionError::Chunk)?;
        if chunks.is_empty() {
            return Err(RagIngestionError::EmptyChunks);
        }
        let mut chunk_ids = BTreeSet::new();
        for chunk in &chunks {
            if chunk.document() != &expected_document {
                return Err(RagIngestionError::ChunkDocumentMismatch);
            }
            if !chunk_ids.insert(chunk.chunk_id()) {
                return Err(RagIngestionError::DuplicateChunkId);
            }
        }

        let inputs = chunks.iter().map(EmbeddingInput::from_chunk).collect();
        let embeddings = self
            .embeddings
            .embed(embedding_model.clone(), inputs)
            .await
            .map_err(RagIngestionError::Embed)?;
        if embeddings.len() != chunks.len() {
            return Err(RagIngestionError::EmbeddingCountMismatch {
                chunks: chunks.len(),
                embeddings: embeddings.len(),
            });
        }
        let chunk_count = chunks.len();
        let records = chunks
            .into_iter()
            .zip(embeddings)
            .map(|(chunk, embedding)| EmbeddedChunk::new(chunk, embedding_model.clone(), embedding))
            .collect();
        self.index
            .upsert(records)
            .await
            .map_err(RagIngestionError::Index)?;
        Ok(IngestionReport {
            document: expected_document,
            embedding_model,
            chunk_count,
        })
    }
}

/// Ingestion failure that never returns loaded document text or embedding vectors.
#[derive(Debug, thiserror::Error)]
pub enum RagIngestionError<LoaderError, ChunkerError, EmbeddingProviderError, IndexError>
where
    LoaderError: StdError + Send + Sync + 'static,
    ChunkerError: StdError + Send + Sync + 'static,
    EmbeddingProviderError: StdError + Send + Sync + 'static,
    IndexError: StdError + Send + Sync + 'static,
{
    /// The application document source could not load the requested revision.
    #[error("RAG ingestion document load failed")]
    Load(#[source] LoaderError),
    /// The application chunker could not split the loaded document.
    #[error("RAG ingestion chunking failed")]
    Chunk(#[source] ChunkerError),
    /// The embedding provider did not return a usable batch.
    #[error("RAG ingestion embedding failed")]
    Embed(#[source] EmbeddingProviderError),
    /// The vector index could not persist the complete replacement batch.
    #[error("RAG vector index upsert failed")]
    Index(#[source] IndexError),
    /// The source returned a different document revision from the one scheduled.
    #[error("RAG ingestion source returned an unexpected document revision")]
    DocumentMismatch,
    /// The chunker did not emit a bounded chunk for this document revision.
    #[error("RAG ingestion produced no chunks")]
    EmptyChunks,
    /// A chunk belonged to a document revision other than the scheduled request.
    #[error("RAG ingestion chunk belonged to an unexpected document revision")]
    ChunkDocumentMismatch,
    /// The chunker emitted an ambiguous duplicate chunk identifier.
    #[error("RAG ingestion produced duplicate chunk IDs")]
    DuplicateChunkId,
    /// The embedding provider did not preserve one-result-per-input order.
    #[error("RAG ingestion returned {embeddings} embeddings for {chunks} chunks")]
    EmbeddingCountMismatch {
        /// Number of validated chunks sent to the provider.
        chunks: usize,
        /// Number of vectors returned by the provider.
        embeddings: usize,
    },
}

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

/// Revalidated context returned to an application for explicit prompt construction.
#[derive(Clone, Debug, Eq, PartialEq)]
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

/// Retrieval service that verifies every store result against its original scope.
#[derive(Clone, Debug)]
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
        let chunks = self.store.search(query).await.map_err(RagError::Store)?;
        for chunk in &chunks {
            if chunk.tenant() != scope.tenant() || !scope.permits_document(chunk.document_id()) {
                return Err(RagError::ScopeViolation);
            }
        }
        Ok(RetrievalContext {
            chunks: chunks.into_iter().take(max_chunks).collect(),
        })
    }
}

/// Retrieval failure with a fail-closed ACL result.
#[derive(Debug, thiserror::Error)]
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
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        sync::{Arc, Mutex},
    };

    use futures_util::{future, future::BoxFuture};

    use super::{
        Citation, DocumentChunker, DocumentLoader, DocumentReference, EmbeddedChunk, Embedding,
        EmbeddingInput, EmbeddingProvider, IngestionChunk, IngestionDocument, RagError,
        RagIngestionError, RagIngestionRequest, RagIngestor, RagRetriever, RetrievalQuery,
        RetrievalScope, RetrievalStore, RetrievedChunk, VectorIndex, VectorStoreCapabilities,
        VectorStoreCapability,
    };

    #[derive(Clone)]
    struct FakeStore {
        chunks: Vec<RetrievedChunk>,
    }

    impl RetrievalStore for FakeStore {
        type Error = Infallible;

        fn search(
            &self,
            _: RetrievalQuery,
        ) -> BoxFuture<'static, Result<Vec<RetrievedChunk>, Self::Error>> {
            Box::pin(future::ready(Ok(self.chunks.clone())))
        }
    }

    fn chunk(tenant: &str, document_id: &str) -> RetrievedChunk {
        RetrievedChunk::new(
            "chunk-1",
            document_id,
            tenant,
            "account policy text",
            Citation::new("kb://policy", "Account policy").unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn retriever_returns_only_scope_valid_bounded_context() {
        let retriever = RagRetriever::new(FakeStore {
            chunks: vec![chunk("acme", "doc-1"), chunk("acme", "doc-1")],
        });
        let query = RetrievalQuery::new(
            "refund policy",
            RetrievalScope::new("acme", ["doc-1".to_owned()]).unwrap(),
            1,
        )
        .unwrap();

        let context = retriever.retrieve(query).await.unwrap();

        assert_eq!(context.chunks().len(), 1);
        assert_eq!(context.citations().len(), 1);
        assert!(!format!("{:?}", context.chunks()[0]).contains("account policy text"));
    }

    #[tokio::test]
    async fn retriever_rejects_any_chunk_outside_the_authorized_scope() {
        let retriever = RagRetriever::new(FakeStore {
            chunks: vec![chunk("other-tenant", "doc-1")],
        });
        let query = RetrievalQuery::new(
            "refund policy",
            RetrievalScope::new("acme", ["doc-1".to_owned()]).unwrap(),
            5,
        )
        .unwrap();

        assert!(matches!(
            retriever.retrieve(query).await,
            Err(RagError::ScopeViolation)
        ));
    }

    fn document_reference() -> DocumentReference {
        DocumentReference::new("acme", "doc-1", "v3", "sha256:abc")
            .expect("test document reference is valid")
    }

    fn ingestion_document() -> IngestionDocument {
        IngestionDocument::new(
            document_reference(),
            "internal source document text",
            Citation::new("kb://policy", "Account policy").expect("test citation is valid"),
        )
        .expect("test ingestion document is valid")
    }

    fn ingestion_chunk() -> IngestionChunk {
        IngestionChunk::new(
            "chunk-1",
            document_reference(),
            "internal source document text",
            Citation::new("kb://policy", "Account policy").expect("test citation is valid"),
        )
        .expect("test ingestion chunk is valid")
    }

    #[derive(Clone)]
    struct FakeLoader {
        document: IngestionDocument,
    }

    impl DocumentLoader for FakeLoader {
        type Error = Infallible;

        fn load(
            &self,
            _: RagIngestionRequest,
        ) -> BoxFuture<'static, Result<IngestionDocument, Self::Error>> {
            Box::pin(future::ready(Ok(self.document.clone())))
        }
    }

    #[derive(Clone)]
    struct FakeChunker {
        chunks: Vec<IngestionChunk>,
    }

    impl DocumentChunker for FakeChunker {
        type Error = Infallible;

        fn chunk(
            &self,
            _: IngestionDocument,
        ) -> BoxFuture<'static, Result<Vec<IngestionChunk>, Self::Error>> {
            Box::pin(future::ready(Ok(self.chunks.clone())))
        }
    }

    #[derive(Clone)]
    struct FakeEmbedder {
        embeddings: Vec<Embedding>,
        inputs: Arc<Mutex<Vec<EmbeddingInput>>>,
    }

    impl EmbeddingProvider for FakeEmbedder {
        type Error = Infallible;

        fn embed(
            &self,
            _: String,
            inputs: Vec<EmbeddingInput>,
        ) -> BoxFuture<'static, Result<Vec<Embedding>, Self::Error>> {
            let captured = Arc::clone(&self.inputs);
            let embeddings = self.embeddings.clone();
            Box::pin(async move {
                *captured.lock().expect("test embedding lock is available") = inputs;
                Ok(embeddings)
            })
        }
    }

    #[derive(Clone)]
    struct CapturingIndex {
        chunks: Arc<Mutex<Vec<EmbeddedChunk>>>,
    }

    impl VectorIndex for CapturingIndex {
        type Error = Infallible;

        fn capabilities(&self) -> VectorStoreCapabilities {
            VectorStoreCapabilities::new(true, true, false)
        }

        fn upsert(
            &self,
            chunks: Vec<EmbeddedChunk>,
        ) -> BoxFuture<'static, Result<(), Self::Error>> {
            let captured = Arc::clone(&self.chunks);
            Box::pin(async move {
                *captured.lock().expect("test index lock is available") = chunks;
                Ok(())
            })
        }

        fn delete_document(
            &self,
            _: DocumentReference,
        ) -> BoxFuture<'static, Result<(), Self::Error>> {
            Box::pin(future::ready(Ok(())))
        }
    }

    fn ingestion_request() -> RagIngestionRequest {
        RagIngestionRequest::new(document_reference(), "embedding.default")
            .expect("test ingestion request is valid")
    }

    #[tokio::test]
    async fn ingestor_preserves_revision_metadata_through_embedding_and_upsert() {
        let inputs = Arc::new(Mutex::new(Vec::new()));
        let indexed = Arc::new(Mutex::new(Vec::new()));
        let index = CapturingIndex {
            chunks: Arc::clone(&indexed),
        };
        assert!(
            index
                .capabilities()
                .supports(VectorStoreCapability::MetadataFilter)
        );
        assert!(
            index
                .capabilities()
                .supports(VectorStoreCapability::DeleteByDocument)
        );
        assert!(
            !index
                .capabilities()
                .supports(VectorStoreCapability::HybridSearch)
        );
        let ingestor = RagIngestor::new(
            FakeLoader {
                document: ingestion_document(),
            },
            FakeChunker {
                chunks: vec![ingestion_chunk()],
            },
            FakeEmbedder {
                embeddings: vec![
                    Embedding::new(vec![0.25, -0.5]).expect("test embedding is valid"),
                ],
                inputs: Arc::clone(&inputs),
            },
            index,
        );

        let request = ingestion_request();
        let report = ingestor.ingest(request.clone()).await.unwrap();

        assert_eq!(report.document(), request.document());
        assert_eq!(report.embedding_model(), "embedding.default");
        assert_eq!(report.chunk_count(), 1);
        let inputs = inputs.lock().expect("test embedding lock is available");
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].chunk_id(), "chunk-1");
        assert!(!format!("{:?}", inputs[0]).contains("internal source document text"));
        let indexed = indexed.lock().expect("test index lock is available");
        assert_eq!(indexed.len(), 1);
        assert_eq!(indexed[0].chunk().document(), request.document());
        assert_eq!(indexed[0].embedding_model(), "embedding.default");
        assert_eq!(indexed[0].embedding().values(), &[0.25, -0.5]);
        let serialized = serde_json::to_string(&request).expect("request serializes");
        assert!(!serialized.contains("internal source document text"));
        assert!(!format!("{request:?}").contains("sha256:abc"));
    }

    #[tokio::test]
    async fn embedding_count_mismatch_blocks_the_vector_upsert() {
        let indexed = Arc::new(Mutex::new(Vec::new()));
        let ingestor = RagIngestor::new(
            FakeLoader {
                document: ingestion_document(),
            },
            FakeChunker {
                chunks: vec![ingestion_chunk()],
            },
            FakeEmbedder {
                embeddings: Vec::new(),
                inputs: Arc::new(Mutex::new(Vec::new())),
            },
            CapturingIndex {
                chunks: Arc::clone(&indexed),
            },
        );

        let error = ingestor.ingest(ingestion_request()).await.unwrap_err();

        assert!(matches!(
            error,
            RagIngestionError::EmbeddingCountMismatch {
                chunks: 1,
                embeddings: 0,
            }
        ));
        assert!(
            indexed
                .lock()
                .expect("test index lock is available")
                .is_empty()
        );
    }
}
