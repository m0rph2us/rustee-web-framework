//! Atomic RAG document ingestion orchestration.

use std::{collections::BTreeSet, error::Error as StdError, fmt};

mod model;

pub use model::{
    DEFAULT_EMBEDDING_BATCH_CONTENT_BYTES, DEFAULT_EMBEDDING_BATCH_INPUTS, DocumentChunker,
    DocumentLoader, DocumentReference, DocumentReferenceError, EmbeddedChunk, Embedding,
    EmbeddingBatchLimits, EmbeddingBatchLimitsError, EmbeddingError, EmbeddingInput,
    EmbeddingInputError, EmbeddingProvider, IngestionChunk, IngestionChunkError, IngestionDocument,
    IngestionDocumentError, MAX_DOCUMENT_REFERENCE_FIELD_BYTES, MAX_INGESTION_CHUNK_CONTENT_BYTES,
    MAX_INGESTION_CHUNK_ID_BYTES, RagIngestionRequest, RagIngestionRequestError, VectorIndex,
    VectorStoreCapabilities, VectorStoreCapability,
};

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
#[derive(Clone)]
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

impl<L, C, E, I> fmt::Debug for RagIngestor<L, C, E, I> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RagIngestor")
            .finish_non_exhaustive()
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
    /// Returns a component failure or rejects empty, mismatched, duplicate, oversized, or
    /// dimension-divergent data before the vector index receives a partial upsert.
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

        let mut embeddings = Vec::with_capacity(chunks.len());
        let mut inputs = Vec::new();
        let mut input_content_bytes = 0;
        let mut embedding_dimensions = None;
        let limits = self.embeddings.batch_limits();
        for chunk in &chunks {
            let input = EmbeddingInput::from_chunk(chunk);
            let content_bytes = input.content().len();
            if content_bytes > limits.max_content_bytes() {
                return Err(RagIngestionError::EmbeddingInputTooLarge {
                    content_bytes,
                    max_content_bytes: limits.max_content_bytes(),
                });
            }
            if !inputs.is_empty()
                && (inputs.len() >= limits.max_inputs()
                    || input_content_bytes > limits.max_content_bytes() - content_bytes)
            {
                self.embed_batch(
                    &embedding_model,
                    &mut inputs,
                    &mut embeddings,
                    &mut embedding_dimensions,
                )
                .await?;
                input_content_bytes = 0;
            }
            input_content_bytes += content_bytes;
            inputs.push(input);
        }
        if !inputs.is_empty() {
            self.embed_batch(
                &embedding_model,
                &mut inputs,
                &mut embeddings,
                &mut embedding_dimensions,
            )
            .await?;
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

impl<L, C, E, I> RagIngestor<L, C, E, I>
where
    L: DocumentLoader,
    C: DocumentChunker,
    E: EmbeddingProvider,
    I: VectorIndex,
{
    async fn embed_batch(
        &self,
        embedding_model: &str,
        inputs: &mut Vec<EmbeddingInput>,
        embeddings: &mut Vec<Embedding>,
        embedding_dimensions: &mut Option<usize>,
    ) -> Result<(), RagIngestionError<L::Error, C::Error, E::Error, I::Error>> {
        let expected_inputs = inputs.len();
        let batch_inputs = std::mem::take(inputs);
        let batch_embeddings = self
            .embeddings
            .embed(embedding_model.to_owned(), batch_inputs)
            .await
            .map_err(RagIngestionError::Embed)?;
        if batch_embeddings.len() != expected_inputs {
            return Err(RagIngestionError::EmbeddingCountMismatch {
                chunks: expected_inputs,
                embeddings: batch_embeddings.len(),
            });
        }
        for embedding in &batch_embeddings {
            let dimensions = embedding.values().len();
            if let Some(expected_dimensions) = embedding_dimensions {
                if dimensions != *expected_dimensions {
                    return Err(RagIngestionError::EmbeddingDimensionMismatch {
                        expected_dimensions: *expected_dimensions,
                        received_dimensions: dimensions,
                    });
                }
            } else {
                *embedding_dimensions = Some(dimensions);
            }
        }
        embeddings.extend(batch_embeddings);
        Ok(())
    }
}

/// Ingestion failure with content-free display and debug diagnostics.
#[derive(thiserror::Error)]
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
    /// One chunk cannot fit the configured provider request content budget.
    #[error(
        "RAG ingestion chunk content of {content_bytes} bytes exceeds the provider batch limit of {max_content_bytes} bytes"
    )]
    EmbeddingInputTooLarge {
        /// Content bytes in the rejected chunk.
        content_bytes: usize,
        /// Maximum bytes accepted by one configured provider request.
        max_content_bytes: usize,
    },
    /// One provider response contained a vector with a different dimensionality.
    #[error(
        "RAG ingestion embedding dimensions differed: expected {expected_dimensions}, received {received_dimensions}"
    )]
    EmbeddingDimensionMismatch {
        /// Dimensions established by the first validated provider embedding.
        expected_dimensions: usize,
        /// Dimensions returned by a later provider embedding.
        received_dimensions: usize,
    },
}

impl<LoaderError, ChunkerError, EmbeddingProviderError, IndexError> fmt::Debug
    for RagIngestionError<LoaderError, ChunkerError, EmbeddingProviderError, IndexError>
where
    LoaderError: StdError + Send + Sync + 'static,
    ChunkerError: StdError + Send + Sync + 'static,
    EmbeddingProviderError: StdError + Send + Sync + 'static,
    IndexError: StdError + Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Load(_) => formatter.write_str("RagIngestionError::Load"),
            Self::Chunk(_) => formatter.write_str("RagIngestionError::Chunk"),
            Self::Embed(_) => formatter.write_str("RagIngestionError::Embed"),
            Self::Index(_) => formatter.write_str("RagIngestionError::Index"),
            Self::DocumentMismatch => formatter.write_str("RagIngestionError::DocumentMismatch"),
            Self::EmptyChunks => formatter.write_str("RagIngestionError::EmptyChunks"),
            Self::ChunkDocumentMismatch => {
                formatter.write_str("RagIngestionError::ChunkDocumentMismatch")
            }
            Self::DuplicateChunkId => formatter.write_str("RagIngestionError::DuplicateChunkId"),
            Self::EmbeddingCountMismatch { chunks, embeddings } => formatter
                .debug_struct("RagIngestionError::EmbeddingCountMismatch")
                .field("chunks", chunks)
                .field("embeddings", embeddings)
                .finish(),
            Self::EmbeddingInputTooLarge {
                content_bytes,
                max_content_bytes,
            } => formatter
                .debug_struct("RagIngestionError::EmbeddingInputTooLarge")
                .field("content_bytes", content_bytes)
                .field("max_content_bytes", max_content_bytes)
                .finish(),
            Self::EmbeddingDimensionMismatch {
                expected_dimensions,
                received_dimensions,
            } => formatter
                .debug_struct("RagIngestionError::EmbeddingDimensionMismatch")
                .field("expected_dimensions", expected_dimensions)
                .field("received_dimensions", received_dimensions)
                .finish(),
        }
    }
}
