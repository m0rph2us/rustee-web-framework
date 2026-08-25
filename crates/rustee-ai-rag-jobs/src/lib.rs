//! Typed durable RAG ingestion job contracts.
//!
//! [`RagIngestionJob`] serializes only a [`rustee_ai_rag::RagIngestionRequest`], which contains
//! document revision metadata and an embedding alias but no source document text. A worker loads
//! and indexes content after the durable job begins.

use std::{error::Error as StdError, fmt};

use futures_util::future::BoxFuture;
use rustee_ai_rag::{RagIngestionError, RagIngestionRequest, RagIngestor};
use rustee_jobs::{Job, JobContext, JobHandler};
use serde::{Deserialize, Serialize};

/// Stable typed payload that schedules ingestion of one document revision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RagIngestionJob {
    request: RagIngestionRequest,
}

impl RagIngestionJob {
    /// Creates a job from content-free document revision and embedding metadata.
    #[must_use]
    pub fn new(request: RagIngestionRequest) -> Self {
        Self { request }
    }

    /// Returns the request that a worker resolves after durable delivery starts.
    #[must_use]
    pub fn request(&self) -> &RagIngestionRequest {
        &self.request
    }
}

impl Job for RagIngestionJob {
    const NAME: &'static str = "ai.rag.ingest-document";
    const VERSION: u16 = 1;
}

/// Application boundary that runs one durable RAG ingestion delivery.
pub trait RagIngestionRunner: Clone + Send + Sync + 'static {
    /// Runner-specific failure.
    type Error: StdError + Send + Sync + 'static;

    /// Loads and indexes the requested document revision after a provider delivers the job.
    fn ingest(
        &self,
        request: RagIngestionRequest,
        context: JobContext,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

impl<L, C, E, I> RagIngestionRunner for RagIngestor<L, C, E, I>
where
    L: rustee_ai_rag::DocumentLoader,
    C: rustee_ai_rag::DocumentChunker,
    E: rustee_ai_rag::EmbeddingProvider,
    I: rustee_ai_rag::VectorIndex,
{
    type Error = RagIngestionError<L::Error, C::Error, E::Error, I::Error>;

    fn ingest(
        &self,
        request: RagIngestionRequest,
        _: JobContext,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let ingestor = self.clone();
        Box::pin(async move { ingestor.ingest(request).await.map(|_| ()) })
    }
}

/// [`JobHandler`] adapter that starts RAG ingestion only after durable worker dispatch.
#[derive(Clone)]
pub struct RagIngestionHandler<R> {
    runner: R,
}

impl<R> RagIngestionHandler<R> {
    /// Creates a typed handler from an application runner or [`RagIngestor`].
    #[must_use]
    pub fn new(runner: R) -> Self {
        Self { runner }
    }
}

impl<R> fmt::Debug for RagIngestionHandler<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RagIngestionHandler")
            .field("runner", &"[REDACTED]")
            .finish()
    }
}

impl<R> JobHandler<RagIngestionJob> for RagIngestionHandler<R>
where
    R: RagIngestionRunner,
{
    type Error = RagIngestionJobError<R::Error>;

    fn handle(
        &self,
        payload: RagIngestionJob,
        context: JobContext,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let future = self.runner.ingest(payload.request, context);
        Box::pin(async move { future.await.map_err(RagIngestionJobError::Runner) })
    }
}

/// Sanitized durable RAG ingestion-job failure.
///
/// Its display and debug forms retain only the failure category. The underlying runner error stays
/// available through [`std::error::Error::source`] for trusted retry or dead-letter handling.
#[derive(thiserror::Error)]
pub enum RagIngestionJobError<RunnerError> {
    /// The application ingestion runner did not accept the delivery.
    #[error("RAG ingestion job runner failed")]
    Runner(#[source] RunnerError),
}

impl<RunnerError> fmt::Debug for RagIngestionJobError<RunnerError> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runner(_) => formatter.write_str("RagIngestionJobError::Runner"),
        }
    }
}

#[cfg(test)]
mod tests;
