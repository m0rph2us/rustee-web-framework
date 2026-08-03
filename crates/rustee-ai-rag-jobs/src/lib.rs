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
    type Error = R::Error;

    fn handle(
        &self,
        payload: RagIngestionJob,
        context: JobContext,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        self.runner.ingest(payload.request, context)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        sync::{Arc, Mutex},
    };

    use rustee_ai_rag::{DocumentReference, RagIngestionRequest};
    use rustee_jobs::{DeliveryAction, JobContext, JobEnvelope, JobId, dispatch};

    use super::{RagIngestionHandler, RagIngestionJob, RagIngestionRunner};

    fn request() -> RagIngestionRequest {
        RagIngestionRequest::new(
            DocumentReference::new("acme", "doc-1", "v3", "sha256:abc")
                .expect("test document reference is valid"),
            "embedding.default",
        )
        .expect("test ingestion request is valid")
    }

    type CapturedCall = (RagIngestionRequest, Option<String>);

    #[derive(Clone)]
    struct CapturingRunner {
        calls: Arc<Mutex<Vec<CapturedCall>>>,
    }

    impl RagIngestionRunner for CapturingRunner {
        type Error = Infallible;

        fn ingest(
            &self,
            request: RagIngestionRequest,
            context: JobContext,
        ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
            let calls = Arc::clone(&self.calls);
            Box::pin(async move {
                calls
                    .lock()
                    .expect("test runner lock is available")
                    .push((request, context.idempotency_key().map(ToOwned::to_owned)));
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn durable_job_keeps_document_text_out_of_the_payload_and_forwards_context() {
        let request = request();
        let job = RagIngestionJob::new(request.clone());
        let envelope = JobEnvelope::with_metadata(JobId::new(), job, 123)
            .with_idempotency_key("rag:doc-1:v3")
            .expect("test job idempotency key is valid");
        let encoded = envelope.encode().expect("test job serializes");
        let payload = String::from_utf8(encoded.clone()).expect("serialized job is UTF-8 JSON");
        assert!(!payload.contains("internal source document text"));
        assert!(!format!("{request:?}").contains("sha256:abc"));

        let calls = Arc::new(Mutex::new(Vec::new()));
        let action = dispatch(
            JobEnvelope::<RagIngestionJob>::decode(&encoded).expect("test job decodes"),
            &RagIngestionHandler::new(CapturingRunner {
                calls: Arc::clone(&calls),
            }),
        )
        .await
        .expect("test runner succeeds");

        assert_eq!(action, DeliveryAction::Acknowledge);
        let calls = calls.lock().expect("test runner lock is available");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, request);
        assert_eq!(calls[0].1.as_deref(), Some("rag:doc-1:v3"));
    }

    #[test]
    fn handler_debug_does_not_expose_the_runner() {
        let handler = RagIngestionHandler::new(CapturingRunner {
            calls: Arc::new(Mutex::new(Vec::new())),
        });
        assert_eq!(
            format!("{handler:?}"),
            "RagIngestionHandler { runner: \"[REDACTED]\" }"
        );
    }
}
