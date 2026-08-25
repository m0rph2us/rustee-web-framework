use std::{
    convert::Infallible,
    error::Error as StdError,
    fmt,
    sync::{Arc, Mutex},
};

use rustee_ai_rag::{DocumentReference, RagIngestionRequest};
use rustee_jobs::{DeliveryAction, JobContext, JobEnvelope, JobId, dispatch};

use super::{RagIngestionHandler, RagIngestionJob, RagIngestionJobError, RagIngestionRunner};

struct LeakyRunnerError;

impl fmt::Debug for LeakyRunnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LeakyRunnerError(private-rag-ingestion-runner-detail)")
    }
}

impl fmt::Display for LeakyRunnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("private-rag-ingestion-runner-detail")
    }
}

impl StdError for LeakyRunnerError {}

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

#[test]
fn job_error_diagnostics_redact_runner_details_and_preserve_the_source() {
    let error = RagIngestionJobError::Runner(LeakyRunnerError);

    assert!(!format!("{error:?}").contains("private-rag-ingestion-runner-detail"));
    assert!(
        !error
            .to_string()
            .contains("private-rag-ingestion-runner-detail")
    );
    assert!(StdError::source(&error).is_some());
}
