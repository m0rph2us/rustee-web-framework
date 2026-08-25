use std::{
    convert::Infallible,
    error::Error as StdError,
    fmt,
    sync::{Arc, Mutex},
};

use futures_util::future::BoxFuture;
use rustee_ai_batch::{
    AiBatchArtifactKind, AiBatchArtifactReference, AiBatchCatalog, AiBatchProvider, AiBatchReceipt,
    AiBatchReference, InMemoryAiBatchLedger,
};
use rustee_jobs::{DeliveryAction, JobEnvelope, JobId, dispatch};

use super::{
    AiBatchArtifactReconciliationHandler, AiBatchArtifactReconciliationJob,
    AiBatchArtifactReconciliationJobError, AiBatchArtifactReconciliationRunner,
    AiBatchSubmissionHandler, AiBatchSubmissionJob, AiBatchSubmissionJobError,
};

#[derive(Clone)]
struct Catalog;

impl AiBatchCatalog for Catalog {
    type Work = String;
    type Error = Infallible;

    fn load(
        &self,
        _reference: AiBatchReference,
    ) -> BoxFuture<'static, Result<Self::Work, Self::Error>> {
        Box::pin(async { Ok("private raw prompt and expected completion".to_owned()) })
    }
}

#[derive(Clone)]
struct Provider {
    calls: Arc<Mutex<usize>>,
}

impl AiBatchProvider<String> for Provider {
    type Error = Infallible;

    fn submit(
        &self,
        _reference: AiBatchReference,
        _work: String,
    ) -> BoxFuture<'static, Result<AiBatchReceipt, Self::Error>> {
        let calls = self.calls.clone();
        Box::pin(async move {
            *calls.lock().unwrap() += 1;
            Ok(AiBatchReceipt::new("provider-batch-7").unwrap())
        })
    }
}

fn reference() -> AiBatchReference {
    AiBatchReference::new("tenant-a.v2", "catalog-7", "run-key-7").unwrap()
}

fn envelope(key: &str) -> JobEnvelope<AiBatchSubmissionJob> {
    JobEnvelope::with_metadata(JobId::new(), AiBatchSubmissionJob::new(reference()), 123)
        .with_idempotency_key(key)
        .unwrap()
}

fn artifact_reference() -> AiBatchArtifactReference {
    AiBatchArtifactReference::new(
        reference(),
        AiBatchArtifactKind::Output,
        "file-output-7",
        "artifact-reconcile-7",
    )
    .unwrap()
}

fn artifact_envelope(key: &str) -> JobEnvelope<AiBatchArtifactReconciliationJob> {
    JobEnvelope::with_metadata(
        JobId::new(),
        AiBatchArtifactReconciliationJob::new(artifact_reference()),
        123,
    )
    .with_idempotency_key(key)
    .unwrap()
}

#[derive(Clone)]
struct ArtifactRunner {
    calls: Arc<Mutex<usize>>,
}

impl AiBatchArtifactReconciliationRunner for ArtifactRunner {
    type Error = Infallible;

    fn reconcile_artifact(
        &self,
        _reference: AiBatchArtifactReference,
        _context: rustee_jobs::JobContext,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let calls = self.calls.clone();
        Box::pin(async move {
            *calls.lock().unwrap() += 1;
            Ok(())
        })
    }
}

struct LeakyRunnerError;

impl fmt::Debug for LeakyRunnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LeakyRunnerError(private-batch-runner-detail)")
    }
}

impl fmt::Display for LeakyRunnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("private-batch-runner-detail")
    }
}

impl StdError for LeakyRunnerError {}

#[tokio::test]
async fn durable_job_keeps_raw_work_out_of_payload_and_duplicate_delivery_does_not_resubmit() {
    let encoded = envelope("run-key-7").encode().unwrap();
    let payload = String::from_utf8(encoded.clone()).unwrap();
    assert!(!payload.contains("private raw prompt"));
    let calls = Arc::new(Mutex::new(0));
    let handler = AiBatchSubmissionHandler::new(rustee_ai_batch::AiBatchSubmitter::new(
        Catalog,
        InMemoryAiBatchLedger::new(4).unwrap(),
        Provider {
            calls: calls.clone(),
        },
    ));

    let first = dispatch(
        JobEnvelope::<AiBatchSubmissionJob>::decode(&encoded).unwrap(),
        &handler,
    )
    .await
    .unwrap();
    let second = dispatch(
        JobEnvelope::<AiBatchSubmissionJob>::decode(&encoded).unwrap(),
        &handler,
    )
    .await
    .unwrap();

    assert_eq!(first, DeliveryAction::Acknowledge);
    assert_eq!(second, DeliveryAction::Acknowledge);
    assert_eq!(*calls.lock().unwrap(), 1);
}

#[tokio::test]
async fn handler_rejects_a_durable_key_that_does_not_bind_the_batch_run_key() {
    let calls = Arc::new(Mutex::new(0));
    let handler = AiBatchSubmissionHandler::new(rustee_ai_batch::AiBatchSubmitter::new(
        Catalog,
        InMemoryAiBatchLedger::new(4).unwrap(),
        Provider {
            calls: calls.clone(),
        },
    ));

    let error = dispatch(envelope("other-key"), &handler).await.unwrap_err();

    assert!(matches!(error, AiBatchSubmissionJobError::RunKeyMismatch));
    assert_eq!(*calls.lock().unwrap(), 0);
}

#[tokio::test]
async fn artifact_job_keeps_raw_result_out_of_payload_and_binds_its_reconciliation_key() {
    let encoded = artifact_envelope("artifact-reconcile-7").encode().unwrap();
    let payload = String::from_utf8(encoded.clone()).unwrap();
    assert!(!payload.contains("private model output or provider error message"));
    let calls = Arc::new(Mutex::new(0));
    let handler = AiBatchArtifactReconciliationHandler::new(ArtifactRunner {
        calls: calls.clone(),
    });

    let action = dispatch(
        JobEnvelope::<AiBatchArtifactReconciliationJob>::decode(&encoded).unwrap(),
        &handler,
    )
    .await
    .unwrap();

    assert_eq!(action, DeliveryAction::Acknowledge);
    assert_eq!(*calls.lock().unwrap(), 1);
}

#[tokio::test]
async fn artifact_handler_rejects_a_durable_key_that_does_not_bind_the_artifact() {
    let calls = Arc::new(Mutex::new(0));
    let handler = AiBatchArtifactReconciliationHandler::new(ArtifactRunner {
        calls: calls.clone(),
    });

    let error = dispatch(artifact_envelope("other-key"), &handler)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        AiBatchArtifactReconciliationJobError::KeyMismatch
    ));
    assert_eq!(*calls.lock().unwrap(), 0);
}

#[test]
fn job_error_diagnostics_redact_runner_details_and_preserve_sources() {
    let submission = AiBatchSubmissionJobError::Runner {
        source: LeakyRunnerError,
    };
    let reconciliation = AiBatchArtifactReconciliationJobError::Runner {
        source: LeakyRunnerError,
    };

    for error in [&submission as &dyn StdError, &reconciliation] {
        assert!(!format!("{error:?}").contains("private-batch-runner-detail"));
        assert!(!error.to_string().contains("private-batch-runner-detail"));
        assert!(StdError::source(error).is_some());
    }
}
