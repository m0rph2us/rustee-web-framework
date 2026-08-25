use std::{
    convert::Infallible,
    fmt,
    sync::{Arc, Mutex},
};

use futures_util::future::BoxFuture;
use rustee_ai_eval::AiEvaluationReference;
use rustee_jobs::{DeliveryAction, JobEnvelope, JobId, dispatch};

use super::{AiEvaluationJob, AiEvaluationJobError, AiEvaluationJobHandler, AiEvaluationJobRunner};

struct LeakyRunnerError;

impl fmt::Debug for LeakyRunnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LeakyRunnerError(private-evaluation-runner-detail)")
    }
}

impl fmt::Display for LeakyRunnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("private-evaluation-runner-detail")
    }
}

impl std::error::Error for LeakyRunnerError {}

fn reference() -> AiEvaluationReference {
    AiEvaluationReference::new("tenant-a.v1", "catalog-7", "run-key-7").unwrap()
}

fn envelope(key: &str) -> JobEnvelope<AiEvaluationJob> {
    JobEnvelope::with_metadata(JobId::new(), AiEvaluationJob::new(reference()), 123)
        .with_idempotency_key(key)
        .unwrap()
}

#[derive(Clone)]
struct Runner {
    calls: Arc<Mutex<usize>>,
}

impl AiEvaluationJobRunner for Runner {
    type Error = Infallible;

    fn run_evaluation(
        &self,
        _reference: AiEvaluationReference,
        _context: rustee_jobs::JobContext,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let calls = self.calls.clone();
        Box::pin(async move {
            *calls.lock().unwrap() += 1;
            Ok(())
        })
    }
}

#[tokio::test]
async fn durable_job_keeps_private_evaluation_work_out_of_the_payload() {
    let encoded = envelope("run-key-7").encode().unwrap();
    let payload = String::from_utf8(encoded.clone()).unwrap();
    assert!(!payload.contains("private prompt"));
    assert!(!payload.contains("private expected target"));
    let calls = Arc::new(Mutex::new(0));
    let handler = AiEvaluationJobHandler::new(Runner {
        calls: calls.clone(),
    });

    let action = dispatch(
        JobEnvelope::<AiEvaluationJob>::decode(&encoded).unwrap(),
        &handler,
    )
    .await
    .unwrap();

    assert_eq!(action, DeliveryAction::Acknowledge);
    assert_eq!(*calls.lock().unwrap(), 1);
}

#[tokio::test]
async fn handler_rejects_a_durable_key_that_does_not_bind_the_evaluation_run() {
    let calls = Arc::new(Mutex::new(0));
    let handler = AiEvaluationJobHandler::new(Runner {
        calls: calls.clone(),
    });

    let error = dispatch(envelope("other-key"), &handler).await.unwrap_err();

    assert!(matches!(error, AiEvaluationJobError::RunKeyMismatch));
    assert_eq!(*calls.lock().unwrap(), 0);
}

#[test]
fn job_error_diagnostics_redact_runner_details_and_preserve_the_source() {
    let error = AiEvaluationJobError::Runner {
        source: LeakyRunnerError,
    };

    assert!(!format!("{error:?}").contains("private-evaluation-runner-detail"));
    assert!(
        !error
            .to_string()
            .contains("private-evaluation-runner-detail")
    );
    assert!(std::error::Error::source(&error).is_some());
}
