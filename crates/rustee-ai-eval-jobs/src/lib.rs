//! Reference-only durable jobs for AI evaluation runs.
//!
//! A payload serializes an [`AiEvaluationReference`] and never raw prompts, expected targets,
//! model completions, or grader metadata. The handler binds durable job idempotency to the stable
//! evaluation run key. The application catalog and run ledger decide authorization, retention,
//! reconciliation, and provider retry/DLQ policy after a worker starts.

use std::{error::Error as StdError, fmt, marker::PhantomData};

use futures_util::future::BoxFuture;
use rustee_ai_eval::{
    AiEvaluationCatalog, AiEvaluationExecutor, AiEvaluationGrader, AiEvaluationReference,
    AiEvaluationRunLedger, AiEvaluationSubmissionError, AiEvaluationSubmitter,
};
use rustee_jobs::{Job, JobContext, JobHandler};
use serde::{Deserialize, Serialize};

/// Stable, content-free durable payload that schedules one application evaluation reference.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AiEvaluationJob {
    reference: AiEvaluationReference,
}

impl AiEvaluationJob {
    /// Creates a job from a validated application-owned evaluation reference.
    #[must_use]
    pub const fn new(reference: AiEvaluationReference) -> Self {
        Self { reference }
    }

    /// Returns the catalog reference resolved only after durable dispatch starts.
    #[must_use]
    pub const fn reference(&self) -> &AiEvaluationReference {
        &self.reference
    }
}

impl Job for AiEvaluationJob {
    const NAME: &'static str = "ai.evaluation.run";
    const VERSION: u16 = 1;
}

/// Application boundary for one durable evaluation delivery.
pub trait AiEvaluationJobRunner: Clone + Send + Sync + 'static {
    /// Runner-specific failure.
    type Error: StdError + Send + Sync + 'static;

    /// Resolves and evaluates one content-free reference after durable dispatch starts.
    fn run_evaluation(
        &self,
        reference: AiEvaluationReference,
        context: JobContext,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// [`AiEvaluationSubmitter`] adapter for one concrete application catalog target type.
pub struct AiEvaluationSubmissionJobRunner<C, L, E, G, T> {
    submitter: AiEvaluationSubmitter<C, L, E, G>,
    target: PhantomData<fn() -> T>,
}

impl<C, L, E, G, T> Clone for AiEvaluationSubmissionJobRunner<C, L, E, G, T>
where
    C: Clone,
    L: Clone,
    E: Clone,
    G: Clone,
{
    fn clone(&self) -> Self {
        Self {
            submitter: self.submitter.clone(),
            target: PhantomData,
        }
    }
}

impl<C, L, E, G, T> AiEvaluationSubmissionJobRunner<C, L, E, G, T> {
    /// Connects a reference-backed evaluation submitter to a typed job worker.
    #[must_use]
    pub const fn new(submitter: AiEvaluationSubmitter<C, L, E, G>) -> Self {
        Self {
            submitter,
            target: PhantomData,
        }
    }
}

impl<C, L, E, G, T> fmt::Debug for AiEvaluationSubmissionJobRunner<C, L, E, G, T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiEvaluationSubmissionJobRunner")
            .field("submitter", &"[APPLICATION-OWNED]")
            .finish()
    }
}

impl<C, L, E, G, T> AiEvaluationJobRunner for AiEvaluationSubmissionJobRunner<C, L, E, G, T>
where
    C: AiEvaluationCatalog<T>,
    L: AiEvaluationRunLedger,
    E: AiEvaluationExecutor,
    G: AiEvaluationGrader<T>,
    T: Send + Sync + 'static,
{
    type Error = AiEvaluationSubmissionError<C::Error, L::Error, E::Error, G::Error>;

    fn run_evaluation(
        &self,
        reference: AiEvaluationReference,
        _: JobContext,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let submitter = self.submitter.clone();
        Box::pin(async move { submitter.submit::<T>(reference).await.map(|_| ()) })
    }
}

/// [`JobHandler`] adapter that validates run-key binding before evaluation starts.
#[derive(Clone)]
pub struct AiEvaluationJobHandler<R> {
    runner: R,
}

impl<R> AiEvaluationJobHandler<R> {
    /// Creates a typed handler around an application runner or submitter adapter.
    #[must_use]
    pub const fn new(runner: R) -> Self {
        Self { runner }
    }
}

impl<R> fmt::Debug for AiEvaluationJobHandler<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiEvaluationJobHandler")
            .field("runner", &"[APPLICATION-OWNED]")
            .finish()
    }
}

impl<R> JobHandler<AiEvaluationJob> for AiEvaluationJobHandler<R>
where
    R: AiEvaluationJobRunner,
{
    type Error = AiEvaluationJobError<R::Error>;

    fn handle(
        &self,
        payload: AiEvaluationJob,
        context: JobContext,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let reference = payload.reference;
        if context.idempotency_key() != Some(reference.run_key()) {
            return Box::pin(async { Err(AiEvaluationJobError::RunKeyMismatch) });
        }
        let future = self.runner.run_evaluation(reference, context);
        Box::pin(async move {
            future
                .await
                .map_err(|source| AiEvaluationJobError::Runner { source })
        })
    }
}

/// Sanitized durable evaluation-job failure.
#[derive(Debug, thiserror::Error)]
pub enum AiEvaluationJobError<RunnerError> {
    /// The durable envelope's idempotency key was absent or did not bind the evaluation run key.
    #[error("AI evaluation job idempotency key does not match the evaluation run key")]
    RunKeyMismatch,
    /// The application evaluation runner did not accept the delivery.
    #[error("AI evaluation job runner failed")]
    Runner {
        /// Runner failure for the provider's retry or dead-letter policy.
        #[source]
        source: RunnerError,
    },
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        sync::{Arc, Mutex},
    };

    use futures_util::future::BoxFuture;
    use rustee_ai_eval::AiEvaluationReference;
    use rustee_jobs::{DeliveryAction, JobEnvelope, JobId, dispatch};

    use super::{
        AiEvaluationJob, AiEvaluationJobError, AiEvaluationJobHandler, AiEvaluationJobRunner,
    };

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
}
