//! Reference-only durable jobs for AI provider batch submission and artifact reconciliation.
//!
//! The job payload serializes an [`AiBatchReference`] and never raw provider work. The handler
//! requires that its durable job idempotency key exactly matches the batch run key, then delegates
//! duplicate and ambiguous submission handling to the application submission ledger.

use std::{error::Error as StdError, fmt};

use futures_util::future::BoxFuture;
use rustee_ai_batch::{
    AiBatchArtifactLedger, AiBatchArtifactLoader, AiBatchArtifactProcessor,
    AiBatchArtifactReconciler, AiBatchArtifactReference, AiBatchCatalog, AiBatchProvider,
    AiBatchReference, AiBatchSubmissionError, AiBatchSubmissionLedger, AiBatchSubmitter,
};
use rustee_jobs::{Job, JobContext, JobHandler};
use serde::{Deserialize, Serialize};

/// Stable, content-free durable payload that schedules one application batch reference.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AiBatchSubmissionJob {
    reference: AiBatchReference,
}

/// Stable, content-free durable payload that schedules one provider artifact reconciliation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AiBatchArtifactReconciliationJob {
    reference: AiBatchArtifactReference,
}

impl AiBatchArtifactReconciliationJob {
    /// Creates a job from a validated application-owned provider artifact reference.
    #[must_use]
    pub const fn new(reference: AiBatchArtifactReference) -> Self {
        Self { reference }
    }

    /// Returns the artifact reference resolved only after a worker begins handling the job.
    #[must_use]
    pub const fn reference(&self) -> &AiBatchArtifactReference {
        &self.reference
    }
}

impl Job for AiBatchArtifactReconciliationJob {
    const NAME: &'static str = "ai.batch.reconcile_artifact";
    const VERSION: u16 = 1;
}

impl AiBatchSubmissionJob {
    /// Creates a job from a validated application-owned batch reference.
    #[must_use]
    pub const fn new(reference: AiBatchReference) -> Self {
        Self { reference }
    }

    /// Returns the catalog reference resolved only after a worker begins handling the job.
    #[must_use]
    pub const fn reference(&self) -> &AiBatchReference {
        &self.reference
    }
}

impl Job for AiBatchSubmissionJob {
    const NAME: &'static str = "ai.batch.submit";
    const VERSION: u16 = 1;
}

/// Application boundary for one durable batch submission delivery.
pub trait AiBatchSubmissionRunner: Clone + Send + Sync + 'static {
    /// Runner-specific failure.
    type Error: StdError + Send + Sync + 'static;

    /// Resolves and submits one content-free batch reference after durable dispatch starts.
    fn submit_batch(
        &self,
        reference: AiBatchReference,
        context: JobContext,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

impl<C, L, P> AiBatchSubmissionRunner for AiBatchSubmitter<C, L, P>
where
    C: AiBatchCatalog,
    L: AiBatchSubmissionLedger,
    P: AiBatchProvider<C::Work>,
{
    type Error = AiBatchSubmissionError<C::Error, L::Error, P::Error>;

    fn submit_batch(
        &self,
        reference: AiBatchReference,
        _: JobContext,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let submitter = self.clone();
        Box::pin(async move { submitter.submit(reference).await.map(|_| ()) })
    }
}

/// Application boundary for one durable provider-artifact reconciliation delivery.
///
/// The runner authorizes the tenant-scoped reference, loads the selected provider artifact, and
/// applies domain effects idempotently for each result-row `custom_id` before acknowledging it.
pub trait AiBatchArtifactReconciliationRunner: Clone + Send + Sync + 'static {
    /// Runner-specific failure.
    type Error: StdError + Send + Sync + 'static;

    /// Reconciles one reference-only provider artifact after durable dispatch starts.
    fn reconcile_artifact(
        &self,
        reference: AiBatchArtifactReference,
        context: JobContext,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

impl<L, R, P> AiBatchArtifactReconciliationRunner for AiBatchArtifactReconciler<L, R, P>
where
    L: AiBatchArtifactLedger,
    R: AiBatchArtifactLoader,
    P: AiBatchArtifactProcessor<R::Artifact>,
{
    type Error = rustee_ai_batch::AiBatchArtifactReconciliationError<L::Error, R::Error, P::Error>;

    fn reconcile_artifact(
        &self,
        reference: AiBatchArtifactReference,
        _: JobContext,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let reconciler = self.clone();
        Box::pin(async move { reconciler.reconcile(reference).await.map(|_| ()) })
    }
}

/// [`JobHandler`] adapter that validates run-key binding before batch submission.
#[derive(Clone)]
pub struct AiBatchSubmissionHandler<R> {
    runner: R,
}

/// [`JobHandler`] adapter that validates artifact reconciliation-key binding before work starts.
#[derive(Clone)]
pub struct AiBatchArtifactReconciliationHandler<R> {
    runner: R,
}

impl<R> AiBatchArtifactReconciliationHandler<R> {
    /// Creates a typed handler around an application runner or [`AiBatchArtifactReconciler`].
    #[must_use]
    pub const fn new(runner: R) -> Self {
        Self { runner }
    }
}

impl<R> AiBatchSubmissionHandler<R> {
    /// Creates a typed handler around an application runner or [`AiBatchSubmitter`].
    #[must_use]
    pub const fn new(runner: R) -> Self {
        Self { runner }
    }
}

impl<R> fmt::Debug for AiBatchSubmissionHandler<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiBatchSubmissionHandler")
            .field("runner", &"[APPLICATION-OWNED]")
            .finish()
    }
}

impl<R> fmt::Debug for AiBatchArtifactReconciliationHandler<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiBatchArtifactReconciliationHandler")
            .field("runner", &"[APPLICATION-OWNED]")
            .finish()
    }
}

impl<R> JobHandler<AiBatchSubmissionJob> for AiBatchSubmissionHandler<R>
where
    R: AiBatchSubmissionRunner,
{
    type Error = AiBatchSubmissionJobError<R::Error>;

    fn handle(
        &self,
        payload: AiBatchSubmissionJob,
        context: JobContext,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let reference = payload.reference;
        if context.idempotency_key() != Some(reference.run_key()) {
            return Box::pin(async { Err(AiBatchSubmissionJobError::RunKeyMismatch) });
        }
        let future = self.runner.submit_batch(reference, context);
        Box::pin(async move {
            future
                .await
                .map_err(|source| AiBatchSubmissionJobError::Runner { source })
        })
    }
}

impl<R> JobHandler<AiBatchArtifactReconciliationJob> for AiBatchArtifactReconciliationHandler<R>
where
    R: AiBatchArtifactReconciliationRunner,
{
    type Error = AiBatchArtifactReconciliationJobError<R::Error>;

    fn handle(
        &self,
        payload: AiBatchArtifactReconciliationJob,
        context: JobContext,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let reference = payload.reference;
        if context.idempotency_key() != Some(reference.reconciliation_key()) {
            return Box::pin(async { Err(AiBatchArtifactReconciliationJobError::KeyMismatch) });
        }
        let future = self.runner.reconcile_artifact(reference, context);
        Box::pin(async move {
            future
                .await
                .map_err(|source| AiBatchArtifactReconciliationJobError::Runner { source })
        })
    }
}

/// Sanitized durable batch-job failure.
#[derive(Debug, thiserror::Error)]
pub enum AiBatchSubmissionJobError<RunnerError> {
    /// The durable envelope's idempotency key was absent or did not bind the batch run key.
    #[error("AI batch job idempotency key does not match the batch run key")]
    RunKeyMismatch,
    /// The application batch runner did not accept the delivery.
    #[error("AI batch job runner failed")]
    Runner {
        /// Runner failure for the provider's retry or dead-letter policy.
        #[source]
        source: RunnerError,
    },
}

/// Sanitized durable artifact-reconciliation job failure.
#[derive(Debug, thiserror::Error)]
pub enum AiBatchArtifactReconciliationJobError<RunnerError> {
    /// The durable envelope's idempotency key was absent or did not bind the artifact key.
    #[error("AI batch artifact job idempotency key does not match the reconciliation key")]
    KeyMismatch,
    /// The application artifact runner did not accept the delivery.
    #[error("AI batch artifact job runner failed")]
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
    use rustee_ai_batch::{
        AiBatchArtifactKind, AiBatchArtifactReference, AiBatchCatalog, AiBatchProvider,
        AiBatchReceipt, AiBatchReference, InMemoryAiBatchLedger,
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
}
