//! Durable AI batch-artifact reconciliation job contracts and dispatch.

use std::{error::Error as StdError, fmt};

use futures_util::future::BoxFuture;
use rustee_ai_batch::{
    AiBatchArtifactLedger, AiBatchArtifactLoader, AiBatchArtifactProcessor,
    AiBatchArtifactReconciler, AiBatchArtifactReference,
};
use rustee_jobs::{Job, JobContext, JobHandler};
use serde::{Deserialize, Serialize};

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

impl<R> fmt::Debug for AiBatchArtifactReconciliationHandler<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiBatchArtifactReconciliationHandler")
            .field("runner", &"[APPLICATION-OWNED]")
            .finish()
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

/// Sanitized durable artifact-reconciliation job failure.
#[derive(thiserror::Error)]
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

impl<RunnerError> fmt::Debug for AiBatchArtifactReconciliationJobError<RunnerError> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeyMismatch => {
                formatter.write_str("AiBatchArtifactReconciliationJobError::KeyMismatch")
            }
            Self::Runner { .. } => {
                formatter.write_str("AiBatchArtifactReconciliationJobError::Runner")
            }
        }
    }
}
