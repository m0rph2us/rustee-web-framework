//! Durable AI batch-submission job contracts and dispatch.

use std::{error::Error as StdError, fmt};

use futures_util::future::BoxFuture;
use rustee_ai_batch::{
    AiBatchCatalog, AiBatchProvider, AiBatchReference, AiBatchSubmissionError,
    AiBatchSubmissionLedger, AiBatchSubmitter,
};
use rustee_jobs::{Job, JobContext, JobHandler};
use serde::{Deserialize, Serialize};

/// Stable, content-free durable payload that schedules one application batch reference.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AiBatchSubmissionJob {
    reference: AiBatchReference,
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

/// [`JobHandler`] adapter that validates run-key binding before batch submission.
#[derive(Clone)]
pub struct AiBatchSubmissionHandler<R> {
    runner: R,
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

/// Sanitized durable batch-submission job failure.
#[derive(thiserror::Error)]
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

impl<RunnerError> fmt::Debug for AiBatchSubmissionJobError<RunnerError> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RunKeyMismatch => {
                formatter.write_str("AiBatchSubmissionJobError::RunKeyMismatch")
            }
            Self::Runner { .. } => formatter.write_str("AiBatchSubmissionJobError::Runner"),
        }
    }
}
