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
///
/// Its display and debug forms retain only the failure category. The underlying runner error stays
/// available through [`std::error::Error::source`] for trusted retry or dead-letter handling.
#[derive(thiserror::Error)]
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

impl<RunnerError> fmt::Debug for AiEvaluationJobError<RunnerError> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RunKeyMismatch => formatter.write_str("AiEvaluationJobError::RunKeyMismatch"),
            Self::Runner { .. } => formatter.write_str("AiEvaluationJobError::Runner"),
        }
    }
}

#[cfg(test)]
mod tests;
