//! Durable reference-backed evaluation reservation, execution, and completion coordination.

use std::{error::Error as StdError, fmt};

use futures_util::future::BoxFuture;

use crate::{
    model::{AiEvaluationGrader, AiEvaluationReference, AiEvaluationSuite},
    runner::{AiEvaluationExecutor, AiEvaluationReport, AiEvaluationRunError, AiEvaluationRunner},
};

/// Application-owned authorized loader for one evaluation catalog entry.
///
/// The returned suite may contain raw prompts, expected targets, or private grader metadata. It
/// stays in the trusted worker process and is never serialized by this crate's reference or run
/// ledger APIs.
pub trait AiEvaluationCatalog<T>: Clone + Send + Sync + 'static
where
    T: Send + Sync + 'static,
{
    /// Catalog lookup or authorization failure.
    type Error: StdError + Send + Sync + 'static;

    /// Loads the exact tenant-scoped evaluation suite after durable dispatch begins.
    fn load(
        &self,
        reference: AiEvaluationReference,
    ) -> BoxFuture<'static, Result<AiEvaluationSuite<T>, Self::Error>>;
}

/// State returned when one scoped evaluation run is reserved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AiEvaluationRunReservation {
    /// This caller owns the first evaluation attempt for the exact reference.
    Reserved,
    /// A prior evaluation completed and must not be rerun automatically.
    Completed,
    /// A prior attempt may have loaded a catalog, called a provider, or graded a result without a
    /// durable completion record.
    Pending,
}

/// Application-owned durable idempotency boundary for one evaluation run.
///
/// A production ledger must atomically reserve a scoped run key and survive worker restarts.
/// There is intentionally no release-and-retry operation: failures after reservation remain
/// pending until application owners review provider usage, grading policy, and any report sink.
pub trait AiEvaluationRunLedger: Clone + Send + Sync + 'static {
    /// Ledger-specific failure.
    type Error: StdError + Send + Sync + 'static;

    /// Atomically reserves one run, returns a prior completion, or exposes ambiguity.
    fn reserve(
        &self,
        reference: AiEvaluationReference,
    ) -> BoxFuture<'static, Result<AiEvaluationRunReservation, Self::Error>>;

    /// Records that the application completed and accepted the evaluation run.
    fn record_completed(
        &self,
        reference: AiEvaluationReference,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// Result of one reference-backed evaluation submission.
pub enum AiEvaluationSubmission {
    /// This call loaded a catalog, ran every case, and durably recorded completion.
    Completed(AiEvaluationReport),
    /// A prior durable completion blocked catalog loading and model execution.
    ExistingCompletion,
}

impl fmt::Debug for AiEvaluationSubmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Completed(report) => formatter.debug_tuple("Completed").field(report).finish(),
            Self::ExistingCompletion => formatter.write_str("ExistingCompletion"),
        }
    }
}

/// Coordinator for catalog loading, atomic run reservation, fail-fast evaluation, and completion.
#[derive(Clone)]
pub struct AiEvaluationSubmitter<C, L, E, G> {
    catalog: C,
    ledger: L,
    runner: AiEvaluationRunner<E, G>,
}

impl<C, L, E, G> AiEvaluationSubmitter<C, L, E, G> {
    /// Creates a reference-backed coordinator from explicit application boundaries.
    #[must_use]
    pub const fn new(catalog: C, ledger: L, runner: AiEvaluationRunner<E, G>) -> Self {
        Self {
            catalog,
            ledger,
            runner,
        }
    }
}

impl<C, L, E, G> fmt::Debug for AiEvaluationSubmitter<C, L, E, G> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiEvaluationSubmitter")
            .field("catalog", &"[APPLICATION-OWNED]")
            .field("ledger", &"[APPLICATION-OWNED]")
            .field("runner", &"[APPLICATION-OWNED]")
            .finish()
    }
}

impl<C, L, E, G> AiEvaluationSubmitter<C, L, E, G>
where
    L: AiEvaluationRunLedger,
    E: AiEvaluationExecutor,
{
    /// Runs one application catalog entry at most once for its stable scoped run key.
    ///
    /// # Errors
    ///
    /// A catalog, executor, grader, or completion-record failure leaves the reservation pending.
    /// The coordinator never reloads the suite or makes another provider call automatically;
    /// application owners must reconcile and choose a new explicit run key when appropriate.
    pub async fn submit<T>(
        &self,
        reference: AiEvaluationReference,
    ) -> Result<
        AiEvaluationSubmission,
        AiEvaluationSubmissionError<C::Error, L::Error, E::Error, G::Error>,
    >
    where
        C: AiEvaluationCatalog<T>,
        G: AiEvaluationGrader<T>,
        T: Send + Sync + 'static,
    {
        match self
            .ledger
            .reserve(reference.clone())
            .await
            .map_err(|source| AiEvaluationSubmissionError::LedgerReserve { source })?
        {
            AiEvaluationRunReservation::Completed => {
                return Ok(AiEvaluationSubmission::ExistingCompletion);
            }
            AiEvaluationRunReservation::Pending => {
                return Err(AiEvaluationSubmissionError::Pending { reference });
            }
            AiEvaluationRunReservation::Reserved => {}
        }

        let suite = self
            .catalog
            .load(reference.clone())
            .await
            .map_err(|source| AiEvaluationSubmissionError::Catalog { source })?;
        let report = self
            .runner
            .run(&suite)
            .await
            .map_err(|source| AiEvaluationSubmissionError::Run { source })?;
        self.ledger
            .record_completed(reference)
            .await
            .map_err(|source| AiEvaluationSubmissionError::LedgerRecord { source })?;
        Ok(AiEvaluationSubmission::Completed(report))
    }
}

/// One sanitized reference-backed evaluation failure.
///
/// Its display and debug forms retain only the failure category. Generic application sources stay
/// available through [`std::error::Error::source`] without being rendered into routine diagnostics.
#[derive(thiserror::Error)]
pub enum AiEvaluationSubmissionError<CatalogError, LedgerError, ExecutorError, GraderError> {
    /// Atomic run reservation failed before catalog loading or model execution.
    #[error("AI evaluation run ledger reservation failed")]
    LedgerReserve {
        /// Application ledger failure.
        #[source]
        source: LedgerError,
    },
    /// A prior attempt is ambiguous and must not be rerun automatically.
    #[error("AI evaluation run is pending reconciliation")]
    Pending {
        /// Content-free reference that requires application reconciliation.
        reference: AiEvaluationReference,
    },
    /// The application catalog failed after reservation and before model execution.
    #[error("AI evaluation catalog load failed")]
    Catalog {
        /// Application catalog failure.
        #[source]
        source: CatalogError,
    },
    /// The executor or trusted grader failed during the sequential evaluation run.
    #[error("AI evaluation run failed")]
    Run {
        /// Sanitized runner failure.
        #[source]
        source: AiEvaluationRunError<ExecutorError, GraderError>,
    },
    /// The run completed but its durable completion record did not persist.
    #[error("AI evaluation completion recording failed")]
    LedgerRecord {
        /// Application ledger failure.
        #[source]
        source: LedgerError,
    },
}

impl<CatalogError, LedgerError, ExecutorError, GraderError> fmt::Debug
    for AiEvaluationSubmissionError<CatalogError, LedgerError, ExecutorError, GraderError>
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LedgerReserve { .. } => {
                formatter.write_str("AiEvaluationSubmissionError::LedgerReserve")
            }
            Self::Pending { .. } => formatter.write_str("AiEvaluationSubmissionError::Pending"),
            Self::Catalog { .. } => formatter.write_str("AiEvaluationSubmissionError::Catalog"),
            Self::Run { .. } => formatter.write_str("AiEvaluationSubmissionError::Run"),
            Self::LedgerRecord { .. } => {
                formatter.write_str("AiEvaluationSubmissionError::LedgerRecord")
            }
        }
    }
}

impl<CatalogError, LedgerError, ExecutorError, GraderError>
    AiEvaluationSubmissionError<CatalogError, LedgerError, ExecutorError, GraderError>
{
    /// Returns the safe reference when an ambiguous prior reservation blocks execution.
    #[must_use]
    pub fn pending_reference(&self) -> Option<&AiEvaluationReference> {
        match self {
            Self::Pending { reference } => Some(reference),
            _ => None,
        }
    }
}
