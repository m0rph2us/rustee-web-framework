//! Ephemeral artifact loading, processing, and explicit reconciliation orchestration.

use std::{error::Error as StdError, fmt};

use futures_util::future::BoxFuture;

use super::{AiBatchArtifactLedger, AiBatchArtifactReference, AiBatchArtifactReservation};

/// Application boundary that authorizes and loads one selected provider artifact.
///
/// The returned value may contain raw provider bytes, parsed rows, or a streaming reader, but it
/// never enters a Rustee durable payload or framework log.
pub trait AiBatchArtifactLoader: Clone + Send + Sync + 'static {
    /// Application-owned, short-lived artifact value.
    type Artifact: Send + 'static;
    /// Artifact access, authorization, or decoding failure.
    type Error: StdError + Send + Sync + 'static;

    /// Authorizes and loads the exact tenant-scoped artifact after durable dispatch begins.
    fn load_artifact(
        &self,
        reference: AiBatchArtifactReference,
    ) -> BoxFuture<'static, Result<Self::Artifact, Self::Error>>;
}

/// Application boundary that applies one loaded artifact's domain effects.
///
/// Implementations must make each result-row side effect idempotent using an application-owned
/// domain key and the provider row's stable `custom_id`. A whole-artifact retry may occur after an
/// interrupted process even though the framework deliberately does not replay automatically.
pub trait AiBatchArtifactProcessor<Artifact>: Clone + Send + Sync + 'static
where
    Artifact: Send + 'static,
{
    /// Application result-validation or persistence failure.
    type Error: StdError + Send + Sync + 'static;

    /// Validates and applies the artifact's intended domain effects.
    fn process_artifact(
        &self,
        reference: AiBatchArtifactReference,
        artifact: Artifact,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// Visible outcome of one explicit artifact reconciliation request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AiBatchArtifactReconciliationDisposition {
    /// The artifact was loaded, processed, and marked reconciled during this call.
    Reconciled,
    /// A prior durable reconciliation was reused without artifact access.
    ExistingReconciliation,
}

/// Coordinator for application-owned artifact loading, processing, and completion recording.
#[derive(Clone)]
pub struct AiBatchArtifactReconciler<L, R, P> {
    ledger: L,
    loader: R,
    processor: P,
}

impl<L, R, P> AiBatchArtifactReconciler<L, R, P> {
    /// Creates an explicit artifact reconciliation coordinator.
    #[must_use]
    pub const fn new(ledger: L, loader: R, processor: P) -> Self {
        Self {
            ledger,
            loader,
            processor,
        }
    }
}

impl<L, R, P> fmt::Debug for AiBatchArtifactReconciler<L, R, P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiBatchArtifactReconciler")
            .field("ledger", &"[APPLICATION-OWNED]")
            .field("loader", &"[APPLICATION-OWNED]")
            .field("processor", &"[APPLICATION-OWNED]")
            .finish()
    }
}

impl<L, R, P> AiBatchArtifactReconciler<L, R, P>
where
    L: AiBatchArtifactLedger,
    R: AiBatchArtifactLoader,
    P: AiBatchArtifactProcessor<R::Artifact>,
{
    /// Reconciles one provider artifact without automatic replay after ambiguity.
    ///
    /// # Errors
    ///
    /// A loader, processor, or record failure leaves the exact artifact reservation pending. The
    /// coordinator never fetches it again or reapplies model outputs automatically; application
    /// reconciliation must inspect its per-row idempotency ledger and explicitly choose a retry
    /// with a new reconciliation key.
    pub async fn reconcile(
        &self,
        reference: AiBatchArtifactReference,
    ) -> Result<
        AiBatchArtifactReconciliationDisposition,
        AiBatchArtifactReconciliationError<L::Error, R::Error, P::Error>,
    > {
        match self
            .ledger
            .reserve(reference.clone())
            .await
            .map_err(|source| AiBatchArtifactReconciliationError::LedgerReserve { source })?
        {
            AiBatchArtifactReservation::Reconciled => {
                return Ok(AiBatchArtifactReconciliationDisposition::ExistingReconciliation);
            }
            AiBatchArtifactReservation::Pending => {
                return Err(AiBatchArtifactReconciliationError::Pending { reference });
            }
            AiBatchArtifactReservation::Reserved => {}
        }

        let artifact = self
            .loader
            .load_artifact(reference.clone())
            .await
            .map_err(|source| AiBatchArtifactReconciliationError::Loader { source })?;
        self.processor
            .process_artifact(reference.clone(), artifact)
            .await
            .map_err(|source| AiBatchArtifactReconciliationError::Processor { source })?;
        self.ledger
            .record_reconciled(reference)
            .await
            .map_err(|source| AiBatchArtifactReconciliationError::LedgerRecord { source })?;
        Ok(AiBatchArtifactReconciliationDisposition::Reconciled)
    }
}

/// Sanitized artifact reconciliation failure.
#[derive(thiserror::Error)]
pub enum AiBatchArtifactReconciliationError<LedgerError, LoaderError, ProcessorError> {
    /// Atomic artifact reservation failed before provider artifact access.
    #[error("AI batch artifact ledger reservation failed")]
    LedgerReserve {
        /// Application ledger failure.
        #[source]
        source: LedgerError,
    },
    /// A prior attempt has an ambiguous outcome that the application must reconcile explicitly.
    #[error("AI batch artifact reconciliation is pending")]
    Pending {
        /// Safe artifact reference that must not be automatically replayed.
        reference: AiBatchArtifactReference,
    },
    /// Artifact access or structural parsing failed after reservation.
    #[error("AI batch artifact loading failed")]
    Loader {
        /// Application loader failure.
        #[source]
        source: LoaderError,
    },
    /// Domain result validation or persistence failed after the artifact was loaded.
    #[error("AI batch artifact processing failed")]
    Processor {
        /// Application processor failure.
        #[source]
        source: ProcessorError,
    },
    /// Domain effects may be present but artifact completion could not be durably recorded.
    #[error("AI batch artifact reconciliation recording failed")]
    LedgerRecord {
        /// Application ledger failure.
        #[source]
        source: LedgerError,
    },
}

impl<LedgerError, LoaderError, ProcessorError> fmt::Debug
    for AiBatchArtifactReconciliationError<LedgerError, LoaderError, ProcessorError>
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LedgerReserve { .. } => {
                formatter.write_str("AiBatchArtifactReconciliationError::LedgerReserve")
            }
            Self::Pending { .. } => {
                formatter.write_str("AiBatchArtifactReconciliationError::Pending")
            }
            Self::Loader { .. } => {
                formatter.write_str("AiBatchArtifactReconciliationError::Loader")
            }
            Self::Processor { .. } => {
                formatter.write_str("AiBatchArtifactReconciliationError::Processor")
            }
            Self::LedgerRecord { .. } => {
                formatter.write_str("AiBatchArtifactReconciliationError::LedgerRecord")
            }
        }
    }
}

impl<LedgerError, LoaderError, ProcessorError>
    AiBatchArtifactReconciliationError<LedgerError, LoaderError, ProcessorError>
{
    /// Returns the safe artifact reference when a prior attempt blocks automatic replay.
    #[must_use]
    pub fn pending_reference(&self) -> Option<&AiBatchArtifactReference> {
        match self {
            Self::Pending { reference } => Some(reference),
            _ => None,
        }
    }
}
