//! At-most-once batch submission coordination.

use std::fmt;

mod contracts;

pub(crate) use contracts::valid_identifier;
pub use contracts::{
    AiBatchCatalog, AiBatchConfigError, AiBatchProvider, AiBatchReceipt, AiBatchReference,
    AiBatchReservation, AiBatchSubmissionLedger, MAX_BATCH_IDENTIFIER_BYTES,
};

/// Visible outcome of one explicit batch submission request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AiBatchSubmission {
    receipt: AiBatchReceipt,
    disposition: AiBatchSubmissionDisposition,
}

impl AiBatchSubmission {
    /// Returns the provider receipt for polling or reconciliation.
    #[must_use]
    pub const fn receipt(&self) -> &AiBatchReceipt {
        &self.receipt
    }

    /// Returns whether this call submitted or reused an existing receipt.
    #[must_use]
    pub const fn disposition(&self) -> AiBatchSubmissionDisposition {
        self.disposition
    }
}

/// Whether the current call made a new provider submission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AiBatchSubmissionDisposition {
    /// The ledger reserved, the catalog loaded, and the provider accepted one submission.
    Submitted,
    /// A prior durable receipt was reused without loading the catalog or contacting the provider.
    ExistingSubmission,
}

/// Coordinator that combines catalog load, atomic reservation, provider submit, and receipt record.
#[derive(Clone)]
pub struct AiBatchSubmitter<C, L, P> {
    catalog: C,
    ledger: L,
    provider: P,
}

impl<C, L, P> AiBatchSubmitter<C, L, P> {
    /// Creates a batch submission coordinator from explicit application boundaries.
    #[must_use]
    pub const fn new(catalog: C, ledger: L, provider: P) -> Self {
        Self {
            catalog,
            ledger,
            provider,
        }
    }
}

impl<C, L, P> fmt::Debug for AiBatchSubmitter<C, L, P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiBatchSubmitter")
            .field("catalog", &"[APPLICATION-OWNED]")
            .field("ledger", &"[APPLICATION-OWNED]")
            .field("provider", &"[APPLICATION-OWNED]")
            .finish()
    }
}

impl<C, L, P> AiBatchSubmitter<C, L, P>
where
    C: AiBatchCatalog,
    L: AiBatchSubmissionLedger,
    P: AiBatchProvider<C::Work>,
{
    /// Submits one application catalog entry at most once for its stable run key.
    ///
    /// # Errors
    ///
    /// Returns a pending ambiguity without reloading or resubmitting. Catalog, provider, and
    /// receipt-record failures do not trigger retries; callers reconcile or start an explicitly
    /// different application run according to their provider billing policy.
    pub async fn submit(
        &self,
        reference: AiBatchReference,
    ) -> Result<AiBatchSubmission, AiBatchSubmissionError<C::Error, L::Error, P::Error>> {
        match self
            .ledger
            .reserve(reference.clone())
            .await
            .map_err(|source| AiBatchSubmissionError::LedgerReserve { source })?
        {
            AiBatchReservation::Submitted(receipt) => {
                return Ok(AiBatchSubmission {
                    receipt,
                    disposition: AiBatchSubmissionDisposition::ExistingSubmission,
                });
            }
            AiBatchReservation::Pending => {
                return Err(AiBatchSubmissionError::Pending { reference });
            }
            AiBatchReservation::Reserved => {}
        }

        let work = self
            .catalog
            .load(reference.clone())
            .await
            .map_err(|source| AiBatchSubmissionError::Catalog { source })?;
        let receipt = self
            .provider
            .submit(reference.clone(), work)
            .await
            .map_err(|source| AiBatchSubmissionError::Provider { source })?;
        self.ledger
            .record_submission(reference, receipt.clone())
            .await
            .map_err(|source| AiBatchSubmissionError::LedgerRecord {
                receipt: receipt.clone(),
                source,
            })?;
        Ok(AiBatchSubmission {
            receipt,
            disposition: AiBatchSubmissionDisposition::Submitted,
        })
    }
}

/// One sanitized submission failure.
#[derive(thiserror::Error)]
pub enum AiBatchSubmissionError<CatalogError, LedgerError, ProviderError> {
    /// Atomic reservation failed before catalog loading or provider contact.
    #[error("AI batch ledger reservation failed")]
    LedgerReserve {
        /// Application ledger failure.
        #[source]
        source: LedgerError,
    },
    /// An ambiguous prior attempt is retained for application reconciliation.
    #[error("AI batch submission is pending reconciliation")]
    Pending {
        /// Safe reference that must not be automatically resubmitted.
        reference: AiBatchReference,
    },
    /// Catalog load failed after reservation and before provider contact.
    #[error("AI batch catalog load failed")]
    Catalog {
        /// Application catalog failure.
        #[source]
        source: CatalogError,
    },
    /// Provider submission failed after reservation; its outcome is treated as ambiguous.
    #[error("AI batch provider submission failed")]
    Provider {
        /// Provider adapter failure.
        #[source]
        source: ProviderError,
    },
    /// Provider accepted a batch but receipt persistence failed; reconcile with this safe receipt.
    #[error("AI batch receipt recording failed")]
    LedgerRecord {
        /// Safe provider receipt for application reconciliation.
        receipt: AiBatchReceipt,
        /// Application ledger failure.
        #[source]
        source: LedgerError,
    },
}

impl<CatalogError, LedgerError, ProviderError> fmt::Debug
    for AiBatchSubmissionError<CatalogError, LedgerError, ProviderError>
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LedgerReserve { .. } => {
                formatter.write_str("AiBatchSubmissionError::LedgerReserve")
            }
            Self::Pending { .. } => formatter.write_str("AiBatchSubmissionError::Pending"),
            Self::Catalog { .. } => formatter.write_str("AiBatchSubmissionError::Catalog"),
            Self::Provider { .. } => formatter.write_str("AiBatchSubmissionError::Provider"),
            Self::LedgerRecord { .. } => {
                formatter.write_str("AiBatchSubmissionError::LedgerRecord")
            }
        }
    }
}

impl<CatalogError, LedgerError, ProviderError>
    AiBatchSubmissionError<CatalogError, LedgerError, ProviderError>
{
    /// Returns the safe reference when an ambiguous prior reservation blocks submission.
    #[must_use]
    pub fn pending_reference(&self) -> Option<&AiBatchReference> {
        match self {
            Self::Pending { reference } => Some(reference),
            _ => None,
        }
    }

    /// Returns the safe receipt when only receipt persistence requires reconciliation.
    #[must_use]
    pub fn unrecorded_receipt(&self) -> Option<&AiBatchReceipt> {
        match self {
            Self::LedgerRecord { receipt, .. } => Some(receipt),
            _ => None,
        }
    }
}
