//! Explicit control-plane contracts for provider asynchronous AI batches.
//!
//! A durable delivery contains an [`AiBatchReference`] only. The application catalog resolves raw
//! prompts, expected answers, documents, and provider-specific work after an authorized worker
//! starts. The submission ledger preserves an ambiguous provider submission as pending, so this
//! crate never retries or resubmits a batch automatically.

use std::{
    collections::BTreeMap,
    error::Error as StdError,
    fmt,
    sync::{Arc, Mutex},
};

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};

const MAX_IDENTIFIER_BYTES: usize = 128;
const MAX_IN_MEMORY_RECORDS: usize = 10_000;

/// Content-free reference to an application-owned batch catalog entry.
///
/// `scope` must be an opaque tenant and policy boundary. `catalog_id` locates the application
/// record to load after authorization. `run_key` is the stable idempotency key shared with the
/// provider and durable worker delivery; none of these values may contain request content.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct AiBatchReference {
    scope: String,
    catalog_id: String,
    run_key: String,
}

impl<'de> Deserialize<'de> for AiBatchReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawAiBatchReference {
            scope: String,
            catalog_id: String,
            run_key: String,
        }

        let raw = RawAiBatchReference::deserialize(deserializer)?;
        Self::new(raw.scope, raw.catalog_id, raw.run_key).map_err(serde::de::Error::custom)
    }
}

impl AiBatchReference {
    /// Creates a bounded, content-free batch reference.
    ///
    /// # Errors
    ///
    /// Returns [`AiBatchConfigError::InvalidIdentifier`] for unsafe identifiers.
    pub fn new(
        scope: impl Into<String>,
        catalog_id: impl Into<String>,
        run_key: impl Into<String>,
    ) -> Result<Self, AiBatchConfigError> {
        let reference = Self {
            scope: scope.into(),
            catalog_id: catalog_id.into(),
            run_key: run_key.into(),
        };
        if !valid_identifier(&reference.scope)
            || !valid_identifier(&reference.catalog_id)
            || !valid_identifier(&reference.run_key)
        {
            return Err(AiBatchConfigError::InvalidIdentifier);
        }
        Ok(reference)
    }

    /// Returns the application-owned tenant/policy isolation scope.
    #[must_use]
    pub fn scope(&self) -> &str {
        &self.scope
    }

    /// Returns the application catalog identifier without loading its contents.
    #[must_use]
    pub fn catalog_id(&self) -> &str {
        &self.catalog_id
    }

    /// Returns the stable provider and delivery idempotency key.
    #[must_use]
    pub fn run_key(&self) -> &str {
        &self.run_key
    }
}

/// Safe provider receipt retained after one accepted asynchronous batch submission.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AiBatchReceipt {
    provider_batch_id: String,
}

impl<'de> Deserialize<'de> for AiBatchReceipt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawAiBatchReceipt {
            provider_batch_id: String,
        }

        let raw = RawAiBatchReceipt::deserialize(deserializer)?;
        Self::new(raw.provider_batch_id).map_err(serde::de::Error::custom)
    }
}

impl AiBatchReceipt {
    /// Creates a safe, bounded provider batch identifier.
    ///
    /// # Errors
    ///
    /// Returns [`AiBatchConfigError::InvalidIdentifier`] when the adapter emits unsafe metadata.
    pub fn new(provider_batch_id: impl Into<String>) -> Result<Self, AiBatchConfigError> {
        let provider_batch_id = provider_batch_id.into();
        if !valid_identifier(&provider_batch_id) {
            return Err(AiBatchConfigError::InvalidIdentifier);
        }
        Ok(Self { provider_batch_id })
    }

    /// Returns the provider's safe batch identifier for application polling/reconciliation.
    #[must_use]
    pub fn provider_batch_id(&self) -> &str {
        &self.provider_batch_id
    }
}

/// Provider-owned artifact category selected for application reconciliation.
///
/// This describes only the provider file's role. It does not imply that the framework downloads,
/// parses, retains, or deletes the file.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AiBatchArtifactKind {
    /// The provider's completed or partial successful-result artifact.
    Output,
    /// The provider's per-row error artifact.
    Error,
}

/// Content-free reference to one provider batch artifact that an application may reconcile.
///
/// The durable `reconciliation_key` must bind one exact worker delivery attempt. `provider_file_id`
/// is a validated opaque provider identifier, never an artifact body. Applications authorize the
/// tenant-scoped batch reference before downloading the selected file and must persist any
/// row-level side effects idempotently, normally by the provider `custom_id` plus a domain key.
/// After an ambiguous attempt, a reviewed explicit retry uses a new reconciliation key rather than
/// releasing or replaying this reference.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct AiBatchArtifactReference {
    batch: AiBatchReference,
    kind: AiBatchArtifactKind,
    provider_file_id: String,
    reconciliation_key: String,
}

impl<'de> Deserialize<'de> for AiBatchArtifactReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawAiBatchArtifactReference {
            batch: AiBatchReference,
            kind: AiBatchArtifactKind,
            provider_file_id: String,
            reconciliation_key: String,
        }

        let raw = RawAiBatchArtifactReference::deserialize(deserializer)?;
        Self::new(
            raw.batch,
            raw.kind,
            raw.provider_file_id,
            raw.reconciliation_key,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl AiBatchArtifactReference {
    /// Creates a bounded, content-free reference to an output or error artifact.
    ///
    /// # Errors
    ///
    /// Returns [`AiBatchConfigError::InvalidIdentifier`] for unsafe file or reconciliation keys.
    pub fn new(
        batch: AiBatchReference,
        kind: AiBatchArtifactKind,
        provider_file_id: impl Into<String>,
        reconciliation_key: impl Into<String>,
    ) -> Result<Self, AiBatchConfigError> {
        let reference = Self {
            batch,
            kind,
            provider_file_id: provider_file_id.into(),
            reconciliation_key: reconciliation_key.into(),
        };
        if !valid_identifier(&reference.provider_file_id)
            || !valid_identifier(&reference.reconciliation_key)
        {
            return Err(AiBatchConfigError::InvalidIdentifier);
        }
        Ok(reference)
    }

    /// Returns the tenant-scoped batch reference that must be authorized before access.
    #[must_use]
    pub const fn batch(&self) -> &AiBatchReference {
        &self.batch
    }

    /// Returns the provider artifact role selected by the application.
    #[must_use]
    pub const fn kind(&self) -> AiBatchArtifactKind {
        self.kind
    }

    /// Returns the opaque provider file identifier, never its contents.
    #[must_use]
    pub fn provider_file_id(&self) -> &str {
        &self.provider_file_id
    }

    /// Returns the stable durable-delivery idempotency key for this reconciliation attempt.
    #[must_use]
    pub fn reconciliation_key(&self) -> &str {
        &self.reconciliation_key
    }
}

/// Invalid batch reference, receipt, or in-memory ledger configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AiBatchConfigError {
    /// Public identifiers may not contain raw content, whitespace, URL syntax, or backend keys.
    #[error(
        "AI batch identifiers must use bounded ASCII letters, digits, underscore, hyphen, or dot"
    )]
    InvalidIdentifier,
    /// The in-memory ledger intentionally has a bounded development/test capacity.
    #[error("AI in-memory batch ledger capacity must be between one and 10,000 records")]
    InvalidInMemoryCapacity,
}

/// State returned when one run key is reserved for provider submission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AiBatchReservation {
    /// This caller owns the first submission attempt for the reference.
    Reserved,
    /// A prior submission is durably recorded; the caller must reuse its receipt.
    Submitted(AiBatchReceipt),
    /// A prior caller may have reached the provider but lacks a durable receipt.
    Pending,
}

/// Application-owned durable idempotency boundary for batch submission.
///
/// A production ledger must survive worker restarts. It must make `reserve` atomic per exact
/// [`AiBatchReference`] and retain `Pending` after a provider error until application
/// reconciliation decides what happened. This trait has no release-and-retry operation.
pub trait AiBatchSubmissionLedger: Clone + Send + Sync + 'static {
    /// Ledger-specific failure.
    type Error: StdError + Send + Sync + 'static;

    /// Atomically reserves one submission, returns its prior receipt, or exposes ambiguity.
    fn reserve(
        &self,
        reference: AiBatchReference,
    ) -> BoxFuture<'static, Result<AiBatchReservation, Self::Error>>;

    /// Records an accepted provider receipt for an existing reservation.
    fn record_submission(
        &self,
        reference: AiBatchReference,
        receipt: AiBatchReceipt,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// Application-owned catalog loader for raw provider batch work.
///
/// The work type can contain prompts, expected answers, files, or provider request bodies, but it
/// remains in the trusted application process and is never serialized by this crate.
pub trait AiBatchCatalog: Clone + Send + Sync + 'static {
    /// Application-owned provider work loaded after authorization.
    type Work: Send + 'static;
    /// Catalog lookup failure.
    type Error: StdError + Send + Sync + 'static;

    /// Loads work for the exact tenant-scoped reference.
    fn load(
        &self,
        reference: AiBatchReference,
    ) -> BoxFuture<'static, Result<Self::Work, Self::Error>>;
}

/// Provider-specific asynchronous batch submission adapter.
///
/// The adapter owns provider request codec, limits, auth, cancellation, polling, partial-result
/// retrieval, billing interpretation, and any native idempotency header. It must bind `run_key`
/// when the provider supports one and return only a safe receipt identifier here.
pub trait AiBatchProvider<Work>: Clone + Send + Sync + 'static
where
    Work: Send + 'static,
{
    /// Provider submission failure.
    type Error: StdError + Send + Sync + 'static;

    /// Submits exactly one loaded batch without automatic retry.
    fn submit(
        &self,
        reference: AiBatchReference,
        work: Work,
    ) -> BoxFuture<'static, Result<AiBatchReceipt, Self::Error>>;
}

/// State returned when one artifact reference is reserved for reconciliation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AiBatchArtifactReservation {
    /// This caller owns the first reconciliation attempt for the exact artifact.
    Reserved,
    /// A prior reconciliation was durably recorded; no artifact reload is allowed.
    Reconciled,
    /// A prior attempt may have downloaded or applied some rows but lacks a final record.
    Pending,
}

/// Application-owned durable idempotency boundary for whole-artifact reconciliation.
///
/// A production ledger must survive worker restarts and reserve an exact
/// [`AiBatchArtifactReference`] atomically. It intentionally has no release-and-retry operation:
/// after a loader, processor, or record failure, applications reconcile partial row effects using
/// their own `custom_id`-bound domain ledger before explicitly scheduling another attempt with a
/// new reconciliation key.
pub trait AiBatchArtifactLedger: Clone + Send + Sync + 'static {
    /// Ledger-specific failure.
    type Error: StdError + Send + Sync + 'static;

    /// Atomically reserves the artifact, returns a prior completion, or exposes ambiguity.
    fn reserve(
        &self,
        reference: AiBatchArtifactReference,
    ) -> BoxFuture<'static, Result<AiBatchArtifactReservation, Self::Error>>;

    /// Records that the application reconciled every intended side effect for the artifact.
    fn record_reconciled(
        &self,
        reference: AiBatchArtifactReference,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

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
#[derive(Debug, thiserror::Error)]
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
#[derive(Debug, thiserror::Error)]
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

/// Bounded in-memory ledger for deterministic tests and local development.
///
/// It is not a production durable idempotency store. Production applications need an atomic,
/// restart-safe ledger with a reconciliation and retention procedure.
#[derive(Clone)]
pub struct InMemoryAiBatchLedger {
    state: Arc<Mutex<BTreeMap<AiBatchReference, InMemoryBatchState>>>,
    capacity: usize,
}

#[derive(Clone)]
enum InMemoryBatchState {
    Pending,
    Submitted(AiBatchReceipt),
}

impl InMemoryAiBatchLedger {
    /// Creates a ledger with a fixed number of retained run keys.
    ///
    /// # Errors
    ///
    /// Returns [`AiBatchConfigError::InvalidInMemoryCapacity`] outside the documented bound.
    pub fn new(capacity: usize) -> Result<Self, AiBatchConfigError> {
        if !(1..=MAX_IN_MEMORY_RECORDS).contains(&capacity) {
            return Err(AiBatchConfigError::InvalidInMemoryCapacity);
        }
        Ok(Self {
            state: Arc::new(Mutex::new(BTreeMap::new())),
            capacity,
        })
    }

    /// Returns the fixed number of retained exact references.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }
}

impl fmt::Debug for InMemoryAiBatchLedger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let records = self
            .state
            .lock()
            .map(|state| state.len())
            .unwrap_or_default();
        formatter
            .debug_struct("InMemoryAiBatchLedger")
            .field("capacity", &self.capacity)
            .field("retained_records", &records)
            .finish()
    }
}

/// In-memory ledger failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InMemoryAiBatchLedgerError {
    /// The fixed local-development capacity was exhausted without implicit eviction.
    #[error("AI in-memory batch ledger capacity is exhausted")]
    CapacityExhausted,
    /// A poisoned lock prevents safely reserving or recording a batch.
    #[error("AI in-memory batch ledger state is unavailable")]
    StateUnavailable,
    /// Receipt recording requires a prior exact reservation.
    #[error("AI in-memory batch ledger has no matching reservation")]
    MissingReservation,
    /// A different receipt cannot replace a durable submitted batch.
    #[error("AI in-memory batch ledger receipt conflicts with an existing submission")]
    ConflictingReceipt,
}

impl AiBatchSubmissionLedger for InMemoryAiBatchLedger {
    type Error = InMemoryAiBatchLedgerError;

    fn reserve(
        &self,
        reference: AiBatchReference,
    ) -> BoxFuture<'static, Result<AiBatchReservation, Self::Error>> {
        let state = self.state.clone();
        let capacity = self.capacity;
        Box::pin(async move {
            let mut state = state
                .lock()
                .map_err(|_| InMemoryAiBatchLedgerError::StateUnavailable)?;
            match state.get(&reference) {
                Some(InMemoryBatchState::Pending) => Ok(AiBatchReservation::Pending),
                Some(InMemoryBatchState::Submitted(receipt)) => {
                    Ok(AiBatchReservation::Submitted(receipt.clone()))
                }
                None => {
                    if state.len() == capacity {
                        return Err(InMemoryAiBatchLedgerError::CapacityExhausted);
                    }
                    state.insert(reference, InMemoryBatchState::Pending);
                    Ok(AiBatchReservation::Reserved)
                }
            }
        })
    }

    fn record_submission(
        &self,
        reference: AiBatchReference,
        receipt: AiBatchReceipt,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let state = self.state.clone();
        Box::pin(async move {
            let mut state = state
                .lock()
                .map_err(|_| InMemoryAiBatchLedgerError::StateUnavailable)?;
            let current = state
                .get_mut(&reference)
                .ok_or(InMemoryAiBatchLedgerError::MissingReservation)?;
            match current {
                InMemoryBatchState::Pending => {
                    *current = InMemoryBatchState::Submitted(receipt);
                    Ok(())
                }
                InMemoryBatchState::Submitted(existing) if existing == &receipt => Ok(()),
                InMemoryBatchState::Submitted(_) => {
                    Err(InMemoryAiBatchLedgerError::ConflictingReceipt)
                }
            }
        })
    }
}

/// Bounded in-memory artifact ledger for deterministic tests and local development.
///
/// It is not a production durable idempotency store. Production applications need an atomic,
/// restart-safe artifact ledger plus a row-level domain idempotency and reconciliation procedure.
#[derive(Clone)]
pub struct InMemoryAiBatchArtifactLedger {
    state: Arc<Mutex<BTreeMap<AiBatchArtifactReference, InMemoryAiBatchArtifactState>>>,
    capacity: usize,
}

#[derive(Clone, Copy)]
enum InMemoryAiBatchArtifactState {
    Pending,
    Reconciled,
}

impl InMemoryAiBatchArtifactLedger {
    /// Creates an artifact ledger with a fixed number of retained exact references.
    ///
    /// # Errors
    ///
    /// Returns [`AiBatchConfigError::InvalidInMemoryCapacity`] outside the documented bound.
    pub fn new(capacity: usize) -> Result<Self, AiBatchConfigError> {
        if !(1..=MAX_IN_MEMORY_RECORDS).contains(&capacity) {
            return Err(AiBatchConfigError::InvalidInMemoryCapacity);
        }
        Ok(Self {
            state: Arc::new(Mutex::new(BTreeMap::new())),
            capacity,
        })
    }

    /// Returns the fixed number of retained exact artifact references.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }
}

impl fmt::Debug for InMemoryAiBatchArtifactLedger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let records = self
            .state
            .lock()
            .map(|state| state.len())
            .unwrap_or_default();
        formatter
            .debug_struct("InMemoryAiBatchArtifactLedger")
            .field("capacity", &self.capacity)
            .field("retained_records", &records)
            .finish()
    }
}

/// In-memory artifact-ledger failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InMemoryAiBatchArtifactLedgerError {
    /// The fixed local-development capacity was exhausted without implicit eviction.
    #[error("AI in-memory batch artifact ledger capacity is exhausted")]
    CapacityExhausted,
    /// A poisoned lock prevents safely reserving or recording an artifact.
    #[error("AI in-memory batch artifact ledger state is unavailable")]
    StateUnavailable,
    /// Completion recording requires a prior exact reservation.
    #[error("AI in-memory batch artifact ledger has no matching reservation")]
    MissingReservation,
}

impl AiBatchArtifactLedger for InMemoryAiBatchArtifactLedger {
    type Error = InMemoryAiBatchArtifactLedgerError;

    fn reserve(
        &self,
        reference: AiBatchArtifactReference,
    ) -> BoxFuture<'static, Result<AiBatchArtifactReservation, Self::Error>> {
        let state = self.state.clone();
        let capacity = self.capacity;
        Box::pin(async move {
            let mut state = state
                .lock()
                .map_err(|_| InMemoryAiBatchArtifactLedgerError::StateUnavailable)?;
            match state.get(&reference) {
                Some(InMemoryAiBatchArtifactState::Pending) => {
                    Ok(AiBatchArtifactReservation::Pending)
                }
                Some(InMemoryAiBatchArtifactState::Reconciled) => {
                    Ok(AiBatchArtifactReservation::Reconciled)
                }
                None => {
                    if state.len() == capacity {
                        return Err(InMemoryAiBatchArtifactLedgerError::CapacityExhausted);
                    }
                    state.insert(reference, InMemoryAiBatchArtifactState::Pending);
                    Ok(AiBatchArtifactReservation::Reserved)
                }
            }
        })
    }

    fn record_reconciled(
        &self,
        reference: AiBatchArtifactReference,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let state = self.state.clone();
        Box::pin(async move {
            let mut state = state
                .lock()
                .map_err(|_| InMemoryAiBatchArtifactLedgerError::StateUnavailable)?;
            let current = state
                .get_mut(&reference)
                .ok_or(InMemoryAiBatchArtifactLedgerError::MissingReservation)?;
            *current = InMemoryAiBatchArtifactState::Reconciled;
            Ok(())
        })
    }
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        sync::{Arc, Mutex},
    };

    use futures_util::future::BoxFuture;

    use super::{
        AiBatchArtifactKind, AiBatchArtifactLedger, AiBatchArtifactLoader,
        AiBatchArtifactProcessor, AiBatchArtifactReconciler,
        AiBatchArtifactReconciliationDisposition, AiBatchArtifactReconciliationError,
        AiBatchArtifactReference, AiBatchArtifactReservation, AiBatchCatalog, AiBatchConfigError,
        AiBatchProvider, AiBatchReceipt, AiBatchReference, AiBatchSubmissionDisposition,
        AiBatchSubmissionError, AiBatchSubmitter, InMemoryAiBatchArtifactLedger,
        InMemoryAiBatchLedger,
    };

    #[derive(Clone)]
    struct Catalog {
        calls: Arc<Mutex<usize>>,
    }

    impl AiBatchCatalog for Catalog {
        type Work = String;
        type Error = Infallible;

        fn load(
            &self,
            _reference: AiBatchReference,
        ) -> BoxFuture<'static, Result<Self::Work, Self::Error>> {
            let calls = self.calls.clone();
            Box::pin(async move {
                *calls.lock().unwrap() += 1;
                Ok("private batch prompt and expected answer".to_owned())
            })
        }
    }

    #[derive(Clone)]
    struct Provider {
        calls: Arc<Mutex<usize>>,
        fail: bool,
    }

    #[derive(Clone, Copy, Debug, thiserror::Error)]
    #[error("test batch provider unavailable")]
    enum ProviderError {
        Unavailable,
    }

    impl AiBatchProvider<String> for Provider {
        type Error = ProviderError;

        fn submit(
            &self,
            _reference: AiBatchReference,
            _work: String,
        ) -> BoxFuture<'static, Result<AiBatchReceipt, Self::Error>> {
            let calls = self.calls.clone();
            let fail = self.fail;
            Box::pin(async move {
                *calls.lock().unwrap() += 1;
                if fail {
                    Err(ProviderError::Unavailable)
                } else {
                    Ok(AiBatchReceipt::new("provider-batch-42").unwrap())
                }
            })
        }
    }

    fn reference() -> AiBatchReference {
        AiBatchReference::new("tenant-a.policy-v2", "catalog-17", "run-20260803-1").unwrap()
    }

    fn artifact_reference() -> AiBatchArtifactReference {
        AiBatchArtifactReference::new(
            reference(),
            AiBatchArtifactKind::Output,
            "file-output-17",
            "reconcile-output-17",
        )
        .unwrap()
    }

    #[derive(Clone)]
    struct ArtifactLoader {
        calls: Arc<Mutex<usize>>,
    }

    impl AiBatchArtifactLoader for ArtifactLoader {
        type Artifact = String;
        type Error = Infallible;

        fn load_artifact(
            &self,
            _reference: AiBatchArtifactReference,
        ) -> BoxFuture<'static, Result<Self::Artifact, Self::Error>> {
            let calls = self.calls.clone();
            Box::pin(async move {
                *calls.lock().unwrap() += 1;
                Ok("private provider batch output body".to_owned())
            })
        }
    }

    #[derive(Clone)]
    struct ArtifactProcessor {
        calls: Arc<Mutex<usize>>,
        fail: bool,
    }

    #[derive(Clone, Copy, Debug, thiserror::Error)]
    #[error("test artifact processor unavailable")]
    enum ArtifactProcessorError {
        Unavailable,
    }

    impl AiBatchArtifactProcessor<String> for ArtifactProcessor {
        type Error = ArtifactProcessorError;

        fn process_artifact(
            &self,
            _reference: AiBatchArtifactReference,
            _artifact: String,
        ) -> BoxFuture<'static, Result<(), Self::Error>> {
            let calls = self.calls.clone();
            let fail = self.fail;
            Box::pin(async move {
                *calls.lock().unwrap() += 1;
                if fail {
                    Err(ArtifactProcessorError::Unavailable)
                } else {
                    Ok(())
                }
            })
        }
    }

    #[test]
    fn references_are_content_free_and_bounded() {
        assert!(AiBatchReference::new("tenant-a", "catalog-1", "run-1").is_ok());
        assert_eq!(
            AiBatchReference::new("tenant a", "catalog-1", "run-1").unwrap_err(),
            AiBatchConfigError::InvalidIdentifier
        );
        assert!(AiBatchReference::new("tenant-a", "private prompt", "run-1").is_err());
        assert!(
            serde_json::from_str::<AiBatchReference>(
                r#"{"scope":"tenant a","catalog_id":"catalog-1","run_key":"run-1"}"#,
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<AiBatchReceipt>(
                r#"{"provider_batch_id":"private receipt text"}"#,
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn accepted_submission_records_a_receipt_and_later_delivery_does_not_resubmit() {
        let catalog_calls = Arc::new(Mutex::new(0));
        let provider_calls = Arc::new(Mutex::new(0));
        let submitter = AiBatchSubmitter::new(
            Catalog {
                calls: catalog_calls.clone(),
            },
            InMemoryAiBatchLedger::new(4).unwrap(),
            Provider {
                calls: provider_calls.clone(),
                fail: false,
            },
        );

        let first = submitter.submit(reference()).await.unwrap();
        let second = submitter.submit(reference()).await.unwrap();

        assert_eq!(first.disposition(), AiBatchSubmissionDisposition::Submitted);
        assert_eq!(
            second.disposition(),
            AiBatchSubmissionDisposition::ExistingSubmission
        );
        assert_eq!(second.receipt().provider_batch_id(), "provider-batch-42");
        assert_eq!(*catalog_calls.lock().unwrap(), 1);
        assert_eq!(*provider_calls.lock().unwrap(), 1);
        assert!(!format!("{submitter:?}").contains("private batch prompt"));
    }

    #[tokio::test]
    async fn provider_failure_stays_pending_without_an_automatic_second_submission() {
        let provider_calls = Arc::new(Mutex::new(0));
        let submitter = AiBatchSubmitter::new(
            Catalog {
                calls: Arc::new(Mutex::new(0)),
            },
            InMemoryAiBatchLedger::new(4).unwrap(),
            Provider {
                calls: provider_calls.clone(),
                fail: true,
            },
        );

        let first = submitter.submit(reference()).await.unwrap_err();
        let second = submitter.submit(reference()).await.unwrap_err();

        assert!(matches!(first, AiBatchSubmissionError::Provider { .. }));
        assert!(matches!(second, AiBatchSubmissionError::Pending { .. }));
        assert_eq!(*provider_calls.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn artifact_reconciliation_keeps_raw_content_ephemeral_and_does_not_repeat_completion() {
        let loader_calls = Arc::new(Mutex::new(0));
        let processor_calls = Arc::new(Mutex::new(0));
        let reconciler = AiBatchArtifactReconciler::new(
            InMemoryAiBatchArtifactLedger::new(4).unwrap(),
            ArtifactLoader {
                calls: loader_calls.clone(),
            },
            ArtifactProcessor {
                calls: processor_calls.clone(),
                fail: false,
            },
        );

        let encoded = serde_json::to_string(&artifact_reference()).unwrap();
        let first = reconciler.reconcile(artifact_reference()).await.unwrap();
        let second = reconciler.reconcile(artifact_reference()).await.unwrap();

        assert!(!encoded.contains("private provider batch output body"));
        assert_eq!(first, AiBatchArtifactReconciliationDisposition::Reconciled);
        assert_eq!(
            second,
            AiBatchArtifactReconciliationDisposition::ExistingReconciliation
        );
        assert_eq!(*loader_calls.lock().unwrap(), 1);
        assert_eq!(*processor_calls.lock().unwrap(), 1);
        assert!(!format!("{reconciler:?}").contains("private provider batch output body"));
    }

    #[tokio::test]
    async fn artifact_processor_failure_stays_pending_without_automatic_replay() {
        let loader_calls = Arc::new(Mutex::new(0));
        let processor_calls = Arc::new(Mutex::new(0));
        let reconciler = AiBatchArtifactReconciler::new(
            InMemoryAiBatchArtifactLedger::new(4).unwrap(),
            ArtifactLoader {
                calls: loader_calls.clone(),
            },
            ArtifactProcessor {
                calls: processor_calls.clone(),
                fail: true,
            },
        );

        let first = reconciler
            .reconcile(artifact_reference())
            .await
            .unwrap_err();
        let second = reconciler
            .reconcile(artifact_reference())
            .await
            .unwrap_err();

        assert!(matches!(
            first,
            AiBatchArtifactReconciliationError::Processor { .. }
        ));
        assert!(matches!(
            second,
            AiBatchArtifactReconciliationError::Pending { .. }
        ));
        assert_eq!(*loader_calls.lock().unwrap(), 1);
        assert_eq!(*processor_calls.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn reviewed_artifact_retry_requires_a_new_reconciliation_key() {
        let ledger = InMemoryAiBatchArtifactLedger::new(4).unwrap();
        let first = artifact_reference();
        let retry = AiBatchArtifactReference::new(
            reference(),
            AiBatchArtifactKind::Output,
            "file-output-17",
            "reconcile-output-17-retry-1",
        )
        .unwrap();

        assert_eq!(
            ledger.reserve(first.clone()).await.unwrap(),
            AiBatchArtifactReservation::Reserved
        );
        assert_eq!(
            ledger.reserve(first).await.unwrap(),
            AiBatchArtifactReservation::Pending
        );
        assert_eq!(
            ledger.reserve(retry).await.unwrap(),
            AiBatchArtifactReservation::Reserved
        );
    }
}
