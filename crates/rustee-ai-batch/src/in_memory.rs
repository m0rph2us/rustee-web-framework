//! Bounded in-memory ledgers for local development and deterministic tests.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};

use futures_util::future::BoxFuture;

use super::{
    AiBatchArtifactLedger, AiBatchArtifactReference, AiBatchArtifactReservation,
    AiBatchConfigError, AiBatchReceipt, AiBatchReference, AiBatchReservation,
    AiBatchSubmissionLedger,
};

const MAX_IN_MEMORY_RECORDS: usize = 10_000;

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
        let records = self.state.lock().ok().map(|state| state.len());
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
        let records = self.state.lock().ok().map(|state| state.len());
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

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use crate::{
        AiBatchArtifactKind, AiBatchArtifactLedger, AiBatchArtifactReference, AiBatchReference,
        AiBatchSubmissionLedger,
    };

    use super::{
        InMemoryAiBatchArtifactLedger, InMemoryAiBatchArtifactLedgerError, InMemoryAiBatchLedger,
        InMemoryAiBatchLedgerError,
    };

    fn batch_reference() -> AiBatchReference {
        AiBatchReference::new("tenant-a", "catalog-a", "run-a")
            .expect("test batch reference must be valid")
    }

    #[tokio::test]
    async fn poisoned_states_fail_closed_and_do_not_masquerade_as_empty_in_debug() {
        let submissions =
            InMemoryAiBatchLedger::new(1).expect("test submission ledger capacity must be valid");
        let submission_state = Arc::clone(&submissions.state);
        let poison_submission = thread::spawn(move || {
            let _guard = submission_state
                .lock()
                .expect("new submission ledger lock must be available");
            panic!("test must poison local AI batch submission state");
        });
        assert!(poison_submission.join().is_err());

        assert_eq!(
            submissions.reserve(batch_reference()).await.unwrap_err(),
            InMemoryAiBatchLedgerError::StateUnavailable
        );
        assert!(format!("{submissions:?}").contains("retained_records: None"));

        let artifacts = InMemoryAiBatchArtifactLedger::new(1)
            .expect("test artifact ledger capacity must be valid");
        let artifact_state = Arc::clone(&artifacts.state);
        let poison_artifact = thread::spawn(move || {
            let _guard = artifact_state
                .lock()
                .expect("new artifact ledger lock must be available");
            panic!("test must poison local AI batch artifact state");
        });
        assert!(poison_artifact.join().is_err());

        let artifact = AiBatchArtifactReference::new(
            batch_reference(),
            AiBatchArtifactKind::Output,
            "file-a",
            "reconcile-a",
        )
        .expect("test artifact reference must be valid");
        assert_eq!(
            artifacts.reserve(artifact).await.unwrap_err(),
            InMemoryAiBatchArtifactLedgerError::StateUnavailable
        );
        assert!(format!("{artifacts:?}").contains("retained_records: None"));
    }
}
