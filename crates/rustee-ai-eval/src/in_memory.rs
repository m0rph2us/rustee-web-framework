//! Local-development in-memory evaluation-run ledger.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};

use futures_util::future::BoxFuture;

use crate::{
    AiEvaluationConfigError, AiEvaluationReference, AiEvaluationRunLedger,
    AiEvaluationRunReservation,
};

const MAX_IN_MEMORY_RUNS: usize = 10_000;

/// Bounded in-memory run ledger for deterministic tests and local development.
///
/// It is not restart-safe. Production applications need a durable atomic ledger and an explicit
/// reconciliation/retention procedure.
#[derive(Clone)]
pub struct InMemoryAiEvaluationRunLedger {
    state: Arc<Mutex<BTreeMap<(String, String), InMemoryEvaluationRunState>>>,
    capacity: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InMemoryEvaluationRunStatus {
    Pending,
    Completed,
}

#[derive(Clone)]
struct InMemoryEvaluationRunState {
    catalog_id: String,
    status: InMemoryEvaluationRunStatus,
}

impl InMemoryAiEvaluationRunLedger {
    /// Creates a ledger with a fixed number of retained scoped run keys.
    ///
    /// # Errors
    ///
    /// Returns [`AiEvaluationConfigError::InvalidInMemoryLedgerCapacity`] outside the documented
    /// local-development bound.
    pub fn new(capacity: usize) -> Result<Self, AiEvaluationConfigError> {
        if !(1..=MAX_IN_MEMORY_RUNS).contains(&capacity) {
            return Err(AiEvaluationConfigError::InvalidInMemoryLedgerCapacity);
        }
        Ok(Self {
            state: Arc::new(Mutex::new(BTreeMap::new())),
            capacity,
        })
    }

    /// Returns the fixed number of retained scoped run keys.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }
}

impl fmt::Debug for InMemoryAiEvaluationRunLedger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let records = self.state.lock().ok().map(|state| state.len());
        formatter
            .debug_struct("InMemoryAiEvaluationRunLedger")
            .field("capacity", &self.capacity)
            .field("retained_records", &records)
            .finish()
    }
}

/// In-memory evaluation ledger failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InMemoryAiEvaluationRunLedgerError {
    /// The fixed local-development capacity was exhausted without implicit eviction.
    #[error("AI in-memory evaluation ledger capacity is exhausted")]
    CapacityExhausted,
    /// A poisoned lock prevents safely reserving or recording a run.
    #[error("AI in-memory evaluation ledger state is unavailable")]
    StateUnavailable,
    /// A scoped run key was reused for a different catalog identity.
    #[error("AI evaluation run key conflicts with an existing catalog identity")]
    IdentityConflict,
    /// Completion recording requires a prior exact reservation.
    #[error("AI in-memory evaluation ledger has no matching reservation")]
    MissingReservation,
}

impl AiEvaluationRunLedger for InMemoryAiEvaluationRunLedger {
    type Error = InMemoryAiEvaluationRunLedgerError;

    fn reserve(
        &self,
        reference: AiEvaluationReference,
    ) -> BoxFuture<'static, Result<AiEvaluationRunReservation, Self::Error>> {
        let state = self.state.clone();
        let capacity = self.capacity;
        Box::pin(async move {
            let mut state = state
                .lock()
                .map_err(|_| InMemoryAiEvaluationRunLedgerError::StateUnavailable)?;
            let key = (reference.scope().to_owned(), reference.run_key().to_owned());
            match state.get(&key) {
                Some(existing) if existing.catalog_id != reference.catalog_id() => {
                    Err(InMemoryAiEvaluationRunLedgerError::IdentityConflict)
                }
                Some(existing) if existing.status == InMemoryEvaluationRunStatus::Pending => {
                    Ok(AiEvaluationRunReservation::Pending)
                }
                Some(_) => Ok(AiEvaluationRunReservation::Completed),
                None => {
                    if state.len() == capacity {
                        return Err(InMemoryAiEvaluationRunLedgerError::CapacityExhausted);
                    }
                    state.insert(
                        key,
                        InMemoryEvaluationRunState {
                            catalog_id: reference.catalog_id().to_owned(),
                            status: InMemoryEvaluationRunStatus::Pending,
                        },
                    );
                    Ok(AiEvaluationRunReservation::Reserved)
                }
            }
        })
    }

    fn record_completed(
        &self,
        reference: AiEvaluationReference,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let state = self.state.clone();
        Box::pin(async move {
            let mut state = state
                .lock()
                .map_err(|_| InMemoryAiEvaluationRunLedgerError::StateUnavailable)?;
            let key = (reference.scope().to_owned(), reference.run_key().to_owned());
            let existing = state
                .get_mut(&key)
                .ok_or(InMemoryAiEvaluationRunLedgerError::MissingReservation)?;
            if existing.catalog_id != reference.catalog_id() {
                return Err(InMemoryAiEvaluationRunLedgerError::IdentityConflict);
            }
            existing.status = InMemoryEvaluationRunStatus::Completed;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{AiEvaluationReference, AiEvaluationRunLedger};

    use super::{InMemoryAiEvaluationRunLedger, InMemoryAiEvaluationRunLedgerError};

    #[tokio::test]
    async fn poisoned_state_fails_closed_for_reservation_and_completion() {
        let ledger = InMemoryAiEvaluationRunLedger::new(1).unwrap();
        let state = Arc::clone(&ledger.state);
        let _ = std::thread::spawn(move || {
            let _guard = state.lock().unwrap();
            panic!("poisoned local evaluation ledger state");
        })
        .join();
        let reference = AiEvaluationReference::new("tenant-a", "catalog-a", "run-a").unwrap();

        assert_eq!(
            ledger.reserve(reference.clone()).await.unwrap_err(),
            InMemoryAiEvaluationRunLedgerError::StateUnavailable
        );
        assert_eq!(
            ledger.record_completed(reference).await.unwrap_err(),
            InMemoryAiEvaluationRunLedgerError::StateUnavailable
        );
        assert!(format!("{ledger:?}").contains("None"));
    }
}
