//! Bounded pending-artifact query and result models.

use std::{fmt, num::NonZeroUsize};

use rustee_ai_batch::AiBatchArtifactReference;

const MAX_PENDING_LIMIT: usize = 1_000;

/// A bounded request for unresolved provider artifact reconciliations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PendingAiBatchArtifactLimit(NonZeroUsize);

impl PendingAiBatchArtifactLimit {
    /// Creates a non-zero, bounded pending-artifact query limit.
    ///
    /// # Errors
    ///
    /// Returns [`PendingAiBatchArtifactLimitError`] when `limit` is zero or too large.
    pub fn new(limit: usize) -> Result<Self, PendingAiBatchArtifactLimitError> {
        let limit = NonZeroUsize::new(limit).ok_or(PendingAiBatchArtifactLimitError::Zero)?;
        if limit.get() > MAX_PENDING_LIMIT {
            return Err(PendingAiBatchArtifactLimitError::TooLarge);
        }
        Ok(Self(limit))
    }

    /// Returns the configured number of records.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

impl Default for PendingAiBatchArtifactLimit {
    fn default() -> Self {
        Self(NonZeroUsize::new(100).expect("default pending artifact limit is non-zero"))
    }
}

/// Invalid pending-artifact query limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PendingAiBatchArtifactLimitError {
    /// A reconciliation query must request at least one record.
    #[error("pending AI batch artifact limit must be non-zero")]
    Zero,
    /// A reconciliation query would retain too many rows in one response.
    #[error("pending AI batch artifact limit exceeds the supported maximum")]
    TooLarge,
}

/// One provider artifact whose previous reconciliation attempt is still ambiguous.
///
/// This metadata is deliberately returned only to application-owned reconciliation code. Its
/// debug representation redacts every identifier; applications authorize the batch scope and
/// inspect their row-level domain idempotency ledger before scheduling another attempt.
#[derive(Clone, Eq, PartialEq)]
pub struct PendingAiBatchArtifact {
    reference: AiBatchArtifactReference,
}

impl PendingAiBatchArtifact {
    /// Returns the content-free artifact reference for application reconciliation.
    #[must_use]
    pub const fn reference(&self) -> &AiBatchArtifactReference {
        &self.reference
    }

    pub(super) const fn new(reference: AiBatchArtifactReference) -> Self {
        Self { reference }
    }
}

impl fmt::Debug for PendingAiBatchArtifact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingAiBatchArtifact")
            .field("reference", &"[REDACTED]")
            .finish()
    }
}
