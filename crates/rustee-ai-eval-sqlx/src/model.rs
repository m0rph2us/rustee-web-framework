//! SQL-independent pending evaluation-run models.

use std::{fmt, num::NonZeroUsize};

use rustee_ai_eval::AiEvaluationReference;

const MAX_PENDING_LIMIT: usize = 1_000;

/// A bounded request for unresolved evaluation runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PendingAiEvaluationRunLimit(NonZeroUsize);

impl PendingAiEvaluationRunLimit {
    /// Creates a non-zero, bounded pending-run query limit.
    ///
    /// # Errors
    ///
    /// Returns [`PendingAiEvaluationRunLimitError`] when `limit` is zero or too large.
    pub fn new(limit: usize) -> Result<Self, PendingAiEvaluationRunLimitError> {
        let limit = NonZeroUsize::new(limit).ok_or(PendingAiEvaluationRunLimitError::Zero)?;
        if limit.get() > MAX_PENDING_LIMIT {
            return Err(PendingAiEvaluationRunLimitError::TooLarge);
        }
        Ok(Self(limit))
    }

    /// Returns the configured number of records.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

impl Default for PendingAiEvaluationRunLimit {
    fn default() -> Self {
        Self(NonZeroUsize::new(100).expect("default pending evaluation limit is non-zero"))
    }
}

/// Invalid pending-evaluation query limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PendingAiEvaluationRunLimitError {
    /// A reconciliation query must request at least one record.
    #[error("pending AI evaluation run limit must be non-zero")]
    Zero,
    /// A reconciliation query would retain too many rows in one response.
    #[error("pending AI evaluation run limit exceeds the supported maximum")]
    TooLarge,
}

/// One evaluation whose prior catalog, provider, grader, or record attempt is ambiguous.
///
/// This metadata is returned only to application-owned reconciliation code. Application owners
/// authorize the scope, inspect provider usage/report sinks, and choose a new run key only when a
/// fresh evaluation is safe.
#[derive(Clone, Eq, PartialEq)]
pub struct PendingAiEvaluationRun {
    reference: AiEvaluationReference,
}

impl PendingAiEvaluationRun {
    pub(crate) const fn from_reference(reference: AiEvaluationReference) -> Self {
        Self { reference }
    }

    /// Returns the content-free reference for application reconciliation.
    #[must_use]
    pub const fn reference(&self) -> &AiEvaluationReference {
        &self.reference
    }
}

impl fmt::Debug for PendingAiEvaluationRun {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingAiEvaluationRun")
            .field("reference", &"[REDACTED]")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use rustee_ai_eval::AiEvaluationReference;

    use super::{
        PendingAiEvaluationRun, PendingAiEvaluationRunLimit, PendingAiEvaluationRunLimitError,
    };

    #[test]
    fn pending_limit_is_non_zero_and_bounded() {
        assert_eq!(
            PendingAiEvaluationRunLimit::new(0).unwrap_err(),
            PendingAiEvaluationRunLimitError::Zero
        );
        assert_eq!(
            PendingAiEvaluationRunLimit::new(1_001).unwrap_err(),
            PendingAiEvaluationRunLimitError::TooLarge
        );
        assert_eq!(PendingAiEvaluationRunLimit::new(1).unwrap().get(), 1);
    }

    #[test]
    fn pending_run_debug_redacts_its_reference() {
        let pending = PendingAiEvaluationRun::from_reference(
            AiEvaluationReference::new("tenant-a", "catalog-7", "run-7").unwrap(),
        );

        let rendered = format!("{pending:?}");
        assert!(!rendered.contains("tenant-a"));
        assert!(!rendered.contains("run-7"));
    }
}
