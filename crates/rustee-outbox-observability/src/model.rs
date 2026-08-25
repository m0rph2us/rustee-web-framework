use std::{collections::BTreeMap, time::Duration};

use rustee_outbox_sqlx::{RelayPassKind, RelayPassOutcome};

/// Bounded outbox relay count names used by [`OutboxRelayMetricsSnapshot`].
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RelayRowCount {
    /// Rows leased at the beginning of reportable passes.
    Claimed,
    /// Rows confirmed after broker acknowledgement.
    Published,
    /// Rows released for a later retry after publisher failure.
    RetryScheduled,
    /// Confirmation or release operations that had already lost their lease.
    LeaseLost,
}

impl RelayRowCount {
    /// Returns the exporter-safe count label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Claimed => "claimed",
            Self::Published => "published",
            Self::RetryScheduled => "retry_scheduled",
            Self::LeaseLost => "lease_lost",
        }
    }
}

/// Immutable view of metrics collected by [`super::OutboxRelayMetrics`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxRelayMetricsSnapshot {
    in_flight: u64,
    started: u64,
    completed: u64,
    outcome_counts: BTreeMap<(RelayPassKind, RelayPassOutcome), u64>,
    row_counts: BTreeMap<(RelayPassKind, RelayRowCount), u64>,
    duration_bucket_counts: Vec<(Duration, u64)>,
    total_duration: Duration,
}

impl OutboxRelayMetricsSnapshot {
    pub(super) fn from_state(
        in_flight: u64,
        started: u64,
        completed: u64,
        outcome_counts: BTreeMap<(RelayPassKind, RelayPassOutcome), u64>,
        row_counts: BTreeMap<(RelayPassKind, RelayRowCount), u64>,
        duration_bucket_counts: Vec<(Duration, u64)>,
        total_duration: Duration,
    ) -> Self {
        Self {
            in_flight,
            started,
            completed,
            outcome_counts,
            row_counts,
            duration_bucket_counts,
            total_duration,
        }
    }

    /// Returns relay pass futures currently executing.
    #[must_use]
    pub const fn in_flight(&self) -> u64 {
        self.in_flight
    }

    /// Returns relay pass futures that started.
    #[must_use]
    pub const fn started(&self) -> u64 {
        self.started
    }

    /// Returns relay pass futures that emitted a terminal result.
    #[must_use]
    pub const fn completed(&self) -> u64 {
        self.completed
    }

    /// Returns the count for one fixed kind and outcome.
    #[must_use]
    pub fn outcome(&self, kind: RelayPassKind, outcome: RelayPassOutcome) -> u64 {
        self.outcome_counts
            .get(&(kind, outcome))
            .copied()
            .unwrap_or(0)
    }

    /// Returns an aggregate row count for one fixed relay kind and count name.
    #[must_use]
    pub fn rows(&self, kind: RelayPassKind, count: RelayRowCount) -> u64 {
        self.row_counts.get(&(kind, count)).copied().unwrap_or(0)
    }

    /// Iterates outcome counts in stable kind/outcome order.
    pub fn outcome_counts(
        &self,
    ) -> impl Iterator<Item = (RelayPassKind, RelayPassOutcome, u64)> + '_ {
        self.outcome_counts
            .iter()
            .map(|((kind, outcome), &count)| (*kind, *outcome, count))
    }

    /// Iterates aggregate row counts in stable kind/count order.
    pub fn row_counts(&self) -> impl Iterator<Item = (RelayPassKind, RelayRowCount, u64)> + '_ {
        self.row_counts
            .iter()
            .map(|((kind, count), &value)| (*kind, *count, value))
    }

    /// Iterates cumulative histogram counts by duration upper bound.
    pub fn duration_bucket_counts(&self) -> impl Iterator<Item = (Duration, u64)> + '_ {
        self.duration_bucket_counts.iter().copied()
    }

    /// Returns the sum of all completed relay pass durations.
    #[must_use]
    pub const fn total_duration(&self) -> Duration {
        self.total_duration
    }
}
