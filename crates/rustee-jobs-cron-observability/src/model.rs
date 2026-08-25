use std::{collections::BTreeMap, time::Duration};

use rustee_jobs_cron_sqlx::RecurringJobFireOutcome;

/// Bounded recurring-scheduler count names used by [`RecurringJobFireMetricsSnapshot`].
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RecurringJobFireRowCount {
    /// Due schedule rows claimed during reportable passes.
    Claimed,
    /// Fresh job envelopes atomically staged to the outbox.
    Staged,
    /// Due schedule rows deferred without an outbox row because their shared window was full.
    RateLimited,
}

impl RecurringJobFireRowCount {
    /// Returns the stable exporter-safe count label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Claimed => "claimed",
            Self::Staged => "staged",
            Self::RateLimited => "rate_limited",
        }
    }
}

/// Immutable view of metrics collected by [`super::RecurringJobFireMetrics`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecurringJobFireMetricsSnapshot {
    in_flight: u64,
    started: u64,
    completed: u64,
    outcome_counts: BTreeMap<RecurringJobFireOutcome, u64>,
    row_counts: BTreeMap<RecurringJobFireRowCount, u64>,
    duration_bucket_counts: Vec<(Duration, u64)>,
    total_duration: Duration,
}

impl RecurringJobFireMetricsSnapshot {
    pub(super) fn from_state(
        in_flight: u64,
        started: u64,
        completed: u64,
        outcome_counts: BTreeMap<RecurringJobFireOutcome, u64>,
        row_counts: BTreeMap<RecurringJobFireRowCount, u64>,
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

    /// Returns scheduler pass futures currently executing.
    #[must_use]
    pub const fn in_flight(&self) -> u64 {
        self.in_flight
    }

    /// Returns scheduler pass futures that started.
    #[must_use]
    pub const fn started(&self) -> u64 {
        self.started
    }

    /// Returns scheduler pass futures that emitted a terminal result.
    #[must_use]
    pub const fn completed(&self) -> u64 {
        self.completed
    }

    /// Returns the count for one fixed terminal outcome.
    #[must_use]
    pub fn outcome(&self, outcome: RecurringJobFireOutcome) -> u64 {
        self.outcome_counts.get(&outcome).copied().unwrap_or(0)
    }

    /// Returns one aggregate row count by fixed count name.
    #[must_use]
    pub fn rows(&self, count: RecurringJobFireRowCount) -> u64 {
        self.row_counts.get(&count).copied().unwrap_or(0)
    }

    /// Iterates outcome counts in stable order.
    pub fn outcome_counts(&self) -> impl Iterator<Item = (RecurringJobFireOutcome, u64)> + '_ {
        self.outcome_counts
            .iter()
            .map(|(&outcome, &count)| (outcome, count))
    }

    /// Iterates aggregate row counts in stable order.
    pub fn row_counts(&self) -> impl Iterator<Item = (RecurringJobFireRowCount, u64)> + '_ {
        self.row_counts
            .iter()
            .map(|(&count, &value)| (count, value))
    }

    /// Iterates cumulative histogram counts by duration upper bound.
    pub fn duration_bucket_counts(&self) -> impl Iterator<Item = (Duration, u64)> + '_ {
        self.duration_bucket_counts.iter().copied()
    }

    /// Returns the sum of all completed pass durations.
    #[must_use]
    pub const fn total_duration(&self) -> Duration {
        self.total_duration
    }
}
