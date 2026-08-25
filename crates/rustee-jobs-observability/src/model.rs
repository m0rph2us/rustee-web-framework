use std::{collections::BTreeMap, time::Duration};

use rustee_jobs::JobDeliveryOutcome;

/// Immutable view of metrics collected by [`super::JobMetrics`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobMetricsSnapshot {
    in_flight: u64,
    started: u64,
    completed: u64,
    outcome_counts: BTreeMap<(String, JobDeliveryOutcome), u64>,
    duration_bucket_counts: Vec<(Duration, u64)>,
    total_duration: Duration,
}

impl JobMetricsSnapshot {
    pub(super) fn from_state(
        in_flight: u64,
        started: u64,
        completed: u64,
        outcome_counts: BTreeMap<(String, JobDeliveryOutcome), u64>,
        duration_bucket_counts: Vec<(Duration, u64)>,
        total_duration: Duration,
    ) -> Self {
        Self {
            in_flight,
            started,
            completed,
            outcome_counts,
            duration_bucket_counts,
            total_duration,
        }
    }

    /// Returns deliveries currently executing.
    #[must_use]
    pub const fn in_flight(&self) -> u64 {
        self.in_flight
    }

    /// Returns deliveries whose worker task started.
    #[must_use]
    pub const fn started(&self) -> u64 {
        self.started
    }

    /// Returns deliveries that emitted a final result.
    #[must_use]
    pub const fn completed(&self) -> u64 {
        self.completed
    }

    /// Returns the count for one bounded provider and settlement outcome.
    #[must_use]
    pub fn outcome(&self, provider: &str, outcome: JobDeliveryOutcome) -> u64 {
        self.outcome_counts
            .get(&(provider.to_owned(), outcome))
            .copied()
            .unwrap_or(0)
    }

    /// Iterates outcome counts in stable provider/outcome order.
    pub fn outcome_counts(&self) -> impl Iterator<Item = (&str, JobDeliveryOutcome, u64)> + '_ {
        self.outcome_counts
            .iter()
            .map(|((provider, outcome), &count)| (provider.as_str(), *outcome, count))
    }

    /// Iterates cumulative histogram counts by duration upper bound.
    pub fn duration_bucket_counts(&self) -> impl Iterator<Item = (Duration, u64)> + '_ {
        self.duration_bucket_counts.iter().copied()
    }

    /// Returns the sum of all completed delivery durations.
    #[must_use]
    pub const fn total_duration(&self) -> Duration {
        self.total_duration
    }
}
