use std::{collections::BTreeMap, time::Duration};

use rustee_events_kafka_sqlx::{KafkaDelayedRetryBacklog, KafkaDelayedRetryRelayOutcome};

/// Immutable view of metrics collected by [`super::KafkaDelayedRetryRelayMetrics`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KafkaDelayedRetryRelayMetricsSnapshot {
    pub(super) in_flight: u64,
    pub(super) started: u64,
    pub(super) completed: u64,
    pub(super) outcome_counts: BTreeMap<KafkaDelayedRetryRelayOutcome, u64>,
    pub(super) published: u64,
    pub(super) backlog: Option<KafkaDelayedRetryBacklog>,
    pub(super) duration_bucket_counts: Vec<(Duration, u64)>,
    pub(super) total_duration: Duration,
}

impl KafkaDelayedRetryRelayMetricsSnapshot {
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

    /// Returns terminal passes for one fixed outcome.
    #[must_use]
    pub fn outcome(&self, outcome: KafkaDelayedRetryRelayOutcome) -> u64 {
        self.outcome_counts.get(&outcome).copied().unwrap_or(0)
    }

    /// Iterates terminal outcome counts in stable outcome order.
    pub fn outcome_counts(
        &self,
    ) -> impl Iterator<Item = (KafkaDelayedRetryRelayOutcome, u64)> + '_ {
        self.outcome_counts
            .iter()
            .map(|(&outcome, &count)| (outcome, count))
    }

    /// Returns records confirmed in fully successful passes.
    #[must_use]
    pub const fn published(&self) -> u64 {
        self.published
    }

    /// Returns the latest application-recorded aggregate database backlog snapshot.
    #[must_use]
    pub const fn backlog(&self) -> Option<KafkaDelayedRetryBacklog> {
        self.backlog
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
