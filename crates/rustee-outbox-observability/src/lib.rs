//! Exporter-neutral metrics for `Rustee` transactional-outbox relays.
//!
//! The collector implements [`OutboxRelayObserver`] and is attached to an event or job relay with
//! its `with_relay_observer` builder. It records only the fixed relay kind, terminal outcome,
//! bounded counts, and global duration. Destinations, message IDs, payloads, broker endpoints,
//! and error text never enter this collector.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use rustee_outbox_sqlx::{
    OutboxRelayObserver, RelayPassFinished, RelayPassKind, RelayPassOutcome, RelayPassStarted,
    RelayReport,
};

const MAX_DURATION_BUCKETS: usize = 32;

/// Default cumulative upper bounds for outbox relay pass duration histograms.
pub const DEFAULT_RELAY_PASS_DURATION_BUCKETS: [Duration; 12] = [
    Duration::from_millis(1),
    Duration::from_millis(5),
    Duration::from_millis(10),
    Duration::from_millis(25),
    Duration::from_millis(50),
    Duration::from_millis(100),
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_millis(2500),
    Duration::from_secs(5),
    Duration::from_secs(10),
];

/// Stable names for relay metrics exported by an application adapter.
pub mod metric_names {
    /// Count of relay passes whose future started.
    pub const RELAY_PASSES_TOTAL: &str = "rustee_outbox_relay_passes_total";
    /// Number of relay pass futures currently executing in this process.
    pub const RELAY_PASSES_IN_FLIGHT: &str = "rustee_outbox_relay_passes_in_flight";
    /// Sum of completed relay pass durations in seconds.
    pub const RELAY_PASS_DURATION_SECONDS: &str = "rustee_outbox_relay_pass_duration_seconds";
    /// Count of relay pass outcomes by fixed kind and outcome labels.
    pub const RELAY_PASS_OUTCOMES_TOTAL: &str = "rustee_outbox_relay_pass_outcomes_total";
    /// Aggregate claimed, published, retry-scheduled, or lease-lost row counts by fixed kind.
    pub const RELAY_ROWS_TOTAL: &str = "rustee_outbox_relay_rows_total";
}

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

/// Thread-safe, exporter-neutral collector for transactional-outbox relay passes.
///
/// Relay kind and outcome are fixed enums. The collector deliberately exposes no dynamic route,
/// provider, tenant, or message labels.
#[derive(Clone, Debug)]
pub struct OutboxRelayMetrics {
    state: Arc<Mutex<OutboxRelayMetricsState>>,
}

impl Default for OutboxRelayMetrics {
    fn default() -> Self {
        Self::with_duration_buckets(DEFAULT_RELAY_PASS_DURATION_BUCKETS)
            .expect("default outbox relay duration buckets must be valid")
    }
}

impl OutboxRelayMetrics {
    /// Creates an empty collector with default duration buckets.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty collector with explicit cumulative duration histogram bounds.
    ///
    /// At most thirty-two non-zero durations are accepted, in strictly increasing order.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxRelayMetricsConfigError`] when the bounds are empty, too numerous, zero,
    /// or not strictly increasing.
    pub fn with_duration_buckets(
        buckets: impl IntoIterator<Item = Duration>,
    ) -> Result<Self, OutboxRelayMetricsConfigError> {
        let buckets = buckets.into_iter().collect::<Vec<_>>();
        validate_duration_buckets(&buckets)?;
        Ok(Self {
            state: Arc::new(Mutex::new(OutboxRelayMetricsState {
                duration_bucket_counts: vec![0; buckets.len()],
                duration_buckets: buckets,
                ..OutboxRelayMetricsState::default()
            })),
        })
    }

    /// Returns a point-in-time snapshot for an exporter or readiness diagnostic.
    ///
    /// # Panics
    ///
    /// Panics only if a concurrent metrics update poisoned the internal mutex.
    #[must_use]
    pub fn snapshot(&self) -> OutboxRelayMetricsSnapshot {
        let state = self
            .state
            .lock()
            .expect("outbox relay metrics lock must not be poisoned");
        OutboxRelayMetricsSnapshot {
            in_flight: state.in_flight,
            started: state.started,
            completed: state.completed,
            outcome_counts: state.outcome_counts.clone(),
            row_counts: state.row_counts.clone(),
            duration_bucket_counts: state
                .duration_buckets
                .iter()
                .copied()
                .zip(state.duration_bucket_counts.iter().copied())
                .collect(),
            total_duration: state.total_duration,
        }
    }

    fn started(&self) {
        let mut state = self
            .state
            .lock()
            .expect("outbox relay metrics lock must not be poisoned");
        state.in_flight = state.in_flight.saturating_add(1);
        state.started = state.started.saturating_add(1);
    }

    fn finished(&self, pass: RelayPassFinished) {
        let mut state = self
            .state
            .lock()
            .expect("outbox relay metrics lock must not be poisoned");
        state.in_flight = state.in_flight.saturating_sub(1);
        state.completed = state.completed.saturating_add(1);
        *state
            .outcome_counts
            .entry((pass.kind(), pass.outcome()))
            .or_default() += 1;
        if let Some(report) = pass.report() {
            record_report(&mut state, pass.kind(), report);
        }
        let OutboxRelayMetricsState {
            duration_buckets,
            duration_bucket_counts,
            ..
        } = &mut *state;
        for (upper_bound, count) in duration_buckets.iter().zip(duration_bucket_counts) {
            if pass.duration() <= *upper_bound {
                *count = count.saturating_add(1);
            }
        }
        state.total_duration = state.total_duration.saturating_add(pass.duration());
    }
}

impl OutboxRelayObserver for OutboxRelayMetrics {
    fn on_relay_pass_started(&self, _pass: RelayPassStarted) {
        self.started();
    }

    fn on_relay_pass_finished(&self, pass: RelayPassFinished) {
        self.finished(pass);
    }
}

fn record_report(state: &mut OutboxRelayMetricsState, kind: RelayPassKind, report: RelayReport) {
    for (count, value) in [
        (RelayRowCount::Claimed, report.claimed),
        (RelayRowCount::Published, report.published),
        (RelayRowCount::RetryScheduled, report.retry_scheduled),
        (RelayRowCount::LeaseLost, report.lease_lost),
    ] {
        *state.row_counts.entry((kind, count)).or_default() += value as u64;
    }
}

#[derive(Debug, Default)]
struct OutboxRelayMetricsState {
    in_flight: u64,
    started: u64,
    completed: u64,
    outcome_counts: BTreeMap<(RelayPassKind, RelayPassOutcome), u64>,
    row_counts: BTreeMap<(RelayPassKind, RelayRowCount), u64>,
    duration_buckets: Vec<Duration>,
    duration_bucket_counts: Vec<u64>,
    total_duration: Duration,
}

/// Invalid duration histogram configuration rejected before collection starts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxRelayMetricsConfigError {
    /// No finite histogram upper bound was configured.
    EmptyDurationBuckets,
    /// More than thirty-two finite histogram upper bounds were configured.
    TooManyDurationBuckets,
    /// A histogram upper bound was zero.
    ZeroDurationBucket,
    /// Histogram upper bounds were duplicated or out of ascending order.
    UnorderedDurationBuckets,
}

impl fmt::Display for OutboxRelayMetricsConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptyDurationBuckets => "at least one duration histogram bucket is required",
            Self::TooManyDurationBuckets => {
                "outbox relay duration histogram supports at most 32 finite buckets"
            }
            Self::ZeroDurationBucket => {
                "outbox relay duration histogram buckets must be greater than zero"
            }
            Self::UnorderedDurationBuckets => {
                "outbox relay duration histogram buckets must be strictly increasing"
            }
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for OutboxRelayMetricsConfigError {}

fn validate_duration_buckets(buckets: &[Duration]) -> Result<(), OutboxRelayMetricsConfigError> {
    if buckets.is_empty() {
        return Err(OutboxRelayMetricsConfigError::EmptyDurationBuckets);
    }
    if buckets.len() > MAX_DURATION_BUCKETS {
        return Err(OutboxRelayMetricsConfigError::TooManyDurationBuckets);
    }
    if buckets.iter().any(Duration::is_zero) {
        return Err(OutboxRelayMetricsConfigError::ZeroDurationBucket);
    }
    if buckets.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(OutboxRelayMetricsConfigError::UnorderedDurationBuckets);
    }
    Ok(())
}

/// Immutable view of metrics collected by [`OutboxRelayMetrics`].
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

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use rustee_outbox_sqlx::{RelayPassKind, RelayPassObservation, RelayPassOutcome, RelayReport};

    use super::{OutboxRelayMetrics, OutboxRelayMetricsConfigError, RelayRowCount};

    #[test]
    fn collector_tracks_reportable_and_abandoned_passes() {
        let metrics = OutboxRelayMetrics::new();
        let observer = Arc::new(metrics.clone());
        RelayPassObservation::start(observer.clone(), RelayPassKind::Job).finish(
            RelayPassOutcome::Succeeded,
            Some(RelayReport {
                claimed: 3,
                published: 2,
                retry_scheduled: 1,
                lease_lost: 0,
            }),
        );
        drop(RelayPassObservation::start(observer, RelayPassKind::Event));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.in_flight(), 0);
        assert_eq!(snapshot.started(), 2);
        assert_eq!(snapshot.completed(), 2);
        assert_eq!(
            snapshot.outcome(RelayPassKind::Job, RelayPassOutcome::Succeeded),
            1
        );
        assert_eq!(
            snapshot.outcome(RelayPassKind::Event, RelayPassOutcome::Abandoned),
            1
        );
        assert_eq!(snapshot.rows(RelayPassKind::Job, RelayRowCount::Claimed), 3);
        assert_eq!(
            snapshot.rows(RelayPassKind::Job, RelayRowCount::Published),
            2
        );
        assert_eq!(
            snapshot.rows(RelayPassKind::Job, RelayRowCount::RetryScheduled),
            1
        );
        assert_eq!(snapshot.duration_bucket_counts().count(), 12);
    }

    #[test]
    fn collector_rejects_unbounded_histogram_configuration() {
        assert_eq!(
            OutboxRelayMetrics::with_duration_buckets(std::iter::empty::<Duration>()).unwrap_err(),
            OutboxRelayMetricsConfigError::EmptyDurationBuckets
        );
        assert_eq!(
            OutboxRelayMetrics::with_duration_buckets([
                Duration::from_secs(2),
                Duration::from_secs(1),
            ])
            .unwrap_err(),
            OutboxRelayMetricsConfigError::UnorderedDurationBuckets
        );
    }
}
