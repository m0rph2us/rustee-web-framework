use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use rustee_observability_core::{
    DEFAULT_DURATION_BUCKETS, DurationHistogram, DurationHistogramConfigError,
};
use rustee_outbox_sqlx::{
    OutboxRelayObserver, RelayPassFinished, RelayPassKind, RelayPassOutcome, RelayPassStarted,
    RelayReport,
};

use super::{OutboxRelayMetricsSnapshot, RelayRowCount};

/// Default cumulative upper bounds for outbox relay pass duration histograms.
pub const DEFAULT_RELAY_PASS_DURATION_BUCKETS: [Duration; 12] = DEFAULT_DURATION_BUCKETS;

/// Thread-safe, exporter-neutral collector for transactional-outbox relay passes.
///
/// Relay kind and outcome are fixed enums. The collector deliberately exposes no dynamic route,
/// provider, tenant, or message labels.
#[derive(Clone, Debug)]
pub struct OutboxRelayMetrics {
    pub(super) state: Arc<Mutex<OutboxRelayMetricsState>>,
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
        let duration_histogram =
            DurationHistogram::new(buckets).map_err(OutboxRelayMetricsConfigError::from)?;
        Ok(Self {
            state: Arc::new(Mutex::new(OutboxRelayMetricsState::new(duration_histogram))),
        })
    }

    /// Returns a point-in-time snapshot for an exporter or readiness diagnostic.
    ///
    /// The collector recovers its state after a metrics-only panic so observation never
    /// interrupts relay processing.
    #[must_use]
    pub fn snapshot(&self) -> OutboxRelayMetricsSnapshot {
        let state = self.lock_state();
        OutboxRelayMetricsSnapshot::from_state(
            state.in_flight,
            state.started,
            state.completed,
            state.outcome_counts.clone(),
            state.row_counts.clone(),
            state.duration_histogram.bucket_counts().collect(),
            state.duration_histogram.total_duration(),
        )
    }

    fn started(&self) {
        let mut state = self.lock_state();
        state.in_flight = state.in_flight.saturating_add(1);
        state.started = state.started.saturating_add(1);
    }

    fn finished(&self, pass: RelayPassFinished) {
        let mut state = self.lock_state();
        state.in_flight = state.in_flight.saturating_sub(1);
        state.completed = state.completed.saturating_add(1);
        let count = state
            .outcome_counts
            .entry((pass.kind(), pass.outcome()))
            .or_default();
        *count = count.saturating_add(1);
        if let Some(report) = pass.report() {
            record_report(&mut state, pass.kind(), report);
        }
        state.duration_histogram.observe(pass.duration());
    }

    pub(super) fn lock_state(&self) -> MutexGuard<'_, OutboxRelayMetricsState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
        let aggregate = state.row_counts.entry((kind, count)).or_default();
        *aggregate = aggregate.saturating_add(value as u64);
    }
}

#[derive(Debug)]
pub(super) struct OutboxRelayMetricsState {
    pub(super) in_flight: u64,
    pub(super) started: u64,
    pub(super) completed: u64,
    pub(super) outcome_counts: BTreeMap<(RelayPassKind, RelayPassOutcome), u64>,
    pub(super) row_counts: BTreeMap<(RelayPassKind, RelayRowCount), u64>,
    pub(super) duration_histogram: DurationHistogram,
}

impl OutboxRelayMetricsState {
    fn new(duration_histogram: DurationHistogram) -> Self {
        Self {
            in_flight: 0,
            started: 0,
            completed: 0,
            outcome_counts: BTreeMap::new(),
            row_counts: BTreeMap::new(),
            duration_histogram,
        }
    }
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

impl From<DurationHistogramConfigError> for OutboxRelayMetricsConfigError {
    fn from(error: DurationHistogramConfigError) -> Self {
        match error {
            DurationHistogramConfigError::EmptyBuckets => Self::EmptyDurationBuckets,
            DurationHistogramConfigError::TooManyBuckets => Self::TooManyDurationBuckets,
            DurationHistogramConfigError::ZeroBucket => Self::ZeroDurationBucket,
            DurationHistogramConfigError::UnorderedBuckets => Self::UnorderedDurationBuckets,
        }
    }
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
