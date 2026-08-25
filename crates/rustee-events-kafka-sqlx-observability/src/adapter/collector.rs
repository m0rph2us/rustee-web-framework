use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use rustee_events_kafka_sqlx::{
    KafkaDelayedRetryBacklog, KafkaDelayedRetryRelayObserver, KafkaDelayedRetryRelayOutcome,
    KafkaDelayedRetryRelayPassFinished, KafkaDelayedRetryRelayPassStarted,
};
use rustee_observability_core::{
    DEFAULT_DURATION_BUCKETS, DurationHistogram, DurationHistogramConfigError,
};

use super::KafkaDelayedRetryRelayMetricsSnapshot;

/// Default cumulative upper bounds for delayed-retry relay pass duration histograms.
pub const DEFAULT_KAFKA_DELAYED_RETRY_RELAY_PASS_DURATION_BUCKETS: [Duration; 12] =
    DEFAULT_DURATION_BUCKETS;

/// Thread-safe exporter-neutral collector for Kafka delayed-retry relay passes.
///
/// Labels are limited to [`KafkaDelayedRetryRelayOutcome`]. Topic names, IDs, payloads, keys,
/// endpoints, retry configuration, and raw database or Kafka errors never enter the collector.
#[derive(Clone, Debug)]
pub struct KafkaDelayedRetryRelayMetrics {
    pub(super) state: Arc<Mutex<KafkaDelayedRetryRelayMetricsState>>,
}

impl Default for KafkaDelayedRetryRelayMetrics {
    fn default() -> Self {
        Self::with_duration_buckets(DEFAULT_KAFKA_DELAYED_RETRY_RELAY_PASS_DURATION_BUCKETS)
            .expect("default delayed-retry relay duration buckets must be valid")
    }
}

impl KafkaDelayedRetryRelayMetrics {
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
    /// Returns [`KafkaDelayedRetryRelayMetricsConfigError`] when the bounds are empty, too
    /// numerous, zero, or not strictly increasing.
    pub fn with_duration_buckets(
        buckets: impl IntoIterator<Item = Duration>,
    ) -> Result<Self, KafkaDelayedRetryRelayMetricsConfigError> {
        let duration_histogram = DurationHistogram::new(buckets)
            .map_err(KafkaDelayedRetryRelayMetricsConfigError::from)?;
        Ok(Self {
            state: Arc::new(Mutex::new(KafkaDelayedRetryRelayMetricsState::new(
                duration_histogram,
            ))),
        })
    }

    /// Records the latest aggregate database backlog snapshot.
    ///
    /// The caller chooses the query timeout and polling cadence by calling
    /// [`PostgresKafkaDelayedRetryRelay::backlog`](rustee_events_kafka_sqlx::PostgresKafkaDelayedRetryRelay::backlog).
    /// This method keeps no history and cannot perform a database operation.
    pub fn record_backlog(&self, backlog: KafkaDelayedRetryBacklog) {
        let mut state = self.lock_state();
        state.backlog = Some(backlog);
    }

    /// Returns a point-in-time snapshot for an exporter or diagnostic endpoint.
    ///
    /// The collector recovers its state after a metrics-only panic so observation never
    /// interrupts delayed-retry processing.
    #[must_use]
    pub fn snapshot(&self) -> KafkaDelayedRetryRelayMetricsSnapshot {
        let state = self.lock_state();
        KafkaDelayedRetryRelayMetricsSnapshot {
            in_flight: state.in_flight,
            started: state.started,
            completed: state.completed,
            outcome_counts: state.outcome_counts.clone(),
            published: state.published,
            backlog: state.backlog,
            duration_bucket_counts: state.duration_histogram.bucket_counts().collect(),
            total_duration: state.duration_histogram.total_duration(),
        }
    }

    fn started(&self) {
        let mut state = self.lock_state();
        state.in_flight = state.in_flight.saturating_add(1);
        state.started = state.started.saturating_add(1);
    }

    fn finished(&self, pass: KafkaDelayedRetryRelayPassFinished) {
        let mut state = self.lock_state();
        state.in_flight = state.in_flight.saturating_sub(1);
        state.completed = state.completed.saturating_add(1);
        let count = state.outcome_counts.entry(pass.outcome()).or_default();
        *count = count.saturating_add(1);
        if let Some(published) = pass.published() {
            state.published = state.published.saturating_add(u64::from(published));
        }
        state.duration_histogram.observe(pass.duration());
    }

    pub(super) fn lock_state(&self) -> MutexGuard<'_, KafkaDelayedRetryRelayMetricsState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl KafkaDelayedRetryRelayObserver for KafkaDelayedRetryRelayMetrics {
    fn on_relay_pass_started(&self, _pass: KafkaDelayedRetryRelayPassStarted) {
        self.started();
    }

    fn on_relay_pass_finished(&self, pass: KafkaDelayedRetryRelayPassFinished) {
        self.finished(pass);
    }
}

#[derive(Debug)]
pub(super) struct KafkaDelayedRetryRelayMetricsState {
    pub(super) in_flight: u64,
    pub(super) started: u64,
    pub(super) completed: u64,
    pub(super) outcome_counts: BTreeMap<KafkaDelayedRetryRelayOutcome, u64>,
    pub(super) published: u64,
    pub(super) backlog: Option<KafkaDelayedRetryBacklog>,
    pub(super) duration_histogram: DurationHistogram,
}

impl KafkaDelayedRetryRelayMetricsState {
    fn new(duration_histogram: DurationHistogram) -> Self {
        Self {
            in_flight: 0,
            started: 0,
            completed: 0,
            outcome_counts: BTreeMap::new(),
            published: 0,
            backlog: None,
            duration_histogram,
        }
    }
}

/// Invalid duration histogram configuration rejected before collection starts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaDelayedRetryRelayMetricsConfigError {
    /// No finite histogram upper bound was configured.
    EmptyDurationBuckets,
    /// More than thirty-two finite histogram upper bounds were configured.
    TooManyDurationBuckets,
    /// A histogram upper bound was zero.
    ZeroDurationBucket,
    /// Histogram upper bounds were duplicated or out of ascending order.
    UnorderedDurationBuckets,
}

impl From<DurationHistogramConfigError> for KafkaDelayedRetryRelayMetricsConfigError {
    fn from(error: DurationHistogramConfigError) -> Self {
        match error {
            DurationHistogramConfigError::EmptyBuckets => Self::EmptyDurationBuckets,
            DurationHistogramConfigError::TooManyBuckets => Self::TooManyDurationBuckets,
            DurationHistogramConfigError::ZeroBucket => Self::ZeroDurationBucket,
            DurationHistogramConfigError::UnorderedBuckets => Self::UnorderedDurationBuckets,
        }
    }
}

impl fmt::Display for KafkaDelayedRetryRelayMetricsConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptyDurationBuckets => "at least one duration histogram bucket is required",
            Self::TooManyDurationBuckets => {
                "Kafka delayed-retry relay duration histogram supports at most 32 finite buckets"
            }
            Self::ZeroDurationBucket => {
                "Kafka delayed-retry relay duration histogram buckets must be greater than zero"
            }
            Self::UnorderedDurationBuckets => {
                "Kafka delayed-retry relay duration histogram buckets must be strictly increasing"
            }
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for KafkaDelayedRetryRelayMetricsConfigError {}
