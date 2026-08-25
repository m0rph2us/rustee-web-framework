use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use rustee_jobs::{
    JobDeliveryFinished, JobDeliveryObserver, JobDeliveryOutcome, JobDeliveryStarted,
};
use rustee_observability_core::{
    DEFAULT_DURATION_BUCKETS, DurationHistogram, DurationHistogramConfigError,
};

use super::JobMetricsSnapshot;

pub(super) const MAX_PROVIDER_LABELS: usize = 16;
const MAX_DIRECT_PROVIDER_LABELS: usize = MAX_PROVIDER_LABELS - 1;
pub(super) const OTHER_PROVIDER: &str = "other";

/// Default cumulative upper bounds for job delivery duration histograms.
pub const DEFAULT_JOB_DELIVERY_DURATION_BUCKETS: [Duration; 12] = DEFAULT_DURATION_BUCKETS;

/// Thread-safe, exporter-neutral durable-job metric collector.
///
/// Provider identifiers are implementation constants, but the collector nevertheless retains at
/// most fifteen direct values and aggregates later values under `other`, keeping at most sixteen
/// provider labels. Job type, ID, queue name, trace context, and error text are deliberately
/// absent from labels.
#[derive(Clone, Debug)]
pub struct JobMetrics {
    pub(super) state: Arc<Mutex<JobMetricsState>>,
}

impl Default for JobMetrics {
    fn default() -> Self {
        Self::with_duration_buckets(DEFAULT_JOB_DELIVERY_DURATION_BUCKETS)
            .expect("default job delivery duration buckets must be valid")
    }
}

impl JobMetrics {
    /// Creates an empty collector with the default duration buckets.
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
    /// Returns [`JobMetricsConfigError`] when the bounds are empty, too numerous, zero, or not
    /// strictly increasing.
    pub fn with_duration_buckets(
        buckets: impl IntoIterator<Item = Duration>,
    ) -> Result<Self, JobMetricsConfigError> {
        let duration_histogram =
            DurationHistogram::new(buckets).map_err(JobMetricsConfigError::from)?;
        Ok(Self {
            state: Arc::new(Mutex::new(JobMetricsState::new(duration_histogram))),
        })
    }

    /// Returns a point-in-time snapshot for an exporter or readiness diagnostic.
    ///
    /// The collector recovers its state after a metrics-only panic so observation never
    /// interrupts job processing.
    #[must_use]
    pub fn snapshot(&self) -> JobMetricsSnapshot {
        let state = self.lock_state();
        JobMetricsSnapshot::from_state(
            state.in_flight,
            state.started,
            state.completed,
            state.outcome_counts.clone(),
            state.duration_histogram.bucket_counts().collect(),
            state.duration_histogram.total_duration(),
        )
    }

    fn started(&self) {
        let mut state = self.lock_state();
        state.in_flight = state.in_flight.saturating_add(1);
        state.started = state.started.saturating_add(1);
    }

    fn finished(&self, delivery: JobDeliveryFinished) {
        let mut state = self.lock_state();
        state.in_flight = state.in_flight.saturating_sub(1);
        state.completed = state.completed.saturating_add(1);
        let provider = bounded_provider(&mut state, delivery.provider());
        let count = state
            .outcome_counts
            .entry((provider, delivery.outcome()))
            .or_default();
        *count = count.saturating_add(1);
        state.duration_histogram.observe(delivery.duration());
    }

    pub(super) fn lock_state(&self) -> MutexGuard<'_, JobMetricsState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl JobDeliveryObserver for JobMetrics {
    fn on_delivery_started(&self, _delivery: JobDeliveryStarted) {
        self.started();
    }

    fn on_delivery_finished(&self, delivery: JobDeliveryFinished) {
        self.finished(delivery);
    }
}

#[derive(Debug)]
pub(super) struct JobMetricsState {
    pub(super) in_flight: u64,
    pub(super) started: u64,
    pub(super) completed: u64,
    direct_providers: BTreeSet<String>,
    pub(super) outcome_counts: BTreeMap<(String, JobDeliveryOutcome), u64>,
    pub(super) duration_histogram: DurationHistogram,
}

impl JobMetricsState {
    fn new(duration_histogram: DurationHistogram) -> Self {
        Self {
            in_flight: 0,
            started: 0,
            completed: 0,
            direct_providers: BTreeSet::new(),
            outcome_counts: BTreeMap::new(),
            duration_histogram,
        }
    }
}

fn bounded_provider(state: &mut JobMetricsState, provider: &str) -> String {
    if provider == OTHER_PROVIDER {
        return OTHER_PROVIDER.to_owned();
    }
    if state.direct_providers.contains(provider) {
        return provider.to_owned();
    }
    // Reserve one of the bounded labels for later aggregation before it is needed.
    if state.direct_providers.len() < MAX_DIRECT_PROVIDER_LABELS {
        let provider = provider.to_owned();
        state.direct_providers.insert(provider.clone());
        return provider;
    }
    OTHER_PROVIDER.to_owned()
}

/// Invalid duration histogram configuration rejected before collection starts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobMetricsConfigError {
    /// No finite histogram upper bound was configured.
    EmptyDurationBuckets,
    /// More than thirty-two finite histogram upper bounds were configured.
    TooManyDurationBuckets,
    /// A histogram upper bound was zero.
    ZeroDurationBucket,
    /// Histogram upper bounds were duplicated or out of ascending order.
    UnorderedDurationBuckets,
}

impl From<DurationHistogramConfigError> for JobMetricsConfigError {
    fn from(error: DurationHistogramConfigError) -> Self {
        match error {
            DurationHistogramConfigError::EmptyBuckets => Self::EmptyDurationBuckets,
            DurationHistogramConfigError::TooManyBuckets => Self::TooManyDurationBuckets,
            DurationHistogramConfigError::ZeroBucket => Self::ZeroDurationBucket,
            DurationHistogramConfigError::UnorderedBuckets => Self::UnorderedDurationBuckets,
        }
    }
}

impl fmt::Display for JobMetricsConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptyDurationBuckets => "at least one duration histogram bucket is required",
            Self::TooManyDurationBuckets => {
                "job delivery duration histogram supports at most 32 finite buckets"
            }
            Self::ZeroDurationBucket => {
                "job delivery duration histogram buckets must be greater than zero"
            }
            Self::UnorderedDurationBuckets => {
                "job delivery duration histogram buckets must be strictly increasing"
            }
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for JobMetricsConfigError {}
