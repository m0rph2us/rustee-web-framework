use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use rustee_jobs_cron_sqlx::{
    RecurringJobFireFinished, RecurringJobFireObserver, RecurringJobFireOutcome,
    RecurringJobFireReport, RecurringJobFireStarted,
};
use rustee_observability_core::{
    DEFAULT_DURATION_BUCKETS, DurationHistogram, DurationHistogramConfigError,
};

use super::{RecurringJobFireMetricsSnapshot, RecurringJobFireRowCount};

/// Default cumulative upper bounds for recurring-scheduler pass duration histograms.
pub const DEFAULT_SCHEDULER_FIRE_PASS_DURATION_BUCKETS: [Duration; 12] = DEFAULT_DURATION_BUCKETS;

/// Thread-safe, exporter-neutral collector for recurring scheduler passes.
///
/// The pass has no provider, route, schedule, or tenant label. The only dimensions are fixed
/// terminal outcomes and fixed aggregate count names, so repeated schedule registration cannot
/// create new metric series.
#[derive(Clone, Debug)]
pub struct RecurringJobFireMetrics {
    pub(super) state: Arc<Mutex<RecurringJobFireMetricsState>>,
}

impl Default for RecurringJobFireMetrics {
    fn default() -> Self {
        Self::with_duration_buckets(DEFAULT_SCHEDULER_FIRE_PASS_DURATION_BUCKETS)
            .expect("default scheduler fire-pass duration buckets must be valid")
    }
}

impl RecurringJobFireMetrics {
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
    /// Returns [`RecurringJobFireMetricsConfigError`] when the bounds are empty, too numerous,
    /// zero, or not strictly increasing.
    pub fn with_duration_buckets(
        buckets: impl IntoIterator<Item = Duration>,
    ) -> Result<Self, RecurringJobFireMetricsConfigError> {
        let duration_histogram =
            DurationHistogram::new(buckets).map_err(RecurringJobFireMetricsConfigError::from)?;
        Ok(Self {
            state: Arc::new(Mutex::new(RecurringJobFireMetricsState::new(
                duration_histogram,
            ))),
        })
    }

    /// Returns a point-in-time snapshot for an exporter or readiness diagnostic.
    ///
    /// The collector recovers its state after a metrics-only panic so observation never
    /// interrupts scheduler processing.
    #[must_use]
    pub fn snapshot(&self) -> RecurringJobFireMetricsSnapshot {
        let state = self.lock_state();
        RecurringJobFireMetricsSnapshot::from_state(
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

    fn finished(&self, pass: RecurringJobFireFinished) {
        let mut state = self.lock_state();
        state.in_flight = state.in_flight.saturating_sub(1);
        state.completed = state.completed.saturating_add(1);
        let count = state.outcome_counts.entry(pass.outcome()).or_default();
        *count = count.saturating_add(1);
        if let Some(report) = pass.report() {
            record_report(&mut state, report);
        }
        state.duration_histogram.observe(pass.duration());
    }

    pub(super) fn lock_state(&self) -> MutexGuard<'_, RecurringJobFireMetricsState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl RecurringJobFireObserver for RecurringJobFireMetrics {
    fn on_fire_started(&self, _pass: RecurringJobFireStarted) {
        self.started();
    }

    fn on_fire_finished(&self, pass: RecurringJobFireFinished) {
        self.finished(pass);
    }
}

fn record_report(state: &mut RecurringJobFireMetricsState, report: RecurringJobFireReport) {
    for (count, value) in [
        (RecurringJobFireRowCount::Claimed, report.claimed()),
        (RecurringJobFireRowCount::Staged, report.staged()),
        (RecurringJobFireRowCount::RateLimited, report.rate_limited()),
    ] {
        let aggregate = state.row_counts.entry(count).or_default();
        *aggregate = aggregate.saturating_add(u64::from(value));
    }
}

#[derive(Debug)]
pub(super) struct RecurringJobFireMetricsState {
    pub(super) in_flight: u64,
    pub(super) started: u64,
    pub(super) completed: u64,
    pub(super) outcome_counts: BTreeMap<RecurringJobFireOutcome, u64>,
    pub(super) row_counts: BTreeMap<RecurringJobFireRowCount, u64>,
    pub(super) duration_histogram: DurationHistogram,
}

impl RecurringJobFireMetricsState {
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
pub enum RecurringJobFireMetricsConfigError {
    /// No finite histogram upper bound was configured.
    EmptyDurationBuckets,
    /// More than thirty-two finite histogram upper bounds were configured.
    TooManyDurationBuckets,
    /// A histogram upper bound was zero.
    ZeroDurationBucket,
    /// Histogram upper bounds were duplicated or out of ascending order.
    UnorderedDurationBuckets,
}

impl From<DurationHistogramConfigError> for RecurringJobFireMetricsConfigError {
    fn from(error: DurationHistogramConfigError) -> Self {
        match error {
            DurationHistogramConfigError::EmptyBuckets => Self::EmptyDurationBuckets,
            DurationHistogramConfigError::TooManyBuckets => Self::TooManyDurationBuckets,
            DurationHistogramConfigError::ZeroBucket => Self::ZeroDurationBucket,
            DurationHistogramConfigError::UnorderedBuckets => Self::UnorderedDurationBuckets,
        }
    }
}

impl fmt::Display for RecurringJobFireMetricsConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptyDurationBuckets => "at least one duration histogram bucket is required",
            Self::TooManyDurationBuckets => {
                "scheduler fire-pass duration histogram supports at most 32 finite buckets"
            }
            Self::ZeroDurationBucket => {
                "scheduler fire-pass duration histogram buckets must be greater than zero"
            }
            Self::UnorderedDurationBuckets => {
                "scheduler fire-pass duration histogram buckets must be strictly increasing"
            }
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for RecurringJobFireMetricsConfigError {}
