//! Exporter-neutral metrics for `Rustee` `PostgreSQL` recurring-scheduler passes.
//!
//! The collector implements [`RecurringJobFireObserver`] and is attached to a scheduler with
//! [`PostgresRecurringJobs::with_fire_observer`]. It records terminal outcomes, aggregate row
//! counts, and global duration only. Schedule keys, destinations, payloads, tenant context, and
//! storage errors never enter the collector.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use rustee_jobs_cron_sqlx::{
    RecurringJobFireFinished, RecurringJobFireObserver, RecurringJobFireOutcome,
    RecurringJobFireReport, RecurringJobFireStarted,
};

const MAX_DURATION_BUCKETS: usize = 32;

/// Default cumulative upper bounds for recurring-scheduler pass duration histograms.
pub const DEFAULT_SCHEDULER_FIRE_PASS_DURATION_BUCKETS: [Duration; 12] = [
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

/// Stable names for recurring-scheduler metrics exported by an application adapter.
pub mod metric_names {
    /// Count of scheduler passes whose future started.
    pub const FIRE_PASSES_TOTAL: &str = "rustee_scheduler_fire_passes_total";
    /// Number of scheduler pass futures currently executing in this process.
    pub const FIRE_PASSES_IN_FLIGHT: &str = "rustee_scheduler_fire_passes_in_flight";
    /// Sum of completed scheduler pass durations in seconds.
    pub const FIRE_PASS_DURATION_SECONDS: &str = "rustee_scheduler_fire_pass_duration_seconds";
    /// Count of completed scheduler passes by fixed outcome label.
    pub const FIRE_PASS_OUTCOMES_TOTAL: &str = "rustee_scheduler_fire_pass_outcomes_total";
    /// Aggregate claimed, staged, or rate-limited schedule counts by fixed count label.
    pub const FIRE_ROWS_TOTAL: &str = "rustee_scheduler_fire_rows_total";
}

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

/// Thread-safe, exporter-neutral collector for recurring scheduler passes.
///
/// The pass has no provider, route, schedule, or tenant label. The only dimensions are fixed
/// terminal outcomes and fixed aggregate count names, so repeated schedule registration cannot
/// create new metric series.
#[derive(Clone, Debug)]
pub struct RecurringJobFireMetrics {
    state: Arc<Mutex<RecurringJobFireMetricsState>>,
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
        let buckets = buckets.into_iter().collect::<Vec<_>>();
        validate_duration_buckets(&buckets)?;
        Ok(Self {
            state: Arc::new(Mutex::new(RecurringJobFireMetricsState {
                duration_bucket_counts: vec![0; buckets.len()],
                duration_buckets: buckets,
                ..RecurringJobFireMetricsState::default()
            })),
        })
    }

    /// Returns a point-in-time snapshot for an exporter or readiness diagnostic.
    ///
    /// # Panics
    ///
    /// Panics only if a concurrent metrics update poisoned the internal mutex.
    #[must_use]
    pub fn snapshot(&self) -> RecurringJobFireMetricsSnapshot {
        let state = self
            .state
            .lock()
            .expect("scheduler fire metrics lock must not be poisoned");
        RecurringJobFireMetricsSnapshot {
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
            .expect("scheduler fire metrics lock must not be poisoned");
        state.in_flight = state.in_flight.saturating_add(1);
        state.started = state.started.saturating_add(1);
    }

    fn finished(&self, pass: RecurringJobFireFinished) {
        let mut state = self
            .state
            .lock()
            .expect("scheduler fire metrics lock must not be poisoned");
        state.in_flight = state.in_flight.saturating_sub(1);
        state.completed = state.completed.saturating_add(1);
        *state.outcome_counts.entry(pass.outcome()).or_default() += 1;
        if let Some(report) = pass.report() {
            record_report(&mut state, report);
        }
        let RecurringJobFireMetricsState {
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
        *state.row_counts.entry(count).or_default() += u64::from(value);
    }
}

#[derive(Debug, Default)]
struct RecurringJobFireMetricsState {
    in_flight: u64,
    started: u64,
    completed: u64,
    outcome_counts: BTreeMap<RecurringJobFireOutcome, u64>,
    row_counts: BTreeMap<RecurringJobFireRowCount, u64>,
    duration_buckets: Vec<Duration>,
    duration_bucket_counts: Vec<u64>,
    total_duration: Duration,
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

fn validate_duration_buckets(
    buckets: &[Duration],
) -> Result<(), RecurringJobFireMetricsConfigError> {
    if buckets.is_empty() {
        return Err(RecurringJobFireMetricsConfigError::EmptyDurationBuckets);
    }
    if buckets.len() > MAX_DURATION_BUCKETS {
        return Err(RecurringJobFireMetricsConfigError::TooManyDurationBuckets);
    }
    if buckets.iter().any(Duration::is_zero) {
        return Err(RecurringJobFireMetricsConfigError::ZeroDurationBucket);
    }
    if buckets.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(RecurringJobFireMetricsConfigError::UnorderedDurationBuckets);
    }
    Ok(())
}

/// Immutable view of metrics collected by [`RecurringJobFireMetrics`].
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

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use rustee_jobs_cron_sqlx::{
        RecurringJobFireLimit, RecurringJobFireObservation, RecurringJobFireOutcome,
    };

    use super::{
        RecurringJobFireMetrics, RecurringJobFireMetricsConfigError, RecurringJobFireRowCount,
    };

    #[test]
    fn collector_tracks_reportable_and_abandoned_scheduler_passes() {
        let metrics = RecurringJobFireMetrics::new();
        let observer = Arc::new(metrics.clone());
        RecurringJobFireObservation::start(observer.clone(), RecurringJobFireLimit::default())
            .finish(
                RecurringJobFireOutcome::Succeeded,
                Some(rustee_jobs_cron_sqlx::RecurringJobFireReport::default()),
            );
        drop(RecurringJobFireObservation::start(
            observer,
            RecurringJobFireLimit::default(),
        ));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.in_flight(), 0);
        assert_eq!(snapshot.started(), 2);
        assert_eq!(snapshot.completed(), 2);
        assert_eq!(snapshot.outcome(RecurringJobFireOutcome::Succeeded), 1);
        assert_eq!(snapshot.outcome(RecurringJobFireOutcome::Abandoned), 1);
        assert_eq!(snapshot.rows(RecurringJobFireRowCount::Claimed), 0);
        assert_eq!(snapshot.duration_bucket_counts().count(), 12);
    }

    #[test]
    fn collector_rejects_unbounded_histogram_configuration() {
        assert_eq!(
            RecurringJobFireMetrics::with_duration_buckets(std::iter::empty::<Duration>())
                .unwrap_err(),
            RecurringJobFireMetricsConfigError::EmptyDurationBuckets
        );
        assert_eq!(
            RecurringJobFireMetrics::with_duration_buckets([
                Duration::from_secs(2),
                Duration::from_secs(1),
            ])
            .unwrap_err(),
            RecurringJobFireMetricsConfigError::UnorderedDurationBuckets
        );
    }
}
