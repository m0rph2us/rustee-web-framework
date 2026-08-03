//! Exporter-neutral metrics for `Rustee` durable job workers.
//!
//! The collector implements [`JobDeliveryObserver`] and is attached to a provider worker with its
//! `with_delivery_observer` builder. It records only bounded provider and settlement labels;
//! payloads, job IDs, queue routes, delivery handles, and handler error text never enter this
//! collector.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use rustee_jobs::{
    JobDeliveryFinished, JobDeliveryObserver, JobDeliveryOutcome, JobDeliveryStarted,
};

const MAX_DURATION_BUCKETS: usize = 32;
const MAX_PROVIDER_LABELS: usize = 16;
const OTHER_PROVIDER: &str = "other";

/// Default cumulative upper bounds for job delivery duration histograms.
pub const DEFAULT_JOB_DELIVERY_DURATION_BUCKETS: [Duration; 12] = [
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

/// Stable names for job metrics exported by an application adapter.
pub mod metric_names {
    /// Count of provider deliveries whose worker task started.
    pub const JOB_DELIVERIES_TOTAL: &str = "rustee_job_deliveries_total";
    /// Number of provider deliveries currently executing in this process.
    pub const JOB_DELIVERIES_IN_FLIGHT: &str = "rustee_job_deliveries_in_flight";
    /// Sum of completed worker delivery durations in seconds.
    pub const JOB_DELIVERY_DURATION_SECONDS: &str = "rustee_job_delivery_duration_seconds";
    /// Count of settled and unsettled deliveries by bounded provider and outcome labels.
    pub const JOB_DELIVERY_OUTCOMES_TOTAL: &str = "rustee_job_delivery_outcomes_total";
}

/// Thread-safe, exporter-neutral durable-job metric collector.
///
/// Provider identifiers are implementation constants, but the collector nevertheless retains at
/// most sixteen distinct values and aggregates later values under `other`. Job type, ID, queue
/// name, trace context, and error text are deliberately absent from labels.
#[derive(Clone, Debug)]
pub struct JobMetrics {
    state: Arc<Mutex<JobMetricsState>>,
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
        let buckets = buckets.into_iter().collect::<Vec<_>>();
        validate_duration_buckets(&buckets)?;
        Ok(Self {
            state: Arc::new(Mutex::new(JobMetricsState {
                duration_bucket_counts: vec![0; buckets.len()],
                duration_buckets: buckets,
                ..JobMetricsState::default()
            })),
        })
    }

    /// Returns a point-in-time snapshot for an exporter or readiness diagnostic.
    ///
    /// # Panics
    ///
    /// Panics only if a concurrent metrics update poisoned the internal mutex.
    #[must_use]
    pub fn snapshot(&self) -> JobMetricsSnapshot {
        let state = self
            .state
            .lock()
            .expect("job metrics lock must not be poisoned");
        JobMetricsSnapshot {
            in_flight: state.in_flight,
            started: state.started,
            completed: state.completed,
            outcome_counts: state.outcome_counts.clone(),
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
            .expect("job metrics lock must not be poisoned");
        state.in_flight = state.in_flight.saturating_add(1);
        state.started = state.started.saturating_add(1);
    }

    fn finished(&self, delivery: JobDeliveryFinished) {
        let mut state = self
            .state
            .lock()
            .expect("job metrics lock must not be poisoned");
        state.in_flight = state.in_flight.saturating_sub(1);
        state.completed = state.completed.saturating_add(1);
        let provider = bounded_provider(&mut state, delivery.provider());
        *state
            .outcome_counts
            .entry((provider, delivery.outcome()))
            .or_default() += 1;
        let JobMetricsState {
            duration_buckets,
            duration_bucket_counts,
            ..
        } = &mut *state;
        for (upper_bound, count) in duration_buckets.iter().zip(duration_bucket_counts) {
            if delivery.duration() <= *upper_bound {
                *count = count.saturating_add(1);
            }
        }
        state.total_duration = state.total_duration.saturating_add(delivery.duration());
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

#[derive(Debug, Default)]
struct JobMetricsState {
    in_flight: u64,
    started: u64,
    completed: u64,
    outcome_counts: BTreeMap<(String, JobDeliveryOutcome), u64>,
    duration_buckets: Vec<Duration>,
    duration_bucket_counts: Vec<u64>,
    total_duration: Duration,
}

fn bounded_provider(state: &mut JobMetricsState, provider: &str) -> String {
    if state
        .outcome_counts
        .keys()
        .any(|(observed, _)| observed == provider)
        || state
            .outcome_counts
            .keys()
            .map(|(observed, _)| observed)
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            < MAX_PROVIDER_LABELS
    {
        provider.to_owned()
    } else {
        OTHER_PROVIDER.to_owned()
    }
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

fn validate_duration_buckets(buckets: &[Duration]) -> Result<(), JobMetricsConfigError> {
    if buckets.is_empty() {
        return Err(JobMetricsConfigError::EmptyDurationBuckets);
    }
    if buckets.len() > MAX_DURATION_BUCKETS {
        return Err(JobMetricsConfigError::TooManyDurationBuckets);
    }
    if buckets.iter().any(Duration::is_zero) {
        return Err(JobMetricsConfigError::ZeroDurationBucket);
    }
    if buckets.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(JobMetricsConfigError::UnorderedDurationBuckets);
    }
    Ok(())
}

/// Immutable view of metrics collected by [`JobMetrics`].
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

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU16, sync::Arc, time::Duration};

    use rustee_jobs::{JobDeliveryObservation, JobDeliveryOutcome};

    use super::{JobMetrics, JobMetricsConfigError};

    #[test]
    fn collector_tracks_settlement_and_drop_as_unsettled() {
        let metrics = JobMetrics::new();
        let observer = Arc::new(metrics.clone());
        JobDeliveryObservation::start(observer.clone(), "nats_jetstream")
            .finish(NonZeroU16::new(1), JobDeliveryOutcome::Acknowledged);
        drop(JobDeliveryObservation::start(observer, "nats_jetstream"));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.in_flight(), 0);
        assert_eq!(snapshot.started(), 2);
        assert_eq!(snapshot.completed(), 2);
        assert_eq!(
            snapshot.outcome("nats_jetstream", JobDeliveryOutcome::Acknowledged),
            1
        );
        assert_eq!(
            snapshot.outcome("nats_jetstream", JobDeliveryOutcome::Unsettled),
            1
        );
        assert_eq!(snapshot.duration_bucket_counts().count(), 12);
    }

    #[test]
    fn collector_rejects_unbounded_histogram_configuration() {
        assert_eq!(
            JobMetrics::with_duration_buckets(std::iter::empty::<Duration>()).unwrap_err(),
            JobMetricsConfigError::EmptyDurationBuckets
        );
        assert_eq!(
            JobMetrics::with_duration_buckets([Duration::from_secs(2), Duration::from_secs(1)])
                .unwrap_err(),
            JobMetricsConfigError::UnorderedDurationBuckets
        );
    }
}
