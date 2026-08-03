//! Exporter-neutral metrics for `Rustee` event-stream consumers.
//!
//! The collector implements [`EventDeliveryObserver`] and is attached to a provider consumer
//! with its `with_delivery_observer` builder. It records only bounded provider and settlement
//! labels; payloads, event identifiers, topics, partitions, consumer groups, offsets, keys, and
//! handler error text never enter this collector.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use rustee_events::{
    EventDeliveryFinished, EventDeliveryObserver, EventDeliveryOutcome, EventDeliveryStarted,
};

const MAX_DURATION_BUCKETS: usize = 32;
const MAX_PROVIDER_LABELS: usize = 16;
const OTHER_PROVIDER: &str = "other";

/// Default cumulative upper bounds for event-delivery duration histograms.
pub const DEFAULT_EVENT_DELIVERY_DURATION_BUCKETS: [Duration; 12] = [
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

/// Stable names for event-delivery metrics exported by an application adapter.
pub mod metric_names {
    /// Count of provider deliveries whose consumer task started.
    pub const EVENT_DELIVERIES_TOTAL: &str = "rustee_event_deliveries_total";
    /// Number of provider deliveries currently executing in this process.
    pub const EVENT_DELIVERIES_IN_FLIGHT: &str = "rustee_event_deliveries_in_flight";
    /// Sum of completed consumer delivery durations in seconds.
    pub const EVENT_DELIVERY_DURATION_SECONDS: &str = "rustee_event_delivery_duration_seconds";
    /// Count of settled and unsettled deliveries by bounded provider and outcome labels.
    pub const EVENT_DELIVERY_OUTCOMES_TOTAL: &str = "rustee_event_delivery_outcomes_total";
}

/// Thread-safe, exporter-neutral event-delivery metric collector.
///
/// Provider identifiers are implementation constants, but the collector nevertheless retains at
/// most sixteen distinct values and aggregates later values under `other`. Event type, ID,
/// topic, partition, offset, key, trace context, and error text are deliberately absent from
/// labels.
#[derive(Clone, Debug)]
pub struct EventMetrics {
    state: Arc<Mutex<EventMetricsState>>,
}

impl Default for EventMetrics {
    fn default() -> Self {
        Self::with_duration_buckets(DEFAULT_EVENT_DELIVERY_DURATION_BUCKETS)
            .expect("default event delivery duration buckets must be valid")
    }
}

impl EventMetrics {
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
    /// Returns [`EventMetricsConfigError`] when the bounds are empty, too numerous, zero, or not
    /// strictly increasing.
    pub fn with_duration_buckets(
        buckets: impl IntoIterator<Item = Duration>,
    ) -> Result<Self, EventMetricsConfigError> {
        let buckets = buckets.into_iter().collect::<Vec<_>>();
        validate_duration_buckets(&buckets)?;
        Ok(Self {
            state: Arc::new(Mutex::new(EventMetricsState {
                duration_bucket_counts: vec![0; buckets.len()],
                duration_buckets: buckets,
                ..EventMetricsState::default()
            })),
        })
    }

    /// Returns a point-in-time snapshot for an exporter or readiness diagnostic.
    ///
    /// # Panics
    ///
    /// Panics only if a concurrent metrics update poisoned the internal mutex.
    #[must_use]
    pub fn snapshot(&self) -> EventMetricsSnapshot {
        let state = self
            .state
            .lock()
            .expect("event metrics lock must not be poisoned");
        EventMetricsSnapshot {
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
            .expect("event metrics lock must not be poisoned");
        state.in_flight = state.in_flight.saturating_add(1);
        state.started = state.started.saturating_add(1);
    }

    fn finished(&self, delivery: EventDeliveryFinished) {
        let mut state = self
            .state
            .lock()
            .expect("event metrics lock must not be poisoned");
        state.in_flight = state.in_flight.saturating_sub(1);
        state.completed = state.completed.saturating_add(1);
        let provider = bounded_provider(&mut state, delivery.provider());
        *state
            .outcome_counts
            .entry((provider, delivery.outcome()))
            .or_default() += 1;
        let EventMetricsState {
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

impl EventDeliveryObserver for EventMetrics {
    fn on_delivery_started(&self, _delivery: EventDeliveryStarted) {
        self.started();
    }

    fn on_delivery_finished(&self, delivery: EventDeliveryFinished) {
        self.finished(delivery);
    }
}

#[derive(Debug, Default)]
struct EventMetricsState {
    in_flight: u64,
    started: u64,
    completed: u64,
    outcome_counts: BTreeMap<(String, EventDeliveryOutcome), u64>,
    duration_buckets: Vec<Duration>,
    duration_bucket_counts: Vec<u64>,
    total_duration: Duration,
}

fn bounded_provider(state: &mut EventMetricsState, provider: &str) -> String {
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
pub enum EventMetricsConfigError {
    /// No finite histogram upper bound was configured.
    EmptyDurationBuckets,
    /// More than thirty-two finite histogram upper bounds were configured.
    TooManyDurationBuckets,
    /// A histogram upper bound was zero.
    ZeroDurationBucket,
    /// Histogram upper bounds were duplicated or out of ascending order.
    UnorderedDurationBuckets,
}

impl fmt::Display for EventMetricsConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptyDurationBuckets => "at least one duration histogram bucket is required",
            Self::TooManyDurationBuckets => {
                "event delivery duration histogram supports at most 32 finite buckets"
            }
            Self::ZeroDurationBucket => {
                "event delivery duration histogram buckets must be greater than zero"
            }
            Self::UnorderedDurationBuckets => {
                "event delivery duration histogram buckets must be strictly increasing"
            }
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for EventMetricsConfigError {}

fn validate_duration_buckets(buckets: &[Duration]) -> Result<(), EventMetricsConfigError> {
    if buckets.is_empty() {
        return Err(EventMetricsConfigError::EmptyDurationBuckets);
    }
    if buckets.len() > MAX_DURATION_BUCKETS {
        return Err(EventMetricsConfigError::TooManyDurationBuckets);
    }
    if buckets.iter().any(Duration::is_zero) {
        return Err(EventMetricsConfigError::ZeroDurationBucket);
    }
    if buckets.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(EventMetricsConfigError::UnorderedDurationBuckets);
    }
    Ok(())
}

/// Immutable view of metrics collected by [`EventMetrics`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventMetricsSnapshot {
    in_flight: u64,
    started: u64,
    completed: u64,
    outcome_counts: BTreeMap<(String, EventDeliveryOutcome), u64>,
    duration_bucket_counts: Vec<(Duration, u64)>,
    total_duration: Duration,
}

impl EventMetricsSnapshot {
    /// Returns deliveries currently executing.
    #[must_use]
    pub const fn in_flight(&self) -> u64 {
        self.in_flight
    }

    /// Returns deliveries whose consumer task started.
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
    pub fn outcome(&self, provider: &str, outcome: EventDeliveryOutcome) -> u64 {
        self.outcome_counts
            .get(&(provider.to_owned(), outcome))
            .copied()
            .unwrap_or(0)
    }

    /// Iterates outcome counts in stable provider/outcome order.
    pub fn outcome_counts(&self) -> impl Iterator<Item = (&str, EventDeliveryOutcome, u64)> + '_ {
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

    use rustee_events::{EventDeliveryObservation, EventDeliveryOutcome};

    use super::{EventMetrics, EventMetricsConfigError};

    #[test]
    fn collector_tracks_settlement_and_drop_as_unsettled() {
        let metrics = EventMetrics::new();
        let observer = Arc::new(metrics.clone());
        EventDeliveryObservation::start(observer.clone(), "apache_kafka")
            .finish(NonZeroU16::new(1), EventDeliveryOutcome::Acknowledged);
        drop(EventDeliveryObservation::start(observer, "apache_kafka"));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.in_flight(), 0);
        assert_eq!(snapshot.started(), 2);
        assert_eq!(snapshot.completed(), 2);
        assert_eq!(
            snapshot.outcome("apache_kafka", EventDeliveryOutcome::Acknowledged),
            1
        );
        assert_eq!(
            snapshot.outcome("apache_kafka", EventDeliveryOutcome::Unsettled),
            1
        );
        assert_eq!(snapshot.duration_bucket_counts().count(), 12);
    }

    #[test]
    fn collector_rejects_unbounded_histogram_configuration() {
        assert_eq!(
            EventMetrics::with_duration_buckets(std::iter::empty::<Duration>()).unwrap_err(),
            EventMetricsConfigError::EmptyDurationBuckets
        );
        assert_eq!(
            EventMetrics::with_duration_buckets([Duration::from_secs(2), Duration::from_secs(1)])
                .unwrap_err(),
            EventMetricsConfigError::UnorderedDurationBuckets
        );
    }
}
