//! Exporter-neutral metrics for Rustee Kafka `PostgreSQL` delayed-retry relays.
//!
//! Enable the `rdkafka` feature to use the collector. [`KafkaDelayedRetryRelayMetrics`] implements
//! [`KafkaDelayedRetryRelayObserver`] and attaches to a relay with its
//! `with_relay_observer` builder. Applications explicitly poll and record the aggregate-only
//! [`KafkaDelayedRetryBacklog`] snapshot; this crate creates no query task, registry, listener, or
//! alert policy.

#[cfg(feature = "rdkafka")]
mod adapter {
    use std::{
        collections::BTreeMap,
        fmt,
        sync::{Arc, Mutex},
        time::Duration,
    };

    pub use rustee_events_kafka_sqlx::{
        KafkaDelayedRetryBacklog, KafkaDelayedRetryRelayObserver, KafkaDelayedRetryRelayOutcome,
        KafkaDelayedRetryRelayPassFinished, KafkaDelayedRetryRelayPassObservation,
        KafkaDelayedRetryRelayPassStarted,
    };

    const MAX_DURATION_BUCKETS: usize = 32;

    /// Default cumulative upper bounds for delayed-retry relay pass duration histograms.
    pub const DEFAULT_KAFKA_DELAYED_RETRY_RELAY_PASS_DURATION_BUCKETS: [Duration; 12] = [
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

    /// Stable names for delayed-retry metrics exported by an application adapter.
    pub mod metric_names {
        /// Count of delayed-retry relay passes whose future started.
        pub const RELAY_PASSES_TOTAL: &str = "rustee_kafka_delayed_retry_relay_passes_total";
        /// Number of delayed-retry relay pass futures currently executing in this process.
        pub const RELAY_PASSES_IN_FLIGHT: &str =
            "rustee_kafka_delayed_retry_relay_passes_in_flight";
        /// Count of delayed-retry relay pass terminal outcomes by fixed outcome label.
        pub const RELAY_PASS_OUTCOMES_TOTAL: &str =
            "rustee_kafka_delayed_retry_relay_pass_outcomes_total";
        /// Records confirmed after Kafka acknowledgement in fully successful relay passes.
        pub const RELAY_PUBLISHED_TOTAL: &str = "rustee_kafka_delayed_retry_relay_published_total";
        /// Sum of completed delayed-retry relay pass durations in seconds.
        pub const RELAY_PASS_DURATION_SECONDS: &str =
            "rustee_kafka_delayed_retry_relay_pass_duration_seconds";
        /// Latest database-derived delayed-retry backlog row counts by fixed state label.
        pub const BACKLOG_ROWS: &str = "rustee_kafka_delayed_retry_backlog_rows";
        /// Latest database-derived age in seconds of the oldest due delayed-retry row.
        pub const OLDEST_DUE_SECONDS: &str = "rustee_kafka_delayed_retry_oldest_due_seconds";
    }

    /// Thread-safe exporter-neutral collector for Kafka delayed-retry relay passes.
    ///
    /// Labels are limited to [`KafkaDelayedRetryRelayOutcome`]. Topic names, IDs, payloads, keys,
    /// endpoints, retry configuration, and raw database or Kafka errors never enter the collector.
    #[derive(Clone, Debug)]
    pub struct KafkaDelayedRetryRelayMetrics {
        state: Arc<Mutex<KafkaDelayedRetryRelayMetricsState>>,
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
            let buckets = buckets.into_iter().collect::<Vec<_>>();
            validate_duration_buckets(&buckets)?;
            Ok(Self {
                state: Arc::new(Mutex::new(KafkaDelayedRetryRelayMetricsState {
                    duration_bucket_counts: vec![0; buckets.len()],
                    duration_buckets: buckets,
                    ..KafkaDelayedRetryRelayMetricsState::default()
                })),
            })
        }

        /// Records the latest aggregate database backlog snapshot.
        ///
        /// The caller chooses the query timeout and polling cadence by calling
        /// [`PostgresKafkaDelayedRetryRelay::backlog`](rustee_events_kafka_sqlx::PostgresKafkaDelayedRetryRelay::backlog).
        /// This method keeps no history and cannot perform a database operation.
        ///
        /// # Panics
        ///
        /// Panics only if a concurrent metrics update poisoned the internal mutex.
        pub fn record_backlog(&self, backlog: KafkaDelayedRetryBacklog) {
            let mut state = self
                .state
                .lock()
                .expect("Kafka delayed-retry relay metrics lock must not be poisoned");
            state.backlog = Some(backlog);
        }

        /// Returns a point-in-time snapshot for an exporter or diagnostic endpoint.
        ///
        /// # Panics
        ///
        /// Panics only if a concurrent metrics update poisoned the internal mutex.
        #[must_use]
        pub fn snapshot(&self) -> KafkaDelayedRetryRelayMetricsSnapshot {
            let state = self
                .state
                .lock()
                .expect("Kafka delayed-retry relay metrics lock must not be poisoned");
            KafkaDelayedRetryRelayMetricsSnapshot {
                in_flight: state.in_flight,
                started: state.started,
                completed: state.completed,
                outcome_counts: state.outcome_counts.clone(),
                published: state.published,
                backlog: state.backlog,
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
                .expect("Kafka delayed-retry relay metrics lock must not be poisoned");
            state.in_flight = state.in_flight.saturating_add(1);
            state.started = state.started.saturating_add(1);
        }

        fn finished(&self, pass: KafkaDelayedRetryRelayPassFinished) {
            let mut state = self
                .state
                .lock()
                .expect("Kafka delayed-retry relay metrics lock must not be poisoned");
            state.in_flight = state.in_flight.saturating_sub(1);
            state.completed = state.completed.saturating_add(1);
            *state.outcome_counts.entry(pass.outcome()).or_default() += 1;
            if let Some(published) = pass.published() {
                state.published = state.published.saturating_add(u64::from(published));
            }
            let KafkaDelayedRetryRelayMetricsState {
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

    impl KafkaDelayedRetryRelayObserver for KafkaDelayedRetryRelayMetrics {
        fn on_relay_pass_started(&self, _pass: KafkaDelayedRetryRelayPassStarted) {
            self.started();
        }

        fn on_relay_pass_finished(&self, pass: KafkaDelayedRetryRelayPassFinished) {
            self.finished(pass);
        }
    }

    #[derive(Debug, Default)]
    struct KafkaDelayedRetryRelayMetricsState {
        in_flight: u64,
        started: u64,
        completed: u64,
        outcome_counts: BTreeMap<KafkaDelayedRetryRelayOutcome, u64>,
        published: u64,
        backlog: Option<KafkaDelayedRetryBacklog>,
        duration_buckets: Vec<Duration>,
        duration_bucket_counts: Vec<u64>,
        total_duration: Duration,
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

    fn validate_duration_buckets(
        buckets: &[Duration],
    ) -> Result<(), KafkaDelayedRetryRelayMetricsConfigError> {
        if buckets.is_empty() {
            return Err(KafkaDelayedRetryRelayMetricsConfigError::EmptyDurationBuckets);
        }
        if buckets.len() > MAX_DURATION_BUCKETS {
            return Err(KafkaDelayedRetryRelayMetricsConfigError::TooManyDurationBuckets);
        }
        if buckets.iter().any(Duration::is_zero) {
            return Err(KafkaDelayedRetryRelayMetricsConfigError::ZeroDurationBucket);
        }
        if buckets.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(KafkaDelayedRetryRelayMetricsConfigError::UnorderedDurationBuckets);
        }
        Ok(())
    }

    /// Immutable view of metrics collected by [`KafkaDelayedRetryRelayMetrics`].
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct KafkaDelayedRetryRelayMetricsSnapshot {
        in_flight: u64,
        started: u64,
        completed: u64,
        outcome_counts: BTreeMap<KafkaDelayedRetryRelayOutcome, u64>,
        published: u64,
        backlog: Option<KafkaDelayedRetryBacklog>,
        duration_bucket_counts: Vec<(Duration, u64)>,
        total_duration: Duration,
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

    #[cfg(test)]
    mod tests {
        use std::{sync::Arc, time::Duration};

        use rustee_events_kafka_sqlx::{
            KafkaDelayedRetryBacklog, KafkaDelayedRetryRelayObserver,
            KafkaDelayedRetryRelayOutcome, KafkaDelayedRetryRelayPassObservation,
        };

        use super::{KafkaDelayedRetryRelayMetrics, KafkaDelayedRetryRelayMetricsConfigError};

        #[test]
        fn collector_tracks_success_abandonment_and_latest_backlog() {
            let metrics = KafkaDelayedRetryRelayMetrics::new();
            let observer: Arc<dyn KafkaDelayedRetryRelayObserver> = Arc::new(metrics.clone());
            KafkaDelayedRetryRelayPassObservation::start(Arc::clone(&observer))
                .finish(KafkaDelayedRetryRelayOutcome::Succeeded, Some(3));
            drop(KafkaDelayedRetryRelayPassObservation::start(observer));
            metrics.record_backlog(KafkaDelayedRetryBacklog {
                unpublished: 8,
                due: 5,
                leased: 2,
                oldest_due_age: Some(Duration::from_secs(7)),
            });

            let snapshot = metrics.snapshot();
            assert_eq!(snapshot.started(), 2);
            assert_eq!(snapshot.in_flight(), 0);
            assert_eq!(snapshot.completed(), 2);
            assert_eq!(
                snapshot.outcome(KafkaDelayedRetryRelayOutcome::Succeeded),
                1
            );
            assert_eq!(
                snapshot.outcome(KafkaDelayedRetryRelayOutcome::Abandoned),
                1
            );
            assert_eq!(snapshot.published(), 3);
            assert_eq!(snapshot.backlog().unwrap().due, 5);
            assert!(snapshot.total_duration() < Duration::from_secs(1));
        }

        #[test]
        fn collector_rejects_unbounded_duration_configuration() {
            assert!(matches!(
                KafkaDelayedRetryRelayMetrics::with_duration_buckets([]),
                Err(KafkaDelayedRetryRelayMetricsConfigError::EmptyDurationBuckets)
            ));
            assert!(matches!(
                KafkaDelayedRetryRelayMetrics::with_duration_buckets([Duration::ZERO]),
                Err(KafkaDelayedRetryRelayMetricsConfigError::ZeroDurationBucket)
            ));
            assert!(matches!(
                KafkaDelayedRetryRelayMetrics::with_duration_buckets([
                    Duration::from_secs(2),
                    Duration::from_secs(1)
                ]),
                Err(KafkaDelayedRetryRelayMetricsConfigError::UnorderedDurationBuckets)
            ));
        }
    }
}

#[cfg(feature = "rdkafka")]
pub use adapter::*;
