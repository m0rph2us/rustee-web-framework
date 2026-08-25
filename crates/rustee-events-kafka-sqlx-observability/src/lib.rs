//! Exporter-neutral metrics for Rustee Kafka `PostgreSQL` delayed-retry relays.
//!
//! Enable the `rdkafka` feature to use the collector. `KafkaDelayedRetryRelayMetrics` implements
//! `KafkaDelayedRetryRelayObserver` and attaches to a relay with its
//! `with_relay_observer` builder. Applications explicitly poll and record the aggregate-only
//! `KafkaDelayedRetryBacklog` snapshot; this crate creates no query task, registry, listener, or
//! alert policy.

#[cfg(feature = "rdkafka")]
mod adapter;

#[cfg(feature = "rdkafka")]
pub use adapter::{
    DEFAULT_KAFKA_DELAYED_RETRY_RELAY_PASS_DURATION_BUCKETS, KafkaDelayedRetryBacklog,
    KafkaDelayedRetryRelayMetrics, KafkaDelayedRetryRelayMetricsConfigError,
    KafkaDelayedRetryRelayMetricsSnapshot, KafkaDelayedRetryRelayObserver,
    KafkaDelayedRetryRelayOutcome, KafkaDelayedRetryRelayPassFinished,
    KafkaDelayedRetryRelayPassObservation, KafkaDelayedRetryRelayPassStarted, metric_names,
};
