//! Prometheus text exposition for Rustee Kafka `PostgreSQL` delayed-retry relay metrics.
//!
//! Enable the `rdkafka` feature to use this adapter. It has no registry, global state, listener,
//! query loop, or automatic route. An application owns a `KafkaDelayedRetryRelayMetrics`
//! collector and explicitly mounts `metrics_response` where its scrape policy permits.

#[cfg(feature = "rdkafka")]
mod adapter;

#[cfg(feature = "rdkafka")]
pub use adapter::{CONTENT_TYPE_PROMETHEUS, encode, encode_snapshot, metrics_response};
