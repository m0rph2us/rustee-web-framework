pub use rustee_events_kafka_sqlx::{
    KafkaDelayedRetryBacklog, KafkaDelayedRetryRelayObserver, KafkaDelayedRetryRelayOutcome,
    KafkaDelayedRetryRelayPassFinished, KafkaDelayedRetryRelayPassObservation,
    KafkaDelayedRetryRelayPassStarted,
};

mod collector;
mod model;

pub use collector::{
    DEFAULT_KAFKA_DELAYED_RETRY_RELAY_PASS_DURATION_BUCKETS, KafkaDelayedRetryRelayMetrics,
    KafkaDelayedRetryRelayMetricsConfigError,
};
pub use model::KafkaDelayedRetryRelayMetricsSnapshot;

/// Stable names for delayed-retry metrics exported by an application adapter.
pub mod metric_names {
    /// Count of delayed-retry relay passes whose future started.
    pub const RELAY_PASSES_TOTAL: &str = "rustee_kafka_delayed_retry_relay_passes_total";
    /// Number of delayed-retry relay pass futures currently executing in this process.
    pub const RELAY_PASSES_IN_FLIGHT: &str = "rustee_kafka_delayed_retry_relay_passes_in_flight";
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

#[cfg(test)]
mod tests;
