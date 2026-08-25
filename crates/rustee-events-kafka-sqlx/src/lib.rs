//! PostgreSQL-backed delayed retry router for Kafka event failures.
//!
//! Enable the `rdkafka` feature to use the Kafka adapter.

#[cfg(feature = "rdkafka")]
mod adapter;

#[cfg(feature = "rdkafka")]
pub use adapter::{
    KAFKA_DELAYED_RETRY_MIGRATION_SQL, KafkaDelayedRetryBacklog, KafkaDelayedRetryBacklogError,
    KafkaDelayedRetryDelay, KafkaDelayedRetryDelayError, KafkaDelayedRetryReadinessConfig,
    KafkaDelayedRetryReadinessConfigError, KafkaDelayedRetryReadinessError,
    KafkaDelayedRetryRelayBatchSize, KafkaDelayedRetryRelayBatchSizeError,
    KafkaDelayedRetryRelayConfig, KafkaDelayedRetryRelayLoopConfig,
    KafkaDelayedRetryRelayLoopConfigError, KafkaDelayedRetryRelayLoopReport,
    KafkaDelayedRetryRelayObserver, KafkaDelayedRetryRelayOutcome,
    KafkaDelayedRetryRelayPassFinished, KafkaDelayedRetryRelayPassObservation,
    KafkaDelayedRetryRelayPassStarted, NoopKafkaDelayedRetryRelayObserver,
    PostgresKafkaDelayedRetryRelay, PostgresKafkaDelayedRetryRouter,
};
