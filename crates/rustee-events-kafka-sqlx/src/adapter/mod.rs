mod config;
mod observation;
mod relay;
mod router;

pub use config::{
    KafkaDelayedRetryDelay, KafkaDelayedRetryDelayError, KafkaDelayedRetryReadinessConfig,
    KafkaDelayedRetryReadinessConfigError, KafkaDelayedRetryReadinessError,
    KafkaDelayedRetryRelayBatchSize, KafkaDelayedRetryRelayBatchSizeError,
    KafkaDelayedRetryRelayConfig, KafkaDelayedRetryRelayLoopConfig,
    KafkaDelayedRetryRelayLoopConfigError, KafkaDelayedRetryRelayLoopReport,
};
pub use observation::{
    KafkaDelayedRetryRelayObserver, KafkaDelayedRetryRelayOutcome,
    KafkaDelayedRetryRelayPassFinished, KafkaDelayedRetryRelayPassObservation,
    KafkaDelayedRetryRelayPassStarted, NoopKafkaDelayedRetryRelayObserver,
};
pub use relay::{
    KafkaDelayedRetryBacklog, KafkaDelayedRetryBacklogError, PostgresKafkaDelayedRetryRelay,
};
pub use router::PostgresKafkaDelayedRetryRouter;

/// Deployment-owned migration for durable Kafka delayed retries.
pub const KAFKA_DELAYED_RETRY_MIGRATION_SQL: &str =
    include_str!("../../migrations/0001_rustee_kafka_delayed_retries.sql");
