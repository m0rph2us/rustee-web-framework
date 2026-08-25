//! Apache `Kafka` producer and manual-commit consumer integration for `Rustee` events.
//!
//! Enable the `rdkafka` feature to expose the broker-backed publisher, consumer, and failure
//! routing adapters. Configuration types remain available without it for shared application setup.
//!
//! Topics, partitions, retention, ACLs, consumer groups, and schema compatibility are deployment
//! configuration. This crate publishes fully acknowledged event records and exposes explicit,
//! serial manual-commit consumption for one configured topic group.

#[cfg(feature = "rdkafka")]
use rdkafka::{ClientConfig, metadata::Metadata, producer::FutureProducer};

#[cfg(feature = "rdkafka")]
pub use rdkafka;

mod config;
#[cfg(feature = "rdkafka")]
mod consumer;
mod failure;
#[cfg(feature = "rdkafka")]
mod publisher;

pub use config::{
    ConfigError, KafkaConfig, KafkaConsumerConfig, MAX_NATIVE_OPTION_COUNT,
    MAX_NATIVE_OPTION_KEY_BYTES, MAX_NATIVE_OPTION_VALUE_BYTES, MAX_TOPIC_BYTES,
    validate_topic_name,
};
#[cfg(feature = "rdkafka")]
pub use consumer::{KafkaEventConsumer, KafkaLagSnapshotLimit, KafkaPartitionLag};
#[cfg(feature = "rdkafka")]
pub use failure::{
    KafkaDelayedRetryRecord, KafkaFailurePublisher, KafkaFailureRecord, KafkaFailureRouter,
};
pub use failure::{KafkaFailureKind, KafkaRetryAction, KafkaRetryConfig};
#[cfg(feature = "rdkafka")]
pub use publisher::KafkaPublisher;

#[cfg(feature = "rdkafka")]
fn create_producer(config: &KafkaConfig) -> Result<FutureProducer, KafkaError> {
    let mut client = ClientConfig::new();
    for (key, value) in config.options() {
        client.set(key, value);
    }
    // Typed configuration owns these delivery invariants even when TLS/SASL options are added.
    let delivery_timeout_ms = config.delivery_timeout().as_millis().to_string();
    client
        .set("bootstrap.servers", config.bootstrap_servers())
        .set("acks", "all")
        .set("message.timeout.ms", &delivery_timeout_ms)
        .set("allow.auto.create.topics", "false");
    client
        .create::<FutureProducer>()
        .map_err(|_| KafkaError::CreateProducer)
}

#[cfg(feature = "rdkafka")]
fn topic_metadata_is_healthy(metadata: &Metadata, topic: &str) -> bool {
    metadata
        .topics()
        .iter()
        .any(|entry| entry.name() == topic && entry.error().is_none())
}

/// Sanitized operational failures from the Kafka adapter.
#[cfg(feature = "rdkafka")]
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum KafkaError {
    /// librdkafka rejected the producer configuration.
    #[error("Kafka producer configuration failed")]
    CreateProducer,
    /// Broker metadata query failed.
    #[error("Kafka producer readiness check failed")]
    Readiness,
    /// Broker delivery acknowledgement failed.
    #[error("Kafka event delivery failed")]
    Delivery,
    /// librdkafka rejected the consumer configuration.
    #[error("Kafka consumer configuration failed")]
    CreateConsumer,
    /// The configured topic could not be subscribed.
    #[error("Kafka consumer subscription failed")]
    Subscribe,
    /// Receiving a Kafka record failed.
    #[error("Kafka event receive failed")]
    Receive,
    /// A Kafka record did not contain a payload.
    #[error("Kafka event payload was missing")]
    MissingPayload,
    /// The event envelope could not be decoded or did not match the handler type.
    #[error("Kafka event envelope was invalid")]
    Decode,
    /// An event handler failed before the source offset could be committed.
    #[error("Kafka event handler failed")]
    Handler,
    /// Kafka did not confirm a post-success offset commit.
    #[error("Kafka event offset commit failed")]
    Commit,
    /// Kafka did not report consumer-group membership before the configured deadline.
    #[error("Kafka consumer group membership query failed")]
    GroupMembership,
    /// Kafka assignment, position, or watermarks could not be read for a lag snapshot.
    #[error("Kafka event lag snapshot failed")]
    LagSnapshot,
    /// The consumer assignment exceeded the explicit lag-snapshot partition limit.
    #[error("Kafka event lag snapshot assignment exceeded its partition limit")]
    LagSnapshotLimitExceeded,
    /// The retry topic was not subscribed by this event consumer group.
    #[error("Kafka event retry topic is not subscribed by this consumer")]
    RetryTopicNotSubscribed,
    /// A failure router reused the source topic, used one route for retry and DLQ, or exposed an invalid route.
    #[error(
        "Kafka event failure routes must use distinct valid source, retry, and dead-letter topics"
    )]
    FailureRouteConfiguration,
    /// Retry-attempt or preserved-origin headers were malformed, duplicated, or incomplete.
    #[error("Kafka event retry metadata was invalid")]
    RetryMetadata,
    /// Kafka did not acknowledge retry or dead-letter publication.
    #[error("Kafka event failure route publish failed")]
    FailurePublish,
    /// A configured durable failure router could not persist the next retry action.
    #[error("Kafka event failure route could not be persisted")]
    FailureRoute,
}
#[cfg(test)]
mod tests;
