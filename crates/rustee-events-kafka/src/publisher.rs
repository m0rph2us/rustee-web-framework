//! Broker-acknowledged `Kafka` event publication.

use std::{fmt, time::Duration};

use futures_util::future::BoxFuture;
use rdkafka::{
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord, Producer},
};
use rustee_events::{EventMessage, EventPublisher};

use super::config::validate_topic_name;
use super::{ConfigError, KafkaConfig, KafkaError, create_producer, topic_metadata_is_healthy};

/// A `Kafka` producer configured to wait for broker delivery acknowledgement.
///
/// Its `Debug` output keeps the deployment-specific destination topic redacted.
#[cfg(feature = "rdkafka")]
#[derive(Clone)]
pub struct KafkaPublisher {
    producer: FutureProducer,
    topic: String,
    queue_timeout: Duration,
    topic_scoped_readiness: bool,
}

#[cfg(feature = "rdkafka")]
impl KafkaPublisher {
    /// Creates a native `Kafka` producer from explicitly supplied configuration with automatic
    /// topic creation disabled.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::CreateProducer`] when librdkafka rejects the configuration.
    pub fn connect(config: &KafkaConfig) -> Result<Self, KafkaError> {
        let producer = create_producer(config)?;
        Ok(Self {
            producer,
            topic: config.topic().to_owned(),
            queue_timeout: config.queue_timeout(),
            topic_scoped_readiness: true,
        })
    }

    /// Wraps an already-configured producer for dependency injection and tests.
    ///
    /// The caller retains all native client settings, including topic-creation policy. Readiness
    /// therefore uses a full metadata request before it checks the destination topic.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidTopic`] when `topic` is blank, oversized, contains a NUL
    /// byte, or contains whitespace.
    pub fn new(
        producer: FutureProducer,
        topic: impl Into<String>,
        queue_timeout: Duration,
    ) -> Result<Self, ConfigError> {
        let topic = topic.into();
        validate_topic_name(&topic)?;
        Ok(Self {
            producer,
            topic,
            queue_timeout,
            topic_scoped_readiness: false,
        })
    }

    /// Queries broker metadata to make an explicit producer readiness decision.
    ///
    /// # Errors
    ///
    /// Framework-created producers query the destination topic directly because automatic topic
    /// creation is disabled. Injected native producers use full metadata before checking that
    /// topic, preserving the caller-owned configuration.
    ///
    /// Returns [`KafkaError::Readiness`] when broker metadata cannot be read before `timeout`,
    /// or the destination topic is absent or has a broker-reported error.
    pub fn readiness(&self, timeout: Duration) -> Result<(), KafkaError> {
        let metadata = self
            .producer
            .client()
            .fetch_metadata(
                self.topic_scoped_readiness.then_some(self.topic.as_str()),
                timeout,
            )
            .map_err(|_| KafkaError::Readiness)?;
        if topic_metadata_is_healthy(&metadata, &self.topic) {
            Ok(())
        } else {
            Err(KafkaError::Readiness)
        }
    }
}

#[cfg(feature = "rdkafka")]
impl fmt::Debug for KafkaPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaPublisher")
            .field("topic", &"[REDACTED]")
            .field("topic_length", &self.topic.len())
            .field("queue_timeout", &self.queue_timeout)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "rdkafka")]
impl EventPublisher for KafkaPublisher {
    type Error = KafkaError;

    fn publish(&self, message: EventMessage) -> BoxFuture<'static, Result<(), Self::Error>> {
        let producer = self.producer.clone();
        let topic = self.topic.clone();
        let queue_timeout = self.queue_timeout;
        let event_id = message.id().to_string();
        let event_type = message.event_type().to_owned();
        let event_version = message.version().to_string();
        let key = message.key().to_owned();
        let payload = message.into_payload();
        Box::pin(async move {
            let headers = OwnedHeaders::new()
                .insert(Header {
                    key: "rustee-event-id",
                    value: Some(&event_id),
                })
                .insert(Header {
                    key: "rustee-event-type",
                    value: Some(&event_type),
                })
                .insert(Header {
                    key: "rustee-event-version",
                    value: Some(&event_version),
                });
            let record = FutureRecord::to(&topic)
                .key(&key)
                .payload(&payload)
                .headers(headers);
            producer
                .send(record, queue_timeout)
                .await
                .map(|_| ())
                .map_err(|_| KafkaError::Delivery)
        })
    }
}
