//! Apache `Kafka` producer integration for `Rustee` versioned event envelopes.
//!
//! Topics, partitions, retention, ACLs, consumer groups, and schema compatibility are deployment
//! configuration. This crate only appends a fully acknowledged event record to one explicit topic.

use std::{collections::BTreeMap, fmt, num::NonZeroU16, time::Duration};

#[cfg(feature = "rdkafka")]
use futures_util::future::BoxFuture;
#[cfg(feature = "rdkafka")]
use rdkafka::{
    ClientConfig,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::{BorrowedMessage, Header, Headers, Message, OwnedHeaders},
    producer::{FutureProducer, FutureRecord, Producer},
};
#[cfg(feature = "rdkafka")]
use rustee_events::{
    Event, EventDeliveryObservation, EventDeliveryObserver, EventDeliveryOutcome, EventEnvelope,
    EventHandler, EventMessage, EventPublisher, NoopEventDeliveryObserver, dispatch,
};
#[cfg(feature = "rdkafka")]
use std::future::Future;
#[cfg(feature = "rdkafka")]
use std::sync::Arc;

#[cfg(feature = "rdkafka")]
pub use rdkafka;

#[cfg(feature = "rdkafka")]
const RETRY_ATTEMPT_HEADER: &str = "rustee-event-retry-attempt";
#[cfg(feature = "rdkafka")]
const FAILURE_KIND_HEADER: &str = "rustee-event-failure-kind";
#[cfg(feature = "rdkafka")]
const ORIGIN_TOPIC_HEADER: &str = "rustee-event-origin-topic";
#[cfg(feature = "rdkafka")]
const ORIGIN_PARTITION_HEADER: &str = "rustee-event-origin-partition";
#[cfg(feature = "rdkafka")]
const ORIGIN_OFFSET_HEADER: &str = "rustee-event-origin-offset";
const DEFAULT_DELIVERY_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_DELIVERY_TIMEOUT: Duration = Duration::from_millis(i32::MAX as u64);

/// Configuration for an acknowledged `Kafka` event producer.
#[derive(Clone, Eq, PartialEq)]
pub struct KafkaConfig {
    bootstrap_servers: String,
    topic: String,
    queue_timeout: Duration,
    delivery_timeout: Duration,
    options: BTreeMap<String, String>,
}

impl KafkaConfig {
    /// Creates a producer configuration with `acks=all` and finite queue timeout.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidTopic`] when `topic` is blank or contains whitespace.
    pub fn new(
        bootstrap_servers: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        let topic = topic.into();
        validate_topic(&topic)?;
        Ok(Self {
            bootstrap_servers: bootstrap_servers.into(),
            topic,
            queue_timeout: Duration::from_secs(5),
            delivery_timeout: DEFAULT_DELIVERY_TIMEOUT,
            options: BTreeMap::new(),
        })
    }

    /// Sets the bounded time spent waiting for local producer queue capacity.
    #[must_use]
    pub fn with_queue_timeout(mut self, timeout: Duration) -> Self {
        self.queue_timeout = timeout;
        self
    }

    /// Sets the bounded time allowed for broker delivery acknowledgement.
    ///
    /// This is applied as librdkafka's <code>message.timeout.ms</code> after native options, so
    /// an unavailable broker cannot make one publish await indefinitely.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidDeliveryTimeout`] when `timeout` is zero or cannot be
    /// represented by librdkafka's millisecond setting.
    pub fn with_delivery_timeout(mut self, timeout: Duration) -> Result<Self, ConfigError> {
        if timeout.is_zero() || timeout > MAX_DELIVERY_TIMEOUT {
            return Err(ConfigError::InvalidDeliveryTimeout);
        }
        self.delivery_timeout = timeout;
        Ok(self)
    }

    /// Returns the broker delivery acknowledgement deadline.
    #[must_use]
    pub const fn delivery_timeout(&self) -> Duration {
        self.delivery_timeout
    }

    /// Adds a native librdkafka client option such as TLS or SASL configuration.
    ///
    /// Option values are redacted in [`fmt::Debug`] output. The typed bootstrap server and
    /// `acks=all` delivery invariant are applied after these options.
    #[must_use]
    pub fn with_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }

    /// Returns the explicit destination topic.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }
}

impl fmt::Debug for KafkaConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaConfig")
            .field("bootstrap_servers", &"[REDACTED]")
            .field("topic", &self.topic)
            .field("queue_timeout", &self.queue_timeout)
            .field("delivery_timeout", &self.delivery_timeout)
            .field("option_keys", &self.options.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Configuration for one manual-commit `Kafka` event consumer group.
#[derive(Clone, Eq, PartialEq)]
pub struct KafkaConsumerConfig {
    bootstrap_servers: String,
    topic: String,
    group_id: String,
    retry_topic: Option<String>,
    options: BTreeMap<String, String>,
}

impl KafkaConsumerConfig {
    /// Creates a consumer configuration that disables automatic offset commits and stores.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when `topic` or `group_id` is blank or contains whitespace.
    pub fn new(
        bootstrap_servers: impl Into<String>,
        topic: impl Into<String>,
        group_id: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        let topic = topic.into();
        let group_id = group_id.into();
        validate_topic(&topic)?;
        validate_group_id(&group_id)?;
        Ok(Self {
            bootstrap_servers: bootstrap_servers.into(),
            topic,
            group_id,
            retry_topic: None,
            options: BTreeMap::new(),
        })
    }

    /// Subscribes this consumer group to one retry topic in addition to its source topic.
    ///
    /// Retry records retain the original event envelope and are handled by the same typed handler.
    /// A retry topic cannot equal the source topic because that would make the failure route
    /// indistinguishable from an unbounded source redelivery loop.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidTopic`] for an invalid topic name or
    /// [`ConfigError::RetryTopicMatchesSource`] when it equals the source topic.
    pub fn with_retry_topic(mut self, retry_topic: impl Into<String>) -> Result<Self, ConfigError> {
        let retry_topic = retry_topic.into();
        validate_topic(&retry_topic)?;
        if retry_topic == self.topic {
            return Err(ConfigError::RetryTopicMatchesSource);
        }
        self.retry_topic = Some(retry_topic);
        Ok(self)
    }

    /// Adds a native librdkafka client option such as TLS or SASL configuration.
    ///
    /// Option values are redacted in [`fmt::Debug`] output. The typed bootstrap server, group,
    /// and manual commit/store invariants are applied after these options.
    #[must_use]
    pub fn with_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }

    /// Returns the subscribed topic.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the stable consumer-group identifier.
    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the optional retry topic subscribed by this consumer group.
    #[must_use]
    pub fn retry_topic(&self) -> Option<&str> {
        self.retry_topic.as_deref()
    }

    #[cfg(feature = "rdkafka")]
    fn subscription_topics(&self) -> Vec<&str> {
        let mut topics = vec![self.topic.as_str()];
        if let Some(retry_topic) = self.retry_topic.as_deref() {
            topics.push(retry_topic);
        }
        topics
    }
}

impl fmt::Debug for KafkaConsumerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaConsumerConfig")
            .field("bootstrap_servers", &"[REDACTED]")
            .field("topic", &self.topic)
            .field("group_id", &self.group_id)
            .field("retry_topic", &self.retry_topic)
            .field("option_keys", &self.options.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Invalid Kafka event producer configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConfigError {
    /// Topic names must be present and free from whitespace.
    #[error("Kafka event topic must not be blank or contain whitespace")]
    InvalidTopic,
    /// Consumer groups must be present and free from whitespace.
    #[error("Kafka event consumer group must not be blank or contain whitespace")]
    InvalidGroupId,
    /// A retry topic matched the consumer's primary source topic.
    #[error("Kafka retry topic must differ from the source topic")]
    RetryTopicMatchesSource,
    /// Retry and dead-letter topic names must identify different destinations.
    #[error("Kafka retry and dead-letter topics must differ")]
    RetryTopicMatchesDeadLetter,
    /// The configured broker delivery deadline was zero or exceeded librdkafka's range.
    #[error("Kafka broker delivery timeout must be non-zero and at most 2147483647 milliseconds")]
    InvalidDeliveryTimeout,
}

/// Failure-routing configuration for one Kafka event source and its retry/dead-letter topics.
#[derive(Clone, Eq, PartialEq)]
pub struct KafkaRetryConfig {
    retry_topic: String,
    dead_letter_topic: String,
    max_deliveries: NonZeroU16,
}

impl KafkaRetryConfig {
    /// Creates a bounded immediate-retry and dead-letter routing policy.
    ///
    /// Rustee publishes retry records only after broker acknowledgement. It does not pretend that
    /// plain Kafka topics provide delayed scheduling; deployments that need delay must supply a
    /// scheduler that releases retry records at an explicit time.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidTopic`] when a route topic is invalid or
    /// [`ConfigError::RetryTopicMatchesDeadLetter`] when both routes use one topic.
    pub fn new(
        retry_topic: impl Into<String>,
        dead_letter_topic: impl Into<String>,
        max_deliveries: NonZeroU16,
    ) -> Result<Self, ConfigError> {
        let retry_topic = retry_topic.into();
        let dead_letter_topic = dead_letter_topic.into();
        validate_topic(&retry_topic)?;
        validate_topic(&dead_letter_topic)?;
        if retry_topic == dead_letter_topic {
            return Err(ConfigError::RetryTopicMatchesDeadLetter);
        }
        Ok(Self {
            retry_topic,
            dead_letter_topic,
            max_deliveries,
        })
    }

    /// Returns the topic carrying immediately retried records.
    #[must_use]
    pub fn retry_topic(&self) -> &str {
        &self.retry_topic
    }

    /// Returns the topic carrying terminal failed records.
    #[must_use]
    pub fn dead_letter_topic(&self) -> &str {
        &self.dead_letter_topic
    }

    /// Returns the maximum total typed handler/decode deliveries before dead-lettering.
    #[must_use]
    pub const fn max_deliveries(&self) -> NonZeroU16 {
        self.max_deliveries
    }

    /// Chooses the route after the current one-based delivery attempt failed.
    #[must_use]
    pub fn after_failure(&self, attempt: u16) -> KafkaRetryAction {
        if attempt == 0 || attempt >= self.max_deliveries.get() {
            KafkaRetryAction::DeadLetter
        } else {
            KafkaRetryAction::Retry {
                next_attempt: attempt.saturating_add(1),
            }
        }
    }
}

impl fmt::Debug for KafkaRetryConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaRetryConfig")
            .field("retry_topic", &self.retry_topic)
            .field("dead_letter_topic", &self.dead_letter_topic)
            .field("max_deliveries", &self.max_deliveries)
            .finish()
    }
}

/// The Kafka topic transition selected after one failed delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaRetryAction {
    /// Publish the same event envelope to the retry topic with this next attempt number.
    Retry {
        /// One-based attempt number persisted in the retry record header.
        next_attempt: u16,
    },
    /// Publish the original envelope plus failure metadata to the dead-letter topic.
    DeadLetter,
}

/// The source record supplied to a failure router before its offset is committed.
#[cfg(feature = "rdkafka")]
pub struct KafkaFailureRecord<'a> {
    message: &'a BorrowedMessage<'a>,
}

/// A persisted retry record ready for Kafka delivery.
///
/// The payload and partition key are intentionally redacted from its [`Debug`] implementation.
#[cfg(feature = "rdkafka")]
pub struct KafkaDelayedRetryRecord<'a> {
    /// Retry topic selected when the source offset was committed.
    pub retry_topic: &'a str,
    /// Original event topic, including a preserved origin from an earlier retry delivery.
    pub origin_topic: &'a str,
    /// Original event partition.
    pub origin_partition: i32,
    /// Original event offset.
    pub origin_offset: i64,
    /// Sanitized decode or handler failure reason.
    pub failure: KafkaFailureKind,
    /// One-based retry delivery attempt.
    pub attempt: u16,
    /// Optional Kafka partition key.
    pub key: Option<&'a [u8]>,
    /// Serialized versioned event envelope.
    pub payload: &'a [u8],
}

#[cfg(feature = "rdkafka")]
impl fmt::Debug for KafkaDelayedRetryRecord<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaDelayedRetryRecord")
            .field("retry_topic", &self.retry_topic)
            .field("origin_topic", &self.origin_topic)
            .field("origin_partition", &self.origin_partition)
            .field("origin_offset", &self.origin_offset)
            .field("failure", &self.failure)
            .field("attempt", &self.attempt)
            .field("key", &"[REDACTED]")
            .field("payload", &"[REDACTED]")
            .finish()
    }
}

#[cfg(feature = "rdkafka")]
impl fmt::Debug for KafkaFailureRecord<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaFailureRecord")
            .field("payload", &"[REDACTED]")
            .field("key", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "rdkafka")]
impl KafkaFailureRecord<'_> {
    /// Returns the original serialized event envelope.
    #[must_use]
    pub fn payload(&self) -> Option<&[u8]> {
        self.message.payload()
    }
    /// Returns the optional Kafka partition key.
    #[must_use]
    pub fn key(&self) -> Option<&[u8]> {
        self.message.key()
    }
    /// Returns the original source topic, preserving prior retry origin metadata when present.
    #[must_use]
    pub fn origin_topic(&self) -> String {
        FailureOrigin::from_message(self.message).topic
    }
    /// Returns the original source partition.
    #[must_use]
    pub fn origin_partition(&self) -> i32 {
        FailureOrigin::from_message(self.message).partition
    }
    /// Returns the original source offset.
    #[must_use]
    pub fn origin_offset(&self) -> i64 {
        FailureOrigin::from_message(self.message).offset
    }
}

/// Routes a failed record before the consumer synchronously commits its source offset.
#[cfg(feature = "rdkafka")]
pub trait KafkaFailureRouter: Send + Sync {
    /// Returns the retry topic the consumer must subscribe to.
    fn retry_topic(&self) -> &str;
    /// Persists or publishes the next failure action before the source offset is committed.
    fn route<'a>(
        &'a self,
        record: KafkaFailureRecord<'a>,
        failure: KafkaFailureKind,
        attempt: u16,
    ) -> BoxFuture<'a, Result<KafkaRetryAction, KafkaError>>;
}

/// Sanitized reason recorded in retry and dead-letter Kafka headers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaFailureKind {
    /// The typed envelope could not be decoded or did not match the configured handler type.
    Decode,
    /// The typed handler returned an error before its source offset could be committed.
    Handler,
}

impl KafkaFailureKind {
    #[cfg(feature = "rdkafka")]
    const fn as_str(self) -> &'static str {
        match self {
            Self::Decode => "decode",
            Self::Handler => "handler",
        }
    }
}

/// A `Kafka` producer configured to wait for broker delivery acknowledgement.
#[cfg(feature = "rdkafka")]
#[derive(Clone)]
pub struct KafkaPublisher {
    producer: FutureProducer,
    topic: String,
    queue_timeout: Duration,
}

#[cfg(feature = "rdkafka")]
impl KafkaPublisher {
    /// Creates a native `Kafka` producer from explicitly supplied configuration.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::CreateProducer`] when librdkafka rejects the configuration.
    pub fn connect(config: &KafkaConfig) -> Result<Self, KafkaError> {
        let producer = create_producer(config)?;
        Ok(Self {
            producer,
            topic: config.topic.clone(),
            queue_timeout: config.queue_timeout,
        })
    }

    /// Wraps an already-configured producer for dependency injection and tests.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidTopic`] when `topic` is blank or contains whitespace.
    pub fn new(
        producer: FutureProducer,
        topic: impl Into<String>,
        queue_timeout: Duration,
    ) -> Result<Self, ConfigError> {
        let topic = topic.into();
        validate_topic(&topic)?;
        Ok(Self {
            producer,
            topic,
            queue_timeout,
        })
    }

    /// Queries broker metadata to make an explicit producer readiness decision.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Readiness`] when broker metadata cannot be read before `timeout`.
    pub fn readiness(&self, timeout: Duration) -> Result<(), KafkaError> {
        self.producer
            .client()
            .fetch_metadata(Some(&self.topic), timeout)
            .map(|_| ())
            .map_err(|_| KafkaError::Readiness)
    }
}

/// Kafka publisher for immediate retry and terminal dead-letter event records.
///
/// It publishes the original serialized event envelope, event key, one-based retry attempt, and
/// sanitized failure metadata. A source consumer commits only after this publisher receives the
/// configured broker acknowledgement, so a lost source commit can still duplicate a retry record.
#[cfg(feature = "rdkafka")]
#[derive(Clone)]
pub struct KafkaFailurePublisher {
    producer: FutureProducer,
    retry: KafkaRetryConfig,
    queue_timeout: Duration,
}

#[cfg(feature = "rdkafka")]
impl KafkaFailurePublisher {
    /// Creates a retry/dead-letter publisher using the same acknowledged producer settings as an
    /// event publisher.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::CreateProducer`] when librdkafka rejects the producer configuration.
    pub fn connect(config: &KafkaConfig, retry: KafkaRetryConfig) -> Result<Self, KafkaError> {
        Ok(Self {
            producer: create_producer(config)?,
            retry,
            queue_timeout: config.queue_timeout,
        })
    }

    /// Wraps an already-configured producer for dependency injection and tests.
    #[must_use]
    pub fn new(producer: FutureProducer, retry: KafkaRetryConfig, queue_timeout: Duration) -> Self {
        Self {
            producer,
            retry,
            queue_timeout,
        }
    }

    /// Returns the retry/dead-letter routing configuration.
    #[must_use]
    pub fn retry_config(&self) -> &KafkaRetryConfig {
        &self.retry
    }

    /// Queries retry and dead-letter topic metadata for an explicit readiness decision.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Readiness`] when either configured failure-routing topic cannot be
    /// read before `timeout`.
    pub fn readiness(&self, timeout: Duration) -> Result<(), KafkaError> {
        // Requesting full metadata avoids asking a broker to create a missing named topic.
        let metadata = self
            .producer
            .client()
            .fetch_metadata(None, timeout)
            .map_err(|_| KafkaError::Readiness)?;
        for topic in [self.retry.retry_topic(), self.retry.dead_letter_topic()] {
            if !metadata
                .topics()
                .iter()
                .any(|metadata| metadata.name() == topic && metadata.error().is_none())
            {
                return Err(KafkaError::Readiness);
            }
        }
        Ok(())
    }

    /// Publishes a persisted retry record to its originally configured retry topic.
    ///
    /// Durable relays use this method so a later configuration change cannot silently redirect
    /// a row that was already accepted before its source offset was committed.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::FailurePublish`] when Kafka does not acknowledge the record.
    pub async fn publish_delayed_retry(
        &self,
        retry: KafkaDelayedRetryRecord<'_>,
    ) -> Result<(), KafkaError> {
        let attempt = retry.attempt.to_string();
        let partition = retry.origin_partition.to_string();
        let offset = retry.origin_offset.to_string();
        let headers = OwnedHeaders::new()
            .insert(Header {
                key: RETRY_ATTEMPT_HEADER,
                value: Some(&attempt),
            })
            .insert(Header {
                key: FAILURE_KIND_HEADER,
                value: Some(retry.failure.as_str()),
            })
            .insert(Header {
                key: ORIGIN_TOPIC_HEADER,
                value: Some(retry.origin_topic),
            })
            .insert(Header {
                key: ORIGIN_PARTITION_HEADER,
                value: Some(&partition),
            })
            .insert(Header {
                key: ORIGIN_OFFSET_HEADER,
                value: Some(&offset),
            });
        let record = match retry.key {
            Some(key) => FutureRecord::to(retry.retry_topic)
                .key(key)
                .payload(retry.payload)
                .headers(headers),
            None => FutureRecord::to(retry.retry_topic)
                .payload(retry.payload)
                .headers(headers),
        };
        self.producer
            .send(record, self.queue_timeout)
            .await
            .map(|_| ())
            .map_err(|_| KafkaError::FailurePublish)
    }

    async fn route(
        &self,
        message: &BorrowedMessage<'_>,
        failure: KafkaFailureKind,
        attempt: u16,
    ) -> Result<KafkaRetryAction, KafkaError> {
        let action = self.retry.after_failure(attempt);
        let target = match action {
            KafkaRetryAction::Retry { .. } => self.retry.retry_topic(),
            KafkaRetryAction::DeadLetter => self.retry.dead_letter_topic(),
        };
        let next_attempt = match action {
            KafkaRetryAction::Retry { next_attempt } => next_attempt,
            KafkaRetryAction::DeadLetter => attempt,
        };
        let payload = message
            .payload()
            .ok_or(KafkaError::MissingPayload)?
            .to_vec();
        let key = message.key().map(ToOwned::to_owned);
        let origin = FailureOrigin::from_message(message);
        let retry_attempt = next_attempt.to_string();
        let origin_partition = origin.partition.to_string();
        let origin_offset = origin.offset.to_string();
        let headers = OwnedHeaders::new()
            .insert(Header {
                key: RETRY_ATTEMPT_HEADER,
                value: Some(&retry_attempt),
            })
            .insert(Header {
                key: FAILURE_KIND_HEADER,
                value: Some(failure.as_str()),
            })
            .insert(Header {
                key: ORIGIN_TOPIC_HEADER,
                value: Some(&origin.topic),
            })
            .insert(Header {
                key: ORIGIN_PARTITION_HEADER,
                value: Some(&origin_partition),
            })
            .insert(Header {
                key: ORIGIN_OFFSET_HEADER,
                value: Some(&origin_offset),
            });
        let record = if let Some(key) = key.as_deref() {
            FutureRecord::<[u8], [u8]>::to(target)
                .key(key)
                .payload(payload.as_slice())
                .headers(headers)
        } else {
            FutureRecord::<[u8], [u8]>::to(target)
                .payload(payload.as_slice())
                .headers(headers)
        };
        self.producer
            .send(record, self.queue_timeout)
            .await
            .map(|_| action)
            .map_err(|_| KafkaError::FailurePublish)
    }
}

#[cfg(feature = "rdkafka")]
impl KafkaFailureRouter for KafkaFailurePublisher {
    fn retry_topic(&self) -> &str {
        self.retry.retry_topic()
    }

    fn route<'a>(
        &'a self,
        record: KafkaFailureRecord<'a>,
        failure: KafkaFailureKind,
        attempt: u16,
    ) -> BoxFuture<'a, Result<KafkaRetryAction, KafkaError>> {
        Box::pin(async move { self.route(record.message, failure, attempt).await })
    }
}

#[cfg(feature = "rdkafka")]
impl fmt::Debug for KafkaFailurePublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaFailurePublisher")
            .field("retry", &self.retry)
            .field("queue_timeout", &self.queue_timeout)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "rdkafka")]
fn create_producer(config: &KafkaConfig) -> Result<FutureProducer, KafkaError> {
    let mut client = ClientConfig::new();
    for (key, value) in &config.options {
        client.set(key, value);
    }
    // Typed configuration owns these delivery invariants even when TLS/SASL options are added.
    let delivery_timeout_ms = config.delivery_timeout.as_millis().to_string();
    client
        .set("bootstrap.servers", &config.bootstrap_servers)
        .set("acks", "all")
        .set("message.timeout.ms", &delivery_timeout_ms);
    client
        .create::<FutureProducer>()
        .map_err(|_| KafkaError::CreateProducer)
}

#[cfg(feature = "rdkafka")]
impl fmt::Debug for KafkaPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaPublisher")
            .field("topic", &self.topic)
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

/// A manual-commit `Kafka` consumer for one event topic and consumer group.
///
/// It processes records serially. This preserves per-partition offset correctness: a record is
/// committed only after its handler succeeds, and a handler failure leaves the offset uncommitted
/// for retry after process recovery or a rebalance.
#[cfg(feature = "rdkafka")]
pub struct KafkaEventConsumer {
    consumer: StreamConsumer,
    topic: String,
    group_id: String,
    topics: Vec<String>,
    observer: Arc<dyn EventDeliveryObserver>,
}

/// A bounded point-in-time lag observation for one assigned Kafka partition.
///
/// The observation is for diagnostics and metrics collection. Rebalances and retention can change
/// assignment, position, or watermarks immediately after it is returned.
#[cfg(feature = "rdkafka")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KafkaPartitionLag {
    topic: String,
    partition: i32,
    position: Option<i64>,
    low_watermark: i64,
    high_watermark: i64,
    lag: Option<u64>,
}

#[cfg(feature = "rdkafka")]
impl KafkaPartitionLag {
    /// Returns the assigned topic.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the assigned partition number.
    #[must_use]
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the consumer's next position when librdkafka reports a concrete offset.
    #[must_use]
    pub const fn position(&self) -> Option<i64> {
        self.position
    }

    /// Returns the broker's current low watermark.
    #[must_use]
    pub const fn low_watermark(&self) -> i64 {
        self.low_watermark
    }

    /// Returns the broker's current high watermark.
    #[must_use]
    pub const fn high_watermark(&self) -> i64 {
        self.high_watermark
    }

    /// Returns the non-negative distance from the effective position to the high watermark.
    ///
    /// `None` means librdkafka has not established a concrete position for this assigned partition.
    #[must_use]
    pub const fn lag(&self) -> Option<u64> {
        self.lag
    }
}

#[cfg(feature = "rdkafka")]
impl KafkaEventConsumer {
    /// Creates and subscribes a consumer with automatic commit and offset storage disabled.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::CreateConsumer`] when librdkafka rejects the configuration, or
    /// [`KafkaError::Subscribe`] when the topic subscription cannot be established.
    pub fn connect(config: &KafkaConsumerConfig) -> Result<Self, KafkaError> {
        let mut client = ClientConfig::new();
        for (key, value) in &config.options {
            client.set(key, value);
        }
        // Typed configuration owns the group and commit lifecycle invariants.
        client
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("group.id", &config.group_id)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false");
        let consumer = client
            .create::<StreamConsumer>()
            .map_err(|_| KafkaError::CreateConsumer)?;
        let subscription_topics = config.subscription_topics();
        consumer
            .subscribe(&subscription_topics)
            .map_err(|_| KafkaError::Subscribe)?;
        Ok(Self {
            consumer,
            topic: config.topic.clone(),
            group_id: config.group_id.clone(),
            topics: subscription_topics
                .into_iter()
                .map(ToOwned::to_owned)
                .collect(),
            observer: Arc::new(NoopEventDeliveryObserver),
        })
    }

    /// Wraps and subscribes an already-configured native consumer for dependency injection.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Subscribe`] when the topic subscription cannot be established.
    pub fn new(
        consumer: StreamConsumer,
        topic: impl Into<String>,
        group_id: impl Into<String>,
    ) -> Result<Self, KafkaError> {
        let topic = topic.into();
        let group_id = group_id.into();
        validate_topic(&topic).map_err(|_| KafkaError::Subscribe)?;
        validate_group_id(&group_id).map_err(|_| KafkaError::Subscribe)?;
        consumer
            .subscribe(&[topic.as_str()])
            .map_err(|_| KafkaError::Subscribe)?;
        Ok(Self {
            consumer,
            topics: vec![topic.clone()],
            topic,
            group_id,
            observer: Arc::new(NoopEventDeliveryObserver),
        })
    }

    /// Attaches a non-blocking observer for bounded event-delivery lifecycle telemetry.
    ///
    /// Observer panics are isolated from Kafka commit and failure-routing behavior. Use
    /// `rustee-events-observability::EventMetrics` for the built-in exporter-neutral collector.
    #[must_use]
    pub fn with_delivery_observer(mut self, observer: Arc<dyn EventDeliveryObserver>) -> Self {
        self.observer = observer;
        self
    }

    /// Queries metadata for every subscribed source or retry topic before a worker starts.
    ///
    /// This is an explicit, bounded read-only readiness decision. It does not create topics,
    /// wait for group assignment, move offsets, confirm a handler can process a record, or start
    /// a background poll loop.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Readiness`] when broker metadata cannot be read before `timeout` or
    /// a subscribed topic is absent or has a broker-reported error.
    pub fn readiness(&self, timeout: Duration) -> Result<(), KafkaError> {
        // Requesting full metadata avoids asking a broker to create a missing named topic.
        let metadata = self
            .consumer
            .fetch_metadata(None, timeout)
            .map_err(|_| KafkaError::Readiness)?;
        for topic in &self.topics {
            if !metadata
                .topics()
                .iter()
                .any(|metadata| metadata.name() == topic && metadata.error().is_none())
            {
                return Err(KafkaError::Readiness);
            }
        }
        Ok(())
    }

    /// Returns the broker-reported number of members in this consumer group.
    ///
    /// This is an operational snapshot, not a readiness or assignment guarantee. Callers should
    /// use it for diagnostics and bounded health decisions rather than coordinating work.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::GroupMembership`] when the broker cannot report group state before
    /// `timeout`.
    pub fn group_member_count(&self, timeout: Duration) -> Result<usize, KafkaError> {
        let groups = self
            .consumer
            .fetch_group_list(Some(&self.group_id), timeout)
            .map_err(|_| KafkaError::GroupMembership)?;
        Ok(groups
            .groups()
            .iter()
            .find(|group| group.name() == self.group_id)
            .map_or(0, |group| group.members().len()))
    }

    /// Returns one lag observation for each partition currently assigned to this consumer.
    ///
    /// Each broker watermark request is bounded by `timeout`; the whole snapshot can therefore
    /// take longer than one timeout when more than one partition is assigned. A returned snapshot
    /// is not a coordination primitive and may already be stale after a rebalance.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::LagSnapshot`] when assignment, position, or broker watermarks cannot
    /// be read.
    pub fn lag_snapshot(&self, timeout: Duration) -> Result<Vec<KafkaPartitionLag>, KafkaError> {
        let assignment = self
            .consumer
            .assignment()
            .map_err(|_| KafkaError::LagSnapshot)?;
        let positions = self
            .consumer
            .position()
            .map_err(|_| KafkaError::LagSnapshot)?;
        let mut snapshots = Vec::with_capacity(assignment.count());
        for assigned in assignment.elements() {
            let topic = assigned.topic().to_owned();
            let partition = assigned.partition();
            let position = positions
                .find_partition(&topic, partition)
                .and_then(|position| match position.offset() {
                    rdkafka::Offset::Offset(offset) if offset >= 0 => Some(offset),
                    _ => None,
                });
            let (low_watermark, high_watermark) = self
                .consumer
                .fetch_watermarks(&topic, partition, timeout)
                .map_err(|_| KafkaError::LagSnapshot)?;
            let lag = position.map(|position| {
                u64::try_from(
                    high_watermark
                        .saturating_sub(position.max(low_watermark))
                        .max(0),
                )
                .unwrap_or(0)
            });
            snapshots.push(KafkaPartitionLag {
                topic,
                partition,
                position,
                low_watermark,
                high_watermark,
                lag,
            });
        }
        Ok(snapshots)
    }

    /// Receives, dispatches, and synchronously commits records until `shutdown` resolves.
    ///
    /// Shutdown wins over a ready next record. A handler already in progress is allowed to finish,
    /// then its successful record is committed before the consumer returns.
    ///
    /// The consumer is intentionally serial: committing offset <em>n</em> also commits all lower
    /// offsets in that partition. A handler must complete inside its configured Kafka poll interval.
    ///
    /// # Errors
    ///
    /// Returns an error without committing the current record when receive, decoding, handler, or
    /// commit fails.
    pub async fn run_until<E, H, F>(&self, handler: H, shutdown: F) -> Result<(), KafkaError>
    where
        E: Event,
        H: EventHandler<E>,
        F: Future<Output = ()> + Send,
    {
        let mut shutdown = Box::pin(shutdown);
        loop {
            tokio::select! {
                biased;
                () = &mut shutdown => return Ok(()),
                message = self.consumer.recv() => {
                    let message = message.map_err(|_| KafkaError::Receive)?;
                    let observation = EventDeliveryObservation::start(Arc::clone(&self.observer), "apache_kafka");
                    let payload = message.payload().ok_or(KafkaError::MissingPayload)?;
                    let envelope = EventEnvelope::<E>::decode(payload).map_err(|_| KafkaError::Decode)?;
                    dispatch(envelope, &handler).await.map_err(|_| KafkaError::Handler)?;
                    self.consumer
                        .commit_message(&message, CommitMode::Sync)
                        .map_err(|_| KafkaError::Commit)?;
                    observation.finish(None, EventDeliveryOutcome::Acknowledged);
                }
            }
        }
    }

    /// Receives, routes failures to immediate retry or dead-letter Kafka topics, and commits only
    /// after the configured failure publisher reports broker acknowledgement.
    ///
    /// The consumer configuration must subscribe to `failure_publisher`'s retry topic via
    /// [`KafkaConsumerConfig::with_retry_topic`]. Retry records carry a one-based attempt header;
    /// malformed or duplicate attempt headers stop the worker without committing their source
    /// offset. Plain Kafka retry is immediate. Durable delayed delivery belongs in a
    /// deployment-owned failure router and relay, such as the optional
    /// `rustee-events-kafka-sqlx` integration, rather than a hidden sleep in this worker.
    ///
    /// # Errors
    ///
    /// Returns an error without committing the current record when receive, retry metadata,
    /// failure routing, or source-offset commit fails.
    pub async fn run_with_failure_routing<E, H, F>(
        &self,
        handler: H,
        failure_router: &impl KafkaFailureRouter,
        shutdown: F,
    ) -> Result<(), KafkaError>
    where
        E: Event,
        H: EventHandler<E>,
        F: Future<Output = ()> + Send,
    {
        if !self
            .topics
            .iter()
            .any(|topic| topic == failure_router.retry_topic())
        {
            return Err(KafkaError::RetryTopicNotSubscribed);
        }

        let mut shutdown = Box::pin(shutdown);
        loop {
            tokio::select! {
                biased;
                () = &mut shutdown => return Ok(()),
                message = self.consumer.recv() => {
                    let message = message.map_err(|_| KafkaError::Receive)?;
                    let observation = EventDeliveryObservation::start(Arc::clone(&self.observer), "apache_kafka");
                    let attempt = retry_attempt(&message)?;
                    let payload = message.payload().ok_or(KafkaError::MissingPayload)?;
                    let outcome = match EventEnvelope::<E>::decode(payload) {
                        Ok(envelope) => match dispatch(envelope, &handler).await {
                            Ok(()) => EventDeliveryOutcome::Acknowledged,
                            Err(_) => {
                                match failure_router
                                    .route(KafkaFailureRecord { message: &message }, KafkaFailureKind::Handler, attempt)
                                    .await?
                                {
                                    KafkaRetryAction::Retry { .. } => EventDeliveryOutcome::Retried,
                                    KafkaRetryAction::DeadLetter => EventDeliveryOutcome::DeadLettered,
                                }
                            }
                        },
                        Err(_) => {
                            match failure_router
                                .route(KafkaFailureRecord { message: &message }, KafkaFailureKind::Decode, attempt)
                                .await?
                            {
                                KafkaRetryAction::Retry { .. } => EventDeliveryOutcome::Retried,
                                KafkaRetryAction::DeadLetter => EventDeliveryOutcome::DeadLettered,
                            }
                        }
                    };
                    self.consumer
                        .commit_message(&message, CommitMode::Sync)
                        .map_err(|_| KafkaError::Commit)?;
                    observation.finish(NonZeroU16::new(attempt), outcome);
                }
            }
        }
    }
}

#[cfg(feature = "rdkafka")]
impl fmt::Debug for KafkaEventConsumer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaEventConsumer")
            .field("topic", &self.topic)
            .field("group_id", &self.group_id)
            .field("topics", &self.topics)
            .finish_non_exhaustive()
    }
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
    /// The retry topic was not subscribed by this event consumer group.
    #[error("Kafka event retry topic is not subscribed by this consumer")]
    RetryTopicNotSubscribed,
    /// Retry attempt headers were missing a usable single one-based integer.
    #[error("Kafka event retry metadata was invalid")]
    RetryMetadata,
    /// Kafka did not acknowledge retry or dead-letter publication.
    #[error("Kafka event failure route publish failed")]
    FailurePublish,
    /// A configured durable failure router could not persist the next retry action.
    #[error("Kafka event failure route could not be persisted")]
    FailureRoute,
}

#[cfg(feature = "rdkafka")]
#[derive(Clone, Debug)]
struct FailureOrigin {
    topic: String,
    partition: i32,
    offset: i64,
}

#[cfg(feature = "rdkafka")]
impl FailureOrigin {
    fn from_message(message: &BorrowedMessage<'_>) -> Self {
        Self {
            topic: first_header_text(message, ORIGIN_TOPIC_HEADER)
                .unwrap_or_else(|| message.topic().to_owned()),
            partition: first_header_text(message, ORIGIN_PARTITION_HEADER)
                .and_then(|value| value.parse().ok())
                .filter(|partition: &i32| *partition >= 0)
                .unwrap_or_else(|| message.partition()),
            offset: first_header_text(message, ORIGIN_OFFSET_HEADER)
                .and_then(|value| value.parse().ok())
                .filter(|offset: &i64| *offset >= 0)
                .unwrap_or_else(|| message.offset()),
        }
    }
}

#[cfg(feature = "rdkafka")]
fn retry_attempt(message: &BorrowedMessage<'_>) -> Result<u16, KafkaError> {
    let Some(headers) = message.headers() else {
        return Ok(1);
    };
    let mut attempts = headers
        .iter()
        .filter(|header| header.key == RETRY_ATTEMPT_HEADER);
    let Some(header) = attempts.next() else {
        return Ok(1);
    };
    if attempts.next().is_some() {
        return Err(KafkaError::RetryMetadata);
    }
    let value = header
        .value
        .and_then(|value| std::str::from_utf8(value).ok())
        .ok_or(KafkaError::RetryMetadata)?;
    value
        .parse::<u16>()
        .ok()
        .filter(|attempt| *attempt > 0)
        .ok_or(KafkaError::RetryMetadata)
}

#[cfg(feature = "rdkafka")]
fn first_header_text(message: &BorrowedMessage<'_>, name: &str) -> Option<String> {
    let headers = message.headers()?;
    headers
        .iter()
        .find(|header| header.key == name)
        .and_then(|header| header.value)
        .and_then(|value| std::str::from_utf8(value).ok())
        .filter(|value| !value.trim().is_empty() && value.len() <= 255)
        .map(ToOwned::to_owned)
}

fn validate_topic(topic: &str) -> Result<(), ConfigError> {
    if topic.trim().is_empty() || topic.chars().any(char::is_whitespace) {
        return Err(ConfigError::InvalidTopic);
    }
    Ok(())
}

fn validate_group_id(group_id: &str) -> Result<(), ConfigError> {
    if group_id.trim().is_empty() || group_id.chars().any(char::is_whitespace) {
        return Err(ConfigError::InvalidGroupId);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU16, time::Duration};

    use super::{
        ConfigError, KafkaConfig, KafkaConsumerConfig, KafkaRetryAction, KafkaRetryConfig,
    };

    #[test]
    fn native_option_values_are_redacted() {
        let config = KafkaConfig::new("broker:9092", "orders.paid.v1")
            .unwrap()
            .with_option("sasl.password", "secret");
        assert!(!format!("{config:?}").contains("secret"));
    }

    #[test]
    fn topic_with_whitespace_is_rejected() {
        let error = KafkaConfig::new("broker:9092", "orders paid").unwrap_err();
        assert_eq!(error, ConfigError::InvalidTopic);
    }

    #[test]
    fn consumer_group_with_whitespace_is_rejected() {
        let error =
            KafkaConsumerConfig::new("broker:9092", "orders.paid.v1", "billing group").unwrap_err();
        assert_eq!(error, ConfigError::InvalidGroupId);
    }

    #[test]
    fn retry_topic_must_differ_from_the_source_topic() {
        let error = KafkaConsumerConfig::new("broker:9092", "orders.paid.v1", "billing")
            .unwrap()
            .with_retry_topic("orders.paid.v1")
            .unwrap_err();

        assert_eq!(error, ConfigError::RetryTopicMatchesSource);
    }

    #[test]
    fn retry_policy_advances_then_dead_letters_at_its_delivery_budget() {
        let retry = KafkaRetryConfig::new(
            "orders.paid.retry.v1",
            "orders.paid.dlq.v1",
            NonZeroU16::new(3).unwrap(),
        )
        .unwrap();

        assert_eq!(
            retry.after_failure(1),
            KafkaRetryAction::Retry { next_attempt: 2 }
        );
        assert_eq!(
            retry.after_failure(2),
            KafkaRetryAction::Retry { next_attempt: 3 }
        );
        assert_eq!(retry.after_failure(3), KafkaRetryAction::DeadLetter);
    }

    #[test]
    fn retry_and_dead_letter_topics_must_differ() {
        let error = KafkaRetryConfig::new(
            "orders.paid.retry.v1",
            "orders.paid.retry.v1",
            NonZeroU16::new(2).unwrap(),
        )
        .unwrap_err();

        assert_eq!(error, ConfigError::RetryTopicMatchesDeadLetter);
    }

    #[test]
    fn delivery_deadline_must_be_representable_and_non_zero() {
        let zero = KafkaConfig::new("broker:9092", "orders.paid.v1")
            .unwrap()
            .with_delivery_timeout(Duration::ZERO)
            .unwrap_err();
        assert_eq!(zero, ConfigError::InvalidDeliveryTimeout);

        let too_large = KafkaConfig::new("broker:9092", "orders.paid.v1")
            .unwrap()
            .with_delivery_timeout(Duration::from_millis(i32::MAX as u64 + 1))
            .unwrap_err();
        assert_eq!(too_large, ConfigError::InvalidDeliveryTimeout);
    }
}
