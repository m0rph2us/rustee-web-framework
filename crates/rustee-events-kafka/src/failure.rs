//! Kafka failure routing policy, records, and preserved source metadata.

use std::{fmt, num::NonZeroU16};

#[cfg(feature = "rdkafka")]
use futures_util::future::BoxFuture;
#[cfg(feature = "rdkafka")]
use rdkafka::message::{BorrowedMessage, Message};

#[cfg(feature = "rdkafka")]
use super::KafkaError;
use super::{ConfigError, config::validate_topic_name};

#[cfg(feature = "rdkafka")]
mod metadata;
#[cfg(feature = "rdkafka")]
mod publisher;
#[cfg(feature = "rdkafka")]
use metadata::FailureOrigin;
#[cfg(feature = "rdkafka")]
pub(super) use metadata::retry_attempt;
#[cfg(feature = "rdkafka")]
pub use publisher::KafkaFailurePublisher;

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
    /// Returns a configuration error when a route topic is invalid or both routes use one topic.
    pub fn new(
        retry_topic: impl Into<String>,
        dead_letter_topic: impl Into<String>,
        max_deliveries: NonZeroU16,
    ) -> Result<Self, ConfigError> {
        let retry_topic = retry_topic.into();
        let dead_letter_topic = dead_letter_topic.into();
        validate_topic_name(&retry_topic)?;
        validate_topic_name(&dead_letter_topic)?;
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
            .field("retry_topic", &"[REDACTED]")
            .field("retry_topic_length", &self.retry_topic.len())
            .field("dead_letter_topic", &"[REDACTED]")
            .field("dead_letter_topic_length", &self.dead_letter_topic.len())
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
    origin: FailureOrigin,
}

/// A persisted retry record ready for Kafka delivery.
///
/// Payload, partition key, and deployment-routing values are redacted from its Debug
/// implementation.
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
            .field("retry_topic", &"[REDACTED]")
            .field("retry_topic_length", &self.retry_topic.len())
            .field("origin_topic", &"[REDACTED]")
            .field("origin_topic_length", &self.origin_topic.len())
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
    pub(super) fn new<'a>(
        message: &'a BorrowedMessage<'a>,
    ) -> Result<KafkaFailureRecord<'a>, KafkaError> {
        Ok(KafkaFailureRecord {
            message,
            origin: FailureOrigin::from_message(message)?,
        })
    }

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
        self.origin.topic.clone()
    }

    /// Returns the original source partition.
    #[must_use]
    pub fn origin_partition(&self) -> i32 {
        self.origin.partition
    }

    /// Returns the original source offset.
    #[must_use]
    pub fn origin_offset(&self) -> i64 {
        self.origin.offset
    }
}

/// Routes a failed record before the consumer synchronously commits its source offset.
#[cfg(feature = "rdkafka")]
pub trait KafkaFailureRouter: Send + Sync {
    /// Returns the retry topic the consumer must subscribe to.
    fn retry_topic(&self) -> &str;

    /// Returns the terminal dead-letter topic, which must differ from source and retry topics.
    fn dead_letter_topic(&self) -> &str;

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

#[cfg(all(test, feature = "rdkafka"))]
mod tests {
    use super::{KafkaDelayedRetryRecord, KafkaFailureKind};

    #[test]
    fn delayed_retry_debug_output_redacts_deployment_routing_values() {
        let record = KafkaDelayedRetryRecord {
            retry_topic: "tenant.acme.orders.retry.v1",
            origin_topic: "tenant.acme.orders.v1",
            origin_partition: 2,
            origin_offset: 41,
            failure: KafkaFailureKind::Handler,
            attempt: 3,
            key: Some(b"partition-key"),
            payload: br#"{\"private\":true}"#,
        };

        let debug = format!("{record:?}");
        for exposed in [
            "tenant.acme.orders.retry.v1",
            "tenant.acme.orders.v1",
            "partition-key",
            "private",
        ] {
            assert!(!debug.contains(exposed));
        }
        assert!(debug.contains("retry_topic_length"));
        assert!(debug.contains("origin_topic_length"));
    }
}
