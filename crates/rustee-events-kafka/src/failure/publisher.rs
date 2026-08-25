//! Kafka retry and dead-letter publisher execution.

use std::{fmt, time::Duration};

use futures_util::future::BoxFuture;
use rdkafka::{
    message::{BorrowedMessage, Header, Message, OwnedHeaders},
    producer::{FutureProducer, FutureRecord, Producer},
};

use super::{
    KafkaDelayedRetryRecord, KafkaFailureKind, KafkaFailureRecord, KafkaFailureRouter,
    KafkaRetryAction, KafkaRetryConfig,
    metadata::{
        FAILURE_KIND_HEADER, FailureOrigin, ORIGIN_OFFSET_HEADER, ORIGIN_PARTITION_HEADER,
        ORIGIN_TOPIC_HEADER, RETRY_ATTEMPT_HEADER,
    },
};
use crate::{KafkaConfig, KafkaError, create_producer, topic_metadata_is_healthy};

/// Kafka publisher for immediate retry and terminal dead-letter event records.
///
/// It publishes the original serialized event envelope, event key, one-based retry attempt, and
/// sanitized failure metadata. A source consumer commits only after this publisher receives the
/// configured broker acknowledgement, so a lost source commit can still duplicate a retry record.
#[derive(Clone)]
pub struct KafkaFailurePublisher {
    producer: FutureProducer,
    retry: KafkaRetryConfig,
    queue_timeout: Duration,
    topic_scoped_readiness: bool,
}

impl KafkaFailurePublisher {
    /// Creates a retry/dead-letter publisher using the same acknowledged producer settings as an
    /// event publisher.
    ///
    /// # Errors
    ///
    /// Returns a producer-configuration error when librdkafka rejects the producer settings.
    pub fn connect(config: &KafkaConfig, retry: KafkaRetryConfig) -> Result<Self, KafkaError> {
        Ok(Self {
            producer: create_producer(config)?,
            retry,
            queue_timeout: config.queue_timeout(),
            topic_scoped_readiness: true,
        })
    }

    /// Wraps an already-configured producer for dependency injection and tests.
    ///
    /// The caller retains all native client settings, including topic-creation policy. Readiness
    /// therefore uses a full metadata request before it checks the failure-routing topics.
    #[must_use]
    pub fn new(producer: FutureProducer, retry: KafkaRetryConfig, queue_timeout: Duration) -> Self {
        Self {
            producer,
            retry,
            queue_timeout,
            topic_scoped_readiness: false,
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
    /// Framework-created producers query each failure-routing topic directly because automatic
    /// topic creation is disabled. Injected native producers use full metadata before checking
    /// those topics, preserving the caller-owned configuration. Each metadata request is bounded
    /// by `timeout`.
    ///
    /// Returns a readiness error when either configured failure-routing topic cannot be read
    /// before the supplied timeout, is absent, or has a broker-reported error.
    pub fn readiness(&self, timeout: Duration) -> Result<(), KafkaError> {
        let topics = [self.retry.retry_topic(), self.retry.dead_letter_topic()];
        if self.topic_scoped_readiness {
            for topic in topics {
                let metadata = self
                    .producer
                    .client()
                    .fetch_metadata(Some(topic), timeout)
                    .map_err(|_| KafkaError::Readiness)?;
                if !topic_metadata_is_healthy(&metadata, topic) {
                    return Err(KafkaError::Readiness);
                }
            }
            return Ok(());
        }

        let metadata = self
            .producer
            .client()
            .fetch_metadata(None, timeout)
            .map_err(|_| KafkaError::Readiness)?;
        for topic in topics {
            if !topic_metadata_is_healthy(&metadata, topic) {
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
    /// Returns a failure-publication error when Kafka does not acknowledge the record.
    pub async fn publish_delayed_retry(
        &self,
        retry: KafkaDelayedRetryRecord<'_>,
    ) -> Result<(), KafkaError> {
        let headers = failure_headers(
            retry.attempt,
            retry.failure,
            retry.origin_topic,
            retry.origin_partition,
            retry.origin_offset,
        );
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
        origin: FailureOrigin,
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
        let headers = failure_headers(
            next_attempt,
            failure,
            &origin.topic,
            origin.partition,
            origin.offset,
        );
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

impl KafkaFailureRouter for KafkaFailurePublisher {
    fn retry_topic(&self) -> &str {
        self.retry.retry_topic()
    }

    fn dead_letter_topic(&self) -> &str {
        self.retry.dead_letter_topic()
    }

    fn route<'a>(
        &'a self,
        record: KafkaFailureRecord<'a>,
        failure: KafkaFailureKind,
        attempt: u16,
    ) -> BoxFuture<'a, Result<KafkaRetryAction, KafkaError>> {
        Box::pin(async move {
            self.route(record.message, record.origin, failure, attempt)
                .await
        })
    }
}

impl fmt::Debug for KafkaFailurePublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaFailurePublisher")
            .field("retry", &self.retry)
            .field("queue_timeout", &self.queue_timeout)
            .finish_non_exhaustive()
    }
}

fn failure_headers(
    attempt: u16,
    failure: KafkaFailureKind,
    origin_topic: &str,
    origin_partition: i32,
    origin_offset: i64,
) -> OwnedHeaders {
    let attempt = attempt.to_string();
    let partition = origin_partition.to_string();
    let offset = origin_offset.to_string();
    OwnedHeaders::new()
        .insert(Header {
            key: RETRY_ATTEMPT_HEADER,
            value: Some(&attempt),
        })
        .insert(Header {
            key: FAILURE_KIND_HEADER,
            value: Some(failure.as_str()),
        })
        .insert(Header {
            key: ORIGIN_TOPIC_HEADER,
            value: Some(origin_topic),
        })
        .insert(Header {
            key: ORIGIN_PARTITION_HEADER,
            value: Some(&partition),
        })
        .insert(Header {
            key: ORIGIN_OFFSET_HEADER,
            value: Some(&offset),
        })
}

#[cfg(test)]
mod tests {
    use rdkafka::message::Headers;

    use super::{KafkaFailureKind, failure_headers};

    #[test]
    fn failure_headers_preserve_retry_and_origin_metadata() {
        let headers = failure_headers(3, KafkaFailureKind::Handler, "tenant.acme.orders.v1", 2, 41);

        assert_eq!(headers.count(), 5);
        for (name, value) in [
            ("rustee-event-retry-attempt", b"3".as_slice()),
            ("rustee-event-failure-kind", b"handler".as_slice()),
            (
                "rustee-event-origin-topic",
                b"tenant.acme.orders.v1".as_slice(),
            ),
            ("rustee-event-origin-partition", b"2".as_slice()),
            ("rustee-event-origin-offset", b"41".as_slice()),
        ] {
            assert!(
                headers
                    .iter()
                    .any(|header| header.key == name && header.value == Some(value))
            );
        }
    }
}
