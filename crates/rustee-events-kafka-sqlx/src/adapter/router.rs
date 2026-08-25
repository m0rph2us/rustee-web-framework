use std::fmt;

use futures_util::future::BoxFuture;
use rustee_events::MAX_EVENT_ENVELOPE_BYTES;
use rustee_events_kafka::{
    KafkaError, KafkaFailureKind, KafkaFailurePublisher, KafkaFailureRecord, KafkaFailureRouter,
    KafkaRetryAction,
};
use sqlx::PgPool;
use uuid::Uuid;

use super::config::KafkaDelayedRetryDelay;

const MAX_DELAYED_RETRY_PAYLOAD_BYTES: usize = MAX_EVENT_ENVELOPE_BYTES;
const MAX_DELAYED_RETRY_KEY_BYTES: usize = MAX_EVENT_ENVELOPE_BYTES;

/// Stages retry attempts durably before the Kafka consumer commits its source offset.
///
/// Its `Debug` output keeps application pool and deployment-routing values redacted.
#[derive(Clone)]
pub struct PostgresKafkaDelayedRetryRouter {
    pool: PgPool,
    fallback: KafkaFailurePublisher,
    delay: KafkaDelayedRetryDelay,
}

impl fmt::Debug for PostgresKafkaDelayedRetryRouter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresKafkaDelayedRetryRouter")
            .field("pool", &"[REDACTED]")
            .field("fallback", &"[REDACTED]")
            .field("delay", &self.delay)
            .finish_non_exhaustive()
    }
}

impl PostgresKafkaDelayedRetryRouter {
    /// Creates a router with an application-owned pool, fallback publisher, and retry delay.
    #[must_use]
    pub fn new(
        pool: PgPool,
        fallback: KafkaFailurePublisher,
        delay: KafkaDelayedRetryDelay,
    ) -> Self {
        Self {
            pool,
            fallback,
            delay,
        }
    }
}

impl KafkaFailureRouter for PostgresKafkaDelayedRetryRouter {
    fn retry_topic(&self) -> &str {
        self.fallback.retry_config().retry_topic()
    }

    fn dead_letter_topic(&self) -> &str {
        self.fallback.retry_config().dead_letter_topic()
    }

    fn route<'a>(
        &'a self,
        record: KafkaFailureRecord<'a>,
        failure: KafkaFailureKind,
        attempt: u16,
    ) -> BoxFuture<'a, Result<KafkaRetryAction, KafkaError>> {
        Box::pin(async move {
            let action = self.fallback.retry_config().after_failure(attempt);
            let KafkaRetryAction::Retry { next_attempt } = action else {
                return KafkaFailureRouter::route(&self.fallback, record, failure, attempt).await;
            };
            let payload = record.payload().ok_or(KafkaError::MissingPayload)?;
            let key = record.key();
            validate_delayed_retry_record(payload, key)?;
            let failure_kind = match failure {
                KafkaFailureKind::Decode => "decode",
                KafkaFailureKind::Handler => "handler",
            };
            sqlx::query("INSERT INTO rustee_kafka_delayed_retries (id, origin_topic, origin_partition, origin_offset, retry_topic, retry_attempt, failure_kind, event_key, payload, available_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,clock_timestamp()+($10::bigint * INTERVAL '1 millisecond')) ON CONFLICT (origin_topic, origin_partition, origin_offset, retry_attempt) DO NOTHING")
                .bind(Uuid::new_v4())
                .bind(record.origin_topic())
                .bind(record.origin_partition())
                .bind(record.origin_offset())
                .bind(self.retry_topic())
                .bind(i32::from(next_attempt))
                .bind(failure_kind)
                .bind(key)
                .bind(payload)
                .bind(self.delay.milliseconds())
                .execute(&self.pool)
                .await
                .map_err(|_| KafkaError::FailureRoute)?;
            Ok(action)
        })
    }
}

pub(super) fn validate_delayed_retry_record(
    payload: &[u8],
    key: Option<&[u8]>,
) -> Result<(), KafkaError> {
    if payload.is_empty()
        || payload.len() > MAX_DELAYED_RETRY_PAYLOAD_BYTES
        || key.is_some_and(|key| key.len() > MAX_DELAYED_RETRY_KEY_BYTES)
    {
        return Err(KafkaError::FailureRoute);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU16, time::Duration};

    use rustee_events_kafka::{KafkaConfig, KafkaRetryConfig};
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    #[tokio::test]
    async fn router_debug_does_not_delegate_to_pool_or_publisher_diagnostics() {
        let producer = KafkaConfig::new("127.0.0.1:1", "tenant.acme.events.source").unwrap();
        let retry = KafkaRetryConfig::new(
            "tenant.acme.events.retry",
            "tenant.acme.events.dlq",
            NonZeroU16::new(2).unwrap(),
        )
        .unwrap();
        let fallback = KafkaFailurePublisher::connect(&producer, retry).unwrap();
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://rustee:rustee@127.0.0.1:1/rustee")
            .unwrap();
        let router = PostgresKafkaDelayedRetryRouter::new(
            pool,
            fallback,
            KafkaDelayedRetryDelay::new(Duration::from_secs(1)).unwrap(),
        );

        let debug = format!("{router:?}");
        for exposed in [
            "127.0.0.1",
            "tenant.acme.events.source",
            "tenant.acme.events.retry",
            "tenant.acme.events.dlq",
        ] {
            assert!(!debug.contains(exposed));
        }
        assert!(debug.contains("[REDACTED]"));
        assert!(debug.contains("delay"));
        assert_eq!(
            KafkaFailureRouter::dead_letter_topic(&router),
            "tenant.acme.events.dlq"
        );
    }

    #[test]
    fn delayed_retry_record_requires_a_non_empty_bounded_payload_and_key() {
        let allowed_payload = vec![0; MAX_DELAYED_RETRY_PAYLOAD_BYTES];
        let allowed_key = vec![0; MAX_DELAYED_RETRY_KEY_BYTES];
        assert_eq!(
            validate_delayed_retry_record(&allowed_payload, Some(&allowed_key)),
            Ok(())
        );

        let oversized = vec![0; MAX_DELAYED_RETRY_PAYLOAD_BYTES + 1];
        assert_eq!(
            validate_delayed_retry_record(&oversized, None),
            Err(KafkaError::FailureRoute)
        );
        assert_eq!(
            validate_delayed_retry_record(&[], None),
            Err(KafkaError::FailureRoute)
        );
        assert_eq!(
            validate_delayed_retry_record(&[0], Some(&oversized)),
            Err(KafkaError::FailureRoute)
        );
    }
}
