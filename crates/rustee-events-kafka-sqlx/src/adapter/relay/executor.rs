//! One bounded delayed-retry lease, publish, and settlement pass.

use rustee_events_kafka::{
    KafkaDelayedRetryRecord, KafkaError, KafkaFailureKind, validate_topic_name,
};
use sqlx::Row;
use uuid::Uuid;

use super::super::config::KafkaDelayedRetryRelayBatchSize;
use super::super::observation::{
    KafkaDelayedRetryRelayOutcome, KafkaDelayedRetryRelayPassObservation,
};
use super::super::router::validate_delayed_retry_record;
use super::PostgresKafkaDelayedRetryRelay;

const CLAIM_DUE_ROWS_SQL: &str = r"
WITH candidates AS (
    SELECT id
    FROM rustee_kafka_delayed_retries
    WHERE published_at IS NULL
      AND available_at <= clock_timestamp()
      AND (leased_until IS NULL OR leased_until <= clock_timestamp())
    ORDER BY available_at, created_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT $1
), claimed AS (
    UPDATE rustee_kafka_delayed_retries AS retry
    SET lease_token = $2,
        leased_until = clock_timestamp() + ($3::bigint * INTERVAL '1 millisecond'),
        relay_attempt = retry.relay_attempt + 1
    FROM candidates
    WHERE retry.id = candidates.id
    RETURNING retry.*
)
SELECT *
FROM claimed
ORDER BY available_at, created_at, id
";

impl PostgresKafkaDelayedRetryRelay {
    /// Publishes at most `batch_size` due rows in database due order and confirms each only after
    /// Kafka acknowledgement.
    ///
    /// A successful Kafka delivery followed by a failed `PostgreSQL` acknowledgement can publish a
    /// duplicate. Consumers must retain their normal event-id or domain-key idempotency boundary.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::FailureRoute`] when a durable retry row cannot be claimed, decoded,
    /// released, or acknowledged. Returns the Kafka publisher error after releasing every
    /// unpublished row claimed by this pass after the configured delay.
    pub async fn relay_once(
        &self,
        batch_size: KafkaDelayedRetryRelayBatchSize,
    ) -> Result<u16, KafkaError> {
        let observation =
            KafkaDelayedRetryRelayPassObservation::start(std::sync::Arc::clone(&self.observer));
        match self.relay_once_inner(batch_size).await {
            Ok(published) => {
                observation.finish(KafkaDelayedRetryRelayOutcome::Succeeded, Some(published));
                Ok(published)
            }
            Err(error) => {
                observation.finish(KafkaDelayedRetryRelayOutcome::Failed, None);
                Err(error)
            }
        }
    }

    async fn relay_once_inner(
        &self,
        batch_size: KafkaDelayedRetryRelayBatchSize,
    ) -> Result<u16, KafkaError> {
        let token = Uuid::new_v4();
        let rows = sqlx::query(CLAIM_DUE_ROWS_SQL)
            .bind(i64::from(batch_size.get()))
            .bind(token)
            .bind(self.config.lease().milliseconds())
            .fetch_all(&self.pool)
            .await
            .map_err(|_| KafkaError::FailureRoute)?;
        let mut published = 0;
        for row in rows {
            let retry = match Self::retry_from_row(&row) {
                Ok(retry) => retry,
                Err(error) => {
                    self.release_claims(token).await?;
                    return Err(error);
                }
            };
            if let Err(error) = self
                .publisher
                .publish_delayed_retry(KafkaDelayedRetryRecord {
                    retry_topic: &retry.retry_topic,
                    origin_topic: &retry.origin_topic,
                    origin_partition: retry.origin_partition,
                    origin_offset: retry.origin_offset,
                    failure: retry.failure,
                    attempt: retry.attempt,
                    key: retry.event_key.as_deref(),
                    payload: &retry.payload,
                })
                .await
            {
                self.release_claims(token).await?;
                return Err(error);
            }
            let changed = sqlx::query("UPDATE rustee_kafka_delayed_retries SET published_at=clock_timestamp(), leased_until=NULL, lease_token=NULL WHERE id=$1 AND lease_token=$2")
                .bind(retry.id)
                .bind(token)
                .execute(&self.pool)
                .await;
            let Ok(changed) = changed else {
                self.release_claims(token).await?;
                return Err(KafkaError::FailureRoute);
            };
            if changed.rows_affected() == 1 {
                published += 1;
            }
        }
        Ok(published)
    }

    fn retry_from_row(row: &sqlx::postgres::PgRow) -> Result<DelayedRetryRow, KafkaError> {
        let failure = match row
            .try_get::<String, _>("failure_kind")
            .map_err(|_| KafkaError::FailureRoute)?
            .as_str()
        {
            "decode" => KafkaFailureKind::Decode,
            "handler" => KafkaFailureKind::Handler,
            _ => return Err(KafkaError::FailureRoute),
        };
        let attempt = u16::try_from(
            row.try_get::<i32, _>("retry_attempt")
                .map_err(|_| KafkaError::FailureRoute)?,
        )
        .map_err(|_| KafkaError::FailureRoute)?;
        let event_key = row
            .try_get::<Option<Vec<u8>>, _>("event_key")
            .map_err(|_| KafkaError::FailureRoute)?;
        let payload = row
            .try_get::<Vec<u8>, _>("payload")
            .map_err(|_| KafkaError::FailureRoute)?;
        let retry_topic: String = row
            .try_get("retry_topic")
            .map_err(|_| KafkaError::FailureRoute)?;
        let origin_topic: String = row
            .try_get("origin_topic")
            .map_err(|_| KafkaError::FailureRoute)?;
        let origin_partition = row
            .try_get("origin_partition")
            .map_err(|_| KafkaError::FailureRoute)?;
        let origin_offset = row
            .try_get("origin_offset")
            .map_err(|_| KafkaError::FailureRoute)?;
        validate_stored_retry_record(
            &retry_topic,
            &origin_topic,
            origin_partition,
            origin_offset,
            attempt,
            &payload,
            event_key.as_deref(),
        )?;
        Ok(DelayedRetryRow {
            id: row.try_get("id").map_err(|_| KafkaError::FailureRoute)?,
            retry_topic,
            origin_topic,
            origin_partition,
            origin_offset,
            failure,
            attempt,
            event_key,
            payload,
        })
    }

    async fn release_claims(&self, token: Uuid) -> Result<(), KafkaError> {
        sqlx::query("UPDATE rustee_kafka_delayed_retries SET available_at=clock_timestamp()+($1::bigint * INTERVAL '1 millisecond'), leased_until=NULL, lease_token=NULL WHERE published_at IS NULL AND lease_token=$2")
            .bind(self.config.retry_after_failure().milliseconds())
            .bind(token)
            .execute(&self.pool)
            .await
            .map_err(|_| KafkaError::FailureRoute)?;
        Ok(())
    }
}

fn validate_stored_retry_record(
    retry_topic: &str,
    origin_topic: &str,
    origin_partition: i32,
    origin_offset: i64,
    attempt: u16,
    payload: &[u8],
    key: Option<&[u8]>,
) -> Result<(), KafkaError> {
    if attempt < 2 || origin_partition < 0 || origin_offset < 0 {
        return Err(KafkaError::FailureRoute);
    }
    validate_topic_name(retry_topic).map_err(|_| KafkaError::FailureRoute)?;
    validate_topic_name(origin_topic).map_err(|_| KafkaError::FailureRoute)?;
    validate_delayed_retry_record(payload, key)
}

struct DelayedRetryRow {
    id: Uuid,
    retry_topic: String,
    origin_topic: String,
    origin_partition: i32,
    origin_offset: i64,
    failure: KafkaFailureKind,
    attempt: u16,
    event_key: Option<Vec<u8>>,
    payload: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use rustee_events::MAX_EVENT_ENVELOPE_BYTES;
    use rustee_events_kafka::KafkaError;

    use super::{CLAIM_DUE_ROWS_SQL, validate_stored_retry_record};

    #[test]
    fn claimed_rows_retain_the_due_order_selected_for_the_batch() {
        assert!(
            CLAIM_DUE_ROWS_SQL
                .contains("SELECT *\nFROM claimed\nORDER BY available_at, created_at, id")
        );
    }

    #[test]
    fn stored_rows_revalidate_routing_metadata_attempt_payload_and_partition_key_bounds() {
        assert_eq!(
            validate_stored_retry_record(
                "orders.retry.v1",
                "orders.paid.v1",
                0,
                0,
                2,
                &[0],
                Some(&[1]),
            ),
            Ok(())
        );
        assert_eq!(
            validate_stored_retry_record("orders.retry.v1", "orders.paid.v1", 0, 0, 1, &[0], None),
            Err(KafkaError::FailureRoute)
        );
        assert_eq!(
            validate_stored_retry_record("orders.retry.v1", "orders.paid.v1", 0, 0, 2, &[], None),
            Err(KafkaError::FailureRoute)
        );
        assert_eq!(
            validate_stored_retry_record("orders retry", "orders.paid.v1", 0, 0, 2, &[0], None),
            Err(KafkaError::FailureRoute)
        );
        assert_eq!(
            validate_stored_retry_record("orders.retry.v1", "orders paid", 0, 0, 2, &[0], None),
            Err(KafkaError::FailureRoute)
        );
        assert_eq!(
            validate_stored_retry_record("orders.retry.v1", "orders.paid.v1", -1, 0, 2, &[0], None),
            Err(KafkaError::FailureRoute)
        );
        assert_eq!(
            validate_stored_retry_record("orders.retry.v1", "orders.paid.v1", 0, -1, 2, &[0], None),
            Err(KafkaError::FailureRoute)
        );

        let oversized = vec![0; MAX_EVENT_ENVELOPE_BYTES + 1];
        assert_eq!(
            validate_stored_retry_record(
                "orders.retry.v1",
                "orders.paid.v1",
                0,
                0,
                2,
                &oversized,
                None,
            ),
            Err(KafkaError::FailureRoute)
        );
        assert_eq!(
            validate_stored_retry_record(
                "orders.retry.v1",
                "orders.paid.v1",
                0,
                0,
                2,
                &[0],
                Some(&oversized),
            ),
            Err(KafkaError::FailureRoute)
        );
    }
}
