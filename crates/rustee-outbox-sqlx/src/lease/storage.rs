use std::time::Duration;

use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::{
    LeaseConfig, LeaseOutcome, MIN_POSTGRES_INTERVAL, OutboxDestination, OutboxId, OutboxKind,
};

use super::{
    error::OutboxError,
    record::{Lease, LeasedEvent, LeasedJob, StoredLease},
};

/// Shared `PostgreSQL` outbox storage operations.
///
/// This type has no background task. Applications control migration deployment, relay scheduling,
/// readiness, logging, metrics, and graceful shutdown explicitly.
#[derive(Clone, Copy, Debug, Default)]
pub struct PostgresOutbox;

impl PostgresOutbox {
    /// Claims available event rows for exactly one logical destination.
    ///
    /// Expired leases become eligible again. A relay must publish and then call
    /// [`Self::acknowledge_event`], or call [`Self::retry_event`] after a failed publish.
    ///
    /// # Errors
    ///
    /// Returns a database error or [`OutboxError::StoredEvent`] when rows violate the durable
    /// outbox contract and cannot safely reconstruct an event provider message.
    pub async fn lease_events(
        &self,
        pool: &PgPool,
        destination: &OutboxDestination,
        config: LeaseConfig,
    ) -> Result<Vec<LeasedEvent>, OutboxError> {
        let records = self
            .lease(pool, OutboxKind::Event, destination, config)
            .await?;
        records
            .into_iter()
            .map(LeasedEvent::try_from_record)
            .collect()
    }

    /// Claims available durable-job rows for exactly one logical destination.
    ///
    /// # Errors
    ///
    /// Returns a database error or [`OutboxError::StoredJob`] when rows violate the durable
    /// outbox contract and cannot safely reconstruct a job provider message.
    pub async fn lease_jobs(
        &self,
        pool: &PgPool,
        destination: &OutboxDestination,
        config: LeaseConfig,
    ) -> Result<Vec<LeasedJob>, OutboxError> {
        let records = self
            .lease(pool, OutboxKind::Job, destination, config)
            .await?;
        records
            .into_iter()
            .map(LeasedJob::try_from_record)
            .collect()
    }

    /// Confirms an event after the event publisher reports broker acknowledgement.
    ///
    /// A lost lease must be treated as possible duplicate delivery, not as proof that the broker
    /// append failed.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Database`] when the confirmation cannot be persisted.
    pub async fn acknowledge_event(
        &self,
        pool: &PgPool,
        lease: &LeasedEvent,
    ) -> Result<LeaseOutcome, OutboxError> {
        self.acknowledge(pool, lease.lease()).await
    }

    /// Confirms a durable job after its provider reports durable publication.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Database`] when the confirmation cannot be persisted.
    pub async fn acknowledge_job(
        &self,
        pool: &PgPool,
        lease: &LeasedJob,
    ) -> Result<LeaseOutcome, OutboxError> {
        self.acknowledge(pool, lease.lease()).await
    }

    /// Releases an event lease and makes the record eligible after a bounded delay.
    ///
    /// The persisted reason is the constant category `publish_failed`; raw provider error strings
    /// are deliberately not written to the database.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::InvalidDuration`] when a non-zero delay is below one millisecond or
    /// exceeds the bounded lease interval, or [`OutboxError::Database`] when the release cannot
    /// be persisted.
    pub async fn retry_event(
        &self,
        pool: &PgPool,
        lease: &LeasedEvent,
        delay: Duration,
    ) -> Result<LeaseOutcome, OutboxError> {
        self.retry(pool, lease.lease(), delay).await
    }

    /// Releases a durable-job lease and makes the record eligible after a bounded delay.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::InvalidDuration`] when a non-zero delay is below one millisecond or
    /// exceeds the bounded lease interval, or [`OutboxError::Database`] when the release cannot
    /// be persisted.
    pub async fn retry_job(
        &self,
        pool: &PgPool,
        lease: &LeasedJob,
        delay: Duration,
    ) -> Result<LeaseOutcome, OutboxError> {
        self.retry(pool, lease.lease(), delay).await
    }

    async fn lease(
        &self,
        pool: &PgPool,
        kind: OutboxKind,
        destination: &OutboxDestination,
        config: LeaseConfig,
    ) -> Result<Vec<StoredLease>, OutboxError> {
        let batch_size = i64::try_from(config.batch_size().get())
            .expect("lease batch size is bounded below i64::MAX");
        let lease_millis = duration_millis(config.lease_duration())?;
        let token = Uuid::new_v4();
        let rows = sqlx::query(
            "WITH candidates AS ( \\
               SELECT id \\
               FROM rustee_outbox \\
               WHERE published_at IS NULL \\
                 AND kind = $1 \\
                 AND destination = $2 \\
                 AND available_at <= clock_timestamp() \\
                 AND (leased_until IS NULL OR leased_until <= clock_timestamp()) \\
               ORDER BY priority DESC, created_at, id \\
               FOR UPDATE SKIP LOCKED \\
               LIMIT $3 \\
             ), claimed AS ( \\
               UPDATE rustee_outbox AS outbox \\
               SET lease_token = $4, \\
                   leased_until = clock_timestamp() + ($5::bigint * INTERVAL '1 millisecond'), \\
                   relay_attempt = outbox.relay_attempt + 1 \\
               FROM candidates \\
               WHERE outbox.id = candidates.id \\
               RETURNING outbox.id, outbox.message_id, outbox.message_type, \\
                         outbox.schema_version, outbox.ordering_key, outbox.delivery_attempt, \\
                         outbox.payload, outbox.relay_attempt, outbox.priority, outbox.created_at \\
             ) \\
             SELECT id, message_id, message_type, schema_version, ordering_key, delivery_attempt, \\
                    payload, relay_attempt \\
             FROM claimed \\
             ORDER BY priority DESC, created_at, id",
        )
        .bind(kind.as_str())
        .bind(destination.as_str())
        .bind(batch_size)
        .bind(token)
        .bind(lease_millis)
        .fetch_all(pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let id = row.try_get::<Uuid, _>("id")?;
                let stored_error = || match kind {
                    OutboxKind::Event => OutboxError::StoredEvent,
                    OutboxKind::Job => OutboxError::StoredJob,
                };
                let schema_version = u16::try_from(row.try_get::<i32, _>("schema_version")?)
                    .map_err(|_| stored_error())?;
                let delivery_attempt = u16::try_from(row.try_get::<i32, _>("delivery_attempt")?)
                    .map_err(|_| stored_error())?;
                let relay_attempt = u32::try_from(row.try_get::<i32, _>("relay_attempt")?)
                    .map_err(|_| stored_error())?;
                Ok(StoredLease {
                    lease: Lease {
                        id: OutboxId::from_uuid(id),
                        token,
                        relay_attempt,
                    },
                    destination: destination.clone(),
                    message_id: row.try_get("message_id")?,
                    message_type: row.try_get("message_type")?,
                    schema_version,
                    ordering_key: row.try_get("ordering_key")?,
                    delivery_attempt,
                    payload: row.try_get("payload")?,
                })
            })
            .collect()
    }

    async fn acknowledge(&self, pool: &PgPool, lease: &Lease) -> Result<LeaseOutcome, OutboxError> {
        let result = sqlx::query(
            "UPDATE rustee_outbox \\
             SET published_at = clock_timestamp(), lease_token = NULL, leased_until = NULL, \\
                 last_failure_kind = NULL \\
             WHERE id = $1 AND lease_token = $2 AND published_at IS NULL",
        )
        .bind(lease.id.0)
        .bind(lease.token)
        .execute(pool)
        .await?;
        Ok(outcome(result.rows_affected()))
    }

    async fn retry(
        &self,
        pool: &PgPool,
        lease: &Lease,
        delay: Duration,
    ) -> Result<LeaseOutcome, OutboxError> {
        let delay_millis = duration_millis(delay)?;
        let result = sqlx::query(
            "UPDATE rustee_outbox \\
             SET available_at = clock_timestamp() + ($3::bigint * INTERVAL '1 millisecond'), \\
                 lease_token = NULL, leased_until = NULL, last_failure_kind = 'publish_failed' \\
             WHERE id = $1 AND lease_token = $2 AND published_at IS NULL",
        )
        .bind(lease.id.0)
        .bind(lease.token)
        .bind(delay_millis)
        .execute(pool)
        .await?;
        Ok(outcome(result.rows_affected()))
    }
}

pub(super) fn duration_millis(duration: Duration) -> Result<i64, OutboxError> {
    if (!duration.is_zero() && duration < MIN_POSTGRES_INTERVAL)
        || duration > crate::timing::MAX_LEASE_DURATION
    {
        return Err(OutboxError::InvalidDuration);
    }
    i64::try_from(duration.as_millis()).map_err(|_| OutboxError::InvalidDuration)
}

fn outcome(rows_affected: u64) -> LeaseOutcome {
    if rows_affected == 1 {
        LeaseOutcome::Applied
    } else {
        LeaseOutcome::Lost
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::duration_millis;
    use crate::OutboxError;

    #[test]
    fn retry_duration_keeps_explicit_zero_but_rejects_sub_millisecond_values() {
        assert_eq!(duration_millis(Duration::ZERO).unwrap(), 0);
        assert!(matches!(
            duration_millis(Duration::from_nanos(1)),
            Err(OutboxError::InvalidDuration)
        ));
        assert_eq!(
            OutboxError::InvalidDuration.to_string(),
            "outbox duration must be zero or at least 1 millisecond, and at most one hour"
        );
    }
}
