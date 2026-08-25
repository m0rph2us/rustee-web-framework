//! Transactional outbox staging.

use std::fmt;

use sqlx::{Postgres, Transaction, postgres::PgArguments, query::Query};

use super::{
    EventSchedule, JobSchedule, OutboxKind, OutboxMessage, PostgresOutbox, ScheduleEventError,
    ScheduleJobError, StageOutcome,
};

/// Failure while staging one immediately eligible outbox message.
///
/// Display and debug output retain only a safe failure category. The database source remains
/// available through [`std::error::Error::source`] for trusted transaction diagnostics.
#[derive(thiserror::Error)]
pub enum OutboxStageError {
    /// `PostgreSQL` rejected the insert or the outbox migration is unavailable.
    #[error("PostgreSQL outbox staging failed")]
    Database(#[from] sqlx::Error),
}

impl fmt::Debug for OutboxStageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OutboxStageError")
            .field("kind", &"database_failed")
            .finish()
    }
}

impl PostgresOutbox {
    /// Inserts one message using the caller's existing business-data transaction.
    ///
    /// A rollback removes both the business mutation and this staged message. A successful commit
    /// makes it visible to a relay. The unique constraint also suppresses a repeated attempt to
    /// stage the same source message to the same destination.
    ///
    /// # Errors
    ///
    /// Returns a content-free [`OutboxStageError`] when the outbox migration is absent or the
    /// transaction fails. The database source remains available through the error chain.
    pub async fn stage(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        message: &OutboxMessage,
    ) -> Result<StageOutcome, OutboxStageError> {
        let result = bind_message(
            sqlx::query(
                "INSERT INTO rustee_outbox \
             (id, kind, destination, message_id, message_type, schema_version, ordering_key, \
              delivery_attempt, priority, payload) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
             ON CONFLICT (kind, destination, message_id) DO NOTHING",
            ),
            message,
        )
        .execute(&mut **transaction)
        .await?;
        if result.rows_affected() == 1 {
            Ok(StageOutcome::Inserted(message.id))
        } else {
            Ok(StageOutcome::AlreadyPresent)
        }
    }

    /// Inserts one durable job so it becomes eligible for relay after a PostgreSQL-clock delay.
    ///
    /// This stays inside the caller's business-data transaction, so rollback removes both the
    /// business mutation and the scheduled job. The unique source-message constraint still wins:
    /// a duplicate stage returns [`StageOutcome::AlreadyPresent`] and never shifts a job that was
    /// already scheduled. The relay must continue calling [`Self::lease_jobs`] to release due
    /// rows; this type deliberately owns no background task.
    ///
    /// # Errors
    ///
    /// Returns [`ScheduleJobError::NotAJob`] when an event message is supplied, or
    /// [`ScheduleJobError::Database`] when the outbox migration is absent or the transaction
    /// fails.
    pub async fn stage_job_after(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        message: &OutboxMessage,
        schedule: JobSchedule,
    ) -> Result<StageOutcome, ScheduleJobError> {
        if message.kind != OutboxKind::Job {
            return Err(ScheduleJobError::NotAJob);
        }
        self.stage_after(transaction, message, schedule.delay_millis())
            .await
            .map_err(ScheduleJobError::Database)
    }

    /// Inserts one append-only event so it becomes eligible for relay after a PostgreSQL-clock
    /// delay.
    ///
    /// A duplicate stage preserves the first durable availability timestamp. This primitive does
    /// not create a Kafka retry header, commit a broker offset, or decide a retry policy; those
    /// cross-store semantics belong to a provider-specific failure router.
    ///
    /// # Errors
    ///
    /// Returns [`ScheduleEventError::NotAnEvent`] when a job message is supplied, or
    /// [`ScheduleEventError::Database`] when the outbox migration is absent or the transaction
    /// fails.
    pub async fn stage_event_after(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        message: &OutboxMessage,
        schedule: EventSchedule,
    ) -> Result<StageOutcome, ScheduleEventError> {
        if message.kind != OutboxKind::Event {
            return Err(ScheduleEventError::NotAnEvent);
        }
        self.stage_after(transaction, message, schedule.delay_millis())
            .await
            .map_err(ScheduleEventError::Database)
    }

    async fn stage_after(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        message: &OutboxMessage,
        delay_millis: i64,
    ) -> Result<StageOutcome, sqlx::Error> {
        let result = bind_message(
            sqlx::query(
                "INSERT INTO rustee_outbox \
             (id, kind, destination, message_id, message_type, schema_version, ordering_key, \
              delivery_attempt, priority, payload, available_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, \
                     clock_timestamp() + ($11::bigint * INTERVAL '1 millisecond')) \
             ON CONFLICT (kind, destination, message_id) DO NOTHING",
            ),
            message,
        )
        .bind(delay_millis)
        .execute(&mut **transaction)
        .await?;
        if result.rows_affected() == 1 {
            Ok(StageOutcome::Inserted(message.id))
        } else {
            Ok(StageOutcome::AlreadyPresent)
        }
    }
}

fn bind_message<'query>(
    query: Query<'query, Postgres, PgArguments>,
    message: &'query OutboxMessage,
) -> Query<'query, Postgres, PgArguments> {
    query
        .bind(message.id.0)
        .bind(message.kind.as_str())
        .bind(message.destination.as_str())
        .bind(&message.message_id)
        .bind(&message.message_type)
        .bind(i32::from(message.schema_version))
        .bind(&message.ordering_key)
        .bind(i32::from(message.delivery_attempt))
        .bind(i16::from(message.priority.value()))
        .bind(&message.payload)
}
