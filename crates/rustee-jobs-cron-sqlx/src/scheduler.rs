//! `PostgreSQL` registration and transactional due-pass execution for recurring jobs.

use std::{fmt, sync::Arc};

use rustee_outbox_sqlx::{OutboxMessage, OutboxStageError, PostgresOutbox, StageOutcome};
use sqlx::{PgPool, Postgres, Transaction};

use super::model::{StoredRecurringJob, materialized_message};
use super::rate_limit::{ConsumeOutcome, consume_window, defer_schedule};
use super::{
    NoopRecurringJobFireObserver, RecurringJobError, RecurringJobFireLimit,
    RecurringJobFireObservation, RecurringJobFireObserver, RecurringJobFireOutcome,
    RecurringJobFireReport,
};

mod registration;

/// PostgreSQL-backed durable UTC cron scheduler.
///
/// This type does not start a task. Applications choose their own supervisor, polling cadence,
/// readiness policy, metrics, alerting, and graceful shutdown, then call [`Self::fire_due`] for
/// one short, atomic scheduler pass.

#[derive(Clone)]
pub struct PostgresRecurringJobs {
    pool: PgPool,
    observer: Arc<dyn RecurringJobFireObserver>,
}

impl PostgresRecurringJobs {
    /// Creates a scheduler from an application-owned `PostgreSQL` pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            observer: Arc::new(NoopRecurringJobFireObserver),
        }
    }

    /// Attaches one exporter-neutral scheduler pass observer.
    ///
    /// The observer receives bounded aggregate counts and a terminal outcome only. It cannot
    /// inspect schedule definitions, payloads, destinations, tenant context, or storage errors.
    #[must_use]
    pub fn with_fire_observer(mut self, observer: Arc<dyn RecurringJobFireObserver>) -> Self {
        self.observer = observer;
        self
    }

    /// Materializes one fresh job for each due schedule row in a bounded atomic pass.
    ///
    /// The pass uses `FOR UPDATE SKIP LOCKED`, so independently deployed scheduler processes
    /// can call it concurrently. A row is advanced only in the transaction that stages its new
    /// outbox message. A governed schedule consumes a `PostgreSQL` fixed-window permit in that same
    /// transaction, or moves to the next window boundary without staging. Late schedules fire
    /// once, then move to the first cron occurrence after the database clock rather than replaying
    /// every missed interval.
    ///
    /// # Errors
    ///
    /// Returns a sanitized storage error or a stored-schedule error. On any error the transaction
    /// rolls back, leaving each selected schedule eligible for a later corrected pass.
    pub async fn fire_due(
        &self,
        limit: RecurringJobFireLimit,
    ) -> Result<RecurringJobFireReport, RecurringJobError> {
        let observation = RecurringJobFireObservation::start(Arc::clone(&self.observer), limit);
        match self.fire_due_inner(limit).await {
            Ok(report) => {
                observation.finish(RecurringJobFireOutcome::Succeeded, Some(report));
                Ok(report)
            }
            Err(error) => {
                observation.finish(RecurringJobFireOutcome::Failed, None);
                Err(error)
            }
        }
    }

    async fn fire_due_inner(
        &self,
        limit: RecurringJobFireLimit,
    ) -> Result<RecurringJobFireReport, RecurringJobError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(RecurringJobError::storage)?;
        let now = database_clock_unix_ms_transaction(&mut transaction).await?;
        let batch_limit =
            i64::try_from(limit.get().get()).map_err(|_| RecurringJobError::StorageInvariant)?;
        let rows = sqlx::query(
            "SELECT id, destination, job_name, schema_version, payload, cron_expression, time_zone, priority, \
                    rate_limit_key, rate_limit_capacity, rate_limit_window_ms, \
                    floor(EXTRACT(EPOCH FROM next_run_at) * 1000)::bigint AS scheduled_at \
             FROM rustee_recurring_jobs \
             WHERE enabled AND next_run_at <= to_timestamp($1::double precision / 1000.0) \
             ORDER BY next_run_at, id \
             FOR UPDATE SKIP LOCKED \
             LIMIT $2",
        )
        .bind(now)
        .bind(batch_limit)
        .fetch_all(&mut *transaction)
        .await
        .map_err(RecurringJobError::storage)?;

        let claimed = u32::try_from(rows.len()).map_err(|_| RecurringJobError::StorageInvariant)?;
        let mut report = RecurringJobFireReport {
            claimed,
            staged: 0,
            rate_limited: 0,
        };
        for row in rows {
            let stored = StoredRecurringJob::from_row(&row)?;
            if let Some(rate_limit) = stored.rate_limit.as_ref() {
                match consume_window(&mut transaction, rate_limit, now).await? {
                    ConsumeOutcome::Allowed => {}
                    ConsumeOutcome::Deferred { next_window_at } => {
                        defer_schedule(&mut transaction, stored.id, next_window_at).await?;
                        report.rate_limited = report.rate_limited.saturating_add(1);
                        continue;
                    }
                }
            }
            let next_run_at = stored
                .expression
                .next_after_unix_ms(now, &stored.time_zone)?;
            let schedule_id = stored.id;
            let destination = stored.destination.clone();
            let priority = stored.priority;
            let message = materialized_message(stored, now)?;
            let outbox_message = OutboxMessage::from_job_message(destination, message)
                .map_err(|_| RecurringJobError::StoredSchedule)?
                .with_priority(priority);
            match PostgresOutbox
                .stage(&mut transaction, &outbox_message)
                .await
            {
                Ok(StageOutcome::Inserted(_)) => {
                    report.staged = report.staged.saturating_add(1);
                }
                Ok(StageOutcome::AlreadyPresent) => return Err(RecurringJobError::OutboxCollision),
                Err(OutboxStageError::Database(error)) => {
                    return Err(RecurringJobError::storage(error));
                }
            }
            let updated = sqlx::query(
                "UPDATE rustee_recurring_jobs \
                 SET next_run_at = to_timestamp($2::double precision / 1000.0), \
                     last_fired_at = clock_timestamp(), updated_at = clock_timestamp() \
                 WHERE id = $1",
            )
            .bind(schedule_id.0)
            .bind(next_run_at)
            .execute(&mut *transaction)
            .await
            .map_err(RecurringJobError::storage)?;
            if updated.rows_affected() != 1 {
                return Err(RecurringJobError::StorageInvariant);
            }
        }
        transaction
            .commit()
            .await
            .map_err(RecurringJobError::storage)?;
        Ok(report)
    }
}

impl fmt::Debug for PostgresRecurringJobs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresRecurringJobs")
            .finish_non_exhaustive()
    }
}

pub(super) async fn database_clock_unix_ms_transaction(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<i64, RecurringJobError> {
    sqlx::query_scalar("SELECT floor(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint")
        .fetch_one(&mut **transaction)
        .await
        .map_err(RecurringJobError::storage)
}
