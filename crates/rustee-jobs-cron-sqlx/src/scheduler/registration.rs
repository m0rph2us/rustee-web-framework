//! Durable registration and paused-schedule state transitions.

use rustee_jobs::Job;
use sqlx::{Postgres, Row, Transaction};
use uuid::Uuid;

use crate::model::{encode_schedule_payload, validate_materialized_job};
use crate::rate_limit::{capacity_i32, enforce_registration_policy};
use crate::{
    CronExpression, RecurringJob, RecurringJobError, RecurringJobId, RecurringJobKey,
    RecurringJobPauseOutcome, RecurringJobRateLimit, RecurringJobRegistration,
    RecurringJobResumeOutcome, RecurringJobTimeZone,
};

use super::{PostgresRecurringJobs, database_clock_unix_ms_transaction};

impl PostgresRecurringJobs {
    /// Registers one typed recurring job definition using its stable application-owned key.
    ///
    /// This calculates the first occurrence from the `PostgreSQL` clock. Exact repeated
    /// registrations return [`RecurringJobRegistration::AlreadyPresent`]; drift is rejected with
    /// [`RecurringJobError::RegistrationConflict`] so deployment config cannot silently change a
    /// live schedule.
    ///
    /// # Errors
    ///
    /// Returns a sanitized `PostgreSQL` error, an invalid payload error, or a conflict when the
    /// key
    /// already identifies a different definition.
    pub async fn register<J>(
        &self,
        job: &RecurringJob<J>,
    ) -> Result<RecurringJobRegistration, RecurringJobError>
    where
        J: Job,
    {
        let payload = encode_schedule_payload(job.payload())?;
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(RecurringJobError::storage)?;
        if let Some(rate_limit) = job.rate_limit() {
            enforce_registration_policy(&mut transaction, rate_limit).await?;
        }
        let now = database_clock_unix_ms_transaction(&mut transaction).await?;
        let next_run_at = job.expression().next_after_unix_ms(now, job.time_zone())?;
        let id = RecurringJobId::new();
        let occurrence = format!("rustee-recurring:{id}:{next_run_at}");
        validate_materialized_job(
            job.destination().clone(),
            J::NAME,
            J::VERSION,
            &payload,
            job.priority(),
            now,
            occurrence,
        )?;

        let registration =
            if insert_recurring_job_registration(&mut transaction, job, &payload, id, next_run_at)
                .await?
            {
                RecurringJobRegistration::Registered(id)
            } else {
                let existing_id =
                    existing_registration_id_if_identical(&mut transaction, job, &payload)
                        .await?
                        .ok_or(RecurringJobError::RegistrationConflict)?;
                RecurringJobRegistration::AlreadyPresent(existing_id)
            };
        transaction
            .commit()
            .await
            .map_err(RecurringJobError::storage)?;
        Ok(registration)
    }

    /// Pauses a schedule so later scheduler passes do not select it.
    ///
    /// This does not retract a job already committed to the outbox. Pause the downstream worker
    /// or use application-level cancellation when a previously generated job must not execute.
    ///
    /// # Errors
    ///
    /// Returns a sanitized `PostgreSQL` storage error when the update cannot complete.
    pub async fn pause(
        &self,
        key: &RecurringJobKey,
    ) -> Result<RecurringJobPauseOutcome, RecurringJobError> {
        let result = sqlx::query(
            "UPDATE rustee_recurring_jobs SET enabled = false, updated_at = clock_timestamp() \
         WHERE schedule_key = $1 AND enabled",
        )
        .bind(key.as_str())
        .execute(&self.pool)
        .await
        .map_err(RecurringJobError::storage)?;
        if result.rows_affected() == 1 {
            Ok(RecurringJobPauseOutcome::Paused)
        } else {
            Ok(RecurringJobPauseOutcome::NotFoundOrAlreadyPaused)
        }
    }

    /// Resumes a paused schedule from the first local occurrence after the `PostgreSQL` clock.
    ///
    /// Resume deliberately skips occurrences that elapsed while the schedule was paused. It never
    /// retracts an earlier outbox row and does not start a scheduler task; the application-owned
    /// supervisor performs the next [`Self::fire_due`] pass.
    ///
    /// # Errors
    ///
    /// Returns a sanitized `PostgreSQL` storage error or a stored-schedule error. An error rolls
    /// back the enable/next-run update together.
    pub async fn resume(
        &self,
        key: &RecurringJobKey,
    ) -> Result<RecurringJobResumeOutcome, RecurringJobError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(RecurringJobError::storage)?;
        let now = database_clock_unix_ms_transaction(&mut transaction).await?;
        let row = sqlx::query(
            "SELECT id, cron_expression, time_zone FROM rustee_recurring_jobs \
         WHERE schedule_key = $1 AND NOT enabled FOR UPDATE",
        )
        .bind(key.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RecurringJobError::storage)?;
        let Some(row) = row else {
            transaction
                .commit()
                .await
                .map_err(RecurringJobError::storage)?;
            return Ok(RecurringJobResumeOutcome::NotFoundOrAlreadyEnabled);
        };
        let id = row
            .try_get::<Uuid, _>("id")
            .map(RecurringJobId::from_uuid)
            .map_err(RecurringJobError::storage)?;
        let expression = row
            .try_get::<String, _>("cron_expression")
            .map_err(RecurringJobError::storage)
            .and_then(|value| {
                CronExpression::new(value).map_err(|_| RecurringJobError::StoredSchedule)
            })?;
        let time_zone = row
            .try_get::<String, _>("time_zone")
            .map_err(RecurringJobError::storage)
            .and_then(|value| {
                RecurringJobTimeZone::new(value).map_err(|_| RecurringJobError::StoredSchedule)
            })?;
        let next_run_at = expression.next_after_unix_ms(now, &time_zone)?;
        let updated = sqlx::query(
            "UPDATE rustee_recurring_jobs \
         SET enabled = true, next_run_at = to_timestamp($2::double precision / 1000.0), \
             updated_at = clock_timestamp() \
         WHERE id = $1 AND NOT enabled",
        )
        .bind(id.0)
        .bind(next_run_at)
        .execute(&mut *transaction)
        .await
        .map_err(RecurringJobError::storage)?;
        if updated.rows_affected() != 1 {
            return Err(RecurringJobError::StorageInvariant);
        }
        transaction
            .commit()
            .await
            .map_err(RecurringJobError::storage)?;
        Ok(RecurringJobResumeOutcome::Resumed)
    }
}

async fn insert_recurring_job_registration<J>(
    transaction: &mut Transaction<'_, Postgres>,
    job: &RecurringJob<J>,
    payload: &[u8],
    id: RecurringJobId,
    next_run_at: i64,
) -> Result<bool, RecurringJobError>
where
    J: Job,
{
    let rate_limit_key = job.rate_limit().map(|rate_limit| rate_limit.key().as_str());
    let rate_limit_capacity = job
        .rate_limit()
        .map(|rate_limit| i32::try_from(rate_limit.capacity().get()))
        .transpose()
        .map_err(|_| RecurringJobError::StoredSchedule)?;
    let rate_limit_window_ms = job.rate_limit().map(RecurringJobRateLimit::window_ms);

    sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO rustee_recurring_jobs \
         (id, schedule_key, destination, job_name, schema_version, payload, cron_expression, time_zone, priority, \
          rate_limit_key, rate_limit_capacity, rate_limit_window_ms, next_run_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, \
                 to_timestamp($13::double precision / 1000.0)) \
         ON CONFLICT (schedule_key) DO NOTHING \
         RETURNING id",
    )
    .bind(id.0)
    .bind(job.key().as_str())
    .bind(job.destination().as_str())
    .bind(J::NAME)
    .bind(i32::from(J::VERSION))
    .bind(payload)
    .bind(job.expression().as_str())
    .bind(job.time_zone().as_str())
    .bind(i16::from(job.priority().value()))
    .bind(rate_limit_key)
    .bind(rate_limit_capacity)
    .bind(rate_limit_window_ms)
    .bind(next_run_at)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RecurringJobError::storage)
    .map(|inserted| inserted.is_some())
}

async fn existing_registration_id_if_identical<J>(
    transaction: &mut Transaction<'_, Postgres>,
    job: &RecurringJob<J>,
    payload: &[u8],
) -> Result<Option<RecurringJobId>, RecurringJobError>
where
    J: Job,
{
    let row = sqlx::query(
        "SELECT id, destination, job_name, schema_version, payload, cron_expression, time_zone, priority, \
                rate_limit_key, rate_limit_capacity, rate_limit_window_ms \
         FROM rustee_recurring_jobs WHERE schedule_key = $1",
    )
    .bind(job.key().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RecurringJobError::storage)?
    .ok_or(RecurringJobError::StorageInvariant)?;
    let existing_id =
        RecurringJobId::from_uuid(row.try_get("id").map_err(RecurringJobError::storage)?);
    let existing_schema_version = row
        .try_get::<i32, _>("schema_version")
        .map_err(RecurringJobError::storage)?;
    let existing_priority = row
        .try_get::<i16, _>("priority")
        .map_err(RecurringJobError::storage)?;
    let existing_rate_limit_key = row
        .try_get::<Option<String>, _>("rate_limit_key")
        .map_err(RecurringJobError::storage)?;
    let existing_rate_limit_capacity = row
        .try_get::<Option<i32>, _>("rate_limit_capacity")
        .map_err(RecurringJobError::storage)?;
    let existing_rate_limit_window_ms = row
        .try_get::<Option<i64>, _>("rate_limit_window_ms")
        .map_err(RecurringJobError::storage)?;
    let rate_limit_matches = match (
        job.rate_limit(),
        existing_rate_limit_key,
        existing_rate_limit_capacity,
        existing_rate_limit_window_ms,
    ) {
        (None, None, None, None) => true,
        (Some(rate_limit), Some(key), Some(capacity), Some(window_ms)) => {
            key == rate_limit.key().as_str()
                && capacity == capacity_i32(rate_limit)
                && window_ms == rate_limit.window_ms()
        }
        _ => false,
    };
    let is_identical = row
        .try_get::<String, _>("destination")
        .map_err(RecurringJobError::storage)?
        == job.destination().as_str()
        && row
            .try_get::<String, _>("job_name")
            .map_err(RecurringJobError::storage)?
            == J::NAME
        && existing_schema_version == i32::from(J::VERSION)
        && row
            .try_get::<Vec<u8>, _>("payload")
            .map_err(RecurringJobError::storage)?
            == payload
        && row
            .try_get::<String, _>("cron_expression")
            .map_err(RecurringJobError::storage)?
            == job.expression().as_str()
        && row
            .try_get::<String, _>("time_zone")
            .map_err(RecurringJobError::storage)?
            == job.time_zone().as_str()
        && existing_priority == i16::from(job.priority().value())
        && rate_limit_matches;
    Ok(is_identical.then_some(existing_id))
}
