//! Durable row reconstruction and outbox job materialization.

use rustee_jobs::{JobId, JobMessage, MAX_JOB_ENVELOPE_BYTES, is_valid_job_name};
use rustee_json::to_vec_bounded;
use rustee_outbox_sqlx::{OutboxDestination, OutboxMessage, OutboxPriority};
use serde::Serialize;
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use crate::{
    CronExpression, RecurringJobError, RecurringJobId, RecurringJobRateLimit, RecurringJobTimeZone,
};

pub(crate) struct StoredRecurringJob {
    pub(crate) id: RecurringJobId,
    pub(crate) destination: OutboxDestination,
    job_name: String,
    schema_version: u16,
    payload: Value,
    pub(crate) expression: CronExpression,
    pub(crate) time_zone: RecurringJobTimeZone,
    pub(crate) priority: OutboxPriority,
    pub(crate) rate_limit: Option<RecurringJobRateLimit>,
    scheduled_at: i64,
}

impl StoredRecurringJob {
    pub(crate) fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, RecurringJobError> {
        let id = row
            .try_get::<Uuid, _>("id")
            .map(RecurringJobId::from_uuid)
            .map_err(RecurringJobError::storage)?;
        let destination = row
            .try_get::<String, _>("destination")
            .map_err(RecurringJobError::storage)
            .and_then(|value| {
                OutboxDestination::new(value).map_err(|_| RecurringJobError::StoredSchedule)
            })?;
        let job_name = row
            .try_get::<String, _>("job_name")
            .map_err(RecurringJobError::storage)?;
        let schema_version = u16::try_from(
            row.try_get::<i32, _>("schema_version")
                .map_err(RecurringJobError::storage)?,
        )
        .map_err(|_| RecurringJobError::StoredSchedule)?;
        let payload = row
            .try_get::<Vec<u8>, _>("payload")
            .map_err(RecurringJobError::storage)?;
        let payload = decode_stored_template(&job_name, &payload)?;
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
        let priority = row
            .try_get::<i16, _>("priority")
            .map_err(RecurringJobError::storage)
            .and_then(|value| {
                u8::try_from(value)
                    .map(OutboxPriority::new)
                    .map_err(|_| RecurringJobError::StoredSchedule)
            })?;
        let rate_limit_key = row
            .try_get::<Option<String>, _>("rate_limit_key")
            .map_err(RecurringJobError::storage)?;
        let rate_limit_capacity = row
            .try_get::<Option<i32>, _>("rate_limit_capacity")
            .map_err(RecurringJobError::storage)?;
        let rate_limit_window_ms = row
            .try_get::<Option<i64>, _>("rate_limit_window_ms")
            .map_err(RecurringJobError::storage)?;
        let rate_limit = match (rate_limit_key, rate_limit_capacity, rate_limit_window_ms) {
            (None, None, None) => None,
            (Some(key), Some(capacity), Some(window_ms)) => Some(
                RecurringJobRateLimit::from_stored_parts(key, capacity, window_ms)?,
            ),
            _ => return Err(RecurringJobError::StoredSchedule),
        };
        let scheduled_at = row
            .try_get::<i64, _>("scheduled_at")
            .map_err(RecurringJobError::storage)?;
        Ok(Self {
            id,
            destination,
            job_name,
            schema_version,
            payload,
            expression,
            time_zone,
            priority,
            rate_limit,
            scheduled_at,
        })
    }
}

#[derive(Serialize)]
struct MaterializedJobEnvelope {
    id: JobId,
    name: String,
    version: u16,
    payload: Value,
    idempotency_key: Option<String>,
    enqueued_at_unix_ms: u64,
    attempt: u16,
}

pub(crate) fn materialized_message(
    stored: StoredRecurringJob,
    now: i64,
) -> Result<JobMessage, RecurringJobError> {
    let StoredRecurringJob {
        id: schedule_id,
        job_name,
        schema_version,
        payload,
        scheduled_at,
        ..
    } = stored;
    let occurrence = format!("rustee-recurring:{schedule_id}:{scheduled_at}");
    render_job_message(
        JobId::new(),
        &job_name,
        schema_version,
        payload,
        now,
        Some(occurrence),
    )
    .map_err(|error| {
        if matches!(error, RecurringJobError::ClockOutOfRange) {
            error
        } else {
            RecurringJobError::StoredSchedule
        }
    })
}

pub(crate) fn encode_schedule_payload<T>(payload: &T) -> Result<Vec<u8>, RecurringJobError>
where
    T: Serialize + ?Sized,
{
    to_vec_bounded(payload, MAX_JOB_ENVELOPE_BYTES).map_err(|_| RecurringJobError::InvalidPayload)
}

pub(crate) fn validate_materialized_job(
    destination: OutboxDestination,
    job_name: &str,
    schema_version: u16,
    payload: &[u8],
    priority: OutboxPriority,
    now: i64,
    idempotency_key: String,
) -> Result<(), RecurringJobError> {
    let payload = decode_template_payload(payload)?;
    let message = render_job_message(
        JobId::new(),
        job_name,
        schema_version,
        payload,
        now,
        Some(idempotency_key),
    )?;
    OutboxMessage::from_job_message(destination, message)
        .map(|message| message.with_priority(priority))
        .map_err(|_| RecurringJobError::InvalidPayload)?;
    Ok(())
}

fn render_job_message(
    id: JobId,
    job_name: &str,
    schema_version: u16,
    payload: Value,
    now: i64,
    idempotency_key: Option<String>,
) -> Result<JobMessage, RecurringJobError> {
    let enqueued_at_unix_ms = u64::try_from(now).map_err(|_| RecurringJobError::ClockOutOfRange)?;
    let envelope = MaterializedJobEnvelope {
        id,
        name: job_name.to_owned(),
        version: schema_version,
        payload,
        idempotency_key,
        enqueued_at_unix_ms,
        attempt: 1,
    };
    let bytes = to_vec_bounded(&envelope, MAX_JOB_ENVELOPE_BYTES)
        .map_err(|_| RecurringJobError::InvalidPayload)?;
    JobMessage::from_parts(id, job_name, schema_version, 1, bytes)
        .map_err(|_| RecurringJobError::InvalidPayload)
}

fn decode_stored_template(job_name: &str, payload: &[u8]) -> Result<Value, RecurringJobError> {
    if !is_valid_job_name(job_name) {
        return Err(RecurringJobError::StoredSchedule);
    }
    decode_template_payload(payload).map_err(|_| RecurringJobError::StoredSchedule)
}

fn decode_template_payload(payload: &[u8]) -> Result<Value, RecurringJobError> {
    if payload.is_empty() || payload.len() > MAX_JOB_ENVELOPE_BYTES {
        return Err(RecurringJobError::InvalidPayload);
    }
    serde_json::from_slice(payload).map_err(|_| RecurringJobError::InvalidPayload)
}

#[cfg(test)]
mod tests {
    use rustee_jobs::{MAX_JOB_ENVELOPE_BYTES, MAX_JOB_NAME_BYTES};
    use rustee_outbox_sqlx::{OutboxDestination, OutboxPriority};
    use serde_json::Value;

    use super::{
        StoredRecurringJob, decode_stored_template, encode_schedule_payload, materialized_message,
        validate_materialized_job,
    };
    use crate::{CronExpression, RecurringJobError, RecurringJobId, RecurringJobTimeZone};

    #[test]
    fn registration_rejects_payloads_that_exceed_the_job_envelope_budget() {
        let payload = "x".repeat(MAX_JOB_ENVELOPE_BYTES);

        assert!(matches!(
            encode_schedule_payload(&payload),
            Err(RecurringJobError::InvalidPayload)
        ));
    }

    #[test]
    fn materialization_rejects_envelope_expansion_past_the_job_budget() {
        let payload = serde_json::to_vec(&"x".repeat(MAX_JOB_ENVELOPE_BYTES - 3)).unwrap();

        assert!(matches!(
            validate_materialized_job(
                OutboxDestination::new("jobs.billing").unwrap(),
                "billing.reminder",
                1,
                &payload,
                OutboxPriority::NORMAL,
                1_722_643_200_000,
                "rustee-recurring:test".to_owned(),
            ),
            Err(RecurringJobError::InvalidPayload)
        ));
    }

    #[test]
    fn stored_templates_revalidate_json_and_shared_job_name_contracts() {
        assert!(matches!(
            decode_stored_template("billing.reminder", b"not-json"),
            Err(RecurringJobError::StoredSchedule)
        ));
        assert!(matches!(
            decode_stored_template("billing reminder", br"null"),
            Err(RecurringJobError::StoredSchedule)
        ));
        assert!(matches!(
            validate_materialized_job(
                OutboxDestination::new("jobs.billing").unwrap(),
                &"x".repeat(MAX_JOB_NAME_BYTES + 1),
                1,
                br"null",
                OutboxPriority::NORMAL,
                1_722_643_200_000,
                "rustee-recurring:test".to_owned(),
            ),
            Err(RecurringJobError::InvalidPayload)
        ));
    }

    #[test]
    fn stored_materialization_preserves_a_clock_range_error() {
        let stored = StoredRecurringJob {
            id: RecurringJobId::new(),
            destination: OutboxDestination::new("jobs.billing").unwrap(),
            job_name: "billing.reminder".to_owned(),
            schema_version: 1,
            payload: Value::Null,
            expression: CronExpression::new("* * * * * * *").unwrap(),
            time_zone: RecurringJobTimeZone::default(),
            priority: OutboxPriority::NORMAL,
            rate_limit: None,
            scheduled_at: 0,
        };

        assert!(matches!(
            materialized_message(stored, -1),
            Err(RecurringJobError::ClockOutOfRange)
        ));
    }
}
