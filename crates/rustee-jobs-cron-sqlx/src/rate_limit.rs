//! `PostgreSQL` fixed-window governor policy and durable capacity operations.

use std::{fmt, num::NonZeroU32, time::Duration};

use sqlx::{Postgres, Row, Transaction};

use crate::{RecurringJobError, RecurringJobId, schedule::MAX_SCHEDULE_KEY_BYTES};

const MAX_RATE_LIMIT_WINDOW_MS: u64 = 31_536_000_000;

/// A stable application-owned identity shared by rate-governed recurring schedules.
///
/// This is not an HTTP identity and does not use a Redis rate-limit store. Schedules sharing this
/// key must use the same [`RecurringJobRateLimit`] policy; registration rejects a mismatch.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct RecurringJobRateLimitKey(String);

impl RecurringJobRateLimitKey {
    /// Creates a bounded, non-blank rate-governor key.
    ///
    /// # Errors
    ///
    /// Returns [`RecurringJobRateLimitKeyError::InvalidKey`] when the value is blank, has a NUL
    /// byte, or exceeds the durable storage bound.
    pub fn new(key: impl Into<String>) -> Result<Self, RecurringJobRateLimitKeyError> {
        let key = key.into();
        if key.trim().is_empty() || key.contains('\0') || key.len() > MAX_SCHEDULE_KEY_BYTES {
            return Err(RecurringJobRateLimitKeyError::InvalidKey);
        }
        Ok(Self(key))
    }

    /// Returns the stable shared scheduler rate-governor key.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RecurringJobRateLimitKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl fmt::Debug for RecurringJobRateLimitKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecurringJobRateLimitKey")
            .field("byte_len", &self.0.len())
            .finish()
    }
}

/// Invalid recurring scheduler rate-governor key metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RecurringJobRateLimitKeyError {
    /// The key was blank, contained a NUL byte, or exceeded the storage limit.
    #[error("recurring job rate-limit key must be non-blank, NUL-free, and bounded")]
    InvalidKey,
}

/// `PostgreSQL`-clock fixed-window governor for one or more recurring schedules.
///
/// The scheduler consumes one capacity unit immediately before staging a fresh outbox job. A
/// schedule that finds the shared window exhausted is deferred to the next window boundary and
/// does not create an outbox message in that pass.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecurringJobRateLimit {
    key: RecurringJobRateLimitKey,
    capacity: NonZeroU32,
    window: Duration,
}

impl RecurringJobRateLimit {
    /// Creates a bounded `PostgreSQL` fixed-window scheduler policy.
    ///
    /// `capacity` is one fresh scheduled job per unit. The current implementation bounds a
    /// window to 365 days because the durable schema uses signed millisecond timestamps. A
    /// window must be an exact number of milliseconds so the stored policy is never truncated.
    ///
    /// # Errors
    ///
    /// Returns [`RecurringJobRateLimitError`] when capacity or duration cannot be stored by the
    /// durable fixed-window schema.
    pub fn new(
        key: RecurringJobRateLimitKey,
        capacity: NonZeroU32,
        window: Duration,
    ) -> Result<Self, RecurringJobRateLimitError> {
        if capacity.get() > i32::MAX as u32 {
            return Err(RecurringJobRateLimitError::CapacityTooLarge);
        }
        if window.as_millis() == 0
            || !window.subsec_nanos().is_multiple_of(1_000_000)
            || window.as_millis() > u128::from(MAX_RATE_LIMIT_WINDOW_MS)
        {
            return Err(RecurringJobRateLimitError::InvalidWindow);
        }
        Ok(Self {
            key,
            capacity,
            window,
        })
    }

    /// Returns the shared scheduler governor key.
    #[must_use]
    pub fn key(&self) -> &RecurringJobRateLimitKey {
        &self.key
    }

    /// Returns the maximum fresh jobs staged in one fixed window across the shared key.
    #[must_use]
    pub const fn capacity(&self) -> NonZeroU32 {
        self.capacity
    }

    /// Returns the fixed-window duration.
    #[must_use]
    pub const fn window(&self) -> Duration {
        self.window
    }

    pub(super) fn window_ms(&self) -> i64 {
        i64::try_from(self.window.as_millis()).expect("validated rate-limit window fits i64")
    }

    pub(super) fn from_stored_parts(
        key: String,
        capacity: i32,
        window_ms: i64,
    ) -> Result<Self, RecurringJobError> {
        let key =
            RecurringJobRateLimitKey::new(key).map_err(|_| RecurringJobError::StoredSchedule)?;
        let capacity = u32::try_from(capacity)
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or(RecurringJobError::StoredSchedule)?;
        let window_ms = u64::try_from(window_ms).map_err(|_| RecurringJobError::StoredSchedule)?;
        Self::new(key, capacity, Duration::from_millis(window_ms))
            .map_err(|_| RecurringJobError::StoredSchedule)
    }
}

/// Invalid `PostgreSQL` recurring scheduler governor policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RecurringJobRateLimitError {
    /// The capacity exceeded the signed `PostgreSQL` integer storage bound.
    #[error("recurring job rate-limit capacity must fit a PostgreSQL integer")]
    CapacityTooLarge,
    /// The duration was not an exact durable millisecond interval within the fixed-window bound.
    #[error(
        "recurring job rate-limit window must be whole milliseconds from 1 millisecond through 365 days"
    )]
    InvalidWindow,
}

pub(super) async fn enforce_registration_policy(
    transaction: &mut Transaction<'_, Postgres>,
    rate_limit: &RecurringJobRateLimit,
) -> Result<(), RecurringJobError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(rate_limit.key().as_str())
        .execute(&mut **transaction)
        .await
        .map_err(RecurringJobError::storage)?;
    let row = sqlx::query(
        "SELECT rate_limit_capacity, rate_limit_window_ms \
         FROM rustee_recurring_jobs WHERE rate_limit_key = $1 LIMIT 1",
    )
    .bind(rate_limit.key().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RecurringJobError::storage)?;
    let Some(row) = row else {
        return Ok(());
    };
    let capacity = row
        .try_get::<Option<i32>, _>("rate_limit_capacity")
        .map_err(RecurringJobError::storage)?
        .ok_or(RecurringJobError::StoredSchedule)?;
    let window_ms = row
        .try_get::<Option<i64>, _>("rate_limit_window_ms")
        .map_err(RecurringJobError::storage)?
        .ok_or(RecurringJobError::StoredSchedule)?;
    if capacity == capacity_i32(rate_limit) && window_ms == rate_limit.window_ms() {
        Ok(())
    } else {
        Err(RecurringJobError::RateLimitPolicyConflict)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ConsumeOutcome {
    Allowed,
    Deferred { next_window_at: i64 },
}

pub(super) async fn consume_window(
    transaction: &mut Transaction<'_, Postgres>,
    rate_limit: &RecurringJobRateLimit,
    now: i64,
) -> Result<ConsumeOutcome, RecurringJobError> {
    let (window_started_at, next_window_at) = fixed_window_bounds(now, rate_limit.window_ms())?;
    let capacity = capacity_i32(rate_limit);
    let consumed = sqlx::query_scalar::<_, i32>(
        "INSERT INTO rustee_recurring_job_rate_windows \
         (rate_limit_key, capacity, window_ms, window_started_at_ms, consumed) \
         VALUES ($1, $2, $3, $4, 1) \
         ON CONFLICT (rate_limit_key) DO UPDATE \
         SET window_started_at_ms = EXCLUDED.window_started_at_ms, \
             consumed = CASE \
               WHEN rustee_recurring_job_rate_windows.window_started_at_ms = EXCLUDED.window_started_at_ms \
                 THEN rustee_recurring_job_rate_windows.consumed + 1 \
               ELSE 1 \
             END, \
             updated_at = clock_timestamp() \
         WHERE rustee_recurring_job_rate_windows.capacity = EXCLUDED.capacity \
           AND rustee_recurring_job_rate_windows.window_ms = EXCLUDED.window_ms \
           AND ( \
             rustee_recurring_job_rate_windows.window_started_at_ms <> EXCLUDED.window_started_at_ms \
             OR rustee_recurring_job_rate_windows.consumed < rustee_recurring_job_rate_windows.capacity \
           ) \
         RETURNING consumed",
    )
    .bind(rate_limit.key().as_str())
    .bind(capacity)
    .bind(rate_limit.window_ms())
    .bind(window_started_at)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RecurringJobError::storage)?;
    if consumed.is_some() {
        return Ok(ConsumeOutcome::Allowed);
    }

    let row = sqlx::query(
        "SELECT capacity, window_ms FROM rustee_recurring_job_rate_windows \
         WHERE rate_limit_key = $1",
    )
    .bind(rate_limit.key().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RecurringJobError::storage)?
    .ok_or(RecurringJobError::StorageInvariant)?;
    let stored_capacity = row
        .try_get::<i32, _>("capacity")
        .map_err(RecurringJobError::storage)?;
    let stored_window_ms = row
        .try_get::<i64, _>("window_ms")
        .map_err(RecurringJobError::storage)?;
    if stored_capacity != capacity || stored_window_ms != rate_limit.window_ms() {
        return Err(RecurringJobError::RateLimitPolicyConflict);
    }
    Ok(ConsumeOutcome::Deferred { next_window_at })
}

pub(super) async fn defer_schedule(
    transaction: &mut Transaction<'_, Postgres>,
    id: RecurringJobId,
    next_window_at: i64,
) -> Result<(), RecurringJobError> {
    let updated = sqlx::query(
        "UPDATE rustee_recurring_jobs \
         SET next_run_at = to_timestamp($2::double precision / 1000.0), \
             updated_at = clock_timestamp() \
         WHERE id = $1 AND enabled",
    )
    .bind(id.0)
    .bind(next_window_at)
    .execute(&mut **transaction)
    .await
    .map_err(RecurringJobError::storage)?;
    if updated.rows_affected() == 1 {
        Ok(())
    } else {
        Err(RecurringJobError::StorageInvariant)
    }
}

pub(super) fn fixed_window_bounds(
    now: i64,
    window_ms: i64,
) -> Result<(i64, i64), RecurringJobError> {
    if window_ms <= 0 {
        return Err(RecurringJobError::StoredSchedule);
    }
    let window_started_at = now
        .div_euclid(window_ms)
        .checked_mul(window_ms)
        .ok_or(RecurringJobError::ClockOutOfRange)?;
    let next_window_at = window_started_at
        .checked_add(window_ms)
        .ok_or(RecurringJobError::ClockOutOfRange)?;
    Ok((window_started_at, next_window_at))
}

pub(super) fn capacity_i32(rate_limit: &RecurringJobRateLimit) -> i32 {
    i32::try_from(rate_limit.capacity().get())
        .expect("validated rate-limit capacity fits PostgreSQL integer")
}
