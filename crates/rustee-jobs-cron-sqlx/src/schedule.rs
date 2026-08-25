//! Validated recurring-schedule input models and next-occurrence calculation.

use std::{fmt, str::FromStr};

use chrono::{DateTime, LocalResult, TimeZone, Utc};
use chrono_tz::Tz;
use cron::Schedule;
use rustee_jobs::Job;
use rustee_outbox_sqlx::{OutboxDestination, OutboxPriority};

use crate::{RecurringJobError, RecurringJobRateLimit};

pub(super) const MAX_SCHEDULE_KEY_BYTES: usize = 255;
const MAX_CRON_EXPRESSION_BYTES: usize = 255;
const MAX_TIME_ZONE_BYTES: usize = 255;

/// A stable application-owned identity for one recurring schedule definition.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct RecurringJobKey(String);

impl RecurringJobKey {
    /// Creates a bounded, non-blank recurring schedule identity.
    ///
    /// Re-registering an identical definition under this key is safe. A changed destination,
    /// payload, job version, expression, or priority returns a conflict instead of silently
    /// replacing an active production schedule.
    ///
    /// # Errors
    ///
    /// Returns [`RecurringJobKeyError::InvalidKey`] when the value is blank, has a NUL byte, or
    /// exceeds the durable storage bound.
    pub fn new(key: impl Into<String>) -> Result<Self, RecurringJobKeyError> {
        let key = key.into();
        if key.trim().is_empty() || key.contains('\0') || key.len() > MAX_SCHEDULE_KEY_BYTES {
            return Err(RecurringJobKeyError::InvalidKey);
        }
        Ok(Self(key))
    }

    /// Returns the stable application-owned schedule key.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RecurringJobKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl fmt::Debug for RecurringJobKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecurringJobKey")
            .field("byte_len", &self.0.len())
            .finish()
    }
}

/// Invalid recurring schedule key metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RecurringJobKeyError {
    /// The schedule key was blank, contained a NUL byte, or exceeded the storage limit.
    #[error("recurring job key must be non-blank, NUL-free, and bounded")]
    InvalidKey,
}

/// A validated seven-field cron expression evaluated in a schedule-owned IANA time zone.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CronExpression(String);

impl CronExpression {
    /// Parses a cron expression supported by the `cron` crate.
    ///
    /// The expression has no implicit host-local time zone: [`crate::PostgresRecurringJobs`]
    /// always obtains a `PostgreSQL`-clock instant and pairs it with the stored
    /// [`RecurringJobTimeZone`] before calculating the next occurrence.
    ///
    /// # Errors
    ///
    /// Returns [`CronExpressionError::InvalidExpression`] when the expression is blank, too long,
    /// contains a NUL byte, or cannot be parsed by the supported cron parser.
    pub fn new(expression: impl Into<String>) -> Result<Self, CronExpressionError> {
        let expression = expression.into();
        if expression.trim().is_empty()
            || expression.contains('\0')
            || expression.len() > MAX_CRON_EXPRESSION_BYTES
            || Schedule::from_str(&expression).is_err()
        {
            return Err(CronExpressionError::InvalidExpression);
        }
        Ok(Self(expression))
    }

    /// Returns the validated cron expression text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn next_after_unix_ms(
        &self,
        after_unix_ms: i64,
        time_zone: &RecurringJobTimeZone,
    ) -> Result<i64, RecurringJobError> {
        let after = time_zone
            .time_zone()
            .from_utc_datetime(&unix_ms_to_utc(after_unix_ms)?.naive_utc());
        let schedule =
            Schedule::from_str(&self.0).map_err(|_| RecurringJobError::StoredSchedule)?;
        for occurrence in schedule.after(&after) {
            let occurrence_at = occurrence.timestamp_millis();
            if occurrence_at > after_unix_ms && is_earliest_local_occurrence(&occurrence) {
                return Ok(occurrence_at);
            }
        }
        Err(RecurringJobError::StoredSchedule)
    }
}

fn is_earliest_local_occurrence(occurrence: &DateTime<Tz>) -> bool {
    match occurrence
        .timezone()
        .from_local_datetime(&occurrence.naive_local())
    {
        LocalResult::Single(_) => true,
        LocalResult::Ambiguous(earlier, _) => {
            occurrence.timestamp_millis() == earlier.timestamp_millis()
        }
        LocalResult::None => false,
    }
}

fn unix_ms_to_utc(value: i64) -> Result<DateTime<Utc>, RecurringJobError> {
    Utc.timestamp_millis_opt(value)
        .single()
        .ok_or(RecurringJobError::ClockOutOfRange)
}

/// Invalid cron expression metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CronExpressionError {
    /// The cron expression did not fit the supported bounded parser contract.
    #[error("cron expression must be non-blank, NUL-free, bounded, and syntactically valid")]
    InvalidExpression,
}

/// A validated IANA time-zone identifier for one recurring schedule.
///
/// The value is stored with the schedule definition and never follows the host's local time zone.
/// `UTC` is the default for [`RecurringJob::new`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecurringJobTimeZone {
    name: String,
    time_zone: Tz,
}

impl RecurringJobTimeZone {
    /// Parses one bounded IANA time-zone identifier.
    ///
    /// # Errors
    ///
    /// Returns [`RecurringJobTimeZoneError::InvalidTimeZone`] for blank, NUL-containing, or
    /// oversized input and for names that are absent from the bundled IANA time-zone database.
    pub fn new(name: impl Into<String>) -> Result<Self, RecurringJobTimeZoneError> {
        let name = name.into();
        if name.trim().is_empty() || name.contains('\0') || name.len() > MAX_TIME_ZONE_BYTES {
            return Err(RecurringJobTimeZoneError::InvalidTimeZone);
        }
        let time_zone =
            Tz::from_str(&name).map_err(|_| RecurringJobTimeZoneError::InvalidTimeZone)?;
        Ok(Self { name, time_zone })
    }

    /// Returns the durable IANA time-zone identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.name
    }

    fn time_zone(&self) -> Tz {
        self.time_zone
    }
}

impl Default for RecurringJobTimeZone {
    fn default() -> Self {
        Self::new("UTC").expect("UTC is bundled in the IANA time-zone database")
    }
}

/// Invalid IANA time-zone metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RecurringJobTimeZoneError {
    /// The time zone was not a bounded IANA identifier in the bundled time-zone database.
    #[error("recurring job time zone must be a bounded IANA identifier in the bundled database")]
    InvalidTimeZone,
}

/// One typed recurring durable-job definition awaiting registration.
///
/// Each fired occurrence receives a fresh [`rustee_jobs::JobId`]. Build this from a payload rather
/// than a prebuilt [`rustee_jobs::JobEnvelope`], whose stable ID is intentionally for a single
/// delivery.
pub struct RecurringJob<J> {
    key: RecurringJobKey,
    destination: OutboxDestination,
    payload: J,
    expression: CronExpression,
    time_zone: RecurringJobTimeZone,
    priority: OutboxPriority,
    rate_limit: Option<RecurringJobRateLimit>,
}

impl<J> fmt::Debug for RecurringJob<J> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecurringJob")
            .field("key", &self.key)
            .field("destination", &self.destination)
            .field("expression", &self.expression)
            .field("time_zone", &self.time_zone)
            .field("priority", &self.priority)
            .field("rate_limit", &self.rate_limit)
            .field("payload", &"[REDACTED]")
            .finish()
    }
}

impl<J> RecurringJob<J>
where
    J: Job,
{
    /// Creates a recurring definition with normal outbox priority.
    #[must_use]
    pub fn new(
        key: RecurringJobKey,
        destination: OutboxDestination,
        payload: J,
        expression: CronExpression,
    ) -> Self {
        Self {
            key,
            destination,
            payload,
            expression,
            time_zone: RecurringJobTimeZone::default(),
            priority: OutboxPriority::NORMAL,
            rate_limit: None,
        }
    }

    /// Sets the local `PostgreSQL` outbox priority for every fired occurrence.
    ///
    /// This remains only a relay-ordering preference. It is not broker priority, fairness, or a
    /// global rate limit.
    #[must_use]
    pub fn with_priority(mut self, priority: OutboxPriority) -> Self {
        self.priority = priority;
        self
    }

    /// Evaluates the cron expression in this durable IANA time zone.
    ///
    /// A forward DST gap is skipped. A backward DST ambiguity materializes only the earlier UTC
    /// occurrence, so a repeated local wall-clock time fires once. This changes the declarative
    /// schedule definition and therefore conflicts with an existing registration under the same
    /// key unless it is identical.
    #[must_use]
    pub fn with_time_zone(mut self, time_zone: RecurringJobTimeZone) -> Self {
        self.time_zone = time_zone;
        self
    }

    /// Shares a PostgreSQL-clock fixed-window capacity with other schedules using this policy.
    ///
    /// Every generated job consumes one unit in the transaction that stages it to the outbox. A
    /// pass with no remaining capacity defers this schedule to the next fixed-window boundary;
    /// it does not create an outbox message or use the HTTP/Redis rate-limit adapter.
    #[must_use]
    pub fn with_rate_limit(mut self, rate_limit: RecurringJobRateLimit) -> Self {
        self.rate_limit = Some(rate_limit);
        self
    }

    /// Returns the stable registration key.
    #[must_use]
    pub fn key(&self) -> &RecurringJobKey {
        &self.key
    }

    /// Returns the logical outbox destination for generated jobs.
    #[must_use]
    pub fn destination(&self) -> &OutboxDestination {
        &self.destination
    }

    /// Returns the configured cron expression.
    #[must_use]
    pub fn expression(&self) -> &CronExpression {
        &self.expression
    }

    /// Returns the durable IANA time zone used to evaluate the expression.
    #[must_use]
    pub fn time_zone(&self) -> &RecurringJobTimeZone {
        &self.time_zone
    }

    /// Returns the configured local relay priority.
    #[must_use]
    pub const fn priority(&self) -> OutboxPriority {
        self.priority
    }

    /// Returns the optional `PostgreSQL` scheduler governor policy.
    #[must_use]
    pub fn rate_limit(&self) -> Option<&RecurringJobRateLimit> {
        self.rate_limit.as_ref()
    }

    pub(crate) fn payload(&self) -> &J {
        &self.payload
    }
}
