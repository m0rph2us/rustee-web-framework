//! Durable IANA-time-zone cron schedules that atomically materialize fresh Rustee jobs through
//! `PostgreSQL`.
//!
//! Applications add [`RECURRING_JOB_MIGRATION_SQL`] after the Rustee outbox migrations, then call
//! [`PostgresRecurringJobs::fire_due`] from their own supervised loop. The scheduler locks due
//! rows with `FOR UPDATE SKIP LOCKED`, stages one fresh job in the same transaction, and advances
//! the next run before commit. An optional `PostgreSQL` fixed-window governor also consumes or
//! defers in that same transaction. The scheduler therefore survives process restarts without a
//! memory timer or a cross-store handoff gap.
//!
//! Every expression is evaluated in a durable IANA time zone, defaulting to `UTC`. A nonexistent
//! local time during a forward DST transition is skipped; an ambiguous local time during a
//! backward transition fires only its earlier UTC occurrence. A late scheduler run creates at
//! most one job per schedule and advances to the first occurrence after the `PostgreSQL` clock;
//! it intentionally does not replay every missed occurrence. This crate does not provide holiday
//! calendars, broker priority, tenant fairness, external-provider quotas, or an application
//! background task.

use std::{
    fmt,
    num::{NonZeroU32, NonZeroUsize},
    panic::{AssertUnwindSafe, catch_unwind},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{DateTime, LocalResult, TimeZone, Utc};
use chrono_tz::Tz;
use cron::Schedule;
use rustee_jobs::{Job, JobId, JobMessage};
use rustee_outbox_sqlx::{
    OutboxDestination, OutboxMessage, OutboxPriority, PostgresOutbox, StageOutcome,
};
use serde::Serialize;
use serde_json::Value;
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

const MAX_SCHEDULE_KEY_BYTES: usize = 255;
const MAX_CRON_EXPRESSION_BYTES: usize = 255;
const MAX_TIME_ZONE_BYTES: usize = 255;
const MAX_FIRE_BATCH_SIZE: usize = 100;
const MAX_RATE_LIMIT_WINDOW_MS: u64 = 31_536_000_000;

/// The deployment-owned migration for durable recurring Rustee job schedules.
///
/// Apply this after `rustee_outbox_sqlx::OUTBOX_MIGRATION_SQL` and
/// `rustee_outbox_sqlx::OUTBOX_PRIORITY_MIGRATION_SQL`. The application owns migration ordering
/// and must not apply this from HTTP application startup.
pub const RECURRING_JOB_MIGRATION_SQL: &str =
    include_str!("../migrations/0001_rustee_recurring_jobs.sql");

/// The forward-only migration that adds the `PostgreSQL` scheduler rate governor.
///
/// Apply this after [`RECURRING_JOB_MIGRATION_SQL`]. It is deliberately separate so existing
/// schedule deployments can choose and review the new durable governor boundary explicitly.
pub const RECURRING_JOB_RATE_GOVERNOR_MIGRATION_SQL: &str =
    include_str!("../migrations/0002_rustee_recurring_job_rate_governor.sql");

/// The forward-only migration that adds a durable IANA time zone to every recurring schedule.
///
/// Apply this after [`RECURRING_JOB_MIGRATION_SQL`], and after
/// [`RECURRING_JOB_RATE_GOVERNOR_MIGRATION_SQL`] when the optional governor migration is used.
/// Existing rows become `UTC`. The application owns migration ordering and must qualify each
/// bundled time-zone database update before using it for production schedule calculation.
pub const RECURRING_JOB_TIME_ZONE_MIGRATION_SQL: &str =
    include_str!("../migrations/0003_rustee_recurring_job_time_zone.sql");

/// A stable application-owned identity for one recurring schedule definition.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
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

/// Invalid recurring schedule key metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RecurringJobKeyError {
    /// The schedule key was blank, contained a NUL byte, or exceeded the storage limit.
    #[error("recurring job key must be non-blank, NUL-free, and bounded")]
    InvalidKey,
}

/// A stable application-owned identity shared by rate-governed recurring schedules.
///
/// This is not an HTTP identity and does not use a Redis rate-limit store. Schedules sharing this
/// key must use the same [`RecurringJobRateLimit`] policy; registration rejects a mismatch.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
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
    /// window to 365 days because the durable schema uses signed millisecond timestamps.
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
        if window.is_zero() || window.as_millis() > u128::from(MAX_RATE_LIMIT_WINDOW_MS) {
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

    fn window_ms(&self) -> i64 {
        i64::try_from(self.window.as_millis()).expect("validated rate-limit window fits i64")
    }

    fn from_stored_parts(
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
    /// The duration was zero or outside the durable fixed-window bound.
    #[error("recurring job rate-limit window must be a positive duration of at most 365 days")]
    InvalidWindow,
}

/// A validated seven-field cron expression evaluated in a schedule-owned IANA time zone.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CronExpression(String);

impl CronExpression {
    /// Parses a cron expression supported by the `cron` crate.
    ///
    /// The expression has no implicit host-local time zone: [`PostgresRecurringJobs`] always
    /// obtains a `PostgreSQL`-clock instant and pairs it with the stored
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

    fn next_after_unix_ms(
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
/// Each fired occurrence receives a fresh [`JobId`]. Build this from a payload rather than a
/// prebuilt [`rustee_jobs::JobEnvelope`], whose stable ID is intentionally for a single delivery.
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
}

/// Identifier assigned to a durable recurring schedule row.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RecurringJobId(Uuid);

impl RecurringJobId {
    /// Creates a fresh durable recurring schedule identifier.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }
}

impl Default for RecurringJobId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RecurringJobId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Result of declaratively registering one recurring job definition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecurringJobRegistration {
    /// A new durable schedule row was created.
    Registered(RecurringJobId),
    /// An exact existing definition already owns this schedule key.
    AlreadyPresent(RecurringJobId),
}

/// A bounded number of due schedules materialized in one scheduler pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecurringJobFireLimit(NonZeroUsize);

impl RecurringJobFireLimit {
    /// Creates a bounded scheduler pass limit.
    ///
    /// # Errors
    ///
    /// Returns [`RecurringJobFireLimitError::TooLarge`] when the requested pass would lock too
    /// many schedule rows at once.
    pub fn new(limit: NonZeroUsize) -> Result<Self, RecurringJobFireLimitError> {
        if limit.get() > MAX_FIRE_BATCH_SIZE {
            return Err(RecurringJobFireLimitError::TooLarge);
        }
        Ok(Self(limit))
    }

    /// Returns the maximum number of due schedule rows claimed by this pass.
    #[must_use]
    pub const fn get(self) -> NonZeroUsize {
        self.0
    }
}

impl Default for RecurringJobFireLimit {
    fn default() -> Self {
        Self(NonZeroUsize::new(25).expect("default recurring fire limit is non-zero"))
    }
}

/// Invalid recurring scheduler pass limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RecurringJobFireLimitError {
    /// A single pass would lock more than the fixed bounded schedule batch.
    #[error("recurring job fire limit must be at most 100")]
    TooLarge,
}

/// Bounded result of one [`PostgresRecurringJobs::fire_due`] pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RecurringJobFireReport {
    claimed: u32,
    staged: u32,
    rate_limited: u32,
}

impl RecurringJobFireReport {
    /// Returns how many due schedule rows this pass locked.
    #[must_use]
    pub const fn claimed(self) -> u32 {
        self.claimed
    }

    /// Returns how many fresh job envelopes were atomically staged to the outbox.
    #[must_use]
    pub const fn staged(self) -> u32 {
        self.staged
    }

    /// Returns how many due schedules were deferred because their shared fixed window was full.
    ///
    /// These rows did not create an outbox message in this pass and were moved to the next
    /// PostgreSQL-clock window boundary.
    #[must_use]
    pub const fn rate_limited(self) -> u32 {
        self.rate_limited
    }
}

/// Terminal result of one bounded recurring-scheduler pass.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RecurringJobFireOutcome {
    /// The pass committed every selected schedule transition.
    Succeeded,
    /// Storage or stored-schedule validation caused the transaction to roll back.
    Failed,
    /// The caller cancelled or dropped the pass before it returned a terminal result.
    Abandoned,
}

impl RecurringJobFireOutcome {
    /// Returns the stable exporter-safe outcome label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Abandoned => "abandoned",
        }
    }
}

/// Bounded metadata emitted when one scheduler pass starts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecurringJobFireStarted {
    limit: RecurringJobFireLimit,
}

impl RecurringJobFireStarted {
    /// Returns the maximum due rows this pass may claim.
    #[must_use]
    pub const fn limit(self) -> RecurringJobFireLimit {
        self.limit
    }
}

/// Terminal metadata emitted after one scheduler pass finishes or is abandoned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecurringJobFireFinished {
    outcome: RecurringJobFireOutcome,
    report: Option<RecurringJobFireReport>,
    duration: Duration,
}

impl RecurringJobFireFinished {
    /// Returns the sanitized terminal outcome.
    #[must_use]
    pub const fn outcome(self) -> RecurringJobFireOutcome {
        self.outcome
    }

    /// Returns aggregate counts only when the pass committed successfully.
    ///
    /// A failed or externally abandoned transaction returns `None`; callers must not infer how
    /// many staged rows became durable from work that was later rolled back.
    #[must_use]
    pub const fn report(self) -> Option<RecurringJobFireReport> {
        self.report
    }

    /// Returns elapsed pass time, including transaction and outbox staging work.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.duration
    }
}

/// Synchronous, non-blocking observer for a recurring scheduler pass.
///
/// Observers should aggregate locally or hand work to a bounded exporter queue. Observer panics
/// are caught so telemetry cannot alter durable schedule or outbox semantics.
pub trait RecurringJobFireObserver: Send + Sync + 'static {
    /// Records the beginning of one bounded scheduler pass.
    fn on_fire_started(&self, pass: RecurringJobFireStarted);

    /// Records one committed, failed, or externally abandoned scheduler pass.
    fn on_fire_finished(&self, pass: RecurringJobFireFinished);
}

/// No-op observer used unless the scheduler opts into pass observability.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopRecurringJobFireObserver;

impl RecurringJobFireObserver for NoopRecurringJobFireObserver {
    fn on_fire_started(&self, _pass: RecurringJobFireStarted) {}

    fn on_fire_finished(&self, _pass: RecurringJobFireFinished) {}
}

/// In-progress observability value owned by one [`PostgresRecurringJobs::fire_due`] future.
///
/// Dropping this value without [`Self::finish`] records an `abandoned` pass, including task
/// cancellation while a transaction was active. It contains no schedule key, destination,
/// payload, tenant, or storage error detail.
pub struct RecurringJobFireObservation {
    observer: Arc<dyn RecurringJobFireObserver>,
    started_at: Instant,
    finished: bool,
}

impl RecurringJobFireObservation {
    /// Starts observing one bounded recurring scheduler pass.
    #[must_use]
    pub fn start(
        observer: Arc<dyn RecurringJobFireObserver>,
        limit: RecurringJobFireLimit,
    ) -> Self {
        notify_fire_started(&observer, RecurringJobFireStarted { limit });
        Self {
            observer,
            started_at: Instant::now(),
            finished: false,
        }
    }

    /// Emits a terminal outcome after the pass has returned.
    pub fn finish(
        mut self,
        outcome: RecurringJobFireOutcome,
        report: Option<RecurringJobFireReport>,
    ) {
        self.finished = true;
        notify_fire_finished(
            &self.observer,
            RecurringJobFireFinished {
                outcome,
                report,
                duration: self.started_at.elapsed(),
            },
        );
    }
}

impl Drop for RecurringJobFireObservation {
    fn drop(&mut self) {
        if !self.finished {
            notify_fire_finished(
                &self.observer,
                RecurringJobFireFinished {
                    outcome: RecurringJobFireOutcome::Abandoned,
                    report: None,
                    duration: self.started_at.elapsed(),
                },
            );
        }
    }
}

impl fmt::Debug for RecurringJobFireObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecurringJobFireObservation")
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

fn notify_fire_started(
    observer: &Arc<dyn RecurringJobFireObserver>,
    pass: RecurringJobFireStarted,
) {
    let _ = catch_unwind(AssertUnwindSafe(|| observer.on_fire_started(pass)));
}

fn notify_fire_finished(
    observer: &Arc<dyn RecurringJobFireObserver>,
    pass: RecurringJobFireFinished,
) {
    let _ = catch_unwind(AssertUnwindSafe(|| observer.on_fire_finished(pass)));
}

/// Result of pausing a schedule before or after it has been registered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecurringJobPauseOutcome {
    /// A matching enabled schedule was paused. A current `fire_due` transaction may already have
    /// locked and materialized an earlier due occurrence.
    Paused,
    /// No enabled schedule used the supplied key.
    NotFoundOrAlreadyPaused,
}

/// Result of resuming a previously paused recurring schedule.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecurringJobResumeOutcome {
    /// The paused schedule was enabled with a fresh next occurrence after the `PostgreSQL` clock.
    Resumed,
    /// No paused schedule used the supplied key.
    NotFoundOrAlreadyEnabled,
}

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
    #[allow(clippy::too_many_lines)]
    pub async fn register<J>(
        &self,
        job: &RecurringJob<J>,
    ) -> Result<RecurringJobRegistration, RecurringJobError>
    where
        J: Job,
    {
        let payload =
            serde_json::to_vec(&job.payload).map_err(|_| RecurringJobError::InvalidPayload)?;
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(RecurringJobError::storage)?;
        if let Some(rate_limit) = job.rate_limit() {
            enforce_rate_limit_registration_policy(&mut transaction, rate_limit).await?;
        }
        let now = database_clock_unix_ms_transaction(&mut transaction).await?;
        let next_run_at = job.expression.next_after_unix_ms(now, &job.time_zone)?;
        let id = RecurringJobId::new();
        let occurrence = format!("rustee-recurring:{id}:{next_run_at}");
        validate_materialized_job(
            job.destination.clone(),
            J::NAME,
            J::VERSION,
            &payload,
            job.priority,
            now,
            occurrence,
        )?;

        let rate_limit_key = job.rate_limit().map(|rate_limit| rate_limit.key().as_str());
        let rate_limit_capacity = job
            .rate_limit()
            .map(|rate_limit| i32::try_from(rate_limit.capacity().get()))
            .transpose()
            .map_err(|_| RecurringJobError::StoredSchedule)?;
        let rate_limit_window_ms = job.rate_limit().map(RecurringJobRateLimit::window_ms);

        let inserted = sqlx::query_scalar::<_, Uuid>(
            "INSERT INTO rustee_recurring_jobs \
             (id, schedule_key, destination, job_name, schema_version, payload, cron_expression, time_zone, priority, \
              rate_limit_key, rate_limit_capacity, rate_limit_window_ms, next_run_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, \
                     to_timestamp($13::double precision / 1000.0)) \
             ON CONFLICT (schedule_key) DO NOTHING \
             RETURNING id",
        )
        .bind(id.0)
        .bind(job.key.as_str())
        .bind(job.destination.as_str())
        .bind(J::NAME)
        .bind(i32::from(J::VERSION))
        .bind(&payload)
        .bind(job.expression.as_str())
        .bind(job.time_zone.as_str())
        .bind(i16::from(job.priority.value()))
        .bind(rate_limit_key)
        .bind(rate_limit_capacity)
        .bind(rate_limit_window_ms)
        .bind(next_run_at)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RecurringJobError::storage)?;
        if inserted.is_some() {
            transaction
                .commit()
                .await
                .map_err(RecurringJobError::storage)?;
            return Ok(RecurringJobRegistration::Registered(id));
        }

        let row = sqlx::query(
            "SELECT id, destination, job_name, schema_version, payload, cron_expression, time_zone, priority, \
                    rate_limit_key, rate_limit_capacity, rate_limit_window_ms \
             FROM rustee_recurring_jobs WHERE schedule_key = $1",
        )
        .bind(job.key.as_str())
        .fetch_optional(&mut *transaction)
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
                    && capacity == rate_limit_capacity_i32(rate_limit)
                    && window_ms == rate_limit.window_ms()
            }
            _ => false,
        };
        let is_identical = row
            .try_get::<String, _>("destination")
            .map_err(RecurringJobError::storage)?
            == job.destination.as_str()
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
                == job.expression.as_str()
            && row
                .try_get::<String, _>("time_zone")
                .map_err(RecurringJobError::storage)?
                == job.time_zone.as_str()
            && existing_priority == i16::from(job.priority.value())
            && rate_limit_matches;
        if is_identical {
            transaction
                .commit()
                .await
                .map_err(RecurringJobError::storage)?;
            Ok(RecurringJobRegistration::AlreadyPresent(existing_id))
        } else {
            Err(RecurringJobError::RegistrationConflict)
        }
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
                match consume_rate_limit_window(&mut transaction, rate_limit, now).await? {
                    RateLimitConsumeOutcome::Allowed => {}
                    RateLimitConsumeOutcome::Deferred { next_window_at } => {
                        defer_rate_limited_schedule(&mut transaction, stored.id, next_window_at)
                            .await?;
                        report.rate_limited = report.rate_limited.saturating_add(1);
                        continue;
                    }
                }
            }
            let next_run_at = stored
                .expression
                .next_after_unix_ms(now, &stored.time_zone)?;
            let message = materialized_message(&stored, now)?;
            let outbox_message =
                OutboxMessage::from_job_message(stored.destination.clone(), message)
                    .map_err(|_| RecurringJobError::StoredSchedule)?
                    .with_priority(stored.priority);
            match PostgresOutbox
                .stage(&mut transaction, &outbox_message)
                .await
            {
                Ok(StageOutcome::Inserted(_)) => {
                    report.staged = report.staged.saturating_add(1);
                }
                Ok(StageOutcome::AlreadyPresent) => return Err(RecurringJobError::OutboxCollision),
                Err(error) => return Err(RecurringJobError::storage(error)),
            }
            let updated = sqlx::query(
                "UPDATE rustee_recurring_jobs \
                 SET next_run_at = to_timestamp($2::double precision / 1000.0), \
                     last_fired_at = clock_timestamp(), updated_at = clock_timestamp() \
                 WHERE id = $1",
            )
            .bind(stored.id.0)
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

async fn enforce_rate_limit_registration_policy(
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
    if capacity == rate_limit_capacity_i32(rate_limit) && window_ms == rate_limit.window_ms() {
        Ok(())
    } else {
        Err(RecurringJobError::RateLimitPolicyConflict)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RateLimitConsumeOutcome {
    Allowed,
    Deferred { next_window_at: i64 },
}

async fn consume_rate_limit_window(
    transaction: &mut Transaction<'_, Postgres>,
    rate_limit: &RecurringJobRateLimit,
    now: i64,
) -> Result<RateLimitConsumeOutcome, RecurringJobError> {
    let (window_started_at, next_window_at) = fixed_window_bounds(now, rate_limit.window_ms())?;
    let capacity = rate_limit_capacity_i32(rate_limit);
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
        return Ok(RateLimitConsumeOutcome::Allowed);
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
    Ok(RateLimitConsumeOutcome::Deferred { next_window_at })
}

async fn defer_rate_limited_schedule(
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

fn fixed_window_bounds(now: i64, window_ms: i64) -> Result<(i64, i64), RecurringJobError> {
    let window_started_at = now
        .div_euclid(window_ms)
        .checked_mul(window_ms)
        .ok_or(RecurringJobError::ClockOutOfRange)?;
    let next_window_at = window_started_at
        .checked_add(window_ms)
        .ok_or(RecurringJobError::ClockOutOfRange)?;
    Ok((window_started_at, next_window_at))
}

fn rate_limit_capacity_i32(rate_limit: &RecurringJobRateLimit) -> i32 {
    i32::try_from(rate_limit.capacity().get())
        .expect("validated rate-limit capacity fits PostgreSQL integer")
}

impl fmt::Debug for PostgresRecurringJobs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresRecurringJobs")
            .finish_non_exhaustive()
    }
}

/// Sanitized failure from the durable `PostgreSQL` recurring scheduler.
#[derive(thiserror::Error)]
pub enum RecurringJobError {
    /// A job payload could not be serialized into the durable schedule definition.
    #[error("recurring job payload could not be serialized")]
    InvalidPayload,
    /// A durable row no longer satisfies the scheduler's validated registration contract.
    #[error("stored recurring job schedule is invalid")]
    StoredSchedule,
    /// The `PostgreSQL` timestamp was outside the UTC scheduler's representable range.
    #[error("PostgreSQL recurring scheduler clock is outside the supported range")]
    ClockOutOfRange,
    /// A registration key already identifies a different live schedule definition.
    #[error("recurring job key conflicts with an existing schedule definition")]
    RegistrationConflict,
    /// Schedules sharing one rate-governor key must use the same fixed-window policy.
    #[error("recurring job rate-limit key conflicts with an existing fixed-window policy")]
    RateLimitPolicyConflict,
    /// A fresh random job identity unexpectedly collided with an existing outbox message.
    #[error("recurring job occurrence collided with an existing outbox message")]
    OutboxCollision,
    /// A durable query/update violated an invariant expected from the scheduler migration.
    #[error("PostgreSQL recurring scheduler storage invariant was not satisfied")]
    StorageInvariant,
    /// `PostgreSQL` did not complete the storage operation; source detail remains available to
    /// application logs while the debug representation stays redacted.
    #[error("PostgreSQL recurring scheduler storage failed")]
    Storage(#[source] sqlx::Error),
}

impl RecurringJobError {
    fn storage(error: sqlx::Error) -> Self {
        Self::Storage(error)
    }
}

impl fmt::Debug for RecurringJobError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::InvalidPayload => "InvalidPayload",
            Self::StoredSchedule => "StoredSchedule",
            Self::ClockOutOfRange => "ClockOutOfRange",
            Self::RegistrationConflict => "RegistrationConflict",
            Self::RateLimitPolicyConflict => "RateLimitPolicyConflict",
            Self::OutboxCollision => "OutboxCollision",
            Self::StorageInvariant => "StorageInvariant",
            Self::Storage(_) => "Storage",
        };
        formatter
            .debug_tuple("RecurringJobError")
            .field(&name)
            .finish()
    }
}

struct StoredRecurringJob {
    id: RecurringJobId,
    destination: OutboxDestination,
    job_name: String,
    schema_version: u16,
    payload: Vec<u8>,
    expression: CronExpression,
    time_zone: RecurringJobTimeZone,
    priority: OutboxPriority,
    rate_limit: Option<RecurringJobRateLimit>,
    scheduled_at: i64,
}

impl StoredRecurringJob {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, RecurringJobError> {
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
        if job_name.trim().is_empty() || payload.is_empty() {
            return Err(RecurringJobError::StoredSchedule);
        }
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

fn materialized_message(
    stored: &StoredRecurringJob,
    now: i64,
) -> Result<JobMessage, RecurringJobError> {
    let occurrence = format!("rustee-recurring:{}:{}", stored.id, stored.scheduled_at);
    let message = render_job_message(
        JobId::new(),
        &stored.job_name,
        stored.schema_version,
        &stored.payload,
        now,
        Some(occurrence),
    )?;
    Ok(message)
}

fn validate_materialized_job(
    destination: OutboxDestination,
    job_name: &str,
    schema_version: u16,
    payload: &[u8],
    priority: OutboxPriority,
    now: i64,
    idempotency_key: String,
) -> Result<(), RecurringJobError> {
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
    payload: &[u8],
    now: i64,
    idempotency_key: Option<String>,
) -> Result<JobMessage, RecurringJobError> {
    let payload =
        serde_json::from_slice::<Value>(payload).map_err(|_| RecurringJobError::StoredSchedule)?;
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
    let bytes = serde_json::to_vec(&envelope).map_err(|_| RecurringJobError::StoredSchedule)?;
    JobMessage::from_parts(id, job_name, schema_version, 1, bytes)
        .map_err(|_| RecurringJobError::StoredSchedule)
}

async fn database_clock_unix_ms_transaction(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<i64, RecurringJobError> {
    sqlx::query_scalar("SELECT floor(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint")
        .fetch_one(&mut **transaction)
        .await
        .map_err(RecurringJobError::storage)
}

fn unix_ms_to_utc(value: i64) -> Result<DateTime<Utc>, RecurringJobError> {
    Utc.timestamp_millis_opt(value)
        .single()
        .ok_or(RecurringJobError::ClockOutOfRange)
}

#[cfg(test)]
mod tests {
    use std::{
        num::{NonZeroU32, NonZeroUsize},
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use chrono::DateTime;
    use rustee_outbox_sqlx::OutboxDestination;
    use serde::{Deserialize, Serialize};

    use super::{
        CronExpression, CronExpressionError, RecurringJob, RecurringJobFireFinished,
        RecurringJobFireLimit, RecurringJobFireLimitError, RecurringJobFireObservation,
        RecurringJobFireObserver, RecurringJobFireOutcome, RecurringJobFireReport,
        RecurringJobFireStarted, RecurringJobKey, RecurringJobKeyError, RecurringJobRateLimit,
        RecurringJobRateLimitError, RecurringJobRateLimitKey, RecurringJobTimeZone,
        RecurringJobTimeZoneError, fixed_window_bounds,
    };

    #[derive(Deserialize, Serialize)]
    struct RedactedReminder {
        secret: String,
    }

    impl rustee_jobs::Job for RedactedReminder {
        const NAME: &'static str = "billing.redacted-reminder";
        const VERSION: u16 = 1;
    }

    #[derive(Default)]
    struct RecordingFireObserver {
        started: AtomicUsize,
        finished: Mutex<Vec<RecurringJobFireFinished>>,
    }

    impl RecurringJobFireObserver for RecordingFireObserver {
        fn on_fire_started(&self, pass: RecurringJobFireStarted) {
            assert_eq!(pass.limit().get().get(), 25);
            self.started.fetch_add(1, Ordering::Relaxed);
        }

        fn on_fire_finished(&self, pass: RecurringJobFireFinished) {
            self.finished.lock().unwrap().push(pass);
        }
    }

    #[test]
    fn cron_expression_is_bounded_and_has_a_future_utc_occurrence() {
        assert_eq!(
            CronExpression::new("not cron").unwrap_err(),
            CronExpressionError::InvalidExpression
        );
        let expression = CronExpression::new("0 * * * * * *").unwrap();
        let next = expression
            .next_after_unix_ms(1_722_643_200_000, &RecurringJobTimeZone::default())
            .unwrap();
        assert!(next > 1_722_643_200_000);
    }

    #[test]
    fn iana_time_zone_skips_dst_gaps_and_fires_an_ambiguous_wall_time_once() {
        let new_york = RecurringJobTimeZone::new("America/New_York").unwrap();
        let at_two_thirty = CronExpression::new("0 30 2 * * * *").unwrap();
        let before_spring_gap = unix_ms("2026-03-08T06:00:00Z");
        assert_eq!(
            at_two_thirty
                .next_after_unix_ms(before_spring_gap, &new_york)
                .unwrap(),
            unix_ms("2026-03-09T06:30:00Z")
        );

        let at_one_thirty = CronExpression::new("0 30 1 * * * *").unwrap();
        let before_fall_overlap = unix_ms("2026-11-01T04:00:00Z");
        let earlier_occurrence = unix_ms("2026-11-01T05:30:00Z");
        assert_eq!(
            at_one_thirty
                .next_after_unix_ms(before_fall_overlap, &new_york)
                .unwrap(),
            earlier_occurrence
        );
        assert_eq!(
            at_one_thirty
                .next_after_unix_ms(earlier_occurrence, &new_york)
                .unwrap(),
            unix_ms("2026-11-02T06:30:00Z")
        );
    }

    #[test]
    fn recurring_time_zones_are_iana_names_and_default_to_utc() {
        assert_eq!(RecurringJobTimeZone::default().as_str(), "UTC");
        assert_eq!(
            RecurringJobTimeZone::new("not/a-time-zone").unwrap_err(),
            RecurringJobTimeZoneError::InvalidTimeZone
        );
    }

    #[test]
    fn schedule_keys_and_fire_limits_are_bounded() {
        assert_eq!(
            RecurringJobKey::new(" ").unwrap_err(),
            RecurringJobKeyError::InvalidKey
        );
        assert_eq!(
            RecurringJobFireLimit::new(NonZeroUsize::new(101).unwrap()).unwrap_err(),
            RecurringJobFireLimitError::TooLarge
        );
        assert_eq!(RecurringJobFireLimit::default().get().get(), 25);
    }

    #[test]
    fn rate_governor_policy_and_fixed_window_boundaries_are_bounded() {
        assert!(RecurringJobRateLimitKey::new(" ").is_err());
        let key = RecurringJobRateLimitKey::new("provider.billing").unwrap();
        assert_eq!(
            RecurringJobRateLimit::new(key, NonZeroU32::new(1).unwrap(), Duration::ZERO)
                .unwrap_err(),
            RecurringJobRateLimitError::InvalidWindow
        );
        assert_eq!(
            RecurringJobRateLimit::new(
                RecurringJobRateLimitKey::new("provider.billing.large").unwrap(),
                NonZeroU32::new(i32::MAX as u32 + 1).unwrap(),
                Duration::from_secs(60),
            )
            .unwrap_err(),
            RecurringJobRateLimitError::CapacityTooLarge
        );
        assert_eq!(
            fixed_window_bounds(125_001, 60_000).unwrap(),
            (120_000, 180_000)
        );
        assert_eq!(fixed_window_bounds(-1, 60_000).unwrap(), (-60_000, 0));
    }

    #[test]
    fn recurring_job_debug_never_renders_the_template_payload() {
        let job = RecurringJob::new(
            RecurringJobKey::new("billing.redacted-reminder").unwrap(),
            OutboxDestination::new("jobs.billing").unwrap(),
            RedactedReminder {
                secret: "not-for-debug".to_owned(),
            },
            CronExpression::new("* * * * * * *").unwrap(),
        );

        assert!(!format!("{job:?}").contains("not-for-debug"));
    }

    #[test]
    fn fire_observation_records_terminal_and_abandoned_outcomes() {
        let recorder = Arc::new(RecordingFireObserver::default());
        let observer: Arc<dyn RecurringJobFireObserver> = recorder.clone();
        RecurringJobFireObservation::start(observer.clone(), RecurringJobFireLimit::default())
            .finish(
                RecurringJobFireOutcome::Succeeded,
                Some(RecurringJobFireReport::default()),
            );
        drop(RecurringJobFireObservation::start(
            observer,
            RecurringJobFireLimit::default(),
        ));

        assert_eq!(recorder.started.load(Ordering::Relaxed), 2);
        let finished = recorder.finished.lock().unwrap();
        assert_eq!(finished.len(), 2);
        assert_eq!(finished[0].outcome(), RecurringJobFireOutcome::Succeeded);
        assert_eq!(
            finished[0].report(),
            Some(RecurringJobFireReport::default())
        );
        assert_eq!(finished[1].outcome(), RecurringJobFireOutcome::Abandoned);
        assert_eq!(finished[1].report(), None);
    }

    fn unix_ms(value: &str) -> i64 {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .timestamp_millis()
    }
}
