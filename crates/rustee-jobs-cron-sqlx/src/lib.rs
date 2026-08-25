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

mod contract;
mod fire_observation;
mod model;
mod rate_limit;
mod schedule;
mod scheduler;

pub use contract::{
    RecurringJobError, RecurringJobFireLimit, RecurringJobFireLimitError, RecurringJobFireReport,
    RecurringJobId, RecurringJobPauseOutcome, RecurringJobRegistration, RecurringJobResumeOutcome,
};
pub use fire_observation::{
    NoopRecurringJobFireObserver, RecurringJobFireFinished, RecurringJobFireObservation,
    RecurringJobFireObserver, RecurringJobFireOutcome, RecurringJobFireStarted,
};
#[cfg(test)]
use rate_limit::fixed_window_bounds;
pub use rate_limit::{
    RecurringJobRateLimit, RecurringJobRateLimitError, RecurringJobRateLimitKey,
    RecurringJobRateLimitKeyError,
};
pub use schedule::{
    CronExpression, CronExpressionError, RecurringJob, RecurringJobKey, RecurringJobKeyError,
    RecurringJobTimeZone, RecurringJobTimeZoneError,
};
pub use scheduler::PostgresRecurringJobs;

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

#[cfg(test)]
mod tests;
