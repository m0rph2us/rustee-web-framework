//! Bounded delayed-staging and relay-lease timing contracts.

use std::{fmt, num::NonZeroUsize, time::Duration};

const MAX_BATCH_SIZE: usize = 1_000;
pub(super) const MIN_POSTGRES_INTERVAL: Duration = Duration::from_millis(1);
pub(super) const MAX_LEASE_DURATION: Duration = Duration::from_hours(1);
const MAX_JOB_SCHEDULE_DELAY: Duration = Duration::from_hours(8_784);

/// A validated relative delay for one durable job staged through the `PostgreSQL` outbox.
///
/// The delay is evaluated by `PostgreSQL`'s clock when the job is staged, not by an application
/// process clock. The existing [`crate::JobOutboxRelay`] claims the row only after it becomes
/// available; applications still own the relay loop, readiness, metrics, and shutdown lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobSchedule {
    delay: Duration,
}

impl JobSchedule {
    /// Creates one delayed-job schedule relative to staging time.
    ///
    /// Use [`crate::PostgresOutbox::stage`] for immediately eligible messages. One-time job
    /// schedules are bounded to 366 days; cron and recurring schedules remain deployment-owned
    /// workflows.
    ///
    /// # Errors
    ///
    /// Returns [`JobScheduleError::ZeroDelay`] for a delay below `PostgreSQL`'s millisecond
    /// resolution or
    /// [`JobScheduleError::DelayTooLong`] when the delay exceeds the durable scheduling bound.
    pub fn after(delay: Duration) -> Result<Self, JobScheduleError> {
        if delay < MIN_POSTGRES_INTERVAL {
            return Err(JobScheduleError::ZeroDelay);
        }
        if delay > MAX_JOB_SCHEDULE_DELAY {
            return Err(JobScheduleError::DelayTooLong);
        }
        Ok(Self { delay })
    }

    /// Returns the delay evaluated by the `PostgreSQL` staging operation.
    #[must_use]
    pub const fn delay(&self) -> Duration {
        self.delay
    }

    pub(super) fn delay_millis(&self) -> i64 {
        i64::try_from(self.delay.as_millis())
            .expect("validated job schedule delay must fit PostgreSQL milliseconds")
    }
}

/// Invalid one-time durable job schedule configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum JobScheduleError {
    /// Immediate and sub-millisecond jobs use the ordinary outbox staging operation.
    #[error("delayed job schedule must be at least 1 millisecond")]
    ZeroDelay,
    /// Recurring or longer-lived workflows must be handled by a deployment-owned scheduler.
    #[error("delayed job schedule must be at most 366 days")]
    DelayTooLong,
}

/// A validated relative delay for one append-only event staged through the `PostgreSQL` outbox.
///
/// The delay is evaluated by `PostgreSQL`'s clock. The existing [`crate::EventOutboxRelay`]
/// claims the row only after it is due; callers still own relay supervision, broker provisioning,
/// and any retry-attempt metadata required by a particular event provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventSchedule {
    delay: Duration,
}

impl EventSchedule {
    /// Creates one delayed-event schedule relative to staging time.
    ///
    /// Use [`crate::PostgresOutbox::stage`] for immediately eligible messages. Delayed events
    /// are bounded to 366 days; recurring calendars and provider-specific retry routing remain
    /// explicit integrations.
    ///
    /// # Errors
    ///
    /// Returns [`EventScheduleError::ZeroDelay`] for a delay below `PostgreSQL`'s millisecond
    /// resolution or
    /// [`EventScheduleError::DelayTooLong`] when the delay exceeds the durable scheduling bound.
    pub fn after(delay: Duration) -> Result<Self, EventScheduleError> {
        if delay < MIN_POSTGRES_INTERVAL {
            return Err(EventScheduleError::ZeroDelay);
        }
        if delay > MAX_JOB_SCHEDULE_DELAY {
            return Err(EventScheduleError::DelayTooLong);
        }
        Ok(Self { delay })
    }

    /// Returns the PostgreSQL-clock-relative delay.
    #[must_use]
    pub const fn delay(&self) -> Duration {
        self.delay
    }

    pub(super) fn delay_millis(&self) -> i64 {
        i64::try_from(self.delay.as_millis())
            .expect("validated event schedule delay must fit PostgreSQL milliseconds")
    }
}

/// Invalid one-time durable event schedule configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EventScheduleError {
    /// Immediate and sub-millisecond events use the ordinary outbox staging operation.
    #[error("delayed event schedule must be at least 1 millisecond")]
    ZeroDelay,
    /// Recurring or longer-lived workflows must be handled by a dedicated calendar integration.
    #[error("delayed event schedule must be at most 366 days")]
    DelayTooLong,
}

/// Failure while staging a delayed durable job.
#[derive(thiserror::Error)]
pub enum ScheduleJobError {
    /// Only durable jobs, rather than append-only events, can use the delayed-job API.
    #[error("only durable job messages can be staged with a job schedule")]
    NotAJob,
    /// `PostgreSQL` rejected the scheduling insert or the outbox migration is unavailable.
    #[error("PostgreSQL outbox delayed job staging failed")]
    Database(#[from] sqlx::Error),
}

impl fmt::Debug for ScheduleJobError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::NotAJob => "not_a_job",
            Self::Database(_) => "database_failed",
        };
        formatter
            .debug_struct("ScheduleJobError")
            .field("kind", &kind)
            .finish()
    }
}

/// Failure while staging a delayed append-only event.
#[derive(thiserror::Error)]
pub enum ScheduleEventError {
    /// Only append-only events, rather than durable jobs, can use the delayed-event API.
    #[error("only event messages can be staged with an event schedule")]
    NotAnEvent,
    /// `PostgreSQL` rejected the scheduling insert or the outbox migration is unavailable.
    #[error("PostgreSQL outbox delayed event staging failed")]
    Database(#[from] sqlx::Error),
}

impl fmt::Debug for ScheduleEventError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::NotAnEvent => "not_an_event",
            Self::Database(_) => "database_failed",
        };
        formatter
            .debug_struct("ScheduleEventError")
            .field("kind", &kind)
            .finish()
    }
}

/// Configuration for one bounded `SKIP LOCKED` claim operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeaseConfig {
    batch_size: NonZeroUsize,
    lease_duration: Duration,
}

impl LeaseConfig {
    /// Creates a bounded lease configuration.
    ///
    /// # Errors
    ///
    /// Returns [`LeaseConfigError`] when the batch is too large, or a lease duration is below
    /// one millisecond or longer than one hour.
    pub fn new(
        batch_size: NonZeroUsize,
        lease_duration: Duration,
    ) -> Result<Self, LeaseConfigError> {
        if batch_size.get() > MAX_BATCH_SIZE {
            return Err(LeaseConfigError::BatchTooLarge);
        }
        if lease_duration < MIN_POSTGRES_INTERVAL || lease_duration > MAX_LEASE_DURATION {
            return Err(LeaseConfigError::InvalidLeaseDuration);
        }
        Ok(Self {
            batch_size,
            lease_duration,
        })
    }

    /// Returns the maximum number of rows one relay process can claim at a time.
    #[must_use]
    pub const fn batch_size(&self) -> NonZeroUsize {
        self.batch_size
    }

    /// Returns the bounded exclusive-lease duration.
    #[must_use]
    pub const fn lease_duration(&self) -> Duration {
        self.lease_duration
    }
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self::new(
            NonZeroUsize::new(100).expect("100 is non-zero"),
            Duration::from_secs(30),
        )
        .expect("default outbox lease configuration is valid")
    }
}

/// Invalid relay lease configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum LeaseConfigError {
    /// One relay attempted to claim more than the fixed bound.
    #[error("outbox lease batch size must be at most 1000")]
    BatchTooLarge,
    /// The lease duration could not protect one bounded broker publish attempt.
    #[error("outbox lease duration must be at least 1 millisecond and at most one hour")]
    InvalidLeaseDuration,
}

/// Outcome of confirming or releasing a row with a lease token.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseOutcome {
    /// This relay still owned the row and the state transition was persisted.
    Applied,
    /// The row was already confirmed or its lease was replaced by another relay.
    Lost,
}

#[cfg(test)]
mod tests {
    use super::{
        EventSchedule, EventScheduleError, JobSchedule, JobScheduleError, LeaseConfig,
        LeaseConfigError,
    };
    use std::{num::NonZeroUsize, time::Duration};

    #[test]
    fn postgres_backed_timing_rejects_sub_millisecond_values() {
        let sub_millisecond = Duration::from_nanos(1);

        assert_eq!(
            JobSchedule::after(sub_millisecond),
            Err(JobScheduleError::ZeroDelay)
        );
        assert_eq!(
            EventSchedule::after(sub_millisecond),
            Err(EventScheduleError::ZeroDelay)
        );
        assert_eq!(
            LeaseConfig::new(
                NonZeroUsize::new(1).expect("one is non-zero"),
                sub_millisecond,
            ),
            Err(LeaseConfigError::InvalidLeaseDuration)
        );
    }
}
