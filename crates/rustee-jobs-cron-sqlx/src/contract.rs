//! Stable recurring-scheduler public values and sanitized errors.

use std::{fmt, num::NonZeroUsize};

use uuid::Uuid;

const MAX_FIRE_BATCH_SIZE: usize = 100;

/// Identifier assigned to a durable recurring schedule row.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct RecurringJobId(pub(crate) Uuid);

impl RecurringJobId {
    /// Creates a fresh durable recurring schedule identifier.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub(crate) fn from_uuid(id: Uuid) -> Self {
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

impl fmt::Debug for RecurringJobId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RecurringJobId([REDACTED])")
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

/// Bounded result of one [`crate::PostgresRecurringJobs::fire_due`] pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RecurringJobFireReport {
    pub(crate) claimed: u32,
    pub(crate) staged: u32,
    pub(crate) rate_limited: u32,
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
    pub(crate) fn storage(error: sqlx::Error) -> Self {
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
