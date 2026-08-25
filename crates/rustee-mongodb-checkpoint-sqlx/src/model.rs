use std::{fmt, time::Duration};

use rustee_mongodb::ChangeStreamConsumer;

use crate::PostgresChangeStreamCheckpointError;

const MAX_LEASE_IDENTITY_BYTES: usize = 255;
const MAX_LEASE_DURATION: Duration = Duration::from_hours(1);

/// A non-secret, deployment-unique owner identity for a checkpoint-writer lease.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct ChangeStreamLeaseOwner(String);

impl ChangeStreamLeaseOwner {
    /// Validates a non-blank, NUL-free, bounded deployment instance identity.
    ///
    /// # Errors
    ///
    /// Returns [`ChangeStreamLeaseOwnerError::InvalidOwner`] when `owner` is not safe for durable
    /// lease ownership. Use a fresh instance or process identifier on every restart.
    pub fn new(owner: impl Into<String>) -> Result<Self, ChangeStreamLeaseOwnerError> {
        let owner = owner.into();
        if owner.trim().is_empty() || owner.contains('\0') || owner.len() > MAX_LEASE_IDENTITY_BYTES
        {
            return Err(ChangeStreamLeaseOwnerError::InvalidOwner);
        }
        Ok(Self(owner))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ChangeStreamLeaseOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ChangeStreamLeaseOwner")
            .field(&"[REDACTED]")
            .finish()
    }
}

/// Invalid checkpoint-writer lease owner identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ChangeStreamLeaseOwnerError {
    /// The owner was blank, contained a NUL byte, or exceeded the storage bound.
    #[error("change-stream lease owner must be non-blank, NUL-free, and bounded")]
    InvalidOwner,
}

/// A positive bounded lifetime for one database-clock checkpoint-writer lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChangeStreamLeaseDuration(Duration);

impl ChangeStreamLeaseDuration {
    /// Creates a lease duration representable as whole `PostgreSQL` milliseconds.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresChangeStreamCheckpointError::InvalidLeaseDuration`] when `duration` is
    /// too short, fractional at millisecond precision, zero, or too long.
    pub fn new(duration: Duration) -> Result<Self, PostgresChangeStreamCheckpointError> {
        if duration.is_zero()
            || duration < Duration::from_millis(1)
            || !duration.subsec_nanos().is_multiple_of(1_000_000)
            || duration > MAX_LEASE_DURATION
        {
            return Err(PostgresChangeStreamCheckpointError::InvalidLeaseDuration);
        }
        Ok(Self(duration))
    }

    /// Returns the selected lifetime.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }
}

impl Default for ChangeStreamLeaseDuration {
    fn default() -> Self {
        Self(Duration::from_secs(30))
    }
}

/// A database-clock lease owned by one deployment instance.
#[derive(Clone, Eq, PartialEq)]
pub struct ChangeStreamLease {
    consumer: ChangeStreamConsumer,
    owner: ChangeStreamLeaseOwner,
}

impl ChangeStreamLease {
    pub(crate) const fn new(consumer: ChangeStreamConsumer, owner: ChangeStreamLeaseOwner) -> Self {
        Self { consumer, owner }
    }

    pub(crate) const fn consumer(&self) -> &ChangeStreamConsumer {
        &self.consumer
    }

    pub(crate) const fn owner(&self) -> &ChangeStreamLeaseOwner {
        &self.owner
    }
}

impl fmt::Debug for ChangeStreamLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChangeStreamLease")
            .field("consumer", &"[REDACTED]")
            .field("owner", &"[REDACTED]")
            .finish()
    }
}

/// Result of attempting to become one consumer's active checkpoint writer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChangeStreamLeaseAcquire {
    /// This owner may load, handle, and save checkpoints while it renews the lease.
    Acquired(ChangeStreamLease),
    /// A distinct, unexpired owner currently holds the lease.
    Contended,
}

pub(crate) fn lease_milliseconds(
    duration: ChangeStreamLeaseDuration,
) -> Result<i64, PostgresChangeStreamCheckpointError> {
    i64::try_from(duration.get().as_millis())
        .map_err(|_| PostgresChangeStreamCheckpointError::InvalidLeaseDuration)
}
