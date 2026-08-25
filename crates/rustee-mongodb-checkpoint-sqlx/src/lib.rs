//! Optional durable `PostgreSQL` storage for `MongoDB` change-stream resume tokens.
//!
//! Applications add [`CHANGE_STREAM_CHECKPOINT_MIGRATION_SQL`] and then
//! [`CHANGE_STREAM_CHECKPOINT_RESUME_TOKEN_BOUND_MIGRATION_SQL`] to their normal deployment
//! migration sequence. [`PostgresChangeStreamCheckpointStore`] writes the official driver's
//! opaque BSON representation unchanged. It also offers an opt-in database-clock lease for one
//! active checkpoint writer. It neither runs migrations at HTTP startup nor claims exactly-once
//! event handling.

use std::{fmt, time::Duration};

mod model;
mod store;

pub use model::{
    ChangeStreamLease, ChangeStreamLeaseAcquire, ChangeStreamLeaseDuration, ChangeStreamLeaseOwner,
    ChangeStreamLeaseOwnerError,
};
pub use store::PostgresChangeStreamCheckpointStore;

/// Maximum BSON byte length admitted for one durable `MongoDB` change-stream resume token.
pub const MAX_CHANGE_STREAM_RESUME_TOKEN_BYTES: usize = 1024 * 1024;

/// The deployment-owned migration for durable change-stream resume tokens.
pub const CHANGE_STREAM_CHECKPOINT_MIGRATION_SQL: &str =
    include_str!("../migrations/0001_rustee_mongodb_change_stream_checkpoint.sql");

/// Forward-only migration that enforces the resume-token byte bound in `PostgreSQL`.
///
/// Apply this after [`CHANGE_STREAM_CHECKPOINT_MIGRATION_SQL`]. It rejects the migration when an
/// existing checkpoint already exceeds [`MAX_CHANGE_STREAM_RESUME_TOKEN_BYTES`], so deployments
/// must inspect or remove that invalid state explicitly rather than carry it forward silently.
pub const CHANGE_STREAM_CHECKPOINT_RESUME_TOKEN_BOUND_MIGRATION_SQL: &str = include_str!(
    "../migrations/0002_rustee_mongodb_change_stream_checkpoint_resume_token_bound.sql"
);

/// Failure while storing or decoding a durable change-stream checkpoint.
#[derive(thiserror::Error)]
pub enum PostgresChangeStreamCheckpointError {
    /// The stored BSON bytes could not be decoded as a driver resume token.
    #[error("stored MongoDB change-stream checkpoint is invalid")]
    InvalidCheckpoint,
    /// The active writer lease was expired, released, or acquired by another owner.
    #[error("MongoDB change-stream checkpoint lease is no longer owned")]
    LeaseLost,
    /// A lease duration was zero, fractional at millisecond precision, below one millisecond, or
    /// exceeded the supported maximum.
    #[error("MongoDB change-stream checkpoint lease duration is invalid")]
    InvalidLeaseDuration,
    /// A readiness deadline was zero.
    #[error("MongoDB change-stream checkpoint readiness timeout is invalid")]
    InvalidReadinessTimeout,
    /// The readiness query did not finish before its application-supplied deadline.
    #[error("MongoDB change-stream checkpoint readiness timed out after {0:?}")]
    ReadinessTimedOut(Duration),
    /// `PostgreSQL` rejected or could not complete the storage operation.
    #[error("PostgreSQL change-stream checkpoint storage failed")]
    Storage(#[source] sqlx::Error),
}

impl fmt::Debug for PostgresChangeStreamCheckpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::InvalidCheckpoint => "invalid_checkpoint",
            Self::LeaseLost => "lease_lost",
            Self::InvalidLeaseDuration => "invalid_lease_duration",
            Self::InvalidReadinessTimeout => "invalid_readiness_timeout",
            Self::ReadinessTimedOut(_) => "readiness_timed_out",
            Self::Storage(_) => "storage_failed",
        };
        formatter
            .debug_struct("PostgresChangeStreamCheckpointError")
            .field("kind", &kind)
            .finish()
    }
}

impl PostgresChangeStreamCheckpointError {
    pub(crate) fn storage(error: sqlx::Error) -> Self {
        Self::Storage(error)
    }
}

#[cfg(test)]
mod tests;
