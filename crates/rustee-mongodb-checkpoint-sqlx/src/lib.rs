//! Optional durable `PostgreSQL` storage for `MongoDB` change-stream resume tokens.
//!
//! Applications add [`CHANGE_STREAM_CHECKPOINT_MIGRATION_SQL`] to their normal deployment
//! migration sequence. [`PostgresChangeStreamCheckpointStore`] writes the official driver's
//! opaque BSON representation unchanged. It also offers an opt-in database-clock lease for one
//! active checkpoint writer. It neither runs migrations at HTTP startup nor claims exactly-once
//! event handling.

use std::{fmt, time::Duration};

use futures_util::future::BoxFuture;
use rustee_mongodb::{
    ChangeStreamCheckpointStore, ChangeStreamConsumer,
    mongodb::{bson, change_stream::event::ResumeToken},
};
use sqlx::PgPool;

const MAX_LEASE_IDENTITY_BYTES: usize = 255;
const MAX_LEASE_DURATION: Duration = Duration::from_hours(1);

/// The deployment-owned migration for durable change-stream resume tokens.
pub const CHANGE_STREAM_CHECKPOINT_MIGRATION_SQL: &str =
    include_str!("../migrations/0001_rustee_mongodb_change_stream_checkpoint.sql");

/// Durable `PostgreSQL` implementation of [`ChangeStreamCheckpointStore`].
///
/// The primary key is the stable [`ChangeStreamConsumer`] identity. For a single active worker,
/// use [`Self::try_acquire_lease`] and [`Self::save_while_leased`] rather than the generic
/// [`ChangeStreamCheckpointStore::save`] method.
#[derive(Clone)]
pub struct PostgresChangeStreamCheckpointStore {
    pool: PgPool,
}

impl PostgresChangeStreamCheckpointStore {
    /// Creates a checkpoint store from an application-owned `PostgreSQL` pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Executes a database query with an application-supplied deadline for checkpoint readiness.
    ///
    /// A worker must fail-stop when this dependency is unavailable: do not acknowledge a newly
    /// handled change without its durable checkpoint, and let the application supervisor choose
    /// the restart backoff.
    ///
    /// # Errors
    ///
    /// Returns a sanitized storage error when the pool cannot acquire a connection or
    /// `PostgreSQL` rejects the check, or a distinct error if `timeout` elapses. A zero timeout
    /// is rejected before the query starts.
    pub async fn readiness(
        &self,
        timeout: Duration,
    ) -> Result<(), PostgresChangeStreamCheckpointError> {
        if timeout.is_zero() {
            return Err(PostgresChangeStreamCheckpointError::InvalidReadinessTimeout);
        }
        tokio::time::timeout(timeout, sqlx::query("SELECT 1").execute(&self.pool))
            .await
            .map_err(|_| PostgresChangeStreamCheckpointError::ReadinessTimedOut(timeout))?
            .map(|_| ())
            .map_err(PostgresChangeStreamCheckpointError::storage)
    }

    /// Attempts to acquire one database-clock checkpoint-writer lease.
    ///
    /// A result of [`ChangeStreamLeaseAcquire::Contended`] means a different unexpired owner is
    /// active. Owners must be unique for each process start or deployment instance; do not reuse a
    /// static hostname after a crash. Renew before expiry and stop stream handling when renewal
    /// returns [`PostgresChangeStreamCheckpointError::LeaseLost`].
    ///
    /// # Errors
    ///
    /// Returns a storage error when `PostgreSQL` is unavailable or the lease duration cannot be
    /// represented safely.
    pub async fn try_acquire_lease(
        &self,
        consumer: ChangeStreamConsumer,
        owner: ChangeStreamLeaseOwner,
        duration: ChangeStreamLeaseDuration,
    ) -> Result<ChangeStreamLeaseAcquire, PostgresChangeStreamCheckpointError> {
        let milliseconds = lease_milliseconds(duration)?;
        let acquired = sqlx::query_scalar::<_, String>(
            "INSERT INTO rustee_mongodb_change_stream_lease (consumer, owner, expires_at) \
             VALUES ($1, $2, clock_timestamp() + ($3::bigint * INTERVAL '1 millisecond')) \
             ON CONFLICT (consumer) DO UPDATE \
             SET owner = EXCLUDED.owner, expires_at = EXCLUDED.expires_at \
             WHERE rustee_mongodb_change_stream_lease.expires_at <= clock_timestamp() \
                OR rustee_mongodb_change_stream_lease.owner = EXCLUDED.owner \
             RETURNING consumer",
        )
        .bind(consumer.as_str())
        .bind(owner.as_str())
        .bind(milliseconds)
        .fetch_optional(&self.pool)
        .await
        .map_err(PostgresChangeStreamCheckpointError::storage)?;
        Ok(match acquired {
            Some(_) => ChangeStreamLeaseAcquire::Acquired(ChangeStreamLease { consumer, owner }),
            None => ChangeStreamLeaseAcquire::Contended,
        })
    }

    /// Extends one active checkpoint-writer lease using the `PostgreSQL` clock.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresChangeStreamCheckpointError::LeaseLost`] when a different owner acquired
    /// the lease or this owner allowed it to expire.
    pub async fn renew_lease(
        &self,
        lease: &ChangeStreamLease,
        duration: ChangeStreamLeaseDuration,
    ) -> Result<(), PostgresChangeStreamCheckpointError> {
        let milliseconds = lease_milliseconds(duration)?;
        let result = sqlx::query(
            "UPDATE rustee_mongodb_change_stream_lease \
             SET expires_at = clock_timestamp() + ($3::bigint * INTERVAL '1 millisecond') \
             WHERE consumer = $1 AND owner = $2 AND expires_at > clock_timestamp()",
        )
        .bind(lease.consumer.as_str())
        .bind(lease.owner.as_str())
        .bind(milliseconds)
        .execute(&self.pool)
        .await
        .map_err(PostgresChangeStreamCheckpointError::storage)?;
        if result.rows_affected() == 1 {
            Ok(())
        } else {
            Err(PostgresChangeStreamCheckpointError::LeaseLost)
        }
    }

    /// Saves an opaque token only while the supplied lease is still owned and unexpired.
    ///
    /// Call this only after durable, idempotent event handling succeeds. A lost lease makes this
    /// return an error instead of allowing a stale worker to overwrite a newer checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresChangeStreamCheckpointError::LeaseLost`] when the lease is no longer
    /// current, or an error when the token cannot be stored.
    pub async fn save_while_leased(
        &self,
        lease: &ChangeStreamLease,
        resume_token: ResumeToken,
    ) -> Result<(), PostgresChangeStreamCheckpointError> {
        let bytes = bson::to_vec(&resume_token)
            .map_err(|_| PostgresChangeStreamCheckpointError::InvalidCheckpoint)?;
        let saved = sqlx::query_scalar::<_, String>(
            "INSERT INTO rustee_mongodb_change_stream_checkpoint (consumer, resume_token) \
             SELECT $1, $2 \
             WHERE EXISTS ( \
                 SELECT 1 FROM rustee_mongodb_change_stream_lease \
                 WHERE consumer = $1 AND owner = $3 AND expires_at > clock_timestamp() \
             ) \
             ON CONFLICT (consumer) DO UPDATE \
             SET resume_token = EXCLUDED.resume_token, updated_at = clock_timestamp() \
             WHERE EXISTS ( \
                 SELECT 1 FROM rustee_mongodb_change_stream_lease \
                 WHERE consumer = $1 AND owner = $3 AND expires_at > clock_timestamp() \
             ) \
             RETURNING consumer",
        )
        .bind(lease.consumer.as_str())
        .bind(bytes)
        .bind(lease.owner.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(PostgresChangeStreamCheckpointError::storage)?;
        if saved.is_some() {
            Ok(())
        } else {
            Err(PostgresChangeStreamCheckpointError::LeaseLost)
        }
    }

    /// Releases an active checkpoint-writer lease during graceful shutdown.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresChangeStreamCheckpointError::LeaseLost`] if this process no longer owns
    /// the lease. Treat that result as a completed local shutdown, never as permission to save.
    pub async fn release_lease(
        &self,
        lease: ChangeStreamLease,
    ) -> Result<(), PostgresChangeStreamCheckpointError> {
        let result = sqlx::query(
            "DELETE FROM rustee_mongodb_change_stream_lease WHERE consumer = $1 AND owner = $2",
        )
        .bind(lease.consumer.as_str())
        .bind(lease.owner.as_str())
        .execute(&self.pool)
        .await
        .map_err(PostgresChangeStreamCheckpointError::storage)?;
        if result.rows_affected() == 1 {
            Ok(())
        } else {
            Err(PostgresChangeStreamCheckpointError::LeaseLost)
        }
    }
}

impl fmt::Debug for PostgresChangeStreamCheckpointStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresChangeStreamCheckpointStore")
            .finish_non_exhaustive()
    }
}

impl ChangeStreamCheckpointStore for PostgresChangeStreamCheckpointStore {
    type Error = PostgresChangeStreamCheckpointError;

    fn load(
        &self,
        consumer: ChangeStreamConsumer,
    ) -> BoxFuture<'static, Result<Option<ResumeToken>, Self::Error>> {
        let pool = self.pool.clone();
        Box::pin(async move {
            let bytes = sqlx::query_scalar::<_, Vec<u8>>(
                "SELECT resume_token FROM rustee_mongodb_change_stream_checkpoint WHERE consumer = $1",
            )
            .bind(consumer.as_str())
            .fetch_optional(&pool)
            .await
            .map_err(PostgresChangeStreamCheckpointError::storage)?;
            bytes
                .map(|bytes| {
                    bson::from_slice(&bytes)
                        .map_err(|_| PostgresChangeStreamCheckpointError::InvalidCheckpoint)
                })
                .transpose()
        })
    }

    fn save(
        &self,
        consumer: ChangeStreamConsumer,
        resume_token: ResumeToken,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let pool = self.pool.clone();
        Box::pin(async move {
            let bytes = bson::to_vec(&resume_token)
                .map_err(|_| PostgresChangeStreamCheckpointError::InvalidCheckpoint)?;
            sqlx::query(
                "INSERT INTO rustee_mongodb_change_stream_checkpoint (consumer, resume_token) \
                 VALUES ($1, $2) \
                 ON CONFLICT (consumer) DO UPDATE \
                 SET resume_token = EXCLUDED.resume_token, updated_at = clock_timestamp()",
            )
            .bind(consumer.as_str())
            .bind(bytes)
            .execute(&pool)
            .await
            .map_err(PostgresChangeStreamCheckpointError::storage)?;
            Ok(())
        })
    }
}

/// Failure while storing or decoding a durable change-stream checkpoint.
#[derive(Debug, thiserror::Error)]
pub enum PostgresChangeStreamCheckpointError {
    /// The stored BSON bytes could not be decoded as a driver resume token.
    #[error("stored MongoDB change-stream checkpoint is invalid")]
    InvalidCheckpoint,
    /// The active writer lease was expired, released, or acquired by another owner.
    #[error("MongoDB change-stream checkpoint lease is no longer owned")]
    LeaseLost,
    /// A lease duration was zero, below one millisecond, or exceeded the supported maximum.
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

    fn as_str(&self) -> &str {
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
    /// too short, zero, or too long.
    pub fn new(duration: Duration) -> Result<Self, PostgresChangeStreamCheckpointError> {
        if duration.is_zero()
            || duration < Duration::from_millis(1)
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

fn lease_milliseconds(
    duration: ChangeStreamLeaseDuration,
) -> Result<i64, PostgresChangeStreamCheckpointError> {
    i64::try_from(duration.get().as_millis())
        .map_err(|_| PostgresChangeStreamCheckpointError::InvalidLeaseDuration)
}

impl PostgresChangeStreamCheckpointError {
    fn storage(error: sqlx::Error) -> Self {
        Self::Storage(error)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        ChangeStreamLeaseDuration, ChangeStreamLeaseOwner, ChangeStreamLeaseOwnerError,
        PostgresChangeStreamCheckpointError, bson,
    };
    use rustee_mongodb::mongodb::change_stream::event::ResumeToken;

    #[test]
    fn opaque_resume_token_round_trips_as_bson_bytes() {
        let token =
            bson::from_document::<ResumeToken>(bson::doc! { "_data": "checkpoint-7" }).unwrap();
        let bytes = bson::to_vec(&token).unwrap();
        let restored = bson::from_slice::<ResumeToken>(&bytes).unwrap();
        assert_eq!(restored, token);
        assert_eq!(
            bson::from_slice::<ResumeToken>(&[0_u8, 1, 2])
                .map_err(|_| PostgresChangeStreamCheckpointError::InvalidCheckpoint)
                .unwrap_err()
                .to_string(),
            "stored MongoDB change-stream checkpoint is invalid"
        );
    }

    #[test]
    fn lease_metadata_is_bounded_and_redacted() {
        assert_eq!(
            ChangeStreamLeaseOwner::new(" ").unwrap_err(),
            ChangeStreamLeaseOwnerError::InvalidOwner
        );
        assert_eq!(
            ChangeStreamLeaseOwner::new("worker\0a").unwrap_err(),
            ChangeStreamLeaseOwnerError::InvalidOwner
        );
        assert_eq!(
            ChangeStreamLeaseOwner::new("a".repeat(256)).unwrap_err(),
            ChangeStreamLeaseOwnerError::InvalidOwner
        );
        let owner = ChangeStreamLeaseOwner::new("pod-7-attempt-3").unwrap();
        assert!(!format!("{owner:?}").contains("pod-7-attempt-3"));
        assert!(matches!(
            ChangeStreamLeaseDuration::new(Duration::ZERO).unwrap_err(),
            PostgresChangeStreamCheckpointError::InvalidLeaseDuration
        ));
        assert!(matches!(
            ChangeStreamLeaseDuration::new(Duration::from_nanos(1)).unwrap_err(),
            PostgresChangeStreamCheckpointError::InvalidLeaseDuration
        ));
        assert!(matches!(
            ChangeStreamLeaseDuration::new(Duration::from_secs(3_601)).unwrap_err(),
            PostgresChangeStreamCheckpointError::InvalidLeaseDuration
        ));
        assert_eq!(
            ChangeStreamLeaseDuration::new(Duration::from_millis(10))
                .unwrap()
                .get(),
            Duration::from_millis(10)
        );
    }
}
