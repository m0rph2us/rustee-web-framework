use std::{fmt, time::Duration};

use futures_util::future::BoxFuture;
use rustee_mongodb::{
    ChangeStreamCheckpointStore, ChangeStreamConsumer,
    mongodb::{bson, change_stream::event::ResumeToken},
};
use sqlx::PgPool;

use crate::{
    ChangeStreamLease, ChangeStreamLeaseAcquire, ChangeStreamLeaseDuration, ChangeStreamLeaseOwner,
    MAX_CHANGE_STREAM_RESUME_TOKEN_BYTES, PostgresChangeStreamCheckpointError,
    model::lease_milliseconds,
};

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
        let acquired = sqlx::query_scalar::<_, String>(concat!(
            "INSERT INTO rustee_mongodb_change_stream_lease (consumer, owner, expires_at) ",
            "VALUES ($1, $2, clock_timestamp() + ($3::bigint * INTERVAL '1 millisecond')) ",
            "ON CONFLICT (consumer) DO UPDATE ",
            "SET owner = EXCLUDED.owner, expires_at = EXCLUDED.expires_at ",
            "WHERE rustee_mongodb_change_stream_lease.expires_at <= clock_timestamp() ",
            "OR rustee_mongodb_change_stream_lease.owner = EXCLUDED.owner ",
            "RETURNING consumer",
        ))
        .bind(consumer.as_str())
        .bind(owner.as_str())
        .bind(milliseconds)
        .fetch_optional(&self.pool)
        .await
        .map_err(PostgresChangeStreamCheckpointError::storage)?;
        Ok(match acquired {
            Some(_) => ChangeStreamLeaseAcquire::Acquired(ChangeStreamLease::new(consumer, owner)),
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
        let result = sqlx::query(concat!(
            "UPDATE rustee_mongodb_change_stream_lease ",
            "SET expires_at = clock_timestamp() + ($3::bigint * INTERVAL '1 millisecond') ",
            "WHERE consumer = $1 AND owner = $2 AND expires_at > clock_timestamp()",
        ))
        .bind(lease.consumer().as_str())
        .bind(lease.owner().as_str())
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
        let bytes = encode_resume_token(&resume_token)?;
        let saved = sqlx::query_scalar::<_, String>(concat!(
            "INSERT INTO rustee_mongodb_change_stream_checkpoint (consumer, resume_token) ",
            "SELECT $1, $2 ",
            "WHERE EXISTS ( ",
            "SELECT 1 FROM rustee_mongodb_change_stream_lease ",
            "WHERE consumer = $1 AND owner = $3 AND expires_at > clock_timestamp() ",
            ") ",
            "ON CONFLICT (consumer) DO UPDATE ",
            "SET resume_token = EXCLUDED.resume_token, updated_at = clock_timestamp() ",
            "WHERE EXISTS ( ",
            "SELECT 1 FROM rustee_mongodb_change_stream_lease ",
            "WHERE consumer = $1 AND owner = $3 AND expires_at > clock_timestamp() ",
            ") ",
            "RETURNING consumer",
        ))
        .bind(lease.consumer().as_str())
        .bind(bytes)
        .bind(lease.owner().as_str())
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
        .bind(lease.consumer().as_str())
        .bind(lease.owner().as_str())
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
            let max_resume_token_bytes = i64::try_from(MAX_CHANGE_STREAM_RESUME_TOKEN_BYTES)
                .map_err(|_| PostgresChangeStreamCheckpointError::InvalidCheckpoint)?;
            let checkpoint = sqlx::query_as::<_, (Option<Vec<u8>>, bool)>(
                "SELECT CASE WHEN octet_length(resume_token) <= $2 THEN resume_token ELSE NULL END, \
                 octet_length(resume_token) <= $2 \
                 FROM rustee_mongodb_change_stream_checkpoint WHERE consumer = $1",
            )
            .bind(consumer.as_str())
            .bind(max_resume_token_bytes)
            .fetch_optional(&pool)
            .await
            .map_err(PostgresChangeStreamCheckpointError::storage)?;
            match checkpoint {
                None => Ok(None),
                Some((Some(bytes), true)) => decode_resume_token(&bytes).map(Some),
                Some(_) => Err(PostgresChangeStreamCheckpointError::InvalidCheckpoint),
            }
        })
    }

    fn save(
        &self,
        consumer: ChangeStreamConsumer,
        resume_token: ResumeToken,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let pool = self.pool.clone();
        Box::pin(async move {
            let bytes = encode_resume_token(&resume_token)?;
            sqlx::query(concat!(
                "INSERT INTO rustee_mongodb_change_stream_checkpoint (consumer, resume_token) ",
                "VALUES ($1, $2) ",
                "ON CONFLICT (consumer) DO UPDATE ",
                "SET resume_token = EXCLUDED.resume_token, updated_at = clock_timestamp()",
            ))
            .bind(consumer.as_str())
            .bind(bytes)
            .execute(&pool)
            .await
            .map_err(PostgresChangeStreamCheckpointError::storage)?;
            Ok(())
        })
    }
}

pub(crate) fn encode_resume_token(
    resume_token: &ResumeToken,
) -> Result<Vec<u8>, PostgresChangeStreamCheckpointError> {
    let bytes = bson::to_vec(resume_token)
        .map_err(|_| PostgresChangeStreamCheckpointError::InvalidCheckpoint)?;
    (bytes.len() <= MAX_CHANGE_STREAM_RESUME_TOKEN_BYTES)
        .then_some(bytes)
        .ok_or(PostgresChangeStreamCheckpointError::InvalidCheckpoint)
}

pub(crate) fn decode_resume_token(
    bytes: &[u8],
) -> Result<ResumeToken, PostgresChangeStreamCheckpointError> {
    if bytes.len() > MAX_CHANGE_STREAM_RESUME_TOKEN_BYTES {
        return Err(PostgresChangeStreamCheckpointError::InvalidCheckpoint);
    }
    bson::from_slice(bytes).map_err(|_| PostgresChangeStreamCheckpointError::InvalidCheckpoint)
}
