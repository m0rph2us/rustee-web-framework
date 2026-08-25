//! Explicit bounded Redis readiness checks.

use std::{fmt, time::Duration};

use redis::{RedisError, aio::ConnectionManager};

/// Bounded failure returned by an explicit Redis readiness check.
#[derive(thiserror::Error)]
pub enum RedisReadinessError {
    /// A zero deadline cannot bound a dependency check.
    #[error("Redis readiness timeout must be non-zero")]
    ZeroTimeout,
    /// The `PING` command did not complete before the application-supplied deadline.
    #[error("Redis readiness timed out after {0:?}")]
    Timeout(Duration),
    /// Redis rejected or could not complete the `PING` command.
    #[error("Redis readiness failed")]
    Redis(#[from] RedisError),
}

impl fmt::Debug for RedisReadinessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::ZeroTimeout => "zero_timeout",
            Self::Timeout(_) => "timeout",
            Self::Redis(_) => "redis_failed",
        };
        formatter
            .debug_struct("RedisReadinessError")
            .field("kind", &kind)
            .finish()
    }
}

pub(crate) fn validate_readiness_timeout(timeout: Duration) -> Result<(), RedisReadinessError> {
    if timeout.is_zero() {
        return Err(RedisReadinessError::ZeroTimeout);
    }
    Ok(())
}

/// Pings Redis for an explicit bounded readiness check.
///
/// # Errors
///
/// Returns an error when `timeout` is zero, the command does not finish before the caller's
/// deadline, or Redis cannot execute the command. Trusted Redis source details are available via
/// [`std::error::Error::source`] but never render through this error's display or debug output.
pub async fn readiness(
    connection: &ConnectionManager,
    timeout: Duration,
) -> Result<(), RedisReadinessError> {
    validate_readiness_timeout(timeout)?;
    let mut connection = connection.clone();
    tokio::time::timeout(
        timeout,
        redis::cmd("PING").query_async::<()>(&mut connection),
    )
    .await
    .map_err(|_| RedisReadinessError::Timeout(timeout))?
    .map_err(Into::into)
}
