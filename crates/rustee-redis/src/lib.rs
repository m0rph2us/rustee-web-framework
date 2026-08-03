//! Redis integration built around redis-rs' reconnecting `ConnectionManager`.
//!
//! Cache behavior remains explicit: callers choose key namespaces, TTLs, and fallback behavior.

use std::{fmt, time::Duration};

use redis::{AsyncCommands, Client, RedisError, aio::ConnectionManager};
use serde::{Serialize, de::DeserializeOwned};
use tokio::time::timeout;

pub use redis;

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Connection configuration with a redacted debug representation.
#[derive(Clone, Eq, PartialEq)]
pub struct RedisConfig {
    url: String,
    connect_timeout: Duration,
}

impl RedisConfig {
    /// Creates a Redis configuration from a URL held by the application's secret source.
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
        }
    }

    /// Sets the bounded time allowed to establish the initial Redis connection.
    ///
    /// # Errors
    ///
    /// Returns [`RedisConfigError::ZeroConnectTimeout`] when `connect_timeout` is zero.
    pub fn with_connect_timeout(
        mut self,
        connect_timeout: Duration,
    ) -> Result<Self, RedisConfigError> {
        if connect_timeout.is_zero() {
            return Err(RedisConfigError::ZeroConnectTimeout);
        }
        self.connect_timeout = connect_timeout;
        Ok(self)
    }

    /// Returns the initial Redis connection establishment deadline.
    #[must_use]
    pub const fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Creates a redis-rs client without opening a network connection.
    ///
    /// # Errors
    ///
    /// Returns a Redis error when the configured URL is malformed.
    pub fn client(&self) -> Result<Client, RedisError> {
        Client::open(self.url.as_str())
    }
}

impl fmt::Debug for RedisConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RedisConfig([REDACTED])")
    }
}

/// Invalid Redis connection configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RedisConfigError {
    /// The initial Redis connection deadline was zero.
    #[error("Redis connection timeout must be non-zero")]
    ZeroConnectTimeout,
}

/// Sanitized Redis initial connection failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RedisConnectError {
    /// The Redis URL was invalid, the server was unreachable, or the configured deadline elapsed.
    #[error("Redis connection failed")]
    Connection,
}

/// Connects a cloneable, reconnecting Redis connection manager.
///
/// # Errors
///
/// Returns [`RedisConnectError::Connection`] when the URL is malformed, the initial connection
/// cannot be created, or the configured deadline elapses. Reconnection after this initial setup
/// remains the `ConnectionManager`'s responsibility.
pub async fn connect(config: &RedisConfig) -> Result<ConnectionManager, RedisConnectError> {
    let client = config.client().map_err(|_| RedisConnectError::Connection)?;
    timeout(config.connect_timeout, ConnectionManager::new(client))
        .await
        .map_err(|_| RedisConnectError::Connection)?
        .map_err(|_| RedisConnectError::Connection)
}

/// Pings Redis for an explicit readiness check.
///
/// # Errors
///
/// Returns a Redis error when the command cannot be executed or the server response is invalid.
pub async fn readiness(connection: &ConnectionManager) -> Result<(), RedisError> {
    let mut connection = connection.clone();
    redis::cmd("PING").query_async::<()>(&mut connection).await
}

/// Errors returned by the explicit JSON cache helpers.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    /// Redis command failure.
    #[error("Redis command failed: {0}")]
    Redis(#[from] RedisError),
    /// JSON serialization or deserialization failure.
    #[error("cache value serialization failed: {0}")]
    Json(#[from] serde_json::Error),
}

/// Reads a JSON value from Redis without hiding cache-miss behavior.
///
/// # Errors
///
/// Returns an error for Redis communication or JSON decoding failure.
pub async fn get_json<T>(connection: &ConnectionManager, key: &str) -> Result<Option<T>, CacheError>
where
    T: DeserializeOwned,
{
    let mut connection = connection.clone();
    let value: Option<String> = connection.get(key).await?;
    value
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(Into::into)
}

/// Atomically removes and deserializes one JSON value without treating a cache miss as an error.
///
/// # Errors
///
/// Returns an error for Redis communication or JSON decoding failure. This requires Redis 6.2 or
/// newer because it uses `GETDEL` rather than a non-atomic read/delete pair.
pub async fn take_json<T>(
    connection: &ConnectionManager,
    key: &str,
) -> Result<Option<T>, CacheError>
where
    T: DeserializeOwned,
{
    let mut connection = connection.clone();
    let value: Option<String> = redis::cmd("GETDEL")
        .arg(key)
        .query_async(&mut connection)
        .await?;
    value
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(Into::into)
}

/// Serializes and stores a JSON value with an explicit TTL in seconds.
///
/// # Errors
///
/// Returns an error for zero TTL, JSON serialization, or Redis communication failure.
pub async fn set_json<T>(
    connection: &ConnectionManager,
    key: &str,
    value: &T,
    ttl_seconds: u64,
) -> Result<(), CacheError>
where
    T: Serialize,
{
    if ttl_seconds == 0 {
        return Err(RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "cache TTL must be greater than zero",
        ))
        .into());
    }
    let encoded = serde_json::to_string(value)?;
    let mut connection = connection.clone();
    connection
        .set_ex::<_, _, ()>(key, encoded, ttl_seconds)
        .await?;
    Ok(())
}

/// Deletes one key without treating a missing key as an error.
///
/// # Errors
///
/// Returns a Redis error when the command cannot be executed.
pub async fn delete(connection: &ConnectionManager, key: &str) -> Result<(), RedisError> {
    let mut connection = connection.clone();
    connection.del::<_, ()>(key).await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{RedisConfig, RedisConfigError};

    #[test]
    fn redis_url_is_not_exposed_in_debug_output() {
        let config = RedisConfig::new("redis://user:password@localhost:6379/0");
        assert!(!format!("{config:?}").contains("password"));
    }

    #[test]
    fn malformed_url_is_rejected_before_connecting() {
        let config = RedisConfig::new("not a redis URL");
        assert!(config.client().is_err());
    }

    #[test]
    fn configuration_requires_a_non_zero_connect_deadline() {
        let error = RedisConfig::new("redis://localhost:6379/0")
            .with_connect_timeout(Duration::ZERO)
            .unwrap_err();
        assert_eq!(error, RedisConfigError::ZeroConnectTimeout);
    }
}
