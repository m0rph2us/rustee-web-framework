//! Redis connection configuration and bounded initial connection setup.

use std::{fmt, time::Duration};

use redis::{Client, aio::ConnectionManager};
use tokio::time::timeout;

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
    /// Returns a content-free [`RedisConnectError::Connection`] when the configured URL is
    /// malformed.
    pub fn client(&self) -> Result<Client, RedisConnectError> {
        Client::open(self.url.as_str()).map_err(|_| RedisConnectError::Connection)
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
    let client = config.client()?;
    timeout(config.connect_timeout, ConnectionManager::new(client))
        .await
        .map_err(|_| RedisConnectError::Connection)?
        .map_err(|_| RedisConnectError::Connection)
}
