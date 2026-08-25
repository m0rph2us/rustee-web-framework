use std::{fmt, sync::Arc, time::Duration};

use lapin::{Connection, ConnectionProperties, uri::AMQPUri};
use tokio::time::timeout;

use crate::{ConfigError, RabbitMqError};

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Redacted connection settings for one `RabbitMQ` broker endpoint.
#[derive(Clone, Eq, PartialEq)]
pub struct RabbitMqConnectionConfig {
    url: String,
    connect_timeout: Duration,
}

impl RabbitMqConnectionConfig {
    /// Creates a connection configuration from an AMQP(S) URL held in a secret source.
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
        }
    }

    /// Sets the bounded time allowed to establish the AMQP connection.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroDuration`] when `connect_timeout` is zero.
    pub fn with_connect_timeout(mut self, connect_timeout: Duration) -> Result<Self, ConfigError> {
        if connect_timeout.is_zero() {
            return Err(ConfigError::ZeroDuration);
        }
        self.connect_timeout = connect_timeout;
        Ok(self)
    }

    /// Returns the AMQP connection establishment deadline.
    #[must_use]
    pub const fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Validates the configured AMQP(S) URL without opening a network connection.
    ///
    /// This lets application startup distinguish malformed secret-source configuration from a
    /// broker outage before it attempts topology readiness or starts a worker.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqError::InvalidConnectionConfig`] when the URL is not an AMQP(S) URI
    /// accepted by the underlying client.
    pub fn validate(&self) -> Result<(), RabbitMqError> {
        self.parsed_uri().map(|_| ())
    }

    /// Opens one `RabbitMQ` connection without declaring application topology.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqError::InvalidConnectionConfig`] when the URL is invalid, or
    /// [`RabbitMqError::Connect`] when the broker is unavailable.
    pub async fn connect(&self) -> Result<RabbitMqConnection, RabbitMqError> {
        let uri = self.parsed_uri()?;
        timeout(
            self.connect_timeout,
            Connection::connect_uri(uri, ConnectionProperties::default()),
        )
        .await
        .map_err(|_| RabbitMqError::Connect)?
        .map(RabbitMqConnection::new)
        .map_err(|_| RabbitMqError::Connect)
    }

    fn parsed_uri(&self) -> Result<AMQPUri, RabbitMqError> {
        self.url
            .parse::<AMQPUri>()
            .map_err(|_| RabbitMqError::InvalidConnectionConfig)
    }
}

impl fmt::Debug for RabbitMqConnectionConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RabbitMqConnectionConfig")
            .field("url", &"[REDACTED]")
            .field("connect_timeout", &self.connect_timeout)
            .finish()
    }
}

/// A shareable connected `RabbitMQ` session used to open isolated AMQP channels.
#[derive(Clone)]
pub struct RabbitMqConnection {
    pub(super) inner: Arc<Connection>,
}

impl RabbitMqConnection {
    /// Wraps an already-connected `lapin` connection for dependency injection and testing.
    #[must_use]
    pub fn new(connection: Connection) -> Self {
        Self {
            inner: Arc::new(connection),
        }
    }

    /// Returns whether the underlying AMQP connection is presently connected.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.inner.status().connected()
    }
}

impl fmt::Debug for RabbitMqConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RabbitMqConnection")
            .field("connected", &self.is_connected())
            .finish()
    }
}
