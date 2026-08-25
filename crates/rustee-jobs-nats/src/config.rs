//! Validated NATS connection, subject, and worker-admission configuration.

use std::{fmt, time::Duration};

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Connection and publishing settings for a `JetStream` job producer.
///
/// Its `Debug` output keeps connection and deployment-routing values redacted.
#[derive(Clone, Eq, PartialEq)]
pub struct NatsConfig {
    url: String,
    subject: String,
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl NatsConfig {
    /// Creates a producer configuration with a finite `JetStream` request timeout.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidServerUrl`] when `url` is not a NATS server address accepted
    /// by the underlying client, or [`ConfigError::InvalidSubject`] when `subject` is not a
    /// concrete publish subject.
    pub fn new(url: impl Into<String>, subject: impl Into<String>) -> Result<Self, ConfigError> {
        let url = url.into();
        validate_server_url(&url)?;
        let subject = subject.into();
        validate_subject(&subject)?;
        Ok(Self {
            url,
            subject,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: Duration::from_secs(5),
        })
    }

    /// Sets the bounded time allowed to establish the NATS connection.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroConnectTimeout`] when `connect_timeout` is zero.
    pub fn with_connect_timeout(mut self, connect_timeout: Duration) -> Result<Self, ConfigError> {
        if connect_timeout.is_zero() {
            return Err(ConfigError::ZeroConnectTimeout);
        }
        self.connect_timeout = connect_timeout;
        Ok(self)
    }

    /// Sets the positive timeout for `JetStream` API requests and publish acknowledgements.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroRequestTimeout`] when `timeout` is zero.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Result<Self, ConfigError> {
        if timeout.is_zero() {
            return Err(ConfigError::ZeroRequestTimeout);
        }
        self.request_timeout = timeout;
        Ok(self)
    }

    /// Returns the NATS connection establishment deadline.
    #[must_use]
    pub const fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Returns the deadline applied to `JetStream` API requests and publish acknowledgements.
    #[must_use]
    pub const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// Returns the concrete `JetStream` subject used for durable jobs.
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }

    pub(crate) fn url(&self) -> &str {
        &self.url
    }
}

impl fmt::Debug for NatsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NatsConfig")
            .field("url", &"[REDACTED]")
            .field("subject", &"[REDACTED]")
            .field("subject_length", &self.subject.len())
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

/// Invalid NATS producer or worker configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConfigError {
    /// The server URL was not accepted by the NATS client before connecting.
    #[error("NATS server URL is invalid")]
    InvalidServerUrl,
    /// The subject was empty, had an empty token, whitespace, or a subscription wildcard.
    #[error("NATS job publish subject must be a concrete dot-delimited subject")]
    InvalidSubject,
    /// The configured pull consumer is ephemeral and cannot survive worker recovery.
    #[error("NATS job worker requires a durable pull consumer")]
    EphemeralConsumer,
    /// The configured consumer cannot acknowledge individual completed jobs.
    #[error("NATS job worker requires explicit consumer acknowledgements")]
    NonExplicitAcknowledgement,
    /// The idle pull request timeout was zero.
    #[error("NATS job worker pull request expiry must be non-zero")]
    ZeroPullRequestExpiry,
    /// The initial NATS connection deadline was zero.
    #[error("NATS connection timeout must be non-zero")]
    ZeroConnectTimeout,
    /// The `JetStream` API request deadline was zero.
    #[error("NATS JetStream request timeout must be non-zero")]
    ZeroRequestTimeout,
}

fn validate_server_url(url: &str) -> Result<(), ConfigError> {
    let server = url
        .parse::<async_nats::ServerAddr>()
        .map_err(|_| ConfigError::InvalidServerUrl)?;
    if server.into_inner().host_str().is_none() {
        return Err(ConfigError::InvalidServerUrl);
    }
    Ok(())
}

pub(crate) fn validate_subject(subject: &str) -> Result<(), ConfigError> {
    if subject.trim().is_empty()
        || subject.chars().any(char::is_whitespace)
        || subject.contains('*')
        || subject.contains('>')
        || subject.split('.').any(str::is_empty)
    {
        return Err(ConfigError::InvalidSubject);
    }
    Ok(())
}
