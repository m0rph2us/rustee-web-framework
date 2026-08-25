use std::{fmt, time::Duration};

use futures_util::future::BoxFuture;
use rustee_jobs::{JobMessage, JobPublisher};
use rustee_redis::redis::{self, aio::ConnectionManager};

use crate::{
    ATTEMPT_FIELD, ConfigError, PAYLOAD_FIELD, RedisStreamsError,
    config::{DEFAULT_OPERATION_TIMEOUT, validate_key, validate_operation_timeout},
    operation::bounded,
};

/// Acknowledged Redis Streams publisher for serialized `Rustee` jobs.
///
/// Its `Debug` output keeps the deployment stream identifier redacted.
#[derive(Clone)]
pub struct RedisStreamsPublisher {
    connection: ConnectionManager,
    stream: String,
    operation_timeout: Duration,
}

impl RedisStreamsPublisher {
    /// Wraps a reconnecting Redis connection and one deployment-provisioned stream.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidKey`] when `stream` is blank, contains whitespace, or is
    /// outside the provider's bounded key length.
    pub fn new(
        connection: ConnectionManager,
        stream: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        let stream = stream.into();
        validate_key(&stream)?;
        Ok(Self {
            connection,
            stream,
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
        })
    }

    /// Sets the maximum time one Redis Streams readiness or publish operation may use.
    ///
    /// The injected connection manager owns reconnect policy. Rustee cancels the current command
    /// and returns a sanitized provider error once this deadline expires.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroDuration`] when `operation_timeout` is zero.
    pub fn with_operation_timeout(
        mut self,
        operation_timeout: Duration,
    ) -> Result<Self, ConfigError> {
        validate_operation_timeout(operation_timeout)?;
        self.operation_timeout = operation_timeout;
        Ok(self)
    }

    /// Verifies that the configured stream can be inspected without mutating Redis state.
    ///
    /// # Errors
    ///
    /// Returns [`RedisStreamsError::Readiness`] when the stream is absent or Redis cannot answer.
    pub async fn readiness(&self) -> Result<(), RedisStreamsError> {
        let mut connection = self.connection.clone();
        bounded(
            self.operation_timeout,
            redis::cmd("XINFO")
                .arg("STREAM")
                .arg(&self.stream)
                .query_async::<redis::Value>(&mut connection),
        )
        .await
        .map(|_| ())
        .map_err(|()| RedisStreamsError::Readiness)
    }

    /// Returns the configured destination stream.
    #[must_use]
    pub fn stream(&self) -> &str {
        &self.stream
    }

    /// Returns the maximum time one Redis Streams readiness or publish operation may use.
    #[must_use]
    pub const fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }
}

impl fmt::Debug for RedisStreamsPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisStreamsPublisher")
            .field("stream", &"[REDACTED]")
            .field("stream_length", &self.stream.len())
            .field("operation_timeout", &self.operation_timeout)
            .finish_non_exhaustive()
    }
}

impl JobPublisher for RedisStreamsPublisher {
    type Error = RedisStreamsError;

    fn publish(&self, message: JobMessage) -> BoxFuture<'static, Result<(), Self::Error>> {
        let mut connection = self.connection.clone();
        let stream = self.stream.clone();
        let operation_timeout = self.operation_timeout;
        let attempt = message.attempt();
        let payload = message.into_payload();
        Box::pin(async move {
            bounded(
                operation_timeout,
                redis::cmd("XADD")
                    .arg(stream)
                    .arg("*")
                    .arg(PAYLOAD_FIELD)
                    .arg(payload)
                    .arg(ATTEMPT_FIELD)
                    .arg(attempt)
                    .query_async::<String>(&mut connection),
            )
            .await
            .map(|_| ())
            .map_err(|()| RedisStreamsError::Publish)
        })
    }
}
