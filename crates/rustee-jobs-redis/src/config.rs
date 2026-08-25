//! Validated Redis Streams worker configuration and provider bounds.

use std::{fmt, time::Duration};

const DEFAULT_BLOCK_TIMEOUT: Duration = Duration::from_millis(250);
pub(crate) const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_RECLAIM_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_RECLAIM_IDLE: Duration = Duration::from_secs(30);
const DEFAULT_BATCH_SIZE: usize = 64;
const MAX_BATCH_SIZE: usize = 1_000;
const RETRY_KEY_NAMESPACE: &str = "rustee:jobs:retry:v1";

/// Consumer-group and retry settings for one Redis Streams job worker.
///
/// Its `Debug` output keeps deployment routing identifiers and internal retry keys redacted.
/// Retry keys use a versioned, length-delimited stream/group namespace so separate worker routes
/// cannot share delayed-delivery storage when their identifiers contain Redis key delimiters.
#[derive(Clone, Eq, PartialEq)]
pub struct RedisStreamsWorkerConfig {
    stream: String,
    group: String,
    consumer: String,
    dead_letter_stream: String,
    retry_schedule_key: String,
    retry_payload_key: String,
    retry_attempt_key: String,
    block_timeout_ms: usize,
    operation_timeout: Duration,
    reclaim_interval: Duration,
    reclaim_idle_ms: usize,
    batch_size: usize,
}

impl RedisStreamsWorkerConfig {
    /// Creates a worker configuration for a pre-existing stream, consumer group, and DLQ stream.
    ///
    /// The consumer name must be unique per concurrently running worker process. Internal retry
    /// keys are deterministically scoped to this source stream and consumer group.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when an identifier is unsafe, a DLQ equals the source stream, or
    /// a bounded duration cannot be represented by Redis milliseconds.
    pub fn new(
        stream: impl Into<String>,
        group: impl Into<String>,
        consumer: impl Into<String>,
        dead_letter_stream: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        let stream = stream.into();
        let group = group.into();
        let consumer = consumer.into();
        let dead_letter_stream = dead_letter_stream.into();
        validate_key(&stream)?;
        validate_group_or_consumer(&group)?;
        validate_group_or_consumer(&consumer)?;
        validate_key(&dead_letter_stream)?;
        if stream == dead_letter_stream {
            return Err(ConfigError::DeadLetterMatchesSource);
        }
        Ok(Self {
            retry_schedule_key: retry_key(&stream, &group, "schedule"),
            retry_payload_key: retry_key(&stream, &group, "payload"),
            retry_attempt_key: retry_key(&stream, &group, "attempt"),
            stream,
            group,
            consumer,
            dead_letter_stream,
            block_timeout_ms: nonzero_duration_to_millis(DEFAULT_BLOCK_TIMEOUT)?,
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
            reclaim_interval: DEFAULT_RECLAIM_INTERVAL,
            reclaim_idle_ms: nonzero_duration_to_millis(DEFAULT_RECLAIM_IDLE)?,
            batch_size: DEFAULT_BATCH_SIZE,
        })
    }

    /// Sets the bounded duration of an idle `XREADGROUP` call.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroDuration`] or [`ConfigError::DurationOutOfRange`] for an
    /// unsupported Redis millisecond value.
    pub fn with_block_timeout(mut self, timeout: Duration) -> Result<Self, ConfigError> {
        self.block_timeout_ms = nonzero_duration_to_millis(timeout)?;
        self.validate_operation_timeout()?;
        Ok(self)
    }

    /// Sets the outer deadline for one Redis Streams command.
    ///
    /// This deadline must be longer than the configured blocking read so an idle
    /// `XREADGROUP` request can complete normally. The connection manager's reconnect
    /// policy remains application-owned inside this adapter boundary.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroDuration`] for zero and
    /// [`ConfigError::OperationTimeoutNotLongerThanBlock`] when it cannot contain the current
    /// blocking read duration.
    pub fn with_operation_timeout(
        mut self,
        operation_timeout: Duration,
    ) -> Result<Self, ConfigError> {
        validate_operation_timeout(operation_timeout)?;
        self.operation_timeout = operation_timeout;
        self.validate_operation_timeout()?;
        Ok(self)
    }

    /// Sets how often this worker promotes due retries and looks for abandoned pending entries.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroDuration`] when `interval` is zero.
    pub fn with_reclaim_interval(
        mut self,
        reclaim_interval: Duration,
    ) -> Result<Self, ConfigError> {
        if reclaim_interval.is_zero() {
            return Err(ConfigError::ZeroDuration);
        }
        self.reclaim_interval = reclaim_interval;
        Ok(self)
    }

    /// Sets the minimum pending-entry idle time before another consumer can reclaim it.
    ///
    /// This must exceed the longest un-heartbeated handler execution that the deployment permits.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroDuration`] or [`ConfigError::DurationOutOfRange`] for an
    /// unsupported Redis millisecond value.
    pub fn with_reclaim_idle(mut self, reclaim_idle: Duration) -> Result<Self, ConfigError> {
        self.reclaim_idle_ms = nonzero_duration_to_millis(reclaim_idle)?;
        Ok(self)
    }

    /// Sets the maximum records fetched or reclaimed in one provider operation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidBatchSize`] outside `1..={MAX_BATCH_SIZE}`.
    pub fn with_batch_size(mut self, batch_size: usize) -> Result<Self, ConfigError> {
        if !(1..=MAX_BATCH_SIZE).contains(&batch_size) {
            return Err(ConfigError::InvalidBatchSize);
        }
        self.batch_size = batch_size;
        Ok(self)
    }

    /// Returns the deployment-provisioned source stream.
    #[must_use]
    pub fn stream(&self) -> &str {
        &self.stream
    }

    /// Returns the deployment-provisioned consumer group.
    #[must_use]
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Returns the unique worker consumer name.
    #[must_use]
    pub fn consumer(&self) -> &str {
        &self.consumer
    }

    /// Returns the deployment-provisioned dead-letter stream.
    #[must_use]
    pub fn dead_letter_stream(&self) -> &str {
        &self.dead_letter_stream
    }

    /// Returns the provider-private collision-safe retry schedule key.
    #[must_use]
    pub fn retry_schedule_key(&self) -> &str {
        &self.retry_schedule_key
    }

    /// Returns the provider-private collision-safe retry payload hash key.
    #[must_use]
    pub fn retry_payload_key(&self) -> &str {
        &self.retry_payload_key
    }

    /// Returns the provider-private collision-safe retry attempt hash key.
    #[must_use]
    pub fn retry_attempt_key(&self) -> &str {
        &self.retry_attempt_key
    }

    /// Returns the outer deadline for one Redis Streams command.
    #[must_use]
    pub const fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }

    pub(crate) const fn block_timeout_ms(&self) -> usize {
        self.block_timeout_ms
    }

    pub(crate) const fn reclaim_interval(&self) -> Duration {
        self.reclaim_interval
    }

    pub(crate) const fn reclaim_idle_ms(&self) -> usize {
        self.reclaim_idle_ms
    }

    pub(crate) const fn batch_size(&self) -> usize {
        self.batch_size
    }

    fn validate_operation_timeout(&self) -> Result<(), ConfigError> {
        validate_operation_timeout(self.operation_timeout)?;
        let block_timeout_ms =
            u64::try_from(self.block_timeout_ms).map_err(|_| ConfigError::DurationOutOfRange)?;
        if self.operation_timeout <= Duration::from_millis(block_timeout_ms) {
            return Err(ConfigError::OperationTimeoutNotLongerThanBlock);
        }
        Ok(())
    }
}

impl fmt::Debug for RedisStreamsWorkerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisStreamsWorkerConfig")
            .field("stream", &"[REDACTED]")
            .field("stream_length", &self.stream.len())
            .field("group", &"[REDACTED]")
            .field("group_length", &self.group.len())
            .field("consumer", &"[REDACTED]")
            .field("consumer_length", &self.consumer.len())
            .field("dead_letter_stream", &"[REDACTED]")
            .field("dead_letter_stream_length", &self.dead_letter_stream.len())
            .field("retry_schedule_key", &"[REDACTED]")
            .field("retry_schedule_key_length", &self.retry_schedule_key.len())
            .field("retry_payload_key", &"[REDACTED]")
            .field("retry_payload_key_length", &self.retry_payload_key.len())
            .field("retry_attempt_key", &"[REDACTED]")
            .field("retry_attempt_key_length", &self.retry_attempt_key.len())
            .field("block_timeout_ms", &self.block_timeout_ms)
            .field("operation_timeout", &self.operation_timeout)
            .field("reclaim_interval", &self.reclaim_interval)
            .field("reclaim_idle_ms", &self.reclaim_idle_ms)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

/// Invalid Redis Streams job provider configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConfigError {
    /// A Redis key was blank, whitespace-containing, or too long for this provider boundary.
    #[error("Redis Streams job key must be non-blank, whitespace-free, and bounded")]
    InvalidKey,
    /// A Redis consumer group or consumer name was blank, whitespace-containing, or too long.
    #[error(
        "Redis Streams job group and consumer names must be non-blank, whitespace-free, and bounded"
    )]
    InvalidGroupOrConsumer,
    /// The dead-letter stream must be distinct from the source stream.
    #[error("Redis Streams dead-letter stream must differ from the source stream")]
    DeadLetterMatchesSource,
    /// A time setting must use a positive duration.
    #[error("Redis Streams job duration must be greater than zero")]
    ZeroDuration,
    /// A duration cannot be represented as Redis milliseconds on this target.
    #[error("Redis Streams job duration cannot be represented as Redis milliseconds")]
    DurationOutOfRange,
    /// An operation deadline cannot contain the configured blocking read.
    #[error("Redis Streams operation timeout must be longer than the blocking read timeout")]
    OperationTimeoutNotLongerThanBlock,
    /// A fetch or reclaim batch was outside the bounded provider range.
    #[error("Redis Streams job batch size must be between 1 and 1000")]
    InvalidBatchSize,
}

pub(crate) fn validate_key(value: &str) -> Result<(), ConfigError> {
    if value.trim().is_empty() || value.len() > 512 || value.chars().any(char::is_whitespace) {
        return Err(ConfigError::InvalidKey);
    }
    Ok(())
}

fn retry_key(stream: &str, group: &str, record_kind: &str) -> String {
    format!(
        "{RETRY_KEY_NAMESPACE}:{}:{stream}:{}:{group}:{record_kind}",
        stream.len(),
        group.len(),
    )
}

pub(crate) fn validate_operation_timeout(operation_timeout: Duration) -> Result<(), ConfigError> {
    if operation_timeout.is_zero() {
        Err(ConfigError::ZeroDuration)
    } else {
        Ok(())
    }
}

fn validate_group_or_consumer(value: &str) -> Result<(), ConfigError> {
    if value.trim().is_empty() || value.len() > 255 || value.chars().any(char::is_whitespace) {
        return Err(ConfigError::InvalidGroupOrConsumer);
    }
    Ok(())
}

pub(crate) fn duration_to_millis(duration: Duration) -> Result<usize, ConfigError> {
    if !duration.subsec_nanos().is_multiple_of(1_000_000) {
        return Err(ConfigError::DurationOutOfRange);
    }
    usize::try_from(duration.as_millis()).map_err(|_| ConfigError::DurationOutOfRange)
}

pub(crate) fn nonzero_duration_to_millis(duration: Duration) -> Result<usize, ConfigError> {
    if duration.is_zero() {
        return Err(ConfigError::ZeroDuration);
    }
    let milliseconds = duration_to_millis(duration)?;
    if milliseconds == 0 {
        return Err(ConfigError::DurationOutOfRange);
    }
    Ok(milliseconds)
}
