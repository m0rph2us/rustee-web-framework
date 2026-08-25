//! Source/DLQ topology, redrive, polling, and visibility-lease worker policy.

use std::{fmt, time::Duration};

use super::{
    ConfigError, DEFAULT_REQUEST_TIMEOUT, MAX_VISIBILITY_SECONDS, SqsQueueTarget,
    validate_request_timeout, validate_whole_seconds,
};

const MAX_LONG_POLL_SECONDS: u64 = 20;
const MAX_REDRIVE_RECEIVE_COUNT: u16 = 1_000;
const DEFAULT_LONG_POLL: Duration = Duration::from_secs(20);
const DEFAULT_VISIBILITY_TIMEOUT: Duration = Duration::from_mins(2);
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_HANDLER_TIMEOUT: Duration = Duration::from_mins(30);

/// Deployment and lease settings for one SQS worker.
#[derive(Clone, Eq, PartialEq)]
pub struct SqsWorkerConfig {
    source: SqsQueueTarget,
    dead_letter: SqsQueueTarget,
    expected_redrive_max_receive_count: u16,
    long_poll: Duration,
    request_timeout: Duration,
    visibility_timeout: Duration,
    heartbeat_interval: Duration,
    handler_timeout: Duration,
}

impl SqsWorkerConfig {
    /// Creates a worker configuration for pre-provisioned source and dead-letter queues.
    ///
    /// The source queue must use an SQS redrive policy pointing to `dead_letter` with exactly
    /// `expected_redrive_max_receive_count`. Rustee also sends malformed, unknown, and exhausted
    /// deliveries directly to that target before deleting the source receipt. The broker redrive
    /// policy remains the recovery path for a process that loses a receipt before settlement.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when source and DLQ targets differ in queue mode, point to the same
    /// queue, or the expected redrive receive count is outside the SQS range.
    pub fn new(
        source: SqsQueueTarget,
        dead_letter: SqsQueueTarget,
        expected_redrive_max_receive_count: u16,
    ) -> Result<Self, ConfigError> {
        if source.queue_url() == dead_letter.queue_url() {
            return Err(ConfigError::DeadLetterMatchesSource);
        }
        if source.kind().is_fifo() != dead_letter.kind().is_fifo() {
            return Err(ConfigError::QueueKindMismatch);
        }
        if !(1..=MAX_REDRIVE_RECEIVE_COUNT).contains(&expected_redrive_max_receive_count) {
            return Err(ConfigError::InvalidRedriveReceiveCount);
        }
        let config = Self {
            source,
            dead_letter,
            expected_redrive_max_receive_count,
            long_poll: DEFAULT_LONG_POLL,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            visibility_timeout: DEFAULT_VISIBILITY_TIMEOUT,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            handler_timeout: DEFAULT_HANDLER_TIMEOUT,
        };
        config.validate_lease_settings()?;
        Ok(config)
    }

    /// Sets the SQS long-poll duration used for an idle worker receive request.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidLongPoll`] unless the duration is a whole 1 through 20
    /// seconds, the SQS receive request range.
    pub fn with_long_poll(mut self, long_poll: Duration) -> Result<Self, ConfigError> {
        validate_whole_seconds(long_poll, 1, MAX_LONG_POLL_SECONDS)
            .map_err(|()| ConfigError::InvalidLongPoll)?;
        self.long_poll = long_poll;
        self.validate_lease_settings()?;
        Ok(self)
    }

    /// Sets the maximum time any one SQS request may occupy this worker.
    ///
    /// It must exceed the configured long-poll receive duration so an idle receive is never
    /// cancelled before SQS can complete it. The injected AWS SDK client's retry policy remains
    /// application-owned inside this outer deadline.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroRequestTimeout`] for a zero duration,
    /// [`ConfigError::RequestTimeoutNotLongerThanLongPoll`] when it cannot contain the current
    /// SQS long poll, or [`ConfigError::InvalidHeartbeatInterval`] when it leaves no safe
    /// renewal window in the current visibility lease.
    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Result<Self, ConfigError> {
        validate_request_timeout(request_timeout)?;
        self.request_timeout = request_timeout;
        self.validate_lease_settings()?;
        Ok(self)
    }

    /// Sets the visibility lease applied when SQS returns a source message.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidVisibilityTimeout`] outside SQS's whole-second 1 through
    /// 43,200 second range, or when the existing heartbeat/handler bounds cannot honor it.
    pub fn with_visibility_timeout(
        mut self,
        visibility_timeout: Duration,
    ) -> Result<Self, ConfigError> {
        validate_whole_seconds(visibility_timeout, 1, MAX_VISIBILITY_SECONDS)
            .map_err(|()| ConfigError::InvalidVisibilityTimeout)?;
        self.visibility_timeout = visibility_timeout;
        self.validate_lease_settings()?;
        Ok(self)
    }

    /// Sets how often an active handler extends its SQS visibility lease.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidHeartbeatInterval`] when the interval is zero or when the
    /// interval plus one bounded renewal request does not fit strictly within the configured
    /// visibility timeout.
    pub fn with_heartbeat_interval(
        mut self,
        heartbeat_interval: Duration,
    ) -> Result<Self, ConfigError> {
        self.heartbeat_interval = heartbeat_interval;
        self.validate_lease_settings()?;
        Ok(self)
    }

    /// Sets the maximum one-delivery handler time managed by this worker.
    ///
    /// The value stays below SQS's 12-hour visibility ceiling, leaving room for one final bounded
    /// renewal request and its complete visibility period. A timed-out handler is dropped and
    /// follows the ordinary retry path.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidHandlerTimeout`] when the timeout is zero or cannot fit
    /// within the current SQS visibility-renewal window.
    pub fn with_handler_timeout(mut self, handler_timeout: Duration) -> Result<Self, ConfigError> {
        self.handler_timeout = handler_timeout;
        self.validate_lease_settings()?;
        Ok(self)
    }

    /// Returns the deployment-provisioned source queue.
    #[must_use]
    pub fn source(&self) -> &SqsQueueTarget {
        &self.source
    }

    /// Returns the deployment-provisioned direct dead-letter queue.
    #[must_use]
    pub fn dead_letter(&self) -> &SqsQueueTarget {
        &self.dead_letter
    }

    /// Returns the exact deployment redrive `maxReceiveCount` expected at readiness.
    #[must_use]
    pub const fn expected_redrive_max_receive_count(&self) -> u16 {
        self.expected_redrive_max_receive_count
    }

    /// Returns the maximum time any one SQS request may use.
    #[must_use]
    pub const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    pub(crate) const fn long_poll(&self) -> Duration {
        self.long_poll
    }

    pub(crate) const fn visibility_timeout(&self) -> Duration {
        self.visibility_timeout
    }

    pub(crate) const fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    pub(crate) const fn handler_timeout(&self) -> Duration {
        self.handler_timeout
    }

    fn validate_lease_settings(&self) -> Result<(), ConfigError> {
        validate_request_timeout(self.request_timeout)?;
        if self.request_timeout <= self.long_poll {
            return Err(ConfigError::RequestTimeoutNotLongerThanLongPoll);
        }
        let renewal_deadline = self.heartbeat_interval.checked_add(self.request_timeout);
        if self.heartbeat_interval.is_zero()
            || renewal_deadline.is_none_or(|deadline| deadline >= self.visibility_timeout)
        {
            return Err(ConfigError::InvalidHeartbeatInterval);
        }
        let max_handler_timeout = Duration::from_secs(MAX_VISIBILITY_SECONDS)
            .checked_sub(self.visibility_timeout)
            .and_then(|remaining| remaining.checked_sub(self.request_timeout))
            .unwrap_or_default();
        if self.handler_timeout.is_zero() || self.handler_timeout >= max_handler_timeout {
            return Err(ConfigError::InvalidHandlerTimeout);
        }
        Ok(())
    }
}

impl fmt::Debug for SqsWorkerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqsWorkerConfig")
            .field("source", &self.source)
            .field("dead_letter", &self.dead_letter)
            .field(
                "expected_redrive_max_receive_count",
                &self.expected_redrive_max_receive_count,
            )
            .field("long_poll", &self.long_poll)
            .field("request_timeout", &self.request_timeout)
            .field("visibility_timeout", &self.visibility_timeout)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("handler_timeout", &self.handler_timeout)
            .finish()
    }
}
