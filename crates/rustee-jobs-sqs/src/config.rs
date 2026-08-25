//! Validated deployment settings shared by SQS publisher and worker adapters.

use std::time::Duration;

pub(crate) const MAX_VISIBILITY_SECONDS: u64 = 43_200;
pub(crate) const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(25);

mod target;
mod worker;

pub use target::{SqsQueueKind, SqsQueueTarget};
pub use worker::SqsWorkerConfig;

/// SQS worker configuration validation errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConfigError {
    /// A queue URL was blank, oversized, not path-only HTTP(S), had no host, or embedded
    /// credentials.
    #[error("SQS queue URL must be a bounded path-only absolute HTTP(S) URL without credentials")]
    InvalidQueueUrl,
    /// A FIFO message group was blank, oversized, or used unsupported characters.
    #[error("SQS FIFO message group must use the bounded SQS identifier character set")]
    InvalidFifoMessageGroup,
    /// Source and direct-DLQ queue URLs are the same.
    #[error("SQS source and direct DLQ must differ")]
    DeadLetterMatchesSource,
    /// Source and direct-DLQ queue modes differ.
    #[error("SQS source and direct DLQ must both be Standard or both be FIFO")]
    QueueKindMismatch,
    /// The expected SQS redrive max receive count is outside the SQS range.
    #[error("SQS redrive max receive count must be in 1..=1000")]
    InvalidRedriveReceiveCount,
    /// The long poll is not a whole 1 through 20 seconds.
    #[error("SQS long poll must be a whole 1 through 20 seconds")]
    InvalidLongPoll,
    /// An SQS request deadline was zero.
    #[error("SQS request timeout must be non-zero")]
    ZeroRequestTimeout,
    /// A worker request deadline cannot contain its configured SQS long poll.
    #[error("SQS worker request timeout must be longer than the configured long poll")]
    RequestTimeoutNotLongerThanLongPoll,
    /// The visibility timeout is not a whole 1 through 43,200 seconds.
    #[error("SQS visibility timeout must be a whole 1 through 43,200 seconds")]
    InvalidVisibilityTimeout,
    /// The heartbeat cannot renew the configured visibility lease safely.
    #[error("SQS heartbeat and request timeout must fit strictly within visibility timeout")]
    InvalidHeartbeatInterval,
    /// The handler timeout cannot fit into the SQS visibility renewal window.
    #[error(
        "SQS handler timeout must leave room for a final visibility renewal before twelve hours"
    )]
    InvalidHandlerTimeout,
}

pub(crate) fn validate_request_timeout(request_timeout: Duration) -> Result<(), ConfigError> {
    if request_timeout.is_zero() {
        Err(ConfigError::ZeroRequestTimeout)
    } else {
        Ok(())
    }
}

pub(crate) fn validate_whole_seconds(
    value: Duration,
    minimum_seconds: u64,
    maximum_seconds: u64,
) -> Result<u32, ()> {
    if value.subsec_nanos() != 0
        || value.as_secs() < minimum_seconds
        || value.as_secs() > maximum_seconds
    {
        return Err(());
    }
    u32::try_from(value.as_secs()).map_err(|_| ())
}

pub(crate) fn duration_seconds(value: Duration) -> Result<i32, ()> {
    let seconds = validate_whole_seconds(value, 0, MAX_VISIBILITY_SECONDS)?;
    i32::try_from(seconds).map_err(|_| ())
}
