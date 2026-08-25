//! Amazon SQS publishing and visibility-lease delivery for `Rustee` jobs.
//!
//! Queue creation, queue type, redrive policy, IAM, encryption, and retention are deployment
//! owned. This adapter verifies those settings at readiness and never mutates them. It keeps the
//! SQS acknowledgement model explicit: a successful handler deletes its receipt, a retry changes
//! visibility, and a poison or exhausted delivery is sent to the configured DLQ before the source
//! receipt is deleted. All three paths retain at-least-once semantics.

pub use aws_sdk_sqs;

mod config;
mod delivery;
mod publisher;
mod readiness;
mod worker;

pub use config::{ConfigError, SqsQueueKind, SqsQueueTarget, SqsWorkerConfig};
pub use delivery::SqsDelivery;
pub use publisher::SqsPublisher;
pub use worker::SqsWorker;

/// Sanitized Amazon SQS provider failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SqsError {
    /// The publisher could not obtain a successful SQS send response.
    #[error("SQS job publish failed")]
    Publish,
    /// A `JobMessage` body was not valid UTF-8 for SQS text transport.
    #[error("SQS job message body was not valid UTF-8")]
    InvalidMessageBody,
    /// A read-only queue inspection could not complete.
    #[error("SQS readiness check failed")]
    Readiness,
    /// A configured Standard/FIFO mode differed from the actual queue attribute.
    #[error("SQS configured queue type did not match deployment")]
    QueueType,
    /// The source queue redrive policy did not match the configured direct DLQ route.
    #[error("SQS redrive policy did not match worker configuration")]
    RedrivePolicy,
    /// A long-poll receive request failed.
    #[error("SQS receive failed")]
    Receive,
    /// An SQS delivery omitted or malformed body, receipt, message ID, or receive-count metadata.
    #[error("SQS delivery metadata was invalid")]
    DeliveryMetadata,
    /// A visibility heartbeat request failed; the receipt is deliberately left unsettled.
    #[error("SQS visibility lease renewal failed")]
    VisibilityLease,
    /// Retry visibility could not be changed; the receipt is deliberately left unsettled.
    #[error("SQS retry visibility update failed")]
    RetryVisibility,
    /// Direct DLQ send failed; the source receipt is deliberately left unsettled.
    #[error("SQS direct dead-letter publish failed")]
    DeadLetterPublish,
    /// A completed source receipt could not be deleted.
    #[error("SQS source receipt delete failed")]
    Delete,
    /// The core retry policy cannot be represented as bounded whole-second SQS visibility values.
    #[error("SQS retry policy is incompatible with visibility timeout semantics")]
    RetryPolicyMismatch,
    /// An internal worker task panicked or was cancelled unexpectedly.
    #[error("SQS worker task ended unexpectedly")]
    WorkerTask,
    /// Active tasks did not settle during graceful shutdown and were aborted without deletion.
    #[error("SQS worker drain timed out")]
    DrainTimeout,
}

#[cfg(test)]
mod tests;
