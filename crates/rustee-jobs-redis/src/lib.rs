//! Redis Streams publishing and consumer-group delivery for `Rustee` jobs.
//!
//! Streams, consumer groups, retention, ACLs, and dead-letter streams are deployment-owned. A
//! worker verifies that its configured consumer group already exists; it never provisions it.
//! Retry records use a provider-private sorted set and hashes so the requested retry delay survives
//! worker restart. The configured reclaim idle time applies only to deliveries abandoned by a
//! worker before it could settle them.

pub use rustee_redis::redis;

mod config;
mod delivery;
mod operation;
mod publisher;
mod worker;

pub use config::{ConfigError, RedisStreamsWorkerConfig};
pub use delivery::RedisStreamsDelivery;
pub use publisher::RedisStreamsPublisher;
pub use worker::RedisStreamsWorker;

pub(crate) const PAYLOAD_FIELD: &str = "payload";
pub(crate) const ATTEMPT_FIELD: &str = "attempt";

/// Sanitized operational failures from the Redis Streams provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RedisStreamsError {
    /// Redis did not accept a durable stream append.
    #[error("Redis Streams job publish failed")]
    Publish,
    /// Redis could not inspect a configured source or dead-letter stream.
    #[error("Redis Streams job readiness check failed")]
    Readiness,
    /// The deployment did not pre-provision the configured consumer group.
    #[error("Redis Streams job consumer group is not configured")]
    ConsumerGroup,
    /// A consumer-group read failed.
    #[error("Redis Streams job receive failed")]
    Receive,
    /// Pending recovery or its delivery-count inspection failed.
    #[error("Redis Streams pending job recovery failed")]
    Reclaim,
    /// Redis reported a pending record whose stream entry had been removed by retention or trim.
    #[error("Redis Streams claimed job entry was missing")]
    ClaimedEntryMissing,
    /// A message omitted required provider metadata or had an unrepresentable cumulative attempt.
    #[error("Redis Streams job delivery metadata was invalid")]
    DeliveryMetadata,
    /// A consumer lost PEL ownership before it could settle its selected delivery.
    #[error("Redis Streams job delivery ownership was lost")]
    DeliveryOwnershipLost,
    /// Redis could not atomically acknowledge one successful delivery.
    #[error("Redis Streams job acknowledgement failed")]
    Acknowledge,
    /// Redis could not atomically persist a delayed retry and settle its source delivery.
    #[error("Redis Streams job retry scheduling failed")]
    RetrySchedule,
    /// Redis could not atomically promote due retries to the source stream.
    #[error("Redis Streams job retry promotion failed")]
    RetryPromotion,
    /// The requested retry budget or delay range is not a usable Rustee retry policy.
    #[error("Redis Streams retry policy is invalid")]
    RetryPolicy,
    /// Redis could not atomically write a dead-letter entry and settle its source delivery.
    #[error("Redis Streams job dead-letter publish failed")]
    DeadLetter,
    /// A worker task panicked or was cancelled before settling its delivery.
    #[error("Redis Streams job worker task failed")]
    WorkerTask,
    /// Active handlers did not finish before the configured shutdown drain deadline.
    #[error("Redis Streams job worker drain timed out")]
    DrainTimeout,
}

#[cfg(test)]
mod tests;
