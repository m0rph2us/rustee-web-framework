//! NATS `JetStream` publishing and delivery acknowledgement helpers for `Rustee` jobs.
//!
//! Streams, durable consumers, and dead-letter subjects are deployment-owned infrastructure. This
//! crate never creates or mutates them during application or worker startup.

pub use async_nats;

mod config;
mod delivery;
mod publisher;
mod worker;

pub use config::{ConfigError, NatsConfig};
pub use delivery::JetStreamDelivery;
pub use publisher::JetStreamPublisher;
pub use worker::JetStreamWorker;

/// Sanitized operational failures from the NATS adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum NatsError {
    /// NATS connection setup failed.
    #[error("NATS connection failed")]
    Connect,
    /// `JetStream` publish request failed.
    #[error("NATS JetStream publish failed")]
    Publish,
    /// `JetStream` did not acknowledge the publish request.
    #[error("NATS JetStream publish acknowledgement failed")]
    PublishAcknowledgement,
    /// `JetStream` account readiness query failed.
    #[error("NATS JetStream readiness check failed")]
    Readiness,
    /// NATS did not accept a successful-delivery acknowledgement.
    #[error("NATS JetStream acknowledgement failed")]
    Acknowledge,
    /// NATS did not accept a retry negative acknowledgement.
    #[error("NATS JetStream negative acknowledgement failed")]
    NegativeAcknowledge,
    /// A consumed message did not contain valid `JetStream` delivery metadata.
    #[error("NATS JetStream delivery metadata was invalid")]
    DeliveryMetadata,
    /// Receiving a pull-consumer message failed or the delivery stream ended unexpectedly.
    #[error("NATS JetStream job receive failed")]
    Receive,
    /// A dead-letter publish request failed before `JetStream` accepted it.
    #[error("NATS JetStream dead-letter publish failed")]
    DeadLetterPublish,
    /// `JetStream` did not acknowledge a dead-letter publish.
    #[error("NATS JetStream dead-letter publish acknowledgement failed")]
    DeadLetterPublishAcknowledgement,
    /// The supplied consumer limits cannot satisfy the Rustee worker configuration.
    #[error("NATS JetStream consumer configuration is incompatible with the Rustee worker")]
    ConsumerConfiguration,
    /// The requested retry budget or delay range is not a usable Rustee retry policy.
    #[error("NATS JetStream retry policy is invalid")]
    RetryPolicy,
    /// A worker task panicked or was cancelled before completing its acknowledgement decision.
    #[error("NATS JetStream worker task failed")]
    WorkerTask,
    /// Active handlers did not finish before the configured shutdown drain deadline.
    #[error("NATS JetStream worker drain timed out")]
    DrainTimeout,
}

#[cfg(test)]
mod tests;
