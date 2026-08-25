//! `RabbitMQ` quorum-queue publishing and delivery for `Rustee` jobs.
//!
//! Quorum queues, direct exchanges, dead-letter exchanges, bindings, delivery limits, and native
//! delayed-retry policies are deployment-owned. The adapter uses passive checks at readiness and
//! never creates or mutates that topology. A retry rejects and requeues the original delivery so
//! `RabbitMQ` 4.3's quorum-queue delayed retry retains it durably. Poison messages and exhausted
//! retries are publisher-confirmed on the explicit dead-letter route before their source delivery
//! is acknowledged. Both paths intentionally retain at-least-once semantics.

pub use lapin;

mod connection;
mod delivery;
mod publisher;
mod topology;
mod worker;

pub use connection::{RabbitMqConnection, RabbitMqConnectionConfig};
pub use delivery::RabbitMqDelivery;
pub use publisher::RabbitMqPublisher;
pub use topology::{
    ConfigError, RabbitMqNativeRetryConfig, RabbitMqPublisherConfig, RabbitMqWorkerConfig,
};
pub use worker::RabbitMqWorker;

/// Sanitized operational failures from the `RabbitMQ` adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RabbitMqError {
    /// The configured AMQP(S) connection URL was invalid.
    #[error("RabbitMQ connection configuration is invalid")]
    InvalidConnectionConfig,
    /// AMQP connection setup failed.
    #[error("RabbitMQ connection failed")]
    Connect,
    /// A pre-provisioned queue or exchange could not be inspected.
    #[error("RabbitMQ job topology readiness check failed")]
    Readiness,
    /// The caller supplied a zero topology readiness deadline.
    #[error("RabbitMQ topology readiness timeout must be non-zero")]
    InvalidReadinessTimeout,
    /// A topology readiness check did not finish before its caller-supplied deadline.
    #[error("RabbitMQ job topology readiness check timed out")]
    ReadinessTimeout,
    /// A dedicated publisher-confirm channel could not be opened.
    #[error("RabbitMQ publisher channel setup failed")]
    PublisherChannel,
    /// A consumer channel could not be opened or closed.
    #[error("RabbitMQ consumer channel operation failed")]
    ConsumerChannel,
    /// The worker concurrency cannot be represented by the configured quorum-queue prefetch limit.
    #[error("RabbitMQ worker concurrency is incompatible with quorum-queue prefetch")]
    WorkerConfiguration,
    /// The broker did not establish or continue a manual-ack consumer stream.
    #[error("RabbitMQ job receive failed")]
    Receive,
    /// The broker did not accept cancellation of the consumer during shutdown.
    #[error("RabbitMQ consumer cancellation failed")]
    ConsumerCancel,
    /// The source job publish request could not be sent.
    #[error("RabbitMQ job publish failed")]
    Publish,
    /// The source job publish was not publisher-confirmed.
    #[error("RabbitMQ job publish confirmation failed")]
    PublishConfirmation,
    /// The source job publish received a broker negative acknowledgement.
    #[error("RabbitMQ job publish was negatively acknowledged")]
    PublishNack,
    /// The source job publish was mandatory but had no matching route.
    #[error("RabbitMQ job publish was unroutable")]
    PublishUnroutable,
    /// The source job publish confirmation exceeded its configured timeout.
    #[error("RabbitMQ job publish confirmation timed out")]
    PublishTimeout,
    /// The dead-letter publish request could not be sent.
    #[error("RabbitMQ dead-letter publish failed")]
    DeadLetterPublish,
    /// The dead-letter publish was not publisher-confirmed.
    #[error("RabbitMQ dead-letter confirmation failed")]
    DeadLetterConfirmation,
    /// The dead-letter publish received a broker negative acknowledgement.
    #[error("RabbitMQ dead-letter publish was negatively acknowledged")]
    DeadLetterNack,
    /// The dead-letter publish was mandatory but had no matching route.
    #[error("RabbitMQ dead-letter publish was unroutable")]
    DeadLetterUnroutable,
    /// The dead-letter confirmation exceeded its configured timeout.
    #[error("RabbitMQ dead-letter confirmation timed out")]
    DeadLetterTimeout,
    /// `RabbitMQ` could not accept an acknowledgement for the original delivery.
    #[error("RabbitMQ delivery acknowledgement failed")]
    Acknowledge,
    /// `RabbitMQ` could not return the source delivery to the native delayed-retry queue state.
    #[error("RabbitMQ delayed retry return failed")]
    RetryReturn,
    /// The native broker retry policy does not exactly match the requested Rustee retry policy.
    #[error("RabbitMQ native delayed retry policy is incompatible with the Rustee retry policy")]
    RetryPolicyMismatch,
    /// The broker acquired-delivery header was zero or used an unsupported AMQP value type.
    #[error("RabbitMQ delivery attempt metadata was invalid")]
    DeliveryMetadata,
    /// A worker task panicked or was cancelled before choosing an acknowledgement action.
    #[error("RabbitMQ worker task failed")]
    WorkerTask,
    /// Active handlers did not finish before the worker drain deadline.
    #[error("RabbitMQ worker drain timed out")]
    DrainTimeout,
}

#[cfg(test)]
mod tests;
