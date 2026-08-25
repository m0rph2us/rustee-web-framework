//! Read-only `RabbitMQ` management API audit for `Rustee` quorum job topology.
//!
//! This crate is deliberately separate from the AMQP worker. It uses management credentials only
//! to fetch one queue snapshot, then validates the effective queue type, durability, delayed
//! retry, delivery limit, and DLX route expected by a
//! [`rustee_jobs_rabbitmq::RabbitMqWorkerConfig`]. It never creates or mutates `RabbitMQ`
//! topology.

mod audit;
mod config;
mod snapshot;
mod transport;

pub use audit::{RabbitMqTopologyAuditor, RabbitMqTopologyReport};
pub use config::{RabbitMqManagementConfig, RabbitMqManagementConfigError};
pub use snapshot::QueueSnapshot;

/// Sanitized failures from the read-only management audit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RabbitMqManagementError {
    /// The bounded HTTP client could not be constructed.
    #[error("RabbitMQ management client initialization failed")]
    Client,
    /// The configured management endpoint was not safe or usable.
    #[error("RabbitMQ management endpoint was invalid")]
    InvalidEndpoint,
    /// The management endpoint request did not complete successfully.
    #[error("RabbitMQ management request failed")]
    Request,
    /// The expected worker queue did not exist at the endpoint.
    #[error("RabbitMQ management queue was not found")]
    QueueNotFound,
    /// The management API response did not match the bounded snapshot contract.
    #[error("RabbitMQ management response was malformed")]
    MalformedResponse,
    /// The management API response exceeded the configured in-memory size limit.
    #[error("RabbitMQ management response exceeded the configured size limit")]
    ResponseTooLarge,
    /// The observed queue did not satisfy the Rustee worker topology contract.
    #[error("RabbitMQ queue topology does not match the Rustee worker contract")]
    TopologyMismatch,
}

#[cfg(test)]
mod tests;
