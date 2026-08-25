//! Explicit Confluent Schema Registry verification for Rustee event schema artifacts.
//!
//! The adapter is a deployment-time [`rustee_events_schema::EventSchemaRegistry`] implementation.
//! It validates a registry's effective compatibility setting, looks up an exact `JSON` schema
//! artifact, and registers it only when absent. It does not alter Kafka records, headers, topics,
//! consumer offsets, or retry a request. Applications choose when to invoke an
//! [`rustee_events_schema::EventSchemaCatalog`].

mod config;
mod registry;
mod transport;
mod wire;

pub use config::{
    ConfluentSchemaRegistryAuth, ConfluentSchemaRegistryConfig, ConfluentSchemaRegistryConfigError,
};
pub use registry::ConfluentSchemaRegistry;

/// Sanitized Confluent Schema Registry adapter failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConfluentSchemaRegistryError {
    /// The HTTP client could not be constructed.
    #[error("Confluent Schema Registry client initialization failed")]
    Client,
    /// A validated base URL could not construct the requested API path.
    #[error("Confluent Schema Registry endpoint was invalid")]
    InvalidEndpoint,
    /// A request failed or returned an unexpected non-success status.
    #[error("Confluent Schema Registry request failed")]
    Request,
    /// A successful response did not match the expected response shape.
    #[error("Confluent Schema Registry response was malformed")]
    MalformedResponse,
    /// A successful registry JSON response exceeded the configured memory bound.
    #[error("Confluent Schema Registry response exceeded the configured size limit")]
    ResponseTooLarge,
    /// The remote effective compatibility policy did not equal the local schema declaration.
    #[error("Confluent Schema Registry compatibility policy did not match the local declaration")]
    CompatibilityPolicyMismatch,
    /// The registry rejected a schema as incompatible with its subject history.
    #[error("Confluent Schema Registry rejected the schema as incompatible")]
    IncompatibleSchema,
    /// The registry did not accept the requested registration.
    #[error("Confluent Schema Registry rejected the schema registration")]
    RegistrationRejected,
    /// A successful registration could not be looked up again as an exact artifact.
    #[error("Confluent Schema Registry registration was not visible for exact verification")]
    RegistrationNotVisible,
    /// The remote subject, version, format, or source did not equal the local schema artifact.
    #[error("Confluent Schema Registry returned a different schema artifact")]
    ArtifactMismatch,
}

#[cfg(test)]
mod tests;
