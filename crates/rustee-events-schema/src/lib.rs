//! Provider-neutral `JSON Schema` declarations for Rustee versioned events.
//!
//! A schema catalog is an application startup/release artifact, not a Kafka publish path. The
//! application chooses the registry implementation, compatibility check, topic mapping, rollout
//! order, and consumer fleet evidence. This crate deliberately does not publish schemas, inject
//! broker headers, mutate topics, or infer JSON compatibility from two schema documents.

mod catalog;
mod model;

pub use catalog::{
    EventSchemaCatalog, EventSchemaRegistry, RegisteredEventSchema, SchemaCatalogError,
    SchemaVerificationError,
};
pub use model::{
    EventSchema, SchemaCompatibility, SchemaConfigError, SchemaFingerprint, SchemaFormat,
    SchemaSubject,
};

#[cfg(test)]
mod tests;
