//! `MongoDB` client lifecycle, transaction, change-stream, and tenant BSON-boundary helpers.
//!
//! This crate intentionally leaves collection queries, BSON persistence models, and HTTP DTOs in
//! application code. It configures one long-lived official driver client per process.

pub use mongodb;
pub use rustee_tenant::TenantContext;

mod change_stream;
mod client;
mod tenant_scope;

pub use change_stream::{
    ChangeStreamCheckpointStore, ChangeStreamConsumer, ChangeStreamConsumerError, ChangeStreamNext,
    next_change_until,
};
pub use client::{
    ConfigError, MongoConfig, MongoConnectError, MongoReadinessError, begin_transaction,
    begin_transaction_with_options, connect, database, readiness, shutdown,
};
pub use tenant_scope::{MONGO_TENANT_FIELD, MongoTenantScope, TenantScopeError};

#[cfg(test)]
mod tests;
