//! Explicit `SQLx` pool, readiness, and migration helpers.
//!
//! Enable the `sqlite` feature to expose `SQLite` pool connection and readiness helpers.
//!
//! Migrations belong in a deployment job, not application startup. This crate exposes a runner
//! for that job but does not call it from Rustee's HTTP server.

mod migration;
mod pool;
mod tenant;

pub use migration::run_migrations;
pub use pool::{
    ConnectError, DatabaseReadinessError, PoolConfig, PoolConfigError, connect, readiness,
};
#[cfg(feature = "sqlite")]
pub use pool::{connect_sqlite, sqlite_readiness};
pub use rustee_tenant::TenantContext;
pub use sqlx::{
    Error, PgPool, Postgres, Transaction,
    migrate::{MigrateError, Migrator},
    postgres::{PgConnectOptions, PgPoolOptions},
};
#[cfg(feature = "sqlite")]
pub use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
pub use tenant::{POSTGRES_TENANT_SETTING, begin_tenant_transaction};

#[cfg(test)]
mod tests;
