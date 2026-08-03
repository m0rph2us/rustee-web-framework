//! Explicit `SQLx` pool, readiness, and migration helpers.
//!
//! Migrations belong in a deployment job, not application startup. This crate exposes a runner
//! for that job but does not call it from Rustee's HTTP server.

use std::time::Duration;

pub use rustee_tenant::TenantContext;
pub use sqlx::{
    Error, PgPool, Postgres, Transaction,
    migrate::{MigrateError, Migrator},
    postgres::{PgConnectOptions, PgPoolOptions},
};

/// `PostgreSQL` custom setting read by tenant row-level-security policies.
pub const POSTGRES_TENANT_SETTING: &str = "rustee.tenant_id";

/// Failure while creating the initial `PostgreSQL` pool connection.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// `SQLx` rejected the URL or could not establish a pool connection.
    #[error(transparent)]
    Sqlx(#[from] Error),
    /// The initial pool connection exceeded the configured deadline.
    #[error("database connection timed out after {0:?}")]
    Timeout(Duration),
}

/// `PostgreSQL` pool settings with finite operational defaults.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PoolConfig {
    /// Maximum live connections held by this process.
    pub max_connections: u32,
    /// Minimum idle connections maintained by this process.
    pub min_connections: u32,
    /// Maximum time spent waiting for a pool connection.
    pub acquire_timeout: Duration,
    /// Maximum time spent establishing an individual database connection.
    pub connect_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 20,
            min_connections: 0,
            acquire_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(5),
        }
    }
}

impl PoolConfig {
    /// Applies this configuration to a `PostgreSQL` `SQLx` pool builder.
    #[must_use]
    pub fn apply(self, options: PgPoolOptions) -> PgPoolOptions {
        options
            .max_connections(self.max_connections)
            .min_connections(self.min_connections)
            .acquire_timeout(self.acquire_timeout)
    }
}

/// Creates a `PostgreSQL` pool with explicit connection limits.
///
/// # Errors
///
/// Returns an error when the URL is invalid, a connection cannot be established, or the initial
/// connection exceeds `PoolConfig::connect_timeout`.
pub async fn connect(database_url: &str, config: PoolConfig) -> Result<PgPool, ConnectError> {
    let options = database_url.parse::<PgConnectOptions>()?;
    tokio::time::timeout(
        config.connect_timeout,
        config.apply(PgPoolOptions::new()).connect_with(options),
    )
    .await
    .map_err(|_| ConnectError::Timeout(config.connect_timeout))?
    .map_err(Into::into)
}

/// Executes a bounded query used by a readiness endpoint.
///
/// # Errors
///
/// Returns the driver error when the pool cannot acquire a connection or `PostgreSQL` rejects the
/// query.
pub async fn readiness(pool: &PgPool) -> Result<(), Error> {
    sqlx::query("SELECT 1").execute(pool).await.map(|_| ())
}

/// Begins a `PostgreSQL` transaction scoped to one trusted tenant.
///
/// The helper sets [`POSTGRES_TENANT_SETTING`] with `PostgreSQL`'s transaction-local
/// `set_config(..., true)`. Tables must have an application-owned row-level-security policy that
/// compares their tenant column to `current_setting('rustee.tenant_id', true)`; Rustee never
/// enables or migrates RLS policies automatically. The setting is cleared on commit or rollback.
///
/// # Errors
///
/// Returns the driver error when the transaction cannot begin or `PostgreSQL` rejects the scoped
/// setting.
pub async fn begin_tenant_transaction<'a>(
    pool: &'a PgPool,
    tenant: &TenantContext,
) -> Result<Transaction<'a, Postgres>, Error> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config($1, $2, true)")
        .bind(POSTGRES_TENANT_SETTING)
        .bind(tenant.tenant())
        .execute(&mut *transaction)
        .await?;
    Ok(transaction)
}

/// Runs migrations from a deployment-controlled [`Migrator`].
///
/// # Errors
///
/// Returns a migration error when metadata cannot be read, a migration fails, or migration lock
/// acquisition fails. Do not invoke this from HTTP application startup.
pub async fn run_migrations(migrator: &Migrator, pool: &PgPool) -> Result<(), MigrateError> {
    migrator.run(pool).await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::PoolConfig;

    #[test]
    fn default_pool_limits_are_finite() {
        let config = PoolConfig::default();
        assert_eq!(config.max_connections, 20);
        assert_eq!(config.min_connections, 0);
        assert_eq!(config.acquire_timeout, Duration::from_secs(10));
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
    }
}
