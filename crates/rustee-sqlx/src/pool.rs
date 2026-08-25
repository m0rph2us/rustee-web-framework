use std::{fmt, time::Duration};

use sqlx::{
    Database, Error, PgPool,
    pool::PoolOptions,
    postgres::{PgConnectOptions, PgPoolOptions},
};

/// Invalid explicit `SQLx` pool configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PoolConfigError {
    /// A pool without any connection capacity cannot satisfy readiness or application requests.
    #[error("pool max_connections must be greater than zero")]
    ZeroMaxConnections,
    /// `SQLx` would silently clamp this relationship, which can hide an operator configuration error.
    #[error("pool min_connections cannot exceed max_connections")]
    MinimumExceedsMaximum,
    /// A zero deadline turns normal scheduling into an immediate pool-acquisition failure.
    #[error("pool acquire_timeout must be greater than zero")]
    ZeroAcquireTimeout,
    /// A zero deadline turns normal scheduling into an immediate initial-connection failure.
    #[error("pool connect_timeout must be greater than zero")]
    ZeroConnectTimeout,
}

/// Failure while creating the initial database pool connection.
///
/// Display and debug output retain only a safe failure category. The driver source remains
/// available through [`std::error::Error::source`] for trusted startup diagnostics.
#[derive(thiserror::Error)]
pub enum ConnectError {
    /// The supplied pool limits cannot represent an operational connection pool.
    #[error("database pool configuration is invalid: {0}")]
    InvalidConfig(#[from] PoolConfigError),
    /// `SQLx` rejected the URL or could not establish a pool connection.
    #[error("database connection failed")]
    Sqlx(#[from] Error),
    /// The initial pool connection exceeded the configured deadline.
    #[error("database connection timed out after {0:?}")]
    Timeout(Duration),
}

impl fmt::Debug for ConnectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::InvalidConfig(_) => "invalid_configuration",
            Self::Sqlx(_) => "connection_failed",
            Self::Timeout(_) => "timeout",
        };
        formatter
            .debug_struct("ConnectError")
            .field("kind", &kind)
            .finish()
    }
}

/// Failure while executing a database readiness query.
///
/// Display and debug output retain only a safe failure category. The driver source remains
/// available through [`std::error::Error::source`] for trusted diagnostics.
#[derive(thiserror::Error)]
pub enum DatabaseReadinessError {
    /// A zero deadline cannot represent an operational readiness check.
    #[error("database readiness timeout must be greater than zero")]
    ZeroTimeout,
    /// The readiness query did not complete before the caller-supplied deadline.
    #[error("database readiness timed out after {0:?}")]
    Timeout(Duration),
    /// `SQLx` could not acquire a connection or execute the readiness query.
    #[error("database readiness failed")]
    Sqlx(#[from] Error),
}

impl fmt::Debug for DatabaseReadinessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::ZeroTimeout => "zero_timeout",
            Self::Timeout(_) => "timeout",
            Self::Sqlx(_) => "readiness_failed",
        };
        formatter
            .debug_struct("DatabaseReadinessError")
            .field("kind", &kind)
            .finish()
    }
}

pub(crate) fn validate_readiness_timeout(timeout: Duration) -> Result<(), DatabaseReadinessError> {
    if timeout.is_zero() {
        return Err(DatabaseReadinessError::ZeroTimeout);
    }
    Ok(())
}

/// `SQLx` pool settings with finite operational defaults.
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
    /// Validates relationships and deadlines that `SQLx` does not reject at configuration time.
    ///
    /// # Errors
    ///
    /// Returns an error when connection capacity or either deadline is zero, or when the configured
    /// minimum idle connections exceed the maximum connection capacity.
    pub const fn validate(self) -> Result<(), PoolConfigError> {
        if self.max_connections == 0 {
            return Err(PoolConfigError::ZeroMaxConnections);
        }
        if self.min_connections > self.max_connections {
            return Err(PoolConfigError::MinimumExceedsMaximum);
        }
        if self.acquire_timeout.is_zero() {
            return Err(PoolConfigError::ZeroAcquireTimeout);
        }
        if self.connect_timeout.is_zero() {
            return Err(PoolConfigError::ZeroConnectTimeout);
        }
        Ok(())
    }

    /// Applies this configuration to an `SQLx` pool builder.
    ///
    /// Call [`Self::validate`] first when applying application-supplied configuration directly.
    #[must_use]
    pub fn apply<DB>(self, options: PoolOptions<DB>) -> PoolOptions<DB>
    where
        DB: Database,
    {
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
    config.validate()?;
    let options = database_url.parse::<PgConnectOptions>()?;
    tokio::time::timeout(
        config.connect_timeout,
        config.apply(PgPoolOptions::new()).connect_with(options),
    )
    .await
    .map_err(|_| ConnectError::Timeout(config.connect_timeout))?
    .map_err(Into::into)
}

/// Executes a `PostgreSQL` query used by a readiness endpoint within a caller-supplied deadline.
///
/// # Errors
///
/// Returns an error when the deadline is zero, the query exceeds the deadline, the pool cannot
/// acquire a connection, or `PostgreSQL` rejects the query.
pub async fn readiness(pool: &PgPool, timeout: Duration) -> Result<(), DatabaseReadinessError> {
    validate_readiness_timeout(timeout)?;
    tokio::time::timeout(timeout, sqlx::query("SELECT 1").execute(pool))
        .await
        .map_err(|_| DatabaseReadinessError::Timeout(timeout))?
        .map(|_| ())
        .map_err(Into::into)
}

/// Creates a `SQLite` pool with the same explicit connection limits as `PostgreSQL`.
///
/// This candidate adapter is available only with the `sqlite` feature. It does not provide
/// `PostgreSQL` tenant-RLS or outbox behavior.
///
/// # Errors
///
/// Returns an error when the URL is invalid, a connection cannot be established, or the initial
/// connection exceeds `PoolConfig::connect_timeout`.
#[cfg(feature = "sqlite")]
pub async fn connect_sqlite(
    database_url: &str,
    config: PoolConfig,
) -> Result<sqlx::SqlitePool, ConnectError> {
    config.validate()?;
    let options = database_url.parse::<sqlx::sqlite::SqliteConnectOptions>()?;
    tokio::time::timeout(
        config.connect_timeout,
        config
            .apply(sqlx::sqlite::SqlitePoolOptions::new())
            .connect_with(options),
    )
    .await
    .map_err(|_| ConnectError::Timeout(config.connect_timeout))?
    .map_err(Into::into)
}

/// Executes a `SQLite` query used by a readiness endpoint within a caller-supplied deadline.
///
/// # Errors
///
/// Returns an error when the deadline is zero, the query exceeds the deadline, the pool cannot
/// acquire a connection, or `SQLite` rejects the query.
#[cfg(feature = "sqlite")]
pub async fn sqlite_readiness(
    pool: &sqlx::SqlitePool,
    timeout: Duration,
) -> Result<(), DatabaseReadinessError> {
    validate_readiness_timeout(timeout)?;
    tokio::time::timeout(timeout, sqlx::query("SELECT 1").execute(pool))
        .await
        .map_err(|_| DatabaseReadinessError::Timeout(timeout))?
        .map(|_| ())
        .map_err(Into::into)
}
