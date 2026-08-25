use std::{error::Error as StdError, io, time::Duration};

use crate::pool::validate_readiness_timeout;
use crate::{ConnectError, DatabaseReadinessError, Error, PoolConfig, PoolConfigError};

#[test]
fn default_pool_limits_are_finite() {
    let config = PoolConfig::default();
    assert_eq!(config.max_connections, 20);
    assert_eq!(config.min_connections, 0);
    assert_eq!(config.acquire_timeout, Duration::from_secs(10));
    assert_eq!(config.connect_timeout, Duration::from_secs(5));
}

#[test]
fn pool_configuration_rejects_non_operational_values() {
    let defaults = PoolConfig::default();
    assert_eq!(
        PoolConfig {
            max_connections: 0,
            ..defaults
        }
        .validate(),
        Err(PoolConfigError::ZeroMaxConnections)
    );
    assert_eq!(
        PoolConfig {
            min_connections: defaults.max_connections + 1,
            ..defaults
        }
        .validate(),
        Err(PoolConfigError::MinimumExceedsMaximum)
    );
    assert_eq!(
        PoolConfig {
            acquire_timeout: Duration::ZERO,
            ..defaults
        }
        .validate(),
        Err(PoolConfigError::ZeroAcquireTimeout)
    );
    assert_eq!(
        PoolConfig {
            connect_timeout: Duration::ZERO,
            ..defaults
        }
        .validate(),
        Err(PoolConfigError::ZeroConnectTimeout)
    );
}

#[tokio::test]
async fn postgres_connect_rejects_invalid_configuration_before_connecting() {
    let error = crate::connect(
        "postgres://private-user:private-password@private-host/private-db",
        PoolConfig {
            max_connections: 0,
            ..PoolConfig::default()
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        ConnectError::InvalidConfig(PoolConfigError::ZeroMaxConnections)
    ));
}

#[test]
fn connection_error_diagnostics_redact_driver_details_and_preserve_the_source() {
    let error = ConnectError::Sqlx(Error::Configuration(Box::new(io::Error::other(
        "postgres://private-user:private-password@private-host/private-db",
    ))));

    assert_eq!(error.to_string(), "database connection failed");
    assert!(!format!("{error:?}").contains("private-password"));
    assert!(StdError::source(&error).is_some());
}

#[test]
fn readiness_requires_a_positive_timeout() {
    assert!(matches!(
        validate_readiness_timeout(Duration::ZERO),
        Err(DatabaseReadinessError::ZeroTimeout)
    ));
}

#[test]
fn readiness_error_diagnostics_redact_driver_details_and_preserve_the_source() {
    let error = DatabaseReadinessError::Sqlx(Error::Configuration(Box::new(io::Error::other(
        "postgres://private-user:private-password@private-host/private-db",
    ))));

    assert_eq!(error.to_string(), "database readiness failed");
    assert!(!format!("{error:?}").contains("private-password"));
    assert!(StdError::source(&error).is_some());
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_feature_connects_and_executes_the_readiness_query() {
    let config = PoolConfig {
        max_connections: 1,
        ..PoolConfig::default()
    };
    let pool = crate::connect_sqlite("sqlite::memory:", config)
        .await
        .unwrap();

    crate::sqlite_readiness(&pool, Duration::from_secs(1))
        .await
        .unwrap();
    let answer: i64 = sqlx::query_scalar("SELECT 40 + 2")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(answer, 42);

    pool.close().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_connect_rejects_invalid_configuration_before_connecting() {
    let error = crate::connect_sqlite(
        "sqlite::memory:",
        PoolConfig {
            max_connections: 0,
            ..PoolConfig::default()
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        ConnectError::InvalidConfig(PoolConfigError::ZeroMaxConnections)
    ));
}
