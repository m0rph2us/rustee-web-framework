//! Stable `PostgreSQL` API-key store facade, readiness, and sanitized diagnostics.

use std::{fmt, time::Duration};

use futures_util::future::BoxFuture;
use rustee_auth::{ApiKeyError, ApiKeyFingerprint, ApiKeyFingerprintStore, Principal};
use sqlx::PgPool;

mod authentication;
mod registration;
mod rotation;

#[cfg(test)]
pub(super) use registration::system_time_unix_seconds;

/// Deployment-owned migration for the keyed API-key credential and success-audit records.
pub const API_KEY_STORE_MIGRATION_SQL: &str =
    include_str!("../migrations/0001_rustee_api_key_store.sql");

/// Forward-only migration that enforces the durable serialized-`Principal` byte bound.
///
/// Apply this after [`API_KEY_STORE_MIGRATION_SQL`]. It rejects the migration when an existing
/// principal exceeds [`crate::MAX_SERIALIZED_PRINCIPAL_BYTES`], so the deployment must repair that
/// invalid row explicitly rather than allow an authentication-time unbounded read.
pub const API_KEY_STORE_PRINCIPAL_BOUND_MIGRATION_SQL: &str =
    include_str!("../migrations/0002_rustee_api_key_principal_bound.sql");

/// Durable `PostgreSQL` implementation of [`ApiKeyFingerprintStore`].
///
/// The store updates `last_used_*` and appends one success audit row in the same transaction that
/// maps a currently active, unexpired fingerprint to its principal. Rejected fingerprints do not
/// create an audit row because they do not identify a registered credential.
#[derive(Clone)]
pub struct PostgresApiKeyStore {
    pool: PgPool,
}

impl PostgresApiKeyStore {
    /// Creates a keyed API-key store from an application-owned `PostgreSQL` pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Executes a database query within an application-supplied readiness deadline.
    ///
    /// API-key authentication must fail closed when its durable identity store is unavailable.
    /// A zero timeout is rejected before a query starts.
    ///
    /// # Errors
    ///
    /// Returns a sanitized storage error when `PostgreSQL` cannot complete the query, or a
    /// distinct timeout or validation error.
    pub async fn readiness(&self, timeout: Duration) -> Result<(), PostgresApiKeyStoreError> {
        if timeout.is_zero() {
            return Err(PostgresApiKeyStoreError::InvalidReadinessTimeout);
        }
        tokio::time::timeout(timeout, sqlx::query("SELECT 1").execute(&self.pool))
            .await
            .map_err(|_| PostgresApiKeyStoreError::ReadinessTimedOut(timeout))?
            .map(|_| ())
            .map_err(PostgresApiKeyStoreError::storage)
    }
}

impl fmt::Debug for PostgresApiKeyStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresApiKeyStore")
            .field("pool", &"[REDACTED]")
            .finish()
    }
}

impl ApiKeyFingerprintStore for PostgresApiKeyStore {
    fn authenticate(
        &self,
        fingerprint: ApiKeyFingerprint,
    ) -> BoxFuture<'static, Result<Principal, ApiKeyError>> {
        let store = self.clone();
        Box::pin(async move { store.authenticate_fingerprint(fingerprint).await })
    }
}

/// Failure while registering, rotating, or revoking durable API-key credentials.
#[derive(thiserror::Error)]
pub enum PostgresApiKeyStoreError {
    /// A fingerprint was already associated with a different record and was not overwritten.
    #[error("API-key fingerprint is already registered")]
    DuplicateFingerprint,
    /// A client-key rotation referenced no currently active, unexpired credential.
    #[error("API-key rotation requires an active, unexpired previous record")]
    MissingActiveRecord,
    /// The supplied expiry cannot be represented as a non-negative Unix timestamp.
    #[error("API-key expiry is invalid")]
    InvalidExpiry,
    /// Principal serialization failed or exceeded the bounded durable representation.
    #[error("API-key principal metadata is invalid")]
    InvalidPrincipal,
    /// A readiness check was requested with no deadline.
    #[error("API-key store readiness timeout must be greater than zero")]
    InvalidReadinessTimeout,
    /// The readiness query did not complete before its deadline.
    #[error("API-key store readiness timed out after {0:?}")]
    ReadinessTimedOut(Duration),
    /// `PostgreSQL` storage did not complete; source detail remains available to application logs.
    #[error("PostgreSQL API-key store operation failed")]
    Storage(#[source] sqlx::Error),
}

impl PostgresApiKeyStoreError {
    fn storage(error: sqlx::Error) -> Self {
        Self::Storage(error)
    }
}

impl fmt::Debug for PostgresApiKeyStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::DuplicateFingerprint => "DuplicateFingerprint",
            Self::MissingActiveRecord => "MissingActiveRecord",
            Self::InvalidExpiry => "InvalidExpiry",
            Self::InvalidPrincipal => "InvalidPrincipal",
            Self::InvalidReadinessTimeout => "InvalidReadinessTimeout",
            Self::ReadinessTimedOut(_) => "ReadinessTimedOut",
            Self::Storage(_) => "Storage",
        };
        formatter
            .debug_tuple("PostgresApiKeyStoreError")
            .field(&name)
            .finish()
    }
}
