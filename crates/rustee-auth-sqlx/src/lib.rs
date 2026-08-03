//! Optional durable `PostgreSQL` store for keyed Rustee API-key authentication.
//!
//! Applications add [`API_KEY_STORE_MIGRATION_SQL`] to a deployment-controlled migration job, then
//! supply [`PostgresApiKeyStore`] to `KeyedApiKeyAuthenticator`. The store receives only an HMAC
//! fingerprint; it never accepts or persists a raw API key. It atomically records successful
//! authentication and last-used metadata, but it does not deliver audit events, manage KMS policy,
//! generate client keys, or migrate a rotated pepper.

use std::{
    fmt,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::future::BoxFuture;
use rustee_auth::{ApiKeyError, ApiKeyFingerprint, ApiKeyFingerprintStore, Principal};
use sqlx::{PgPool, Row};
use uuid::Uuid;

/// Deployment-owned migration for the keyed API-key credential and success-audit records.
pub const API_KEY_STORE_MIGRATION_SQL: &str =
    include_str!("../migrations/0001_rustee_api_key_store.sql");

/// A new active API-key credential represented only by its keyed fingerprint.
pub struct ApiKeyRegistration {
    fingerprint: ApiKeyFingerprint,
    principal: Principal,
    expires_at: Option<SystemTime>,
}

impl ApiKeyRegistration {
    /// Creates a registration for a principal without an expiry.
    #[must_use]
    pub fn new(fingerprint: ApiKeyFingerprint, principal: Principal) -> Self {
        Self {
            fingerprint,
            principal,
            expires_at: None,
        }
    }

    /// Sets a whole-second explicit expiry evaluated against the `PostgreSQL` clock.
    #[must_use]
    pub fn expires_at(mut self, expires_at: SystemTime) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    fn into_storage(self) -> Result<(Vec<u8>, String, Option<i64>), PostgresApiKeyStoreError> {
        let fingerprint = self.fingerprint.as_bytes().to_vec();
        let principal = serde_json::to_string(&self.principal)
            .map_err(|_| PostgresApiKeyStoreError::InvalidPrincipal)?;
        let expires_at = self.expires_at.map(system_time_unix_seconds).transpose()?;
        Ok((fingerprint, principal, expires_at))
    }
}

impl fmt::Debug for ApiKeyRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApiKeyRegistration")
            .field("fingerprint", &"[REDACTED]")
            .field("principal", &"[REDACTED]")
            .field("expires_at", &self.expires_at.is_some())
            .finish()
    }
}

/// Opaque record identity used for revocation and client-key rotation.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ApiKeyRecordId(Uuid);

impl ApiKeyRecordId {
    /// Returns the deployment-owned record identity for an authorized admin workflow.
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Debug for ApiKeyRecordId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApiKeyRecordId([redacted])")
    }
}

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

    /// Registers one active fingerprint and returns an opaque record identity.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresApiKeyStoreError::DuplicateFingerprint`] without overwriting the existing
    /// principal, or a sanitized metadata/storage error when registration cannot complete.
    pub async fn register(
        &self,
        registration: ApiKeyRegistration,
    ) -> Result<ApiKeyRecordId, PostgresApiKeyStoreError> {
        let (fingerprint, principal, expires_at) = registration.into_storage()?;
        let record_id = ApiKeyRecordId(Uuid::new_v4());
        let inserted = sqlx::query(
            "INSERT INTO rustee_api_key_credentials (key_id, fingerprint, principal, expires_at) \
             VALUES ($1, $2, $3::jsonb, CASE WHEN $4::bigint IS NULL THEN NULL ELSE to_timestamp(($4::bigint)::double precision) END) \
             ON CONFLICT (fingerprint) DO NOTHING",
        )
        .bind(record_id.0)
        .bind(fingerprint)
        .bind(principal)
        .bind(expires_at)
        .execute(&self.pool)
        .await
        .map_err(PostgresApiKeyStoreError::storage)?;
        if inserted.rows_affected() == 1 {
            Ok(record_id)
        } else {
            Err(PostgresApiKeyStoreError::DuplicateFingerprint)
        }
    }

    /// Atomically registers a replacement fingerprint and revokes the previous active record.
    ///
    /// A replacement can coexist with the current record when callers use [`Self::register`]
    /// first; this method instead enforces a no-overlap rotation transaction.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresApiKeyStoreError::MissingActiveRecord`] when the previous record was
    /// absent or already revoked. In either error case the replacement is not retained.
    pub async fn rotate(
        &self,
        previous: ApiKeyRecordId,
        replacement: ApiKeyRegistration,
    ) -> Result<ApiKeyRecordId, PostgresApiKeyStoreError> {
        let (fingerprint, principal, expires_at) = replacement.into_storage()?;
        let replacement_id = ApiKeyRecordId(Uuid::new_v4());
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(PostgresApiKeyStoreError::storage)?;
        let inserted = sqlx::query(
            "INSERT INTO rustee_api_key_credentials (key_id, fingerprint, principal, expires_at) \
             VALUES ($1, $2, $3::jsonb, CASE WHEN $4::bigint IS NULL THEN NULL ELSE to_timestamp(($4::bigint)::double precision) END) \
             ON CONFLICT (fingerprint) DO NOTHING",
        )
        .bind(replacement_id.0)
        .bind(fingerprint)
        .bind(principal)
        .bind(expires_at)
        .execute(&mut *transaction)
        .await
        .map_err(PostgresApiKeyStoreError::storage)?;
        if inserted.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(PostgresApiKeyStoreError::storage)?;
            return Err(PostgresApiKeyStoreError::DuplicateFingerprint);
        }

        let revoked = sqlx::query(
            "UPDATE rustee_api_key_credentials \
             SET status = 'revoked', revoked_at = clock_timestamp() \
             WHERE key_id = $1 AND status = 'active'",
        )
        .bind(previous.0)
        .execute(&mut *transaction)
        .await
        .map_err(PostgresApiKeyStoreError::storage)?;
        if revoked.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(PostgresApiKeyStoreError::storage)?;
            return Err(PostgresApiKeyStoreError::MissingActiveRecord);
        }

        transaction
            .commit()
            .await
            .map_err(PostgresApiKeyStoreError::storage)?;
        Ok(replacement_id)
    }

    /// Revokes one active record. Repeating the same revocation is harmless and returns `false`.
    ///
    /// # Errors
    ///
    /// Returns a storage error when the state change cannot complete.
    pub async fn revoke(
        &self,
        record_id: ApiKeyRecordId,
    ) -> Result<bool, PostgresApiKeyStoreError> {
        let revoked = sqlx::query(
            "UPDATE rustee_api_key_credentials \
             SET status = 'revoked', revoked_at = clock_timestamp() \
             WHERE key_id = $1 AND status = 'active'",
        )
        .bind(record_id.0)
        .execute(&self.pool)
        .await
        .map_err(PostgresApiKeyStoreError::storage)?;
        Ok(revoked.rows_affected() == 1)
    }

    async fn authenticate_fingerprint(
        &self,
        fingerprint: ApiKeyFingerprint,
    ) -> Result<Principal, ApiKeyError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| ApiKeyError::ProviderUnavailable)?;
        let row = sqlx::query(
            "UPDATE rustee_api_key_credentials \
             SET last_used_at = clock_timestamp(), last_used_count = last_used_count + 1 \
             WHERE fingerprint = $1 AND status = 'active' \
               AND (expires_at IS NULL OR expires_at > clock_timestamp()) \
             RETURNING key_id, principal::text AS principal",
        )
        .bind(fingerprint.as_bytes().as_slice())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| ApiKeyError::ProviderUnavailable)?;
        let Some(row) = row else {
            return Err(ApiKeyError::RejectedApiKey);
        };

        let key_id = row
            .try_get::<Uuid, _>("key_id")
            .map_err(|_| ApiKeyError::ProviderUnavailable)?;
        let serialized_principal = row
            .try_get::<String, _>("principal")
            .map_err(|_| ApiKeyError::ProviderUnavailable)?;
        let principal = serde_json::from_str(&serialized_principal)
            .map_err(|_| ApiKeyError::ProviderUnavailable)?;
        sqlx::query("INSERT INTO rustee_api_key_authentication_audit (key_id) VALUES ($1)")
            .bind(key_id)
            .execute(&mut *transaction)
            .await
            .map_err(|_| ApiKeyError::ProviderUnavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| ApiKeyError::ProviderUnavailable)?;
        Ok(principal)
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
    /// A client-key rotation referenced no currently active credential.
    #[error("API-key rotation requires an active previous record")]
    MissingActiveRecord,
    /// The supplied expiry cannot be represented as a non-negative Unix timestamp.
    #[error("API-key expiry is invalid")]
    InvalidExpiry,
    /// Principal serialization failed before storage.
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

fn system_time_unix_seconds(expires_at: SystemTime) -> Result<i64, PostgresApiKeyStoreError> {
    let seconds = expires_at
        .duration_since(UNIX_EPOCH)
        .map_err(|_| PostgresApiKeyStoreError::InvalidExpiry)?
        .as_secs();
    let seconds = i64::try_from(seconds).map_err(|_| PostgresApiKeyStoreError::InvalidExpiry)?;
    Ok(seconds)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use rustee_auth::{ApiKeyPepper, Principal};

    use sqlx::postgres::PgPoolOptions;

    use super::{
        ApiKeyRegistration, PostgresApiKeyStore, PostgresApiKeyStoreError, system_time_unix_seconds,
    };

    #[test]
    fn registration_debug_redacts_fingerprint_and_principal() {
        let pepper = ApiKeyPepper::new([9; 32]).unwrap();
        let registration = ApiKeyRegistration::new(
            pepper.fingerprint("local-api-key").unwrap(),
            Principal::new("private-subject").unwrap(),
        );

        let rendered = format!("{registration:?}");
        assert!(!rendered.contains("local-api-key"));
        assert!(!rendered.contains("private-subject"));
    }

    #[test]
    fn expiry_before_the_unix_epoch_is_rejected() {
        let before_epoch = SystemTime::UNIX_EPOCH
            .checked_sub(Duration::from_secs(1))
            .unwrap();
        assert!(matches!(
            system_time_unix_seconds(before_epoch),
            Err(PostgresApiKeyStoreError::InvalidExpiry)
        ));
    }

    #[tokio::test]
    async fn readiness_rejects_a_zero_timeout_before_connecting() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://rustee:rustee@127.0.0.1:5432/rustee")
            .unwrap();
        let store = PostgresApiKeyStore::new(pool);

        assert!(matches!(
            store.readiness(Duration::ZERO).await,
            Err(PostgresApiKeyStoreError::InvalidReadinessTimeout)
        ));
    }
}
