//! Registration input admission and durable credential insertion.

use std::time::{SystemTime, UNIX_EPOCH};

use uuid::Uuid;

use crate::{
    MAX_SERIALIZED_PRINCIPAL_BYTES,
    model::{ApiKeyRecordId, ApiKeyRegistration},
};

use super::{PostgresApiKeyStore, PostgresApiKeyStoreError};

impl PostgresApiKeyStore {
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
        let (fingerprint, principal, expires_at) = registration_storage(registration)?;
        let record_id = ApiKeyRecordId::new(Uuid::new_v4());
        let inserted = sqlx::query(
            "INSERT INTO rustee_api_key_credentials (key_id, fingerprint, principal, expires_at) \
             VALUES ($1, $2, $3::jsonb, CASE WHEN $4::bigint IS NULL THEN NULL ELSE to_timestamp(($4::bigint)::double precision) END) \
             ON CONFLICT (fingerprint) DO NOTHING",
        )
        .bind(record_id.as_uuid())
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
}

pub(super) fn registration_storage(
    registration: ApiKeyRegistration,
) -> Result<(Vec<u8>, String, Option<i64>), PostgresApiKeyStoreError> {
    let (fingerprint, principal, expires_at) = registration.into_parts();
    let fingerprint = fingerprint.as_bytes().to_vec();
    let principal = serde_json::to_string(&principal)
        .map_err(|_| PostgresApiKeyStoreError::InvalidPrincipal)?;
    if principal.len() > MAX_SERIALIZED_PRINCIPAL_BYTES {
        return Err(PostgresApiKeyStoreError::InvalidPrincipal);
    }
    let expires_at = expires_at.map(system_time_unix_seconds).transpose()?;
    Ok((fingerprint, principal, expires_at))
}

pub(crate) fn system_time_unix_seconds(
    expires_at: SystemTime,
) -> Result<i64, PostgresApiKeyStoreError> {
    let seconds = expires_at
        .duration_since(UNIX_EPOCH)
        .map_err(|_| PostgresApiKeyStoreError::InvalidExpiry)?
        .as_secs();
    let seconds = i64::try_from(seconds).map_err(|_| PostgresApiKeyStoreError::InvalidExpiry)?;
    Ok(seconds)
}
