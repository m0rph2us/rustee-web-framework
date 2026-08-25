//! Fail-closed fingerprint authentication and success-audit transaction.

use rustee_auth::{ApiKeyError, ApiKeyFingerprint, Principal};
use sqlx::Row;
use uuid::Uuid;

use crate::MAX_SERIALIZED_PRINCIPAL_BYTES;

use super::PostgresApiKeyStore;

impl PostgresApiKeyStore {
    pub(super) async fn authenticate_fingerprint(
        &self,
        fingerprint: ApiKeyFingerprint,
    ) -> Result<Principal, ApiKeyError> {
        let principal_byte_limit = i64::try_from(MAX_SERIALIZED_PRINCIPAL_BYTES)
            .map_err(|_| ApiKeyError::ProviderUnavailable)?;
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
               AND octet_length(principal::text) <= $2 \
             RETURNING key_id, principal::text AS principal",
        )
        .bind(fingerprint.as_bytes().as_slice())
        .bind(principal_byte_limit)
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
