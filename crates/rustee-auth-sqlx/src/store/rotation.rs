//! Credential replacement, rotation, and revocation transactions.

use rustee_auth::ApiKeyFingerprint;
use uuid::Uuid;

use crate::{
    MAX_SERIALIZED_PRINCIPAL_BYTES,
    model::{ApiKeyRecordId, ApiKeyRegistration},
};

use super::{PostgresApiKeyStore, PostgresApiKeyStoreError, registration::registration_storage};

impl PostgresApiKeyStore {
    /// Copies one active, unexpired record's principal and expiry to a replacement fingerprint.
    ///
    /// This is an overlap-preserving primitive for a deployment-owned client-key or pepper
    /// migration. Callers derive `replacement_fingerprint` in their authorized application flow;
    /// this method never receives a raw API key, changes the source record, or writes a success
    /// authentication audit event.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresApiKeyStoreError::MissingActiveRecord`] when `source` is absent, revoked,
    /// or expired according to the database clock, or
    /// [`PostgresApiKeyStoreError::DuplicateFingerprint`] without overwriting an existing
    /// credential when the replacement fingerprint is already registered.
    pub async fn clone_active_record(
        &self,
        source: ApiKeyRecordId,
        replacement_fingerprint: ApiKeyFingerprint,
    ) -> Result<ApiKeyRecordId, PostgresApiKeyStoreError> {
        let replacement_id = ApiKeyRecordId::new(Uuid::new_v4());
        let principal_byte_limit = i64::try_from(MAX_SERIALIZED_PRINCIPAL_BYTES)
            .map_err(|_| PostgresApiKeyStoreError::InvalidPrincipal)?;
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(PostgresApiKeyStoreError::storage)?;
        let inserted = sqlx::query(
            "WITH source AS ( \
             SELECT principal, expires_at FROM rustee_api_key_credentials \
             WHERE key_id = $1 AND status = 'active' \
               AND (expires_at IS NULL OR expires_at > clock_timestamp()) \
               AND octet_length(principal::text) <= $4 FOR UPDATE \
             ) \
             INSERT INTO rustee_api_key_credentials (key_id, fingerprint, principal, expires_at) \
             SELECT $2, $3, source.principal, source.expires_at FROM source \
             ON CONFLICT (fingerprint) DO NOTHING",
        )
        .bind(source.as_uuid())
        .bind(replacement_id.as_uuid())
        .bind(replacement_fingerprint.as_bytes().as_slice())
        .bind(principal_byte_limit)
        .execute(&mut *transaction)
        .await
        .map_err(PostgresApiKeyStoreError::storage)?;
        if inserted.rows_affected() == 1 {
            transaction
                .commit()
                .await
                .map_err(PostgresApiKeyStoreError::storage)?;
            return Ok(replacement_id);
        }

        let source_is_eligible: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM rustee_api_key_credentials \
             WHERE key_id = $1 AND status = 'active' \
               AND (expires_at IS NULL OR expires_at > clock_timestamp()) \
               AND octet_length(principal::text) <= $2)",
        )
        .bind(source.as_uuid())
        .bind(principal_byte_limit)
        .fetch_one(&mut *transaction)
        .await
        .map_err(PostgresApiKeyStoreError::storage)?;
        transaction
            .rollback()
            .await
            .map_err(PostgresApiKeyStoreError::storage)?;
        if source_is_eligible {
            Err(PostgresApiKeyStoreError::DuplicateFingerprint)
        } else {
            Err(PostgresApiKeyStoreError::MissingActiveRecord)
        }
    }

    /// Atomically registers a replacement fingerprint and revokes the previous active record.
    ///
    /// A replacement can coexist with the current record when callers use
    /// [`Self::clone_active_record`] or [`Self::register`] first; this method instead enforces a
    /// no-overlap rotation transaction.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresApiKeyStoreError::MissingActiveRecord`] when the previous record was
    /// absent, revoked, or expired according to the database clock. In either error case the
    /// replacement is not retained.
    pub async fn rotate(
        &self,
        previous: ApiKeyRecordId,
        replacement: ApiKeyRegistration,
    ) -> Result<ApiKeyRecordId, PostgresApiKeyStoreError> {
        let (fingerprint, principal, expires_at) = registration_storage(replacement)?;
        let replacement_id = ApiKeyRecordId::new(Uuid::new_v4());
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
        .bind(replacement_id.as_uuid())
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
             WHERE key_id = $1 AND status = 'active' \
               AND (expires_at IS NULL OR expires_at > clock_timestamp())",
        )
        .bind(previous.as_uuid())
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
        .bind(record_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(PostgresApiKeyStoreError::storage)?;
        Ok(revoked.rows_affected() == 1)
    }
}
