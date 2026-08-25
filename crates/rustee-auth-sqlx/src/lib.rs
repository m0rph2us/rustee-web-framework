//! Optional durable `PostgreSQL` store for keyed Rustee API-key authentication.
//!
//! Applications add [`API_KEY_STORE_MIGRATION_SQL`] and then
//! [`API_KEY_STORE_PRINCIPAL_BOUND_MIGRATION_SQL`] to a deployment-controlled migration job, then
//! supply [`PostgresApiKeyStore`] to `KeyedApiKeyAuthenticator`. The store receives only an HMAC
//! fingerprint; it never accepts or persists a raw API key. It atomically records successful
//! authentication and last-used metadata, but it does not deliver audit events, manage KMS policy,
//! generate client keys, or migrate a rotated pepper.

mod model;
mod store;

pub use model::{ApiKeyRecordId, ApiKeyRegistration};
pub use store::{
    API_KEY_STORE_MIGRATION_SQL, API_KEY_STORE_PRINCIPAL_BOUND_MIGRATION_SQL, PostgresApiKeyStore,
    PostgresApiKeyStoreError,
};

/// Maximum serialized `Principal` byte length admitted by the durable API-key store.
pub const MAX_SERIALIZED_PRINCIPAL_BYTES: usize = 512 * 1024;

#[cfg(test)]
use store::system_time_unix_seconds;

#[cfg(test)]
mod tests {
    use std::{
        error::Error as StdError,
        io,
        time::{Duration, SystemTime},
    };

    use rustee_auth::{ApiKeyPepper, Principal};

    use sqlx::{Error, postgres::PgPoolOptions};

    use super::{
        API_KEY_STORE_PRINCIPAL_BOUND_MIGRATION_SQL, ApiKeyRegistration,
        MAX_SERIALIZED_PRINCIPAL_BYTES, PostgresApiKeyStore, PostgresApiKeyStoreError,
        system_time_unix_seconds,
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
    fn storage_error_diagnostics_redact_driver_details_and_preserve_the_source() {
        let error = PostgresApiKeyStoreError::Storage(Error::Configuration(Box::new(
            io::Error::other("postgres://private-user:private-password@private-host/private-db"),
        )));

        assert_eq!(
            error.to_string(),
            "PostgreSQL API-key store operation failed"
        );
        assert!(!format!("{error:?}").contains("private-password"));
        assert!(StdError::source(&error).is_some());
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

    #[test]
    fn principal_bound_migration_matches_the_authenticated_read_limit() {
        assert!(
            API_KEY_STORE_PRINCIPAL_BOUND_MIGRATION_SQL
                .contains(&MAX_SERIALIZED_PRINCIPAL_BYTES.to_string())
        );
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
