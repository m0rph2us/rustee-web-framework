//! Opt-in `PostgreSQL` contract tests for the keyed API-key store.

use std::time::{Duration, SystemTime};

use rustee_auth::{
    ApiKeyError, ApiKeyFingerprint, ApiKeyFingerprintStore, ApiKeyPepper, Principal,
};
use rustee_auth_sqlx::{
    API_KEY_STORE_MIGRATION_SQL, ApiKeyRegistration, PostgresApiKeyStore, PostgresApiKeyStoreError,
};
use sqlx::{PgPool, postgres::PgPoolOptions};

fn database_url() -> String {
    std::env::var("RUSTEE_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rustee:rustee@127.0.0.1:5432/rustee".to_owned())
}

async fn pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url())
        .await
        .unwrap()
}

async fn reset_schema(pool: &PgPool) {
    sqlx::raw_sql(API_KEY_STORE_MIGRATION_SQL)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("TRUNCATE rustee_api_key_authentication_audit, rustee_api_key_credentials")
        .execute(pool)
        .await
        .unwrap();
}

fn fingerprint(raw_key: &str) -> ApiKeyFingerprint {
    ApiKeyPepper::new([11; 32])
        .unwrap()
        .fingerprint(raw_key)
        .unwrap()
}

fn principal(subject: &str) -> Principal {
    Principal::new(subject)
        .unwrap()
        .with_scope("service:read")
        .unwrap()
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn authentication_updates_last_used_and_audit_then_enforces_rotation_and_revocation() {
    let pool = pool().await;
    reset_schema(&pool).await;
    let store = PostgresApiKeyStore::new(pool.clone());
    let old_fingerprint = fingerprint("api-key-old");
    let old_principal = principal("service-old");
    let old_id = store
        .register(ApiKeyRegistration::new(
            old_fingerprint.clone(),
            old_principal.clone(),
        ))
        .await
        .unwrap();

    assert_eq!(
        store.authenticate(old_fingerprint.clone()).await.unwrap(),
        old_principal
    );
    let last_used_count: i64 = sqlx::query_scalar(
        "SELECT last_used_count FROM rustee_api_key_credentials WHERE key_id = $1",
    )
    .bind(old_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    let audit_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM rustee_api_key_authentication_audit WHERE key_id = $1",
    )
    .bind(old_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(last_used_count, 1);
    assert_eq!(audit_count, 1);

    let replacement_fingerprint = fingerprint("api-key-replacement");
    let replacement_principal = principal("service-replacement");
    let replacement_id = store
        .rotate(
            old_id,
            ApiKeyRegistration::new(
                replacement_fingerprint.clone(),
                replacement_principal.clone(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        store.authenticate(old_fingerprint).await.unwrap_err(),
        ApiKeyError::RejectedApiKey
    );
    assert_eq!(
        store
            .authenticate(replacement_fingerprint.clone())
            .await
            .unwrap(),
        replacement_principal
    );

    assert!(store.revoke(replacement_id).await.unwrap());
    assert!(!store.revoke(replacement_id).await.unwrap());
    assert_eq!(
        store
            .authenticate(replacement_fingerprint)
            .await
            .unwrap_err(),
        ApiKeyError::RejectedApiKey
    );
    let replacement_audits: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM rustee_api_key_authentication_audit WHERE key_id = $1",
    )
    .bind(replacement_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(replacement_audits, 1);
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn expired_and_duplicate_fingerprints_are_rejected_without_overwriting_an_active_record() {
    let pool = pool().await;
    reset_schema(&pool).await;
    let store = PostgresApiKeyStore::new(pool.clone());
    let active_fingerprint = fingerprint("api-key-active");
    let active_id = store
        .register(ApiKeyRegistration::new(
            active_fingerprint.clone(),
            principal("service-active"),
        ))
        .await
        .unwrap();
    let duplicate = store
        .register(ApiKeyRegistration::new(
            active_fingerprint.clone(),
            principal("service-overwrite-attempt"),
        ))
        .await
        .unwrap_err();
    assert!(matches!(
        duplicate,
        PostgresApiKeyStoreError::DuplicateFingerprint
    ));
    assert_eq!(
        store.authenticate(active_fingerprint).await.unwrap(),
        principal("service-active")
    );

    let expired_at = SystemTime::now()
        .checked_sub(Duration::from_secs(1))
        .unwrap();
    let expired_fingerprint = fingerprint("api-key-expired");
    let expired_id = store
        .register(
            ApiKeyRegistration::new(expired_fingerprint.clone(), principal("service-expired"))
                .expires_at(expired_at),
        )
        .await
        .unwrap();
    assert_eq!(
        store.authenticate(expired_fingerprint).await.unwrap_err(),
        ApiKeyError::RejectedApiKey
    );
    let expired_audits: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM rustee_api_key_authentication_audit WHERE key_id = $1",
    )
    .bind(expired_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(expired_audits, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM rustee_api_key_credentials WHERE key_id = $1",
        )
        .bind(active_id.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
}
