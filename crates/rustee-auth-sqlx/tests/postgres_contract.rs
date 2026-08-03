//! Opt-in `PostgreSQL` contract tests for the keyed API-key store.

use std::time::{Duration, Instant, SystemTime};

use rustee_auth::{
    ApiKeyAuthenticator, ApiKeyError, ApiKeyFingerprint, ApiKeyFingerprintStore, ApiKeyPepper,
    ApiKeyPepperRing, Principal, RotatingKeyedApiKeyAuthenticator,
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
async fn rotating_authenticator_reads_a_retained_pepper_record_and_audits_only_the_match() {
    let pool = pool().await;
    reset_schema(&pool).await;
    let store = PostgresApiKeyStore::new(pool.clone());
    let active_pepper = ApiKeyPepper::new([13; 32]).unwrap();
    let retired_pepper = ApiKeyPepper::new([12; 32]).unwrap();
    let raw_key = "api-key-pepper-migration";
    let retired_fingerprint = retired_pepper.fingerprint(raw_key).unwrap();
    let record_id = store
        .register(ApiKeyRegistration::new(
            retired_fingerprint,
            principal("service-retained-pepper"),
        ))
        .await
        .unwrap();
    let authenticator = RotatingKeyedApiKeyAuthenticator::new(
        ApiKeyPepperRing::with_retired(active_pepper, [retired_pepper]).unwrap(),
        store,
    );

    assert_eq!(
        authenticator.authenticate(raw_key).await.unwrap(),
        principal("service-retained-pepper")
    );
    let last_used_count: i64 = sqlx::query_scalar(
        "SELECT last_used_count FROM rustee_api_key_credentials WHERE key_id = $1",
    )
    .bind(record_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    let audit_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM rustee_api_key_authentication_audit WHERE key_id = $1",
    )
    .bind(record_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(last_used_count, 1);
    assert_eq!(audit_count, 1);
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn active_record_clone_preserves_metadata_without_audit_or_source_revocation() {
    let pool = pool().await;
    reset_schema(&pool).await;
    let store = PostgresApiKeyStore::new(pool.clone());
    let source_fingerprint = fingerprint("api-key-migration-source");
    let expires_at = SystemTime::now()
        .checked_add(Duration::from_secs(60))
        .unwrap();
    let source_id = store
        .register(
            ApiKeyRegistration::new(source_fingerprint.clone(), principal("service-migration"))
                .expires_at(expires_at),
        )
        .await
        .unwrap();
    let replacement_fingerprint = ApiKeyPepper::new([12; 32])
        .unwrap()
        .fingerprint("api-key-migration-source")
        .unwrap();
    let replacement_id = store
        .clone_active_record(source_id, replacement_fingerprint.clone())
        .await
        .unwrap();

    assert_eq!(
        store.authenticate(source_fingerprint).await.unwrap(),
        principal("service-migration")
    );
    assert_eq!(
        store
            .authenticate(replacement_fingerprint.clone())
            .await
            .unwrap(),
        principal("service-migration")
    );
    let copied_expiry: bool = sqlx::query_scalar(
        "SELECT source.expires_at = replacement.expires_at \
         FROM rustee_api_key_credentials AS source, rustee_api_key_credentials AS replacement \
         WHERE source.key_id = $1 AND replacement.key_id = $2",
    )
    .bind(source_id.as_uuid())
    .bind(replacement_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(copied_expiry);
    let source_audits: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM rustee_api_key_authentication_audit WHERE key_id = $1",
    )
    .bind(source_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    let replacement_audits: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM rustee_api_key_authentication_audit WHERE key_id = $1",
    )
    .bind(replacement_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(source_audits, 1);
    assert_eq!(replacement_audits, 1);

    assert!(matches!(
        store
            .clone_active_record(source_id, replacement_fingerprint)
            .await,
        Err(PostgresApiKeyStoreError::DuplicateFingerprint)
    ));
    assert!(store.revoke(source_id).await.unwrap());
    assert!(matches!(
        store
            .clone_active_record(source_id, fingerprint("api-key-migration-after-revoke"))
            .await,
        Err(PostgresApiKeyStoreError::MissingActiveRecord)
    ));
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

#[tokio::test]
#[ignore = "requires CI to stop its PostgreSQL container before this contract"]
async fn api_key_store_readiness_fails_within_the_deadline_during_an_outage() {
    assert!(
        std::env::var_os("RUSTEE_AUTH_SQLX_EXPECT_OUTAGE").is_some(),
        "CI must explicitly opt into the stopped-PostgreSQL contract"
    );
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(200))
        .connect_lazy(&database_url())
        .unwrap();
    let store = PostgresApiKeyStore::new(pool);

    let started = Instant::now();
    let error = store
        .readiness(Duration::from_millis(500))
        .await
        .unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(matches!(
        &error,
        PostgresApiKeyStoreError::Storage(_) | PostgresApiKeyStoreError::ReadinessTimedOut(_)
    ));
    assert!(!error.to_string().contains("127.0.0.1"));
    assert!(!format!("{error:?}").contains("127.0.0.1"));
}
