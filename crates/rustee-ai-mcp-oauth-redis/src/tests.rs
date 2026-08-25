use std::{error::Error as StdError, time::Duration};

use rustee_ai_mcp_oauth::McpOAuthTokenStoreKey;
use rustee_redis::{CacheError, is_valid_key_namespace, redis};
use serde::{Deserialize, Serialize};

use crate::{
    DEFAULT_TOKEN_NAMESPACE, DEFAULT_TRANSACTION_NAMESPACE, McpOAuthSecretCipher,
    McpOAuthSecretCipherError, McpOAuthSecretKeyRing, McpOAuthSecretKeyRingError,
    RedisMcpOAuthStoreConfigError, RedisMcpOAuthTokenStoreError,
    RedisMcpOAuthTransactionStoreError,
    crypto::{MAX_ENCRYPTED_PLAINTEXT_BYTES, MAX_SERIALIZED_ENVELOPE_BYTES, valid_key_id},
    store::{
        redacted_namespace_debug_fields, token_storage_key, transaction_storage_key,
        validate_namespace, validate_token_ttl,
    },
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct TestPayload {
    value: String,
}

#[test]
fn encrypted_envelope_binds_its_purpose_and_redis_key() {
    let cipher =
        McpOAuthSecretCipher::new(McpOAuthSecretKeyRing::new("current", [7_u8; 32]).unwrap());
    let envelope = cipher
        .seal(
            "transaction",
            "rustee:test:transaction:state-a",
            &TestPayload {
                value: "sensitive payload".to_owned(),
            },
        )
        .unwrap();
    assert!(!format!("{envelope:?}").contains("sensitive payload"));
    let decoded: TestPayload = cipher
        .open(
            "transaction",
            "rustee:test:transaction:state-a",
            envelope.clone(),
        )
        .unwrap();
    assert_eq!(decoded.value, "sensitive payload");
    assert_eq!(
        cipher
            .open::<TestPayload>("token", "rustee:test:token:user-a", envelope)
            .unwrap_err(),
        McpOAuthSecretCipherError::AuthenticationRejected
    );
}

#[test]
fn encrypted_plaintext_size_admission_stops_at_the_configured_limit() {
    let cipher =
        McpOAuthSecretCipher::new(McpOAuthSecretKeyRing::new("current", [7_u8; 32]).unwrap());

    assert_eq!(
        cipher
            .seal(
                "token",
                "rustee:test:token:user-a",
                &TestPayload {
                    value: "x".repeat(MAX_ENCRYPTED_PLAINTEXT_BYTES),
                },
            )
            .unwrap_err(),
        McpOAuthSecretCipherError::PayloadTooLarge
    );
}

#[test]
fn serialized_envelope_budget_covers_a_near_limit_plaintext() {
    let cipher =
        McpOAuthSecretCipher::new(McpOAuthSecretKeyRing::new("current", [7_u8; 32]).unwrap());
    let envelope = cipher
        .seal(
            "token",
            "rustee:test:token:user-a",
            &TestPayload {
                value: "x".repeat(MAX_ENCRYPTED_PLAINTEXT_BYTES - 64),
            },
        )
        .unwrap();

    assert!(serde_json::to_vec(&envelope).unwrap().len() <= MAX_SERIALIZED_ENVELOPE_BYTES);
}

#[test]
fn key_rotation_reads_retired_envelopes_but_writes_with_the_new_active_key() {
    let old_cipher =
        McpOAuthSecretCipher::new(McpOAuthSecretKeyRing::new("old", [1_u8; 32]).unwrap());
    let old_envelope = old_cipher
        .seal(
            "token",
            "rustee:test:token:user-a",
            &TestPayload {
                value: "old".to_owned(),
            },
        )
        .unwrap();
    let rotated = McpOAuthSecretCipher::new(
        McpOAuthSecretKeyRing::new("new", [2_u8; 32])
            .unwrap()
            .with_retired_key("old", [1_u8; 32])
            .unwrap(),
    );
    let decoded: TestPayload = rotated
        .open("token", "rustee:test:token:user-a", old_envelope)
        .unwrap();
    assert_eq!(decoded.value, "old");
    let new_envelope = rotated
        .seal(
            "token",
            "rustee:test:token:user-a",
            &TestPayload {
                value: "new".to_owned(),
            },
        )
        .unwrap();
    assert_eq!(new_envelope.key_id(), "new");
}

#[test]
fn key_ring_and_redis_configuration_reject_unsafe_values() {
    assert_eq!(
        DEFAULT_TRANSACTION_NAMESPACE,
        "rustee:mcp:oauth:transaction:v1"
    );
    assert_eq!(DEFAULT_TOKEN_NAMESPACE, "rustee:mcp:oauth:token:v1");
    assert!(is_valid_key_namespace("customer-a:mcp:oauth:v1"));
    assert!(!is_valid_key_namespace(""));
    assert!(!is_valid_key_namespace("mcp oauth"));
    assert!(!is_valid_key_namespace("mcp{shared-slot}"));
    assert_eq!(
        validate_namespace("customer-a:mcp:oauth:v1"),
        Ok("customer-a:mcp:oauth:v1".to_owned())
    );
    assert_eq!(
        validate_namespace("mcp oauth"),
        Err(RedisMcpOAuthStoreConfigError::InvalidNamespace)
    );
    assert!(valid_key_id("kms-2026.08"));
    assert!(!valid_key_id("key id"));
    assert_eq!(
        McpOAuthSecretKeyRing::new("", [0_u8; 32]).unwrap_err(),
        McpOAuthSecretKeyRingError::InvalidKeyId
    );
    assert_eq!(
        McpOAuthSecretKeyRing::new("current", [0_u8; 32])
            .unwrap()
            .with_retired_key("current", [1_u8; 32])
            .unwrap_err(),
        McpOAuthSecretKeyRingError::DuplicateKeyId
    );
    assert_eq!(
        validate_token_ttl(Duration::ZERO),
        Err(RedisMcpOAuthStoreConfigError::ZeroTokenTtl)
    );
    assert_eq!(
        validate_token_ttl(Duration::from_secs(1) + Duration::from_nanos(1)),
        Err(RedisMcpOAuthStoreConfigError::FractionalTokenTtl)
    );
    assert_eq!(
        validate_token_ttl(Duration::from_secs(u64::MAX)),
        Err(RedisMcpOAuthStoreConfigError::TokenTtlOutOfRange)
    );
    assert_eq!(validate_token_ttl(Duration::from_secs(1)), Ok(()));
}

#[test]
fn redis_store_namespace_diagnostics_are_redacted() {
    let namespace = "customer-a:mcp:oauth:v1";
    let (value, length) = redacted_namespace_debug_fields(namespace);

    assert_eq!(value, "[REDACTED]");
    assert_eq!(length, namespace.len());
}

#[test]
fn oauth_redis_store_diagnostics_redact_adapter_details_and_preserve_sources() {
    let transaction =
        RedisMcpOAuthTransactionStoreError::Save(CacheError::Redis(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "private-oauth-transaction-redis-detail",
        ))));
    let token = RedisMcpOAuthTokenStoreError::Delete(CacheError::Redis(redis::RedisError::from((
        redis::ErrorKind::InvalidClientConfig,
        "private-oauth-token-redis-detail",
    ))));

    for error in [&transaction as &dyn StdError, &token as &dyn StdError] {
        assert!(!format!("{error:?}").contains("private-oauth-transaction-redis-detail"));
        assert!(!format!("{error:?}").contains("private-oauth-token-redis-detail"));
        assert!(
            !error
                .to_string()
                .contains("private-oauth-transaction-redis-detail")
        );
        assert!(
            !error
                .to_string()
                .contains("private-oauth-token-redis-detail")
        );
        assert!(StdError::source(error).is_some());
    }
}

#[test]
fn transaction_storage_key_is_length_delimited_across_namespace_and_state_boundaries() {
    assert_eq!(
        transaction_storage_key("transaction", "owner:state"),
        "rustee:mcp:oauth:transaction-key:v1:11:transaction:11:owner:state"
    );
    assert_ne!(
        transaction_storage_key("transaction", "owner:state"),
        transaction_storage_key("transaction:owner", "state")
    );
}

#[test]
fn token_storage_key_is_length_delimited_across_namespace_and_slot_boundaries() {
    let first = McpOAuthTokenStoreKey::new("owner:slot").unwrap();
    let second = McpOAuthTokenStoreKey::new("slot").unwrap();

    assert_eq!(
        token_storage_key("token", &first),
        "rustee:mcp:oauth:token-key:v1:5:token:10:owner:slot"
    );
    assert_ne!(
        token_storage_key("token", &first),
        token_storage_key("token:owner", &second)
    );
}
