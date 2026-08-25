//! Internal token-model and token-store regression coverage.

use std::{sync::Arc, thread};

use serde_json::json;
use url::Url;

use super::{
    InMemoryMcpOAuthTokenStore, McpOAuthTokenSecrets, McpOAuthTokenSet, McpOAuthTokenStore,
    McpOAuthTokenStoreKey, store::MAX_IN_MEMORY_TOKEN_SETS,
};
use crate::{InMemoryMcpOAuthStoreError, McpOAuthAccessToken};

#[tokio::test]
async fn poisoned_token_store_returns_an_unavailable_error() {
    let store = InMemoryMcpOAuthTokenStore::default();
    let state = Arc::clone(&store.tokens);
    let poison = thread::spawn(move || {
        let _guard = state.lock().expect("new token lock must be available");
        panic!("test must poison the MCP OAuth token store lock");
    });
    assert!(poison.join().is_err());

    let key = McpOAuthTokenStoreKey::new("tenant-a:user-a:connection-a")
        .expect("test token-store key must be valid");
    let token_set = McpOAuthTokenSet::new(
        Url::parse("https://mcp.example.test/mcp").expect("test resource must be valid"),
        McpOAuthAccessToken::new("access-token", None).expect("test token must be valid"),
        Some("refresh-token".to_owned()),
    )
    .expect("test token set must be valid");

    assert!(matches!(
        store.load(key.clone()).await,
        Err(InMemoryMcpOAuthStoreError::StateUnavailable)
    ));
    assert!(matches!(
        store.save(key.clone(), token_set).await,
        Err(InMemoryMcpOAuthStoreError::StateUnavailable)
    ));
    assert!(matches!(
        store.remove(key).await,
        Err(InMemoryMcpOAuthStoreError::StateUnavailable)
    ));
}

#[tokio::test]
async fn token_store_bounds_new_slots_without_evicting_or_blocking_replacement() {
    let store = InMemoryMcpOAuthTokenStore::default();
    let tokens = token_set();
    {
        let mut state = store
            .tokens
            .lock()
            .expect("new token lock must be available");
        for index in 0..MAX_IN_MEMORY_TOKEN_SETS {
            state.insert(key(index), tokens.clone());
        }
    }

    assert_eq!(
        store
            .save(key(MAX_IN_MEMORY_TOKEN_SETS), tokens.clone())
            .await,
        Err(InMemoryMcpOAuthStoreError::TokenCapacityExhausted)
    );
    store
        .save(key(0), tokens)
        .await
        .expect("an existing token slot must remain replaceable at capacity");
    assert_eq!(
        store
            .tokens
            .lock()
            .expect("new token lock must be available")
            .len(),
        MAX_IN_MEMORY_TOKEN_SETS
    );
}

#[test]
fn durable_token_secrets_revalidate_resource_tokens_and_expiry() {
    let valid = json!({
        "resource": "https://mcp.example.test/mcp",
        "access_token": "access-token",
        "expires_at_unix_seconds": 1,
        "refresh_token": "refresh-token",
    });
    let mut invalid_resource = valid.clone();
    invalid_resource["resource"] = json!("http://mcp.example.test/mcp");
    let mut invalid_access_token = valid.clone();
    invalid_access_token["access_token"] = json!(" ");
    let mut invalid_refresh_token = valid.clone();
    invalid_refresh_token["refresh_token"] = json!("\u{0000}");
    let mut invalid_expiry = valid.clone();
    invalid_expiry["expires_at_unix_seconds"] = json!(u64::MAX);

    assert!(serde_json::from_value::<McpOAuthTokenSecrets>(valid).is_ok());
    for invalid in [
        invalid_resource,
        invalid_access_token,
        invalid_refresh_token,
        invalid_expiry,
    ] {
        assert!(serde_json::from_value::<McpOAuthTokenSecrets>(invalid).is_err());
    }
}

fn key(index: usize) -> McpOAuthTokenStoreKey {
    McpOAuthTokenStoreKey::new(format!("tenant-a:user-a:connection-{index}"))
        .expect("test token-store key must be valid")
}

fn token_set() -> McpOAuthTokenSet {
    McpOAuthTokenSet::new(
        Url::parse("https://mcp.example.test/mcp").expect("test resource must be valid"),
        McpOAuthAccessToken::new("access-token", None).expect("test token must be valid"),
        Some("refresh-token".to_owned()),
    )
    .expect("test token set must be valid")
}
