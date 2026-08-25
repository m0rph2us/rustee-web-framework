//! Internal authorization-transaction regression coverage.

use std::{sync::Arc, thread};

use serde_json::json;
use url::Url;

use super::{
    InMemoryMcpOAuthTransactionStore, MAX_AUTHORIZATION_REDIRECT_BYTES, MAX_IN_MEMORY_TRANSACTIONS,
    McpOAuthAuthorizationRedirect, McpOAuthPendingAuthorization, McpOAuthTransactionStore,
    unix_seconds,
};
use crate::{InMemoryMcpOAuthStoreError, McpOAuthError};

#[test]
fn authorization_redirect_uses_an_inclusive_url_budget() {
    let prefix = "https://auth.example.test/";
    let at_limit = Url::parse(&format!(
        "{prefix}{}",
        "p".repeat(MAX_AUTHORIZATION_REDIRECT_BYTES - prefix.len())
    ))
    .expect("redirect URL at the limit must parse");
    assert_eq!(at_limit.as_str().len(), MAX_AUTHORIZATION_REDIRECT_BYTES);
    assert_eq!(
        McpOAuthAuthorizationRedirect::new(at_limit)
            .expect("redirect URL at the limit must be accepted")
            .location()
            .as_str()
            .len(),
        MAX_AUTHORIZATION_REDIRECT_BYTES
    );

    let above_limit = Url::parse(&format!(
        "{prefix}{}",
        "p".repeat(MAX_AUTHORIZATION_REDIRECT_BYTES - prefix.len() + 1)
    ))
    .expect("redirect URL above the limit must parse");
    assert_eq!(
        McpOAuthAuthorizationRedirect::new(above_limit),
        Err(McpOAuthError::AuthorizationRedirectTooLong)
    );
}

#[tokio::test]
async fn poisoned_transaction_store_returns_an_unavailable_error() {
    let store = InMemoryMcpOAuthTransactionStore::default();
    let state = Arc::clone(&store.transactions);
    let poison = thread::spawn(move || {
        let _guard = state
            .lock()
            .expect("new transaction lock must be available");
        panic!("test must poison the MCP OAuth transaction store lock");
    });
    assert!(poison.join().is_err());

    let transaction = McpOAuthPendingAuthorization {
        state: "s".repeat(43),
        code_verifier: "v".repeat(43),
        token_endpoint: Url::parse("https://auth.example.test/token")
            .expect("test token endpoint must be valid"),
        resource: Url::parse("https://mcp.example.test/mcp").expect("test resource must be valid"),
        expires_at_unix_seconds: valid_expiry(),
    };

    assert!(matches!(
        store.save(transaction).await,
        Err(InMemoryMcpOAuthStoreError::StateUnavailable)
    ));
    assert!(matches!(
        store.take("s".repeat(43)).await,
        Err(InMemoryMcpOAuthStoreError::StateUnavailable)
    ));
}

#[tokio::test]
async fn transaction_store_prunes_expired_state_and_bounds_unconsumed_transactions() {
    let store = InMemoryMcpOAuthTransactionStore::default();
    let expired = transaction("expired", 0);
    store
        .transactions
        .lock()
        .expect("new transaction lock must be available")
        .insert(expired.state.clone(), expired);

    store
        .save(transaction("fresh", valid_expiry()))
        .await
        .expect("expired transaction must be pruned before saving");
    {
        let transactions = store
            .transactions
            .lock()
            .expect("new transaction lock must be available");
        assert_eq!(transactions.len(), 1);
        assert!(transactions.contains_key("fresh"));
    }

    let capacity_store = InMemoryMcpOAuthTransactionStore::default();
    {
        let mut transactions = capacity_store
            .transactions
            .lock()
            .expect("new transaction lock must be available");
        for index in 0..MAX_IN_MEMORY_TRANSACTIONS {
            let pending = transaction(&format!("state-{index}"), valid_expiry());
            transactions.insert(pending.state.clone(), pending);
        }
    }

    assert_eq!(
        capacity_store
            .save(transaction("overflow", valid_expiry()))
            .await,
        Err(InMemoryMcpOAuthStoreError::TransactionCapacityExhausted)
    );
}

#[tokio::test]
async fn transaction_store_rejects_a_duplicate_state_without_replacing_it() {
    let store = InMemoryMcpOAuthTransactionStore::default();
    let original = transaction("state", valid_expiry());
    let mut replacement = transaction("state", valid_expiry());
    replacement.code_verifier = "r".repeat(43);

    store.save(original.clone()).await.unwrap();
    assert_eq!(
        store.save(replacement).await,
        Err(InMemoryMcpOAuthStoreError::DuplicateTransactionState)
    );
    assert_eq!(
        store
            .take("state".to_owned())
            .await
            .unwrap()
            .unwrap()
            .code_verifier,
        original.code_verifier
    );
}

#[test]
fn durable_transaction_deserialization_revalidates_capabilities_and_urls() {
    let valid = json!({
        "state":"s".repeat(43),
        "code_verifier":"v".repeat(43),
        "token_endpoint":"https://auth.example.test/token",
        "resource":"https://mcp.example.test/mcp",
        "expires_at_unix_seconds":valid_expiry(),
    });
    let mut invalid_state = valid.clone();
    invalid_state["state"] = json!("invalid state");
    let mut invalid_endpoint = valid.clone();
    invalid_endpoint["token_endpoint"] = json!("http://auth.example.test/token");
    let mut invalid_expiry = valid.clone();
    invalid_expiry["expires_at_unix_seconds"] = json!(u64::MAX);

    assert!(serde_json::from_value::<McpOAuthPendingAuthorization>(invalid_state).is_err());
    assert!(serde_json::from_value::<McpOAuthPendingAuthorization>(invalid_endpoint).is_err());
    assert!(serde_json::from_value::<McpOAuthPendingAuthorization>(invalid_expiry).is_err());
    assert!(serde_json::from_value::<McpOAuthPendingAuthorization>(valid).is_ok());
}

fn valid_expiry() -> u64 {
    unix_seconds() + 60
}

fn transaction(state: &str, expires_at_unix_seconds: u64) -> McpOAuthPendingAuthorization {
    McpOAuthPendingAuthorization {
        state: state.to_owned(),
        code_verifier: "v".repeat(43),
        token_endpoint: Url::parse("https://auth.example.test/token")
            .expect("test token endpoint must be valid"),
        resource: Url::parse("https://mcp.example.test/mcp").expect("test resource must be valid"),
        expires_at_unix_seconds,
    }
}
