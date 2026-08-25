use std::time::Duration;

use url::Url;

use crate::{McpOAuthClientConfig, McpOAuthConfigError};

use super::{CLIENT_ID, REDIRECT_URI, RESOURCE};

#[test]
fn client_configuration_validates_resource_redirect_and_scope_values() {
    let config = McpOAuthClientConfig::new(
        Url::parse("https://mcp.example.test/mcp").unwrap(),
        "rustee-client",
        Url::parse("http://127.0.0.1:3000/oauth/callback").unwrap(),
    )
    .unwrap()
    .with_scope("orders:read")
    .unwrap();
    assert_eq!(config.scopes().collect::<Vec<_>>(), vec!["orders:read"]);
    assert_eq!(
        McpOAuthClientConfig::new(
            Url::parse("https://mcp.example.test/mcp?token=bad").unwrap(),
            "rustee-client",
            Url::parse("https://app.example.test/callback").unwrap(),
        )
        .unwrap_err(),
        McpOAuthConfigError::InvalidResourceUrl
    );
    assert_eq!(
        McpOAuthClientConfig::new(
            Url::parse(&format!(
                "https://mcp.example.test/{}",
                "p".repeat(crate::config::MAX_URL_BYTES)
            ))
            .unwrap(),
            "rustee-client",
            Url::parse("https://app.example.test/callback").unwrap(),
        )
        .unwrap_err(),
        McpOAuthConfigError::InvalidResourceUrl
    );
    assert_eq!(
        McpOAuthClientConfig::new(
            Url::parse(RESOURCE).unwrap(),
            CLIENT_ID,
            Url::parse(&format!(
                "https://app.example.test/{}",
                "p".repeat(crate::config::MAX_URL_BYTES)
            ))
            .unwrap(),
        )
        .unwrap_err(),
        McpOAuthConfigError::InvalidRedirectUri
    );
    assert_eq!(
        config.clone().with_scope("orders read").unwrap_err(),
        McpOAuthConfigError::InvalidScope
    );
    for scope in ["orders\"read", "orders\\read", "orders:read\u{00e9}"] {
        assert_eq!(
            config.clone().with_scope(scope).unwrap_err(),
            McpOAuthConfigError::InvalidScope
        );
    }
}

#[test]
fn client_configuration_bounds_scope_count_and_transaction_ttl() {
    let mut full = McpOAuthClientConfig::new(
        Url::parse(RESOURCE).unwrap(),
        CLIENT_ID,
        Url::parse(REDIRECT_URI).unwrap(),
    )
    .unwrap();
    for index in 0..crate::config::MAX_SCOPES {
        full = full.with_scope(format!("scope-{index}")).unwrap();
    }
    assert_eq!(
        full.clone().with_scope("scope-0").unwrap().scopes().len(),
        crate::config::MAX_SCOPES
    );
    assert_eq!(
        full.with_scope("one-too-many").unwrap_err(),
        McpOAuthConfigError::TooManyScopes
    );
    let config = McpOAuthClientConfig::new(
        Url::parse(RESOURCE).unwrap(),
        CLIENT_ID,
        Url::parse(REDIRECT_URI).unwrap(),
    )
    .unwrap();
    assert_eq!(
        config.with_http_timeout(Duration::ZERO).unwrap_err(),
        McpOAuthConfigError::ZeroHttpTimeout
    );
    assert_eq!(
        McpOAuthClientConfig::new(
            Url::parse(RESOURCE).unwrap(),
            CLIENT_ID,
            Url::parse(REDIRECT_URI).unwrap(),
        )
        .unwrap()
        .with_transaction_ttl(Duration::ZERO)
        .unwrap_err(),
        McpOAuthConfigError::ZeroTransactionTtl
    );
    assert_eq!(
        McpOAuthClientConfig::new(
            Url::parse(RESOURCE).unwrap(),
            CLIENT_ID,
            Url::parse(REDIRECT_URI).unwrap(),
        )
        .unwrap()
        .with_transaction_ttl(Duration::from_secs(1) + Duration::from_nanos(1))
        .unwrap_err(),
        McpOAuthConfigError::FractionalTransactionTtl
    );
    assert_eq!(
        McpOAuthClientConfig::new(
            Url::parse(RESOURCE).unwrap(),
            CLIENT_ID,
            Url::parse(REDIRECT_URI).unwrap(),
        )
        .unwrap()
        .with_transaction_ttl(crate::config::MAX_TRANSACTION_TTL + Duration::from_secs(1))
        .unwrap_err(),
        McpOAuthConfigError::TransactionTtlTooLong
    );
}
