//! Opt-in Redis contract for encrypted MCP OAuth state and token persistence.

use futures_util::future::BoxFuture;
use rustee_ai_mcp_oauth::{
    McpOAuthAccessToken, McpOAuthAuthorizationFlow, McpOAuthAuthorizationServerMetadata,
    McpOAuthClientConfig, McpOAuthRefreshRequest, McpOAuthTokenExchangeRequest,
    McpOAuthTokenExchanger, McpOAuthTokenSet, McpOAuthTokenStore, McpOAuthTokenStoreKey,
    McpOAuthTransactionStore, UuidMcpOAuthValueGenerator,
};
use rustee_ai_mcp_oauth_redis::{
    McpOAuthSecretCipher, McpOAuthSecretKeyRing, RedisMcpOAuthTokenStore,
    RedisMcpOAuthTransactionStore,
};
use rustee_redis::{RedisConfig, connect, redis::AsyncCommands};
use url::Url;
use uuid::Uuid;

const RESOURCE: &str = "https://mcp.example.test/mcp";
const CLIENT_ID: &str = "rustee-mcp-client";
const REDIRECT_URI: &str = "https://app.example.test/mcp/callback";
const ISSUER: &str = "https://auth.example.test";
const AUTHORIZATION_ENDPOINT: &str = "https://auth.example.test/authorize";
const TOKEN_ENDPOINT: &str = "https://auth.example.test/token";

fn redis_url() -> String {
    std::env::var("RUSTEE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/15".to_owned())
}

#[derive(Clone)]
struct UnusedExchanger;

impl McpOAuthTokenExchanger for UnusedExchanger {
    type Error = std::io::Error;

    fn exchange(
        &self,
        _endpoint: Url,
        _request: McpOAuthTokenExchangeRequest,
    ) -> BoxFuture<'static, Result<McpOAuthTokenSet, Self::Error>> {
        Box::pin(async { Err(std::io::Error::other("not used by authorization begin")) })
    }

    fn refresh(
        &self,
        _endpoint: Url,
        _request: McpOAuthRefreshRequest,
    ) -> BoxFuture<'static, Result<McpOAuthTokenSet, Self::Error>> {
        Box::pin(async { Err(std::io::Error::other("not used by authorization begin")) })
    }
}

fn cipher() -> McpOAuthSecretCipher {
    McpOAuthSecretCipher::new(McpOAuthSecretKeyRing::new("test-2026", [9_u8; 32]).unwrap())
}

fn config() -> McpOAuthClientConfig {
    McpOAuthClientConfig::new(
        Url::parse(RESOURCE).unwrap(),
        CLIENT_ID,
        Url::parse(REDIRECT_URI).unwrap(),
    )
    .unwrap()
}

fn provider() -> McpOAuthAuthorizationServerMetadata {
    McpOAuthAuthorizationServerMetadata::new(
        Url::parse(ISSUER).unwrap(),
        Url::parse(AUTHORIZATION_ENDPOINT).unwrap(),
        Url::parse(TOKEN_ENDPOINT).unwrap(),
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires a Redis server; CI provisions one"]
async fn redis_persists_encrypted_mcp_oauth_state_and_tokens() {
    let connection = connect(&RedisConfig::new(redis_url())).await.unwrap();
    let namespace = format!("rustee:test:mcp:oauth:transaction:{}", Uuid::new_v4());
    let transactions = RedisMcpOAuthTransactionStore::with_namespace(
        connection.clone(),
        cipher(),
        namespace.clone(),
    )
    .unwrap();
    let flow = McpOAuthAuthorizationFlow::new(
        config(),
        provider(),
        transactions.clone(),
        UnusedExchanger,
        UuidMcpOAuthValueGenerator,
    );

    let redirect = flow.begin().await.unwrap();
    let state = redirect
        .location()
        .query_pairs()
        .find_map(|(key, value)| (key == "state").then(|| value.into_owned()))
        .expect("authorization redirect must contain state");
    let mut raw_connection = connection.clone();
    let raw_state: Option<String> = raw_connection
        .get(format!(
            "rustee:mcp:oauth:transaction-key:v1:{}:{}:{}:{}",
            namespace.len(),
            namespace,
            state.len(),
            state,
        ))
        .await
        .unwrap();
    let raw_state = raw_state.expect("transaction envelope must be stored");
    assert!(!raw_state.contains(&state));
    assert!(!raw_state.contains("code_verifier"));

    let first = transactions.take(state.clone()).await.unwrap();
    let replay = transactions.take(state).await.unwrap();
    assert!(first.is_some());
    assert!(replay.is_none());

    let token_namespace = format!("rustee:test:mcp:oauth:token:{}", Uuid::new_v4());
    let tokens = RedisMcpOAuthTokenStore::with_namespace(
        connection.clone(),
        cipher(),
        token_namespace.clone(),
    )
    .unwrap();
    let key = McpOAuthTokenStoreKey::new("tenant-a:user-a:connection-a").unwrap();
    let token_set = McpOAuthTokenSet::new(
        Url::parse(RESOURCE).unwrap(),
        McpOAuthAccessToken::new("access-token-secret", None).unwrap(),
        Some("refresh-token-secret".to_owned()),
    )
    .unwrap();
    tokens.save(key.clone(), token_set).await.unwrap();

    let mut raw_connection = connection.clone();
    let raw_tokens: Option<String> = raw_connection
        .get(format!(
            "rustee:mcp:oauth:token-key:v1:{}:{}:{}:{}",
            token_namespace.len(),
            token_namespace,
            key.as_str().len(),
            key.as_str(),
        ))
        .await
        .unwrap();
    let raw_tokens = raw_tokens.expect("token envelope must be stored");
    assert!(!raw_tokens.contains("access-token-secret"));
    assert!(!raw_tokens.contains("refresh-token-secret"));

    let loaded = tokens.load(key.clone()).await.unwrap().unwrap();
    let secrets = loaded.into_secrets();
    assert_eq!(secrets.access_token_for_encryption(), "access-token-secret");
    assert_eq!(
        secrets.refresh_token_for_encryption(),
        Some("refresh-token-secret")
    );
    tokens.remove(key.clone()).await.unwrap();
    assert!(tokens.load(key).await.unwrap().is_none());
}
