//! Opt-in Redis contract tests for one-time OIDC browser authorization transactions.

use futures_util::future::BoxFuture;
use rustee_auth::{AuthError, Principal};
use rustee_auth_oidc::{
    AuthorizationTransactionStore, IdTokenVerifier, OidcBrowserConfig, OidcBrowserLogin,
    OidcClientAuthentication, OidcDiscovery, OidcProviderMetadata, OidcTokenExchangeRequest,
    OidcTokenExchanger, OidcTokenResponse, UuidAuthorizationValueGenerator,
};
use rustee_auth_oidc_redis::RedisAuthorizationTransactionStore;
use rustee_redis::{RedisConfig, connect};
use url::Url;
use uuid::Uuid;

const ISSUER: &str = "https://issuer.example.test";
const JWKS_URL: &str = "https://issuer.example.test/keys";
const AUTHORIZATION_ENDPOINT: &str = "https://issuer.example.test/authorize";
const TOKEN_ENDPOINT: &str = "https://issuer.example.test/token";

fn redis_url() -> String {
    std::env::var("RUSTEE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/15".to_owned())
}

#[derive(Clone)]
struct StaticDiscovery;

impl OidcDiscovery for StaticDiscovery {
    type Error = std::io::Error;

    fn discover(
        &self,
        _issuer: Url,
    ) -> BoxFuture<'static, Result<OidcProviderMetadata, Self::Error>> {
        let metadata = serde_json::from_value(serde_json::json!({
            "issuer": ISSUER,
            "authorization_endpoint": AUTHORIZATION_ENDPOINT,
            "token_endpoint": TOKEN_ENDPOINT,
            "jwks_uri": JWKS_URL,
        }))
        .expect("test metadata must deserialize");
        Box::pin(async move { Ok(metadata) })
    }
}

#[derive(Clone)]
struct UnusedExchanger;

impl OidcTokenExchanger for UnusedExchanger {
    type Error = std::io::Error;

    fn exchange(
        &self,
        _endpoint: Url,
        _request: OidcTokenExchangeRequest,
    ) -> BoxFuture<'static, Result<OidcTokenResponse, Self::Error>> {
        Box::pin(async { Err(std::io::Error::other("not used by login begin")) })
    }
}

#[derive(Clone)]
struct UnusedVerifier;

impl IdTokenVerifier for UnusedVerifier {
    fn verify_id_token(
        &self,
        _token: &str,
        _expected_nonce: &str,
    ) -> BoxFuture<'static, Result<Principal, AuthError>> {
        Box::pin(async { Err(AuthError::RejectedBearerToken) })
    }
}

#[tokio::test]
#[ignore = "requires a Redis server; CI provisions one"]
async fn redis_store_persists_and_atomically_consumes_an_oidc_transaction() {
    let connection = connect(&RedisConfig::new(redis_url())).await.unwrap();
    let store = RedisAuthorizationTransactionStore::with_namespace(
        connection,
        format!("rustee:test:oidc:transaction:{}", Uuid::new_v4()),
    )
    .unwrap();
    let config = OidcBrowserConfig::new(
        Url::parse(ISSUER).unwrap(),
        "rustee-web",
        Url::parse("https://app.example.test/auth/callback").unwrap(),
        Url::parse(JWKS_URL).unwrap(),
        OidcClientAuthentication::None,
    )
    .unwrap();
    let login = OidcBrowserLogin::new(
        config,
        store.clone(),
        StaticDiscovery,
        UnusedExchanger,
        UnusedVerifier,
        UuidAuthorizationValueGenerator,
    );

    let redirect = login.begin().await.unwrap();
    let state = redirect
        .location()
        .query_pairs()
        .find_map(|(key, value)| (key == "state").then(|| value.into_owned()))
        .expect("authorization redirect must include state");
    let first = store.take(state.clone()).await.unwrap();
    let replay = store.take(state).await.unwrap();

    assert!(first.is_some());
    assert!(replay.is_none());
}
