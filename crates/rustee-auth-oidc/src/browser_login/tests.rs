//! Internal regression coverage for browser-login orchestration.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures_util::future::BoxFuture;
use http::{
    StatusCode,
    header::{LOCATION, SET_COOKIE},
};
use http_body_util::BodyExt;
use rustee_auth::{AuthError, Principal};
use rustee_auth_session::{InMemorySessionStore, SessionCookieConfig, SessionManager};
use rustee_core::IntoResponse;
use tokio::sync::Mutex as AsyncMutex;
use url::Url;

use super::{
    AuthorizationCallback, AuthorizationRedirect, AuthorizationTransactionStore,
    AuthorizationValueGenerator, InMemoryAuthorizationTransactionStore,
    MAX_AUTHORIZATION_CODE_BYTES, MAX_ID_TOKEN_BYTES, MAX_PROVIDER_ERROR_BYTES, MAX_SCOPE_BYTES,
    MAX_SCOPES, OidcBrowserConfig, OidcBrowserConfigError, OidcBrowserLogin, OidcDiscovery,
    OidcLoginError, OidcProviderMetadata, OidcTokenExchangeRequest, OidcTokenExchanger,
    OidcTokenResponse, PendingAuthorization, pkce_challenge, unix_seconds,
};
use crate::{IdTokenVerifier, OidcClientAuthentication};

const ISSUER: &str = "https://issuer.example.test";
const CLIENT_ID: &str = "rustee-web";
const REDIRECT_URI: &str = "https://app.example.test/auth/callback";
const JWKS_URL: &str = "https://issuer.example.test/keys";
const AUTHORIZATION_ENDPOINT: &str = "https://issuer.example.test/authorize";
const TOKEN_ENDPOINT: &str = "https://issuer.example.test/token";

#[derive(Clone, Debug, thiserror::Error)]
#[error("test provider failure")]
struct TestError;

#[derive(Clone)]
struct StaticDiscovery(OidcProviderMetadata);

impl OidcDiscovery for StaticDiscovery {
    type Error = TestError;

    fn discover(
        &self,
        _issuer: Url,
    ) -> BoxFuture<'static, Result<OidcProviderMetadata, Self::Error>> {
        let provider = self.0.clone();
        Box::pin(async move { Ok(provider) })
    }
}

#[derive(Clone, Default)]
struct RecordingExchanger {
    requests: Arc<AsyncMutex<Vec<OidcTokenExchangeRequest>>>,
    calls: Arc<AtomicUsize>,
}

impl OidcTokenExchanger for RecordingExchanger {
    type Error = TestError;

    fn exchange(
        &self,
        endpoint: Url,
        request: OidcTokenExchangeRequest,
    ) -> BoxFuture<'static, Result<OidcTokenResponse, Self::Error>> {
        let requests = Arc::clone(&self.requests);
        let calls = Arc::clone(&self.calls);
        Box::pin(async move {
            assert_eq!(endpoint.as_str(), TOKEN_ENDPOINT);
            calls.fetch_add(1, Ordering::SeqCst);
            requests.lock().await.push(request);
            Ok(OidcTokenResponse::new(Some("signed-id-token".to_owned())))
        })
    }
}

#[derive(Clone)]
struct OversizedIdTokenExchanger;

impl OidcTokenExchanger for OversizedIdTokenExchanger {
    type Error = TestError;

    fn exchange(
        &self,
        _endpoint: Url,
        _request: OidcTokenExchangeRequest,
    ) -> BoxFuture<'static, Result<OidcTokenResponse, Self::Error>> {
        Box::pin(async {
            Ok(OidcTokenResponse::new(Some(
                "x".repeat(MAX_ID_TOKEN_BYTES + 1),
            )))
        })
    }
}

#[derive(Clone, Default)]
struct RecordingVerifier {
    nonces: Arc<AsyncMutex<Vec<String>>>,
}

impl IdTokenVerifier for RecordingVerifier {
    fn verify_id_token(
        &self,
        token: &str,
        expected_nonce: &str,
    ) -> BoxFuture<'static, Result<Principal, AuthError>> {
        let nonces = Arc::clone(&self.nonces);
        let token = token.to_owned();
        let expected_nonce = expected_nonce.to_owned();
        Box::pin(async move {
            assert_eq!(token, "signed-id-token");
            nonces.lock().await.push(expected_nonce);
            Principal::new("alice").map_err(|_| AuthError::RejectedBearerToken)
        })
    }
}

#[derive(Clone)]
struct SequenceGenerator(Arc<StdMutex<VecDeque<String>>>);

impl SequenceGenerator {
    fn new(values: impl IntoIterator<Item = String>) -> Self {
        Self(Arc::new(StdMutex::new(values.into_iter().collect())))
    }
}

impl AuthorizationValueGenerator for SequenceGenerator {
    fn generate(&self) -> String {
        self.0
            .lock()
            .expect("test authorization generator lock must not be poisoned")
            .pop_front()
            .expect("test authorization values must be available")
    }
}

fn provider() -> OidcProviderMetadata {
    serde_json::from_value(serde_json::json!({
        "issuer": ISSUER,
        "authorization_endpoint": AUTHORIZATION_ENDPOINT,
        "token_endpoint": TOKEN_ENDPOINT,
        "jwks_uri": JWKS_URL,
    }))
    .expect("test metadata must deserialize")
}

fn config() -> OidcBrowserConfig {
    OidcBrowserConfig::new(
        Url::parse(ISSUER).expect("test issuer URL must parse"),
        CLIENT_ID,
        Url::parse(REDIRECT_URI).expect("test redirect URL must parse"),
        Url::parse(JWKS_URL).expect("test JWKS URL must parse"),
        OidcClientAuthentication::None,
    )
    .expect("test configuration must be valid")
    .with_scope("profile")
    .expect("test scope must be valid")
}

fn login(
    exchanger: RecordingExchanger,
    verifier: RecordingVerifier,
) -> OidcBrowserLogin<
    InMemoryAuthorizationTransactionStore,
    StaticDiscovery,
    RecordingExchanger,
    RecordingVerifier,
    SequenceGenerator,
> {
    OidcBrowserLogin::new(
        config(),
        InMemoryAuthorizationTransactionStore::default(),
        StaticDiscovery(provider()),
        exchanger,
        verifier,
        SequenceGenerator::new(["s".repeat(43), "n".repeat(43), "v".repeat(43)]),
    )
}

#[path = "tests/diagnostics.rs"]
mod diagnostics;
#[path = "tests/flow.rs"]
mod flow;
#[path = "tests/transaction.rs"]
mod transaction;
