//! OIDC Authorization Code + PKCE browser-login protocol support.
//!
//! This module deliberately stops at a verified [`Principal`]. Applications establish their own
//! Rustee server-side session afterwards, so OAuth tokens never need to be stored in browser
//! cookies or passed to ordinary handlers.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use futures_util::future::BoxFuture;
use http::{HeaderValue, StatusCode, header::LOCATION};
use reqwest::Client;
use rustee_auth::{AuthError, Principal};
use rustee_auth_session::{IssuedSession, SessionManager, SessionStore};
use rustee_core::{Error as HttpError, IntoResponse, Response, empty_body, response};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use url::Url;
use uuid::Uuid;

use crate::IdTokenVerifier;

const DEFAULT_TRANSACTION_TTL: Duration = Duration::from_mins(10);

/// Configuration for one server-side OIDC browser client.
#[derive(Clone, Eq, PartialEq)]
pub struct OidcBrowserConfig {
    issuer: Url,
    client_id: String,
    redirect_uri: Url,
    jwks_url: Url,
    authentication: OidcClientAuthentication,
    scopes: BTreeSet<String>,
    transaction_ttl: Duration,
}

impl OidcBrowserConfig {
    /// Creates a browser-client configuration with the required `openid` scope.
    ///
    /// The configured issuer and endpoints are later compared exactly with discovery metadata.
    /// `jwks_url` binds the browser flow to the same endpoint configured for its ID-token
    /// verifier, rather than accepting a token-selected key endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`OidcBrowserConfigError`] for blank client IDs or invalid HTTPS URLs.
    pub fn new(
        issuer: Url,
        client_id: impl Into<String>,
        redirect_uri: Url,
        jwks_url: Url,
        authentication: OidcClientAuthentication,
    ) -> Result<Self, OidcBrowserConfigError> {
        let client_id = client_id.into();
        if client_id.trim().is_empty() {
            return Err(OidcBrowserConfigError::BlankClientId);
        }
        if !is_valid_issuer_url(&issuer) {
            return Err(OidcBrowserConfigError::InvalidIssuerUrl);
        }
        if !is_valid_https_url(&redirect_uri) {
            return Err(OidcBrowserConfigError::InvalidRedirectUri);
        }
        if !is_valid_https_url(&jwks_url) {
            return Err(OidcBrowserConfigError::InvalidJwksUrl);
        }
        Ok(Self {
            issuer,
            client_id,
            redirect_uri,
            jwks_url,
            authentication,
            scopes: BTreeSet::from(["openid".to_owned()]),
            transaction_ttl: DEFAULT_TRANSACTION_TTL,
        })
    }

    /// Adds one OIDC scope to the authorization request.
    ///
    /// # Errors
    ///
    /// Returns [`OidcBrowserConfigError::InvalidScope`] when a scope is blank or contains
    /// whitespace. Call this separately for every requested scope.
    pub fn with_scope(mut self, scope: impl Into<String>) -> Result<Self, OidcBrowserConfigError> {
        let scope = scope.into();
        if scope.trim().is_empty() || scope.chars().any(char::is_whitespace) {
            return Err(OidcBrowserConfigError::InvalidScope);
        }
        self.scopes.insert(scope);
        Ok(self)
    }

    /// Sets the maximum lifetime of a state/nonce/PKCE authorization transaction.
    ///
    /// # Errors
    ///
    /// Returns [`OidcBrowserConfigError::ZeroTransactionTtl`] for a sub-second or zero TTL.
    pub fn with_transaction_ttl(
        mut self,
        transaction_ttl: Duration,
    ) -> Result<Self, OidcBrowserConfigError> {
        if transaction_ttl.as_secs() == 0 {
            return Err(OidcBrowserConfigError::ZeroTransactionTtl);
        }
        self.transaction_ttl = transaction_ttl;
        Ok(self)
    }

    /// Returns the trusted issuer URL.
    #[must_use]
    pub const fn issuer(&self) -> &Url {
        &self.issuer
    }

    /// Returns the registered OAuth client ID.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the exact registered browser callback URL.
    #[must_use]
    pub const fn redirect_uri(&self) -> &Url {
        &self.redirect_uri
    }

    /// Returns the JWKS URL expected from the OIDC discovery response.
    #[must_use]
    pub const fn jwks_url(&self) -> &Url {
        &self.jwks_url
    }

    /// Returns client authentication used only at the trusted token endpoint.
    #[must_use]
    pub const fn authentication(&self) -> &OidcClientAuthentication {
        &self.authentication
    }

    /// Returns requested scopes in deterministic order.
    pub fn scopes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.scopes.iter().map(String::as_str)
    }

    /// Returns the maximum duration of a pending browser authorization transaction.
    #[must_use]
    pub const fn transaction_ttl(&self) -> Duration {
        self.transaction_ttl
    }
}

impl fmt::Debug for OidcBrowserConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OidcBrowserConfig")
            .field("issuer", &self.issuer)
            .field("client_id", &self.client_id)
            .field("redirect_uri", &self.redirect_uri)
            .field("jwks_url", &self.jwks_url)
            .field("authentication", &self.authentication)
            .field("scopes", &self.scopes)
            .field("transaction_ttl", &self.transaction_ttl)
            .finish()
    }
}

/// Invalid browser-client configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OidcBrowserConfigError {
    /// The client ID was blank.
    #[error("OIDC client ID must not be blank")]
    BlankClientId,
    /// The configured issuer was not an absolute HTTPS issuer URL.
    #[error("OIDC issuer must be an absolute HTTPS URL without credentials, query, or fragment")]
    InvalidIssuerUrl,
    /// The registered redirect URI was invalid for a server-side browser flow.
    #[error("OIDC redirect URI must be an absolute HTTPS URL without credentials or a fragment")]
    InvalidRedirectUri,
    /// The expected JWKS URL was invalid.
    #[error("OIDC JWKS URL must be an absolute HTTPS URL without credentials or a fragment")]
    InvalidJwksUrl,
    /// A requested scope was malformed.
    #[error("OIDC scopes must be non-blank single tokens")]
    InvalidScope,
    /// A pending authorization transaction would expire immediately.
    #[error("OIDC authorization transaction TTL must be at least one second")]
    ZeroTransactionTtl,
    /// A client secret was blank.
    #[error("OIDC client secret must not be blank")]
    BlankClientSecret,
    /// An HTTP timeout was zero.
    #[error("OIDC HTTP timeout must be greater than zero")]
    ZeroHttpTimeout,
    /// A HTTP client could not be constructed.
    #[error("OIDC HTTP client could not be initialized")]
    HttpClientInitialization,
}

/// A secret used only for client authentication at the OIDC token endpoint.
#[derive(Clone, Eq, PartialEq)]
pub struct OidcClientSecret(String);

impl OidcClientSecret {
    /// Stores a non-blank client secret.
    ///
    /// # Errors
    ///
    /// Returns [`OidcBrowserConfigError::BlankClientSecret`] when `value` is blank.
    pub fn new(value: impl Into<String>) -> Result<Self, OidcBrowserConfigError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(OidcBrowserConfigError::BlankClientSecret);
        }
        Ok(Self(value))
    }

    /// Exposes the secret to a trusted custom token-exchanger implementation.
    ///
    /// Callers must not log, serialize, or return this value to an HTTP client.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for OidcClientSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OidcClientSecret([REDACTED])")
    }
}

/// Token-endpoint client authentication selected by the application.
#[derive(Clone, Eq, PartialEq)]
pub enum OidcClientAuthentication {
    /// A public browser client that sends only `client_id` in the token request.
    None,
    /// HTTP Basic authentication as defined for OAuth confidential clients.
    ClientSecretBasic(OidcClientSecret),
    /// A `client_secret` form parameter for providers that explicitly require it.
    ClientSecretPost(OidcClientSecret),
}

impl fmt::Debug for OidcClientAuthentication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => formatter.write_str("OidcClientAuthentication::None"),
            Self::ClientSecretBasic(_) => {
                formatter.write_str("OidcClientAuthentication::ClientSecretBasic([REDACTED])")
            }
            Self::ClientSecretPost(_) => {
                formatter.write_str("OidcClientAuthentication::ClientSecretPost([REDACTED])")
            }
        }
    }
}

/// The OIDC discovery metadata needed for browser authorization-code login.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct OidcProviderMetadata {
    issuer: String,
    authorization_endpoint: Url,
    token_endpoint: Url,
    #[serde(rename = "jwks_uri")]
    jwks_url: Url,
}

impl OidcProviderMetadata {
    /// Returns the provider-declared issuer identifier.
    #[must_use]
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Returns the provider authorization endpoint.
    #[must_use]
    pub const fn authorization_endpoint(&self) -> &Url {
        &self.authorization_endpoint
    }

    /// Returns the provider token endpoint.
    #[must_use]
    pub const fn token_endpoint(&self) -> &Url {
        &self.token_endpoint
    }

    /// Returns the provider JWKS endpoint.
    #[must_use]
    pub const fn jwks_url(&self) -> &Url {
        &self.jwks_url
    }

    fn validate(&self, config: &OidcBrowserConfig) -> Result<(), OidcLoginError> {
        let Ok(issuer) = Url::parse(&self.issuer) else {
            return Err(OidcLoginError::InvalidProviderMetadata);
        };
        if issuer != config.issuer
            || !is_valid_issuer_url(&issuer)
            || !is_valid_https_url(&self.authorization_endpoint)
            || !is_valid_https_url(&self.token_endpoint)
            || !is_valid_https_url(&self.jwks_url)
            || self.jwks_url != config.jwks_url
        {
            return Err(OidcLoginError::InvalidProviderMetadata);
        }
        Ok(())
    }
}

/// Fetches an OIDC discovery document for one configured issuer.
pub trait OidcDiscovery: Clone + Send + Sync + 'static {
    /// Fetcher-specific failure type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Returns provider metadata for the supplied trusted issuer URL.
    fn discover(
        &self,
        issuer: Url,
    ) -> BoxFuture<'static, Result<OidcProviderMetadata, Self::Error>>;
}

/// HTTPS OIDC discovery-document fetcher for production browser login.
#[derive(Clone)]
pub struct HttpOidcDiscovery {
    client: Client,
}

impl HttpOidcDiscovery {
    /// Creates a discovery fetcher with a finite HTTP timeout.
    ///
    /// # Errors
    ///
    /// Returns [`OidcBrowserConfigError`] when the timeout is zero or the client cannot be built.
    pub fn new(timeout: Duration) -> Result<Self, OidcBrowserConfigError> {
        Ok(Self {
            client: http_client(timeout)?,
        })
    }
}

impl fmt::Debug for HttpOidcDiscovery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpOidcDiscovery")
            .finish_non_exhaustive()
    }
}

impl OidcDiscovery for HttpOidcDiscovery {
    type Error = reqwest::Error;

    fn discover(
        &self,
        issuer: Url,
    ) -> BoxFuture<'static, Result<OidcProviderMetadata, Self::Error>> {
        let client = self.client.clone();
        Box::pin(async move {
            client
                .get(discovery_document_url(issuer))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await
        })
    }
}

/// State retained server-side between authorization redirect and callback.
#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub struct PendingAuthorization {
    state: String,
    nonce: String,
    code_verifier: String,
    token_endpoint: Url,
    expires_at_unix_seconds: u64,
}

impl PendingAuthorization {
    /// Returns the opaque state used as the transaction-store key.
    ///
    /// Stores must treat this as a capability value and must not log it.
    #[must_use]
    pub fn state(&self) -> &str {
        &self.state
    }

    fn is_expired(&self) -> bool {
        self.expires_at_unix_seconds <= unix_seconds()
    }

    /// Returns the remaining storage TTL, or `None` when the transaction is expired.
    #[must_use]
    pub fn remaining_ttl_seconds(&self) -> Option<u64> {
        self.expires_at_unix_seconds
            .checked_sub(unix_seconds())
            .filter(|ttl| *ttl > 0)
    }
}

impl fmt::Debug for PendingAuthorization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingAuthorization")
            .field("state", &"[REDACTED]")
            .field("nonce", &"[REDACTED]")
            .field("code_verifier", &"[REDACTED]")
            .field("token_endpoint", &self.token_endpoint)
            .field("expires_at_unix_seconds", &self.expires_at_unix_seconds)
            .finish()
    }
}

/// Durable, atomic state store for one browser authorization transaction.
pub trait AuthorizationTransactionStore: Clone + Send + Sync + 'static {
    /// Store-specific failure type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Saves a newly-created pending authorization transaction.
    fn save(
        &self,
        transaction: PendingAuthorization,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;

    /// Atomically retrieves and consumes a transaction identified by state.
    fn take(
        &self,
        state: String,
    ) -> BoxFuture<'static, Result<Option<PendingAuthorization>, Self::Error>>;
}

/// In-memory authorization transaction store for local development and tests only.
#[derive(Clone, Default)]
pub struct InMemoryAuthorizationTransactionStore {
    transactions: Arc<Mutex<BTreeMap<String, PendingAuthorization>>>,
}

impl fmt::Debug for InMemoryAuthorizationTransactionStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemoryAuthorizationTransactionStore")
            .finish_non_exhaustive()
    }
}

impl AuthorizationTransactionStore for InMemoryAuthorizationTransactionStore {
    type Error = std::convert::Infallible;

    fn save(
        &self,
        transaction: PendingAuthorization,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let transactions = Arc::clone(&self.transactions);
        Box::pin(async move {
            transactions
                .lock()
                .expect("authorization transaction lock must not be poisoned")
                .insert(transaction.state.clone(), transaction);
            Ok(())
        })
    }

    fn take(
        &self,
        state: String,
    ) -> BoxFuture<'static, Result<Option<PendingAuthorization>, Self::Error>> {
        let transactions = Arc::clone(&self.transactions);
        Box::pin(async move {
            Ok(transactions
                .lock()
                .expect("authorization transaction lock must not be poisoned")
                .remove(&state))
        })
    }
}

/// Supplies cryptographically unguessable state, nonce, and PKCE verifier values.
pub trait AuthorizationValueGenerator: Clone + Send + Sync + 'static {
    /// Returns one URL-safe authorization value.
    fn generate(&self) -> String;
}

/// UUID v4-based generator with 244 random bits per generated protocol value.
#[derive(Clone, Copy, Debug, Default)]
pub struct UuidAuthorizationValueGenerator;

impl AuthorizationValueGenerator for UuidAuthorizationValueGenerator {
    fn generate(&self) -> String {
        format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
    }
}

/// A redirect target that a browser login handler returns with HTTP 302 or 303.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizationRedirect {
    location: Url,
}

impl AuthorizationRedirect {
    /// Returns the fully-bound provider authorization URL.
    #[must_use]
    pub const fn location(&self) -> &Url {
        &self.location
    }
}

impl IntoResponse for AuthorizationRedirect {
    fn into_response(self) -> Response {
        let mut response = response(StatusCode::FOUND, empty_body());
        response.headers_mut().insert(
            LOCATION,
            self.location
                .as_str()
                .parse::<HeaderValue>()
                .expect("validated OIDC authorization URL must be a valid Location header"),
        );
        response
    }
}

/// Query values returned by an OIDC authorization callback.
#[derive(Clone, Deserialize)]
pub struct AuthorizationCallback {
    /// The authorization code when the provider accepted login.
    #[serde(default)]
    pub code: Option<String>,
    /// The state returned by the provider and bound to one server-side transaction.
    #[serde(default)]
    pub state: Option<String>,
    /// A provider error code, if authorization was rejected.
    #[serde(default)]
    pub error: Option<String>,
    /// Provider diagnostic text that Rustee intentionally does not expose in responses.
    #[serde(default)]
    pub error_description: Option<String>,
}

impl fmt::Debug for AuthorizationCallback {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationCallback")
            .field("code", &self.code.as_ref().map(|_| "[REDACTED]"))
            .field("state", &self.state.as_ref().map(|_| "[REDACTED]"))
            .field("error", &self.error)
            .field(
                "error_description",
                &self.error_description.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

/// An authorization-code request passed to a trusted token-endpoint adapter.
#[derive(Clone)]
pub struct OidcTokenExchangeRequest {
    client_id: String,
    authentication: OidcClientAuthentication,
    code: String,
    redirect_uri: Url,
    code_verifier: String,
}

impl OidcTokenExchangeRequest {
    /// Returns the configured OAuth client ID.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the configured confidential-client authentication setting.
    #[must_use]
    pub const fn authentication(&self) -> &OidcClientAuthentication {
        &self.authentication
    }

    /// Exposes the provider-issued authorization code only to a trusted exchanger.
    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Returns the exact redirect URI bound to this authorization code.
    #[must_use]
    pub const fn redirect_uri(&self) -> &Url {
        &self.redirect_uri
    }

    /// Exposes the one-time PKCE verifier only to a trusted exchanger.
    #[must_use]
    pub fn code_verifier(&self) -> &str {
        &self.code_verifier
    }
}

impl fmt::Debug for OidcTokenExchangeRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OidcTokenExchangeRequest")
            .field("client_id", &self.client_id)
            .field("authentication", &self.authentication)
            .field("code", &"[REDACTED]")
            .field("redirect_uri", &self.redirect_uri)
            .field("code_verifier", &"[REDACTED]")
            .finish()
    }
}

/// Token-endpoint result limited to the ID token needed to establish a Rustee browser session.
#[derive(Clone)]
pub struct OidcTokenResponse {
    id_token: Option<String>,
}

impl OidcTokenResponse {
    /// Creates a token response from a provider-supplied optional ID token.
    #[must_use]
    pub fn new(id_token: Option<String>) -> Self {
        Self { id_token }
    }

    fn into_id_token(self) -> Option<String> {
        self.id_token.filter(|token| !token.trim().is_empty())
    }
}

impl fmt::Debug for OidcTokenResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OidcTokenResponse")
            .field("id_token", &self.id_token.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

/// Exchanges a verified callback's authorization code at the transaction-bound token endpoint.
pub trait OidcTokenExchanger: Clone + Send + Sync + 'static {
    /// Exchanger-specific failure type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Performs an Authorization Code + PKCE token request.
    fn exchange(
        &self,
        endpoint: Url,
        request: OidcTokenExchangeRequest,
    ) -> BoxFuture<'static, Result<OidcTokenResponse, Self::Error>>;
}

/// HTTPS token-endpoint exchanger for production browser login.
#[derive(Clone)]
pub struct HttpOidcTokenExchanger {
    client: Client,
}

impl HttpOidcTokenExchanger {
    /// Creates a token exchanger with a finite HTTP timeout.
    ///
    /// # Errors
    ///
    /// Returns [`OidcBrowserConfigError`] when the timeout is zero or the client cannot be built.
    pub fn new(timeout: Duration) -> Result<Self, OidcBrowserConfigError> {
        Ok(Self {
            client: http_client(timeout)?,
        })
    }
}

impl fmt::Debug for HttpOidcTokenExchanger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpOidcTokenExchanger")
            .finish_non_exhaustive()
    }
}

impl OidcTokenExchanger for HttpOidcTokenExchanger {
    type Error = reqwest::Error;

    fn exchange(
        &self,
        endpoint: Url,
        request: OidcTokenExchangeRequest,
    ) -> BoxFuture<'static, Result<OidcTokenResponse, Self::Error>> {
        let client = self.client.clone();
        Box::pin(async move {
            let mut form = vec![
                ("grant_type", "authorization_code".to_owned()),
                ("code", request.code),
                ("redirect_uri", request.redirect_uri.into()),
                ("code_verifier", request.code_verifier),
            ];
            let request = match request.authentication {
                OidcClientAuthentication::None => {
                    form.push(("client_id", request.client_id));
                    client.post(endpoint)
                }
                OidcClientAuthentication::ClientSecretPost(secret) => {
                    form.push(("client_id", request.client_id));
                    form.push(("client_secret", secret.expose().to_owned()));
                    client.post(endpoint)
                }
                OidcClientAuthentication::ClientSecretBasic(secret) => {
                    let user = form_encode_component(&request.client_id);
                    let password = form_encode_component(secret.expose());
                    let credentials = STANDARD.encode(format!("{user}:{password}"));
                    client
                        .post(endpoint)
                        .header("authorization", format!("Basic {credentials}"))
                }
            };
            let response: OidcTokenResponseWire = request
                .header("accept", "application/json")
                .form(&form)
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            Ok(OidcTokenResponse::new(response.id_token))
        })
    }
}

#[derive(Deserialize)]
struct OidcTokenResponseWire {
    #[serde(default)]
    id_token: Option<String>,
}

/// Sanitized browser-login failures that are safe to map to application HTTP responses.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OidcLoginError {
    /// The transaction store failed.
    #[error("OIDC authorization transaction service is unavailable")]
    TransactionStoreUnavailable,
    /// Discovery could not be retrieved.
    #[error("OIDC discovery service is unavailable")]
    DiscoveryUnavailable,
    /// Discovery metadata did not match the configured trusted provider.
    #[error("OIDC discovery metadata was rejected")]
    InvalidProviderMetadata,
    /// No matching transaction existed for the returned state.
    #[error("OIDC authorization state was rejected")]
    StateRejected,
    /// The authorization callback arrived after its server-side transaction expired.
    #[error("OIDC authorization transaction expired")]
    TransactionExpired,
    /// Callback content was incomplete or malformed.
    #[error("OIDC authorization callback was rejected")]
    CallbackRejected,
    /// The identity provider denied authorization.
    #[error("OIDC authorization was rejected by the provider")]
    ProviderRejected,
    /// The token endpoint could not be reached or accepted no request.
    #[error("OIDC token endpoint is unavailable")]
    TokenExchangeUnavailable,
    /// The token response was missing an ID token.
    #[error("OIDC token response was rejected")]
    MissingIdToken,
    /// The ID-token verifier could not reach trusted key infrastructure.
    #[error("OIDC identity verification service is unavailable")]
    IdentityProviderUnavailable,
    /// The durable browser session could not be created after successful OIDC verification.
    #[error("OIDC browser session service is unavailable")]
    SessionUnavailable,
    /// The ID token or its transaction-bound nonce was rejected.
    #[error("OIDC identity token was rejected")]
    IdentityTokenRejected,
}

impl OidcLoginError {
    /// Returns whether the failed dependency can be retried by starting a new login transaction.
    #[must_use]
    pub const fn is_provider_unavailable(self) -> bool {
        matches!(
            self,
            Self::TransactionStoreUnavailable
                | Self::DiscoveryUnavailable
                | Self::TokenExchangeUnavailable
                | Self::IdentityProviderUnavailable
                | Self::SessionUnavailable
        )
    }
}

impl IntoResponse for OidcLoginError {
    fn into_response(self) -> Response {
        let (status, code, message) = if self.is_provider_unavailable() {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "oidc_unavailable",
                "OIDC login service is unavailable",
            )
        } else if matches!(self, Self::InvalidProviderMetadata) {
            (
                StatusCode::BAD_GATEWAY,
                "oidc_provider_rejected",
                "OIDC provider metadata was rejected",
            )
        } else {
            (
                StatusCode::BAD_REQUEST,
                "oidc_login_rejected",
                "OIDC login request was rejected",
            )
        };
        HttpError::new(status, code, message).into_response()
    }
}

/// A completed OIDC login containing only a verified application principal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OidcLoginResult {
    principal: Principal,
}

impl OidcLoginResult {
    /// Returns the verified principal suitable for `SessionManager::establish`.
    #[must_use]
    pub fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Consumes the result and returns its verified principal.
    #[must_use]
    pub fn into_principal(self) -> Principal {
        self.principal
    }
}

/// OIDC Authorization Code + PKCE browser-login orchestrator.
#[derive(Clone)]
pub struct OidcBrowserLogin<S, D, E, V, G> {
    config: OidcBrowserConfig,
    transactions: S,
    discovery: D,
    exchanger: E,
    verifier: V,
    generator: G,
}

impl<S, D, E, V, G> OidcBrowserLogin<S, D, E, V, G>
where
    S: AuthorizationTransactionStore,
    D: OidcDiscovery,
    E: OidcTokenExchanger,
    V: IdTokenVerifier,
    G: AuthorizationValueGenerator,
{
    /// Creates a browser-login orchestrator from explicit provider, store, exchange, and verifier
    /// capabilities.
    #[must_use]
    pub fn new(
        config: OidcBrowserConfig,
        transactions: S,
        discovery: D,
        exchanger: E,
        verifier: V,
        generator: G,
    ) -> Self {
        Self {
            config,
            transactions,
            discovery,
            exchanger,
            verifier,
            generator,
        }
    }

    /// Loads trusted discovery metadata, persists a one-time transaction, and builds the provider
    /// authorization redirect.
    ///
    /// # Errors
    ///
    /// Returns a sanitized error when discovery metadata is unavailable or untrusted, when the
    /// transaction store is unavailable, or when the configured value generator is invalid.
    pub async fn begin(&self) -> Result<AuthorizationRedirect, OidcLoginError> {
        let provider = self.discover_provider().await?;
        let state = self.generator.generate();
        let nonce = self.generator.generate();
        let code_verifier = self.generator.generate();
        if !is_valid_authorization_value(&state)
            || !is_valid_authorization_value(&nonce)
            || !is_valid_authorization_value(&code_verifier)
        {
            return Err(OidcLoginError::CallbackRejected);
        }

        let transaction = PendingAuthorization {
            state: state.clone(),
            nonce: nonce.clone(),
            code_verifier: code_verifier.clone(),
            token_endpoint: provider.token_endpoint,
            expires_at_unix_seconds: unix_seconds()
                .saturating_add(self.config.transaction_ttl.as_secs()),
        };
        self.transactions
            .save(transaction)
            .await
            .map_err(|_| OidcLoginError::TransactionStoreUnavailable)?;

        let mut location = provider.authorization_endpoint;
        let scope = self.config.scopes().collect::<Vec<_>>().join(" ");
        location.query_pairs_mut().extend_pairs([
            ("response_type", "code"),
            ("client_id", self.config.client_id()),
            ("redirect_uri", self.config.redirect_uri().as_str()),
            ("scope", &scope),
            ("state", &state),
            ("nonce", &nonce),
            ("code_challenge", &pkce_challenge(&code_verifier)),
            ("code_challenge_method", "S256"),
        ]);
        Ok(AuthorizationRedirect { location })
    }

    /// Atomically consumes one callback state, exchanges its code, and validates the ID-token
    /// nonce before returning a verified principal.
    ///
    /// # Errors
    ///
    /// Returns a sanitized error for an invalid, replayed, expired, or provider-rejected callback;
    /// token-exchange/provider availability failure; or invalid ID token and nonce binding.
    pub async fn complete(
        &self,
        callback: AuthorizationCallback,
    ) -> Result<OidcLoginResult, OidcLoginError> {
        let state = callback.state.ok_or(OidcLoginError::CallbackRejected)?;
        let transaction = self
            .transactions
            .take(state)
            .await
            .map_err(|_| OidcLoginError::TransactionStoreUnavailable)?
            .ok_or(OidcLoginError::StateRejected)?;
        if transaction.is_expired() {
            return Err(OidcLoginError::TransactionExpired);
        }
        if callback.error.is_some() {
            return Err(OidcLoginError::ProviderRejected);
        }
        let code = callback
            .code
            .filter(|code| !code.trim().is_empty())
            .ok_or(OidcLoginError::CallbackRejected)?;
        let token_response = self
            .exchanger
            .exchange(
                transaction.token_endpoint,
                OidcTokenExchangeRequest {
                    client_id: self.config.client_id.clone(),
                    authentication: self.config.authentication.clone(),
                    code,
                    redirect_uri: self.config.redirect_uri.clone(),
                    code_verifier: transaction.code_verifier,
                },
            )
            .await
            .map_err(|_| OidcLoginError::TokenExchangeUnavailable)?;
        let id_token = token_response
            .into_id_token()
            .ok_or(OidcLoginError::MissingIdToken)?;
        let principal = self
            .verifier
            .verify_id_token(&id_token, &transaction.nonce)
            .await
            .map_err(map_id_token_error)?;
        Ok(OidcLoginResult { principal })
    }

    /// Completes a verified callback and establishes a new opaque Rustee browser session.
    ///
    /// The caller applies the returned [`IssuedSession`] to its chosen same-origin success
    /// response; Rustee never accepts a callback-controlled post-login redirect target.
    ///
    /// # Errors
    ///
    /// Returns the same sanitized failures as [`Self::complete`] or
    /// [`OidcLoginError::SessionUnavailable`] when the session store cannot persist the verified
    /// principal.
    pub async fn complete_session<SS>(
        &self,
        callback: AuthorizationCallback,
        sessions: &SessionManager<SS>,
    ) -> Result<IssuedSession, OidcLoginError>
    where
        SS: SessionStore,
    {
        let principal = self.complete(callback).await?.into_principal();
        sessions
            .establish(principal)
            .await
            .map_err(|_| OidcLoginError::SessionUnavailable)
    }

    async fn discover_provider(&self) -> Result<OidcProviderMetadata, OidcLoginError> {
        let provider = self
            .discovery
            .discover(self.config.issuer.clone())
            .await
            .map_err(|_| OidcLoginError::DiscoveryUnavailable)?;
        provider.validate(&self.config)?;
        Ok(provider)
    }
}

impl<S, D, E, V, G> fmt::Debug for OidcBrowserLogin<S, D, E, V, G> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OidcBrowserLogin")
            .field("config", &self.config)
            .field("transaction_store", &std::any::type_name::<S>())
            .field("discovery", &std::any::type_name::<D>())
            .field("exchanger", &std::any::type_name::<E>())
            .field("verifier", &std::any::type_name::<V>())
            .finish_non_exhaustive()
    }
}

fn http_client(timeout: Duration) -> Result<Client, OidcBrowserConfigError> {
    if timeout.is_zero() {
        return Err(OidcBrowserConfigError::ZeroHttpTimeout);
    }
    Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|_| OidcBrowserConfigError::HttpClientInitialization)
}

fn discovery_document_url(mut issuer: Url) -> Url {
    let path = issuer.path().trim_end_matches('/');
    issuer.set_path(&format!("{path}/.well-known/openid-configuration"));
    issuer.set_query(None);
    issuer
}

fn is_valid_issuer_url(url: &Url) -> bool {
    is_valid_https_url(url) && url.query().is_none()
}

fn is_valid_https_url(url: &Url) -> bool {
    url.scheme() == "https"
        && url.host().is_some()
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
}

fn is_valid_authorization_value(value: &str) -> bool {
    (43..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-._~".contains(&byte))
}

fn pkce_challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

fn form_encode_component(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

fn map_id_token_error(error: AuthError) -> OidcLoginError {
    match error {
        AuthError::ProviderUnavailable => OidcLoginError::IdentityProviderUnavailable,
        AuthError::MissingBearerToken
        | AuthError::InvalidBearerToken
        | AuthError::RejectedBearerToken => OidcLoginError::IdentityTokenRejected,
    }
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
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
    use rustee_auth::Principal;
    use rustee_auth_session::{InMemorySessionStore, SessionCookieConfig, SessionManager};
    use rustee_core::IntoResponse;
    use tokio::sync::Mutex as AsyncMutex;

    use super::{
        AuthorizationCallback, AuthorizationTransactionStore, AuthorizationValueGenerator,
        InMemoryAuthorizationTransactionStore, OidcBrowserConfig, OidcBrowserConfigError,
        OidcBrowserLogin, OidcClientAuthentication, OidcDiscovery, OidcLoginError,
        OidcProviderMetadata, OidcTokenExchangeRequest, OidcTokenExchanger, OidcTokenResponse, Url,
        pkce_challenge,
    };
    use crate::{AuthError, IdTokenVerifier};

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

    #[tokio::test]
    async fn begins_pkce_login_and_consumes_state_before_token_exchange() {
        let exchanger = RecordingExchanger::default();
        let verifier = RecordingVerifier::default();
        let login = login(exchanger.clone(), verifier.clone());

        let redirect = login.begin().await.expect("login start must succeed");
        let pairs = redirect
            .location()
            .query_pairs()
            .into_owned()
            .collect::<BTreeMap<_, _>>();
        assert_eq!(redirect.location().origin().ascii_serialization(), ISSUER);
        assert_eq!(pairs.get("response_type"), Some(&"code".to_owned()));
        assert_eq!(pairs.get("client_id"), Some(&CLIENT_ID.to_owned()));
        assert_eq!(pairs.get("redirect_uri"), Some(&REDIRECT_URI.to_owned()));
        assert_eq!(pairs.get("scope"), Some(&"openid profile".to_owned()));
        assert_eq!(pairs.get("state"), Some(&"s".repeat(43)));
        assert_eq!(pairs.get("nonce"), Some(&"n".repeat(43)));
        assert_eq!(pairs.get("code_challenge_method"), Some(&"S256".to_owned()));
        assert_eq!(
            pairs.get("code_challenge"),
            Some(&pkce_challenge(&"v".repeat(43)))
        );
        let redirect_response = redirect.clone().into_response();
        assert_eq!(redirect_response.status(), StatusCode::FOUND);
        assert_eq!(
            redirect_response
                .headers()
                .get(LOCATION)
                .expect("redirect must have Location")
                .to_str()
                .expect("location must be ASCII"),
            redirect.location().as_str()
        );

        let result = login
            .complete(AuthorizationCallback {
                code: Some("one-time-code".to_owned()),
                state: Some("s".repeat(43)),
                error: None,
                error_description: None,
            })
            .await
            .expect("valid callback must establish a principal");
        assert_eq!(result.principal().subject(), "alice");
        assert_eq!(exchanger.calls.load(Ordering::SeqCst), 1);
        assert_eq!(verifier.nonces.lock().await.as_slice(), &["n".repeat(43)]);

        let replay = login
            .complete(AuthorizationCallback {
                code: Some("one-time-code".to_owned()),
                state: Some("s".repeat(43)),
                error: None,
                error_description: None,
            })
            .await;
        assert_eq!(replay.unwrap_err(), OidcLoginError::StateRejected);
        assert_eq!(exchanger.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn provider_error_consumes_a_valid_state_without_exchanging_a_code() {
        let exchanger = RecordingExchanger::default();
        let login = login(exchanger.clone(), RecordingVerifier::default());
        login.begin().await.expect("login start must succeed");

        let rejected = login
            .complete(AuthorizationCallback {
                code: None,
                state: Some("s".repeat(43)),
                error: Some("access_denied".to_owned()),
                error_description: Some("raw provider text".to_owned()),
            })
            .await;

        assert_eq!(rejected.unwrap_err(), OidcLoginError::ProviderRejected);
        assert_eq!(exchanger.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn complete_session_issues_only_the_opaque_session_cookie() {
        let login = login(RecordingExchanger::default(), RecordingVerifier::default());
        let store = InMemorySessionStore::default();
        let sessions = SessionManager::new(
            store,
            SessionCookieConfig::new("rustee_session", 60)
                .expect("test cookie configuration must be valid"),
        );
        login.begin().await.expect("login start must succeed");

        let issued = login
            .complete_session(
                AuthorizationCallback {
                    code: Some("one-time-code".to_owned()),
                    state: Some("s".repeat(43)),
                    error: None,
                    error_description: None,
                },
                &sessions,
            )
            .await
            .expect("verified OIDC callback must create a browser session");
        let mut response = StatusCode::NO_CONTENT.into_response();
        issued.apply_to(&mut response);
        let cookie = response
            .headers()
            .get(SET_COOKIE)
            .expect("session must set a cookie")
            .to_str()
            .expect("cookie header must be valid");

        assert!(cookie.contains("HttpOnly"));
        assert!(cookie.contains("Secure"));
        assert!(!cookie.contains("signed-id-token"));
    }

    #[test]
    fn login_errors_render_sanitized_http_responses() {
        let rejected = OidcLoginError::ProviderRejected.into_response();
        let unavailable = OidcLoginError::TokenExchangeUnavailable.into_response();

        assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
        assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn config_rejects_insecure_urls_and_invalid_scopes() {
        let insecure = OidcBrowserConfig::new(
            Url::parse("http://issuer.example.test").expect("URL must parse"),
            CLIENT_ID,
            Url::parse(REDIRECT_URI).expect("URL must parse"),
            Url::parse(JWKS_URL).expect("URL must parse"),
            OidcClientAuthentication::None,
        );
        let invalid_scope = config().with_scope("profile email");

        assert_eq!(
            insecure.unwrap_err(),
            OidcBrowserConfigError::InvalidIssuerUrl
        );
        assert_eq!(
            invalid_scope.unwrap_err(),
            OidcBrowserConfigError::InvalidScope
        );
    }

    #[tokio::test]
    async fn in_memory_store_has_atomic_take_semantics() {
        let store = InMemoryAuthorizationTransactionStore::default();
        let transaction = super::PendingAuthorization {
            state: "s".repeat(43),
            nonce: "n".repeat(43),
            code_verifier: "v".repeat(43),
            token_endpoint: Url::parse(TOKEN_ENDPOINT).expect("URL must parse"),
            expires_at_unix_seconds: super::unix_seconds() + 60,
        };
        store.save(transaction).await.expect("save must work");
        let first = store.take("s".repeat(43)).await.expect("take must work");
        let second = store.take("s".repeat(43)).await.expect("take must work");

        assert!(first.is_some());
        assert!(second.is_none());
    }
}
