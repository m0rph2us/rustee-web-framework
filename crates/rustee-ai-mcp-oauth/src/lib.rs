//! Bounded OAuth 2.1 authorization support for one Rustee MCP Streamable HTTP resource.
//!
//! This optional adapter keeps user-consent, authorization-code, access-token, and refresh-token
//! lifecycle outside the core MCP transport. It discovers only explicitly selected protected
//! resources, binds authorization and token operations to the canonical resource URI, and never
//! retries the original MCP action after authorization changes.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::future::BoxFuture;
use reqwest::{
    Client, StatusCode,
    header::{ACCEPT, CONTENT_TYPE},
};
use rustee_ai_mcp::{McpHttpConfig, McpHttpConfigError};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use url::{Host, Url};
use uuid::Uuid;

const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_TRANSACTION_TTL: Duration = Duration::from_mins(10);
const MAX_CLIENT_ID_BYTES: usize = 1024;
const MAX_SCOPE_BYTES: usize = 256;
const MAX_TOKEN_BYTES: usize = 16 * 1024;
const MAX_AUTHORIZATION_CODE_BYTES: usize = 16 * 1024;
const MAX_PROVIDER_ERROR_BYTES: usize = 256;
const MAX_DISCOVERY_RESPONSE_BYTES: usize = 512 * 1024;
const MAX_TOKEN_RESPONSE_BYTES: usize = 512 * 1024;
const MAX_WWW_AUTHENTICATE_BYTES: usize = 8192;
const MAX_DISCOVERY_URLS: usize = 3;

/// Explicit public-client settings for one MCP protected resource.
#[derive(Clone, Eq, PartialEq)]
pub struct McpOAuthClientConfig {
    resource: Url,
    client_id: String,
    redirect_uri: Url,
    scopes: BTreeSet<String>,
    http_timeout: Duration,
    transaction_ttl: Duration,
}

impl McpOAuthClientConfig {
    /// Creates one OAuth configuration bound to the exact MCP HTTP endpoint.
    ///
    /// The initial adapter deliberately supports pre-registered public clients only. The
    /// application owns user consent, callback routing, and token persistence.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthConfigError`] for unsafe resource/redirect URLs, a blank client ID, or
    /// an invalid timeout.
    pub fn new(
        resource: Url,
        client_id: impl Into<String>,
        redirect_uri: Url,
    ) -> Result<Self, McpOAuthConfigError> {
        let client_id = client_id.into();
        if !valid_resource_url(&resource) {
            return Err(McpOAuthConfigError::InvalidResourceUrl);
        }
        if !valid_client_id(&client_id) {
            return Err(McpOAuthConfigError::InvalidClientId);
        }
        if !valid_redirect_uri(&redirect_uri) {
            return Err(McpOAuthConfigError::InvalidRedirectUri);
        }
        Ok(Self {
            resource,
            client_id,
            redirect_uri,
            scopes: BTreeSet::new(),
            http_timeout: DEFAULT_HTTP_TIMEOUT,
            transaction_ttl: DEFAULT_TRANSACTION_TTL,
        })
    }

    /// Adds one explicitly selected OAuth scope.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthConfigError::InvalidScope`] for blank, oversized, or whitespace-bearing
    /// values.
    pub fn with_scope(mut self, scope: impl Into<String>) -> Result<Self, McpOAuthConfigError> {
        let scope = scope.into();
        if !valid_scope(&scope) {
            return Err(McpOAuthConfigError::InvalidScope);
        }
        self.scopes.insert(scope);
        Ok(self)
    }

    /// Sets the finite deadline for metadata and token HTTP operations.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthConfigError::ZeroHttpTimeout`] for a zero duration.
    pub fn with_http_timeout(
        mut self,
        http_timeout: Duration,
    ) -> Result<Self, McpOAuthConfigError> {
        if http_timeout.is_zero() {
            return Err(McpOAuthConfigError::ZeroHttpTimeout);
        }
        self.http_timeout = http_timeout;
        Ok(self)
    }

    /// Sets the maximum lifetime of a one-time OAuth state and PKCE verifier transaction.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthConfigError::ZeroTransactionTtl`] when the value is less than one
    /// second. Production transaction stores should enforce the returned TTL as well.
    pub fn with_transaction_ttl(
        mut self,
        transaction_ttl: Duration,
    ) -> Result<Self, McpOAuthConfigError> {
        if transaction_ttl.as_secs() == 0 {
            return Err(McpOAuthConfigError::ZeroTransactionTtl);
        }
        self.transaction_ttl = transaction_ttl;
        Ok(self)
    }

    /// Returns the exact MCP resource URI that tokens must be audience-bound to.
    #[must_use]
    pub const fn resource(&self) -> &Url {
        &self.resource
    }

    /// Returns the pre-registered OAuth public-client identifier.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the exact callback URI registered with the authorization server.
    #[must_use]
    pub const fn redirect_uri(&self) -> &Url {
        &self.redirect_uri
    }

    /// Returns selected scopes in deterministic order.
    pub fn scopes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.scopes.iter().map(String::as_str)
    }

    /// Returns the finite HTTP timeout.
    #[must_use]
    pub const fn http_timeout(&self) -> Duration {
        self.http_timeout
    }

    /// Returns the maximum state/PKCE transaction lifetime.
    #[must_use]
    pub const fn transaction_ttl(&self) -> Duration {
        self.transaction_ttl
    }
}

impl fmt::Debug for McpOAuthClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthClientConfig")
            .field("resource", &self.resource)
            .field("client_id", &self.client_id)
            .field("redirect_uri", &self.redirect_uri)
            .field("scopes", &self.scopes)
            .field("http_timeout", &self.http_timeout)
            .field("transaction_ttl", &self.transaction_ttl)
            .finish()
    }
}

/// Invalid public MCP OAuth client configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum McpOAuthConfigError {
    /// The protected MCP resource was not a clean HTTPS URL or a loopback HTTP test endpoint.
    #[error(
        "MCP OAuth resource must be HTTPS unless loopback, without credentials, query, or fragment"
    )]
    InvalidResourceUrl,
    /// The client identifier was blank or unsafe for an HTTP form request.
    #[error("MCP OAuth client ID must be non-blank, bounded, and free of control characters")]
    InvalidClientId,
    /// The registered callback URI was not HTTPS or loopback HTTP.
    #[error(
        "MCP OAuth redirect URI must be HTTPS or loopback HTTP without credentials or a fragment"
    )]
    InvalidRedirectUri,
    /// A requested OAuth scope was unsafe or exceeded its bounded size.
    #[error("MCP OAuth scope must be a bounded non-blank token without whitespace")]
    InvalidScope,
    /// HTTP metadata and token operations require a finite timeout.
    #[error("MCP OAuth HTTP timeout must be non-zero")]
    ZeroHttpTimeout,
    /// State and PKCE verifier transactions must have a finite lifetime.
    #[error("MCP OAuth authorization transaction TTL must be at least one second")]
    ZeroTransactionTtl,
    /// The application token-slot key was blank, oversized, or unsafe.
    #[error("MCP OAuth token-store key must be bounded and free of control characters")]
    InvalidTokenStoreKey,
}

/// One redacted access token issued for the configured MCP resource.
#[derive(Clone, Eq, PartialEq)]
pub struct McpOAuthAccessToken {
    value: String,
    expires_at: Option<SystemTime>,
}

impl McpOAuthAccessToken {
    /// Creates a bounded opaque bearer token and optional expiry metadata.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthError::InvalidToken`] when the value is blank, oversized, or contains a
    /// control character.
    pub fn new(
        value: impl Into<String>,
        expires_at: Option<SystemTime>,
    ) -> Result<Self, McpOAuthError> {
        let value = value.into();
        if !valid_token(&value) {
            return Err(McpOAuthError::InvalidToken);
        }
        Ok(Self { value, expires_at })
    }

    /// Returns the provider-declared expiry without exposing the bearer token.
    #[must_use]
    pub const fn expires_at(&self) -> Option<SystemTime> {
        self.expires_at
    }

    /// Reports whether the token is expired at the supplied application-controlled instant.
    #[must_use]
    pub fn is_expired_at(&self, now: SystemTime) -> bool {
        self.expires_at.is_some_and(|expires_at| expires_at <= now)
    }

    /// Applies this resource-bound token to a matching MCP HTTP configuration.
    ///
    /// This is explicit by design: a token never alters an existing client or causes a failed
    /// request to be replayed. Applications keep the resulting configuration and token storage
    /// lifecycle within their own authorization boundary.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthError::ResourceMismatch`] for a different MCP endpoint.
    pub fn apply_to_http_config(
        &self,
        config: McpHttpConfig,
        resource: &Url,
    ) -> Result<McpHttpConfig, McpOAuthError> {
        if canonical_resource(config.endpoint()) != canonical_resource(resource) {
            return Err(McpOAuthError::ResourceMismatch);
        }
        config
            .with_bearer_token(self.value.clone())
            .map_err(McpOAuthError::from)
    }
}

impl fmt::Debug for McpOAuthAccessToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthAccessToken")
            .field("value", &"[REDACTED]")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// Sanitized MCP OAuth adapter failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum McpOAuthError {
    /// Token data was blank, oversized, or unsafe for an HTTP bearer header.
    #[error("MCP OAuth token was invalid")]
    InvalidToken,
    /// A token may only be applied to the exact resource it was issued for.
    #[error("MCP OAuth token resource did not match the MCP endpoint")]
    ResourceMismatch,
    /// The underlying MCP configuration rejected an otherwise redacted token handoff.
    #[error("MCP OAuth token could not be applied to the MCP client configuration")]
    HttpConfiguration,
    /// The bounded OAuth HTTP client could not be initialized.
    #[error("MCP OAuth HTTP client could not be initialized")]
    HttpClient,
    /// An authorization metadata request did not complete successfully.
    #[error("MCP OAuth authorization metadata request failed")]
    Transport,
    /// An authorization metadata endpoint returned an unexpected status.
    #[error("MCP OAuth authorization metadata endpoint returned HTTP status {0}")]
    HttpStatus(StatusCode),
    /// Authorization metadata was missing, oversized, malformed, or inconsistent with the resource.
    #[error("MCP OAuth authorization metadata was rejected")]
    InvalidMetadata,
    /// The OAuth challenge did not contain a safe resource metadata URL.
    #[error("MCP OAuth authorization challenge was rejected")]
    InvalidChallenge,
    /// The application-owned atomic authorization transaction service failed.
    #[error("MCP OAuth authorization transaction service is unavailable")]
    TransactionStoreUnavailable,
    /// No matching, unconsumed state existed for the authorization callback.
    #[error("MCP OAuth authorization state was rejected")]
    StateRejected,
    /// The authorization callback arrived after its transaction expired.
    #[error("MCP OAuth authorization transaction expired")]
    TransactionExpired,
    /// Callback data was incomplete, unsafe, or otherwise malformed.
    #[error("MCP OAuth authorization callback was rejected")]
    CallbackRejected,
    /// The authorization server denied the consent or authorization request.
    #[error("MCP OAuth authorization was rejected by the provider")]
    ProviderRejected,
    /// The token endpoint could not be reached or its response could not be accepted.
    #[error("MCP OAuth token exchange was unavailable or rejected")]
    TokenExchangeUnavailable,
    /// The application-owned token store could not complete its operation.
    #[error("MCP OAuth token store is unavailable")]
    TokenStoreUnavailable,
    /// No usable token exists for the requested application-owned key.
    #[error("MCP OAuth token was not available")]
    TokenUnavailable,
    /// The stored access token has expired; the application must explicitly refresh or reauthorize.
    #[error("MCP OAuth access token expired")]
    TokenExpired,
    /// The authorization server did not issue a refresh token for this grant.
    #[error("MCP OAuth refresh token was not available")]
    RefreshTokenUnavailable,
    /// The selected authorization server did not publish an OAuth revocation endpoint.
    #[error("MCP OAuth token revocation was not supported by the authorization server")]
    RevocationUnsupported,
    /// Token revocation could not reach or be accepted by the authorization server.
    #[error("MCP OAuth token revocation was unavailable or rejected")]
    RevocationUnavailable,
}

impl From<McpHttpConfigError> for McpOAuthError {
    fn from(_: McpHttpConfigError) -> Self {
        Self::HttpConfiguration
    }
}

/// Protected-resource metadata discovered for one exact MCP endpoint.
#[derive(Clone, Eq, PartialEq)]
pub struct McpOAuthResourceMetadata {
    resource: Url,
    authorization_servers: Vec<Url>,
    scopes_supported: Vec<String>,
}

impl McpOAuthResourceMetadata {
    /// Returns the resource URI declared by the metadata document.
    #[must_use]
    pub const fn resource(&self) -> &Url {
        &self.resource
    }

    /// Returns explicit authorization-server choices in server-declared order.
    #[must_use]
    pub fn authorization_servers(&self) -> impl ExactSizeIterator<Item = &Url> {
        self.authorization_servers.iter()
    }

    /// Returns declared supported scopes without selecting or requesting them automatically.
    pub fn scopes_supported(&self) -> impl ExactSizeIterator<Item = &str> {
        self.scopes_supported.iter().map(String::as_str)
    }
}

impl fmt::Debug for McpOAuthResourceMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthResourceMetadata")
            .field("resource", &self.resource)
            .field("authorization_servers", &self.authorization_servers)
            .field("scopes_supported", &self.scopes_supported)
            .finish()
    }
}

/// OAuth or `OpenID` Connect authorization-server metadata accepted for MCP PKCE authorization.
#[derive(Clone, Eq, PartialEq)]
pub struct McpOAuthAuthorizationServerMetadata {
    issuer: Url,
    authorization_endpoint: Url,
    token_endpoint: Url,
    revocation_endpoint: Option<Url>,
}

impl McpOAuthAuthorizationServerMetadata {
    /// Creates authorization-server metadata supplied through a separately trusted deployment
    /// configuration. Discovery callers should prefer [`HttpMcpOAuthDiscovery`], which also
    /// verifies the server's advertised PKCE `S256` capability.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthError::InvalidMetadata`] for unsafe issuer or endpoint URLs.
    pub fn new(
        issuer: Url,
        authorization_endpoint: Url,
        token_endpoint: Url,
    ) -> Result<Self, McpOAuthError> {
        if !valid_resource_url(&issuer)
            || !valid_resource_url(&authorization_endpoint)
            || !valid_resource_url(&token_endpoint)
        {
            return Err(McpOAuthError::InvalidMetadata);
        }
        Ok(Self {
            issuer,
            authorization_endpoint,
            token_endpoint,
            revocation_endpoint: None,
        })
    }

    /// Adds the selected server's trusted OAuth revocation endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthError::InvalidMetadata`] for an unsafe endpoint URL.
    pub fn with_revocation_endpoint(
        mut self,
        revocation_endpoint: Url,
    ) -> Result<Self, McpOAuthError> {
        if !valid_resource_url(&revocation_endpoint) {
            return Err(McpOAuthError::InvalidMetadata);
        }
        self.revocation_endpoint = Some(revocation_endpoint);
        Ok(self)
    }

    /// Returns the selected authorization-server issuer.
    #[must_use]
    pub const fn issuer(&self) -> &Url {
        &self.issuer
    }

    /// Returns the trusted authorization endpoint.
    #[must_use]
    pub const fn authorization_endpoint(&self) -> &Url {
        &self.authorization_endpoint
    }

    /// Returns the trusted token endpoint.
    #[must_use]
    pub const fn token_endpoint(&self) -> &Url {
        &self.token_endpoint
    }

    /// Returns the optional trusted OAuth revocation endpoint.
    #[must_use]
    pub const fn revocation_endpoint(&self) -> Option<&Url> {
        self.revocation_endpoint.as_ref()
    }
}

impl fmt::Debug for McpOAuthAuthorizationServerMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthAuthorizationServerMetadata")
            .field("issuer", &self.issuer)
            .field("authorization_endpoint", &self.authorization_endpoint)
            .field("token_endpoint", &self.token_endpoint)
            .field("revocation_endpoint", &self.revocation_endpoint)
            .finish()
    }
}

/// State retained only by the application between an OAuth redirect and its callback.
///
/// The state and verifier are capability values. A durable implementation must encrypt this
/// record at rest, apply [`Self::remaining_ttl_seconds`] as its storage TTL, and make `take`
/// atomic across application instances.
#[derive(Clone, Deserialize, Serialize)]
pub struct McpOAuthPendingAuthorization {
    state: String,
    code_verifier: String,
    token_endpoint: Url,
    resource: Url,
    expires_at_unix_seconds: u64,
}

impl McpOAuthPendingAuthorization {
    /// Returns the opaque state used exclusively as a transaction-store key.
    #[must_use]
    pub fn state(&self) -> &str {
        &self.state
    }

    fn is_expired(&self) -> bool {
        self.expires_at_unix_seconds <= unix_seconds()
    }

    /// Returns the remaining storage TTL, or `None` for an expired transaction.
    #[must_use]
    pub fn remaining_ttl_seconds(&self) -> Option<u64> {
        self.expires_at_unix_seconds
            .checked_sub(unix_seconds())
            .filter(|ttl| *ttl > 0)
    }
}

impl fmt::Debug for McpOAuthPendingAuthorization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthPendingAuthorization")
            .field("state", &"[REDACTED]")
            .field("code_verifier", &"[REDACTED]")
            .field("token_endpoint", &self.token_endpoint)
            .field("resource", &self.resource)
            .field("expires_at_unix_seconds", &self.expires_at_unix_seconds)
            .finish()
    }
}

/// Atomic application-owned storage for one OAuth authorization transaction.
pub trait McpOAuthTransactionStore: Clone + Send + Sync + 'static {
    /// Store-specific failure type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Saves a new short-lived state and PKCE verifier transaction.
    fn save(
        &self,
        transaction: McpOAuthPendingAuthorization,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;

    /// Atomically retrieves and consumes the transaction matching `state`.
    fn take(
        &self,
        state: String,
    ) -> BoxFuture<'static, Result<Option<McpOAuthPendingAuthorization>, Self::Error>>;
}

/// Process-local transaction storage for tests and single-instance local development only.
#[derive(Clone, Default)]
pub struct InMemoryMcpOAuthTransactionStore {
    transactions: Arc<Mutex<BTreeMap<String, McpOAuthPendingAuthorization>>>,
}

impl fmt::Debug for InMemoryMcpOAuthTransactionStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemoryMcpOAuthTransactionStore")
            .finish_non_exhaustive()
    }
}

impl McpOAuthTransactionStore for InMemoryMcpOAuthTransactionStore {
    type Error = std::convert::Infallible;

    fn save(
        &self,
        transaction: McpOAuthPendingAuthorization,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let transactions = Arc::clone(&self.transactions);
        Box::pin(async move {
            transactions
                .lock()
                .expect("MCP OAuth transaction lock must not be poisoned")
                .insert(transaction.state.clone(), transaction);
            Ok(())
        })
    }

    fn take(
        &self,
        state: String,
    ) -> BoxFuture<'static, Result<Option<McpOAuthPendingAuthorization>, Self::Error>> {
        let transactions = Arc::clone(&self.transactions);
        Box::pin(async move {
            Ok(transactions
                .lock()
                .expect("MCP OAuth transaction lock must not be poisoned")
                .remove(&state))
        })
    }
}

/// Supplies URL-safe, high-entropy state and PKCE verifier values.
pub trait McpOAuthValueGenerator: Clone + Send + Sync + 'static {
    /// Returns one independently generated authorization value.
    fn generate(&self) -> String;
}

/// UUID v4-based value generator with 244 random bits for every value.
#[derive(Clone, Copy, Debug, Default)]
pub struct UuidMcpOAuthValueGenerator;

impl McpOAuthValueGenerator for UuidMcpOAuthValueGenerator {
    fn generate(&self) -> String {
        format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
    }
}

/// An authorization URL the application may send to its user agent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpOAuthAuthorizationRedirect {
    location: Url,
}

impl McpOAuthAuthorizationRedirect {
    /// Returns the fully-bound authorization URL.
    #[must_use]
    pub const fn location(&self) -> &Url {
        &self.location
    }
}

/// Query values returned by the configured OAuth callback route.
#[derive(Clone, Deserialize)]
pub struct McpOAuthAuthorizationCallback {
    /// Authorization code supplied after the user grants consent.
    #[serde(default)]
    pub code: Option<String>,
    /// State that selects and consumes exactly one stored transaction.
    #[serde(default)]
    pub state: Option<String>,
    /// Provider result code when authorization was denied.
    #[serde(default)]
    pub error: Option<String>,
    /// Provider diagnostic text. It is deliberately never copied into Rustee errors or logs.
    #[serde(default)]
    pub error_description: Option<String>,
}

impl fmt::Debug for McpOAuthAuthorizationCallback {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthAuthorizationCallback")
            .field("code", &self.code.as_ref().map(|_| "[REDACTED]"))
            .field("state", &self.state.as_ref().map(|_| "[REDACTED]"))
            .field("error", &self.error.as_ref().map(|_| "[REDACTED]"))
            .field(
                "error_description",
                &self.error_description.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

/// A verified authorization-code grant passed only to a trusted token exchanger.
#[derive(Clone)]
pub struct McpOAuthTokenExchangeRequest {
    client_id: String,
    code: String,
    redirect_uri: Url,
    code_verifier: String,
    resource: Url,
}

impl McpOAuthTokenExchangeRequest {
    /// Returns the pre-registered public client ID.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the exact registered callback URI.
    #[must_use]
    pub const fn redirect_uri(&self) -> &Url {
        &self.redirect_uri
    }

    /// Returns the exact MCP resource URI bound to this grant.
    #[must_use]
    pub const fn resource(&self) -> &Url {
        &self.resource
    }

    /// Exposes the authorization code only to a trusted exchanger implementation.
    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Exposes the one-time PKCE verifier only to a trusted exchanger implementation.
    #[must_use]
    pub fn code_verifier(&self) -> &str {
        &self.code_verifier
    }
}

impl fmt::Debug for McpOAuthTokenExchangeRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthTokenExchangeRequest")
            .field("client_id", &self.client_id)
            .field("code", &"[REDACTED]")
            .field("redirect_uri", &self.redirect_uri)
            .field("code_verifier", &"[REDACTED]")
            .field("resource", &self.resource)
            .finish()
    }
}

/// A resource-bound access token and optional refresh token returned by an OAuth token endpoint.
///
/// The value is intentionally opaque. Pass it directly to [`McpOAuthTokenStore`] or use its
/// access token to configure a matching MCP client; only a dedicated encrypted store adapter
/// should call [`Self::into_secrets`].
#[derive(Clone, Eq, PartialEq)]
pub struct McpOAuthTokenSet {
    resource: Url,
    access_token: McpOAuthAccessToken,
    refresh_token: Option<String>,
}

impl McpOAuthTokenSet {
    /// Creates a resource-bound token set from a trusted token-endpoint response.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthError::InvalidToken`] for unsafe access or refresh-token values, and
    /// [`McpOAuthError::InvalidMetadata`] for an unsafe resource URL.
    pub fn new(
        resource: Url,
        access_token: McpOAuthAccessToken,
        refresh_token: Option<String>,
    ) -> Result<Self, McpOAuthError> {
        if !valid_resource_url(&resource) {
            return Err(McpOAuthError::InvalidMetadata);
        }
        if refresh_token
            .as_ref()
            .is_some_and(|token| !valid_token(token))
        {
            return Err(McpOAuthError::InvalidToken);
        }
        Ok(Self {
            resource,
            access_token,
            refresh_token,
        })
    }

    /// Returns the exact MCP protected resource represented by this token set.
    #[must_use]
    pub const fn resource(&self) -> &Url {
        &self.resource
    }

    /// Returns the redacted access-token handle used to configure a matching MCP client.
    #[must_use]
    pub const fn access_token(&self) -> &McpOAuthAccessToken {
        &self.access_token
    }

    /// Applies this token set to a matching MCP HTTP configuration.
    ///
    /// The resource binding comes from the token set itself, so callers cannot accidentally
    /// substitute a different audience URI while configuring the client.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthError::ResourceMismatch`] when the configuration has another endpoint.
    pub fn apply_to_http_config(
        &self,
        config: McpHttpConfig,
    ) -> Result<McpHttpConfig, McpOAuthError> {
        self.access_token
            .apply_to_http_config(config, &self.resource)
    }

    /// Reports whether the provider supplied a refresh token.
    #[must_use]
    pub const fn has_refresh_token(&self) -> bool {
        self.refresh_token.is_some()
    }

    /// Converts the token set to a serializable secret record for a trusted encrypted token-store
    /// adapter. The returned record is not safe for logs, browser sessions, or ordinary caches.
    #[must_use]
    pub fn into_secrets(self) -> McpOAuthTokenSecrets {
        McpOAuthTokenSecrets {
            resource: self.resource,
            access_token: self.access_token.value,
            expires_at_unix_seconds: system_time_to_unix_seconds(self.access_token.expires_at),
            refresh_token: self.refresh_token,
        }
    }

    fn refresh_request(&self, client_id: &str) -> Result<McpOAuthRefreshRequest, McpOAuthError> {
        let refresh_token = self
            .refresh_token
            .clone()
            .ok_or(McpOAuthError::RefreshTokenUnavailable)?;
        Ok(McpOAuthRefreshRequest {
            client_id: client_id.to_owned(),
            refresh_token,
            resource: self.resource.clone(),
        })
    }

    fn revocation_request(&self, client_id: &str) -> McpOAuthRevocationRequest {
        let (token, token_type_hint) = self.refresh_token.as_ref().map_or_else(
            || {
                (
                    self.access_token.value.clone(),
                    McpOAuthRevocationTokenType::AccessToken,
                )
            },
            |token| (token.clone(), McpOAuthRevocationTokenType::RefreshToken),
        );
        McpOAuthRevocationRequest {
            client_id: client_id.to_owned(),
            token,
            token_type_hint,
            resource: self.resource.clone(),
        }
    }
}

impl fmt::Debug for McpOAuthTokenSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthTokenSet")
            .field("resource", &self.resource)
            .field("access_token", &self.access_token)
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

/// Serializable secret payload for a dedicated application-owned encrypted token store.
///
/// This type exposes bearer values solely so a store adapter can encrypt and persist them. It
/// deliberately redacts every secret from `Debug`; it must never be included in ordinary Rustee
/// sessions, audit attributes, logs, URLs, or unencrypted storage.
#[derive(Clone, Deserialize, Serialize)]
pub struct McpOAuthTokenSecrets {
    resource: Url,
    access_token: String,
    expires_at_unix_seconds: Option<u64>,
    refresh_token: Option<String>,
}

impl McpOAuthTokenSecrets {
    /// Returns the resource to which these secrets are bound.
    #[must_use]
    pub const fn resource(&self) -> &Url {
        &self.resource
    }

    /// Returns the bearer value for encryption by a trusted store adapter only.
    #[must_use]
    pub fn access_token_for_encryption(&self) -> &str {
        &self.access_token
    }

    /// Returns the refresh value for encryption by a trusted store adapter only.
    #[must_use]
    pub fn refresh_token_for_encryption(&self) -> Option<&str> {
        self.refresh_token.as_deref()
    }

    /// Returns the provider-declared expiry in Unix seconds.
    #[must_use]
    pub const fn expires_at_unix_seconds(&self) -> Option<u64> {
        self.expires_at_unix_seconds
    }

    /// Restores a validated opaque token set after the application decrypts this record.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthError::InvalidToken`] or [`McpOAuthError::InvalidMetadata`] for a
    /// malformed record.
    pub fn into_token_set(self) -> Result<McpOAuthTokenSet, McpOAuthError> {
        let access_token = McpOAuthAccessToken::new(
            self.access_token,
            self.expires_at_unix_seconds
                .map(unix_seconds_to_system_time),
        )?;
        McpOAuthTokenSet::new(self.resource, access_token, self.refresh_token)
    }
}

impl fmt::Debug for McpOAuthTokenSecrets {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthTokenSecrets")
            .field("resource", &self.resource)
            .field("access_token", &"[REDACTED]")
            .field("expires_at_unix_seconds", &self.expires_at_unix_seconds)
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

/// Application-owned key that identifies one tenant/user/connection token slot.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct McpOAuthTokenStoreKey(String);

impl McpOAuthTokenStoreKey {
    /// Creates an opaque bounded key chosen by the application token-ownership policy.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthConfigError::InvalidTokenStoreKey`] for blank, oversized, or control
    /// character-bearing values.
    pub fn new(value: impl Into<String>) -> Result<Self, McpOAuthConfigError> {
        let value = value.into();
        if value.trim().is_empty()
            || value.len() > MAX_CLIENT_ID_BYTES
            || value.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(McpOAuthConfigError::InvalidTokenStoreKey);
        }
        Ok(Self(value))
    }

    /// Returns this application-owned key for a dedicated token-store adapter.
    ///
    /// Store adapters must treat the value as tenant/user/connection metadata and avoid logging
    /// it with token payloads or provider responses.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for McpOAuthTokenStoreKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpOAuthTokenStoreKey([REDACTED])")
    }
}

/// Encrypted, tenant/user-bound persistence boundary for MCP OAuth token sets.
///
/// Implementations own encryption, key rotation, tenant/user authorization, retention, and
/// cross-instance refresh coordination. The local in-memory implementation is deliberately not
/// a production credential store.
pub trait McpOAuthTokenStore: Clone + Send + Sync + 'static {
    /// Store-specific failure type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Loads the currently persisted token set for one application-owned key.
    fn load(
        &self,
        key: McpOAuthTokenStoreKey,
    ) -> BoxFuture<'static, Result<Option<McpOAuthTokenSet>, Self::Error>>;

    /// Atomically replaces the token set after authorization or a successful refresh.
    fn save(
        &self,
        key: McpOAuthTokenStoreKey,
        tokens: McpOAuthTokenSet,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;

    /// Deletes a revoked or disconnected token set.
    fn remove(&self, key: McpOAuthTokenStoreKey) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// Process-local plain-memory token store for tests and single-instance development only.
#[derive(Clone, Default)]
pub struct InMemoryMcpOAuthTokenStore {
    tokens: Arc<Mutex<BTreeMap<McpOAuthTokenStoreKey, McpOAuthTokenSet>>>,
}

impl fmt::Debug for InMemoryMcpOAuthTokenStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemoryMcpOAuthTokenStore")
            .finish_non_exhaustive()
    }
}

impl McpOAuthTokenStore for InMemoryMcpOAuthTokenStore {
    type Error = std::convert::Infallible;

    fn load(
        &self,
        key: McpOAuthTokenStoreKey,
    ) -> BoxFuture<'static, Result<Option<McpOAuthTokenSet>, Self::Error>> {
        let tokens = Arc::clone(&self.tokens);
        Box::pin(async move {
            Ok(tokens
                .lock()
                .expect("MCP OAuth token lock must not be poisoned")
                .get(&key)
                .cloned())
        })
    }

    fn save(
        &self,
        key: McpOAuthTokenStoreKey,
        token_set: McpOAuthTokenSet,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let tokens = Arc::clone(&self.tokens);
        Box::pin(async move {
            tokens
                .lock()
                .expect("MCP OAuth token lock must not be poisoned")
                .insert(key, token_set);
            Ok(())
        })
    }

    fn remove(&self, key: McpOAuthTokenStoreKey) -> BoxFuture<'static, Result<(), Self::Error>> {
        let tokens = Arc::clone(&self.tokens);
        Box::pin(async move {
            tokens
                .lock()
                .expect("MCP OAuth token lock must not be poisoned")
                .remove(&key);
            Ok(())
        })
    }
}

/// One token refresh grant bound to a configured client and MCP resource.
#[derive(Clone)]
pub struct McpOAuthRefreshRequest {
    client_id: String,
    refresh_token: String,
    resource: Url,
}

impl McpOAuthRefreshRequest {
    /// Returns the pre-registered public client ID.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the exact MCP resource URI that must receive the refreshed token.
    #[must_use]
    pub const fn resource(&self) -> &Url {
        &self.resource
    }

    /// Exposes the refresh token only to a trusted token exchanger implementation.
    #[must_use]
    pub fn refresh_token(&self) -> &str {
        &self.refresh_token
    }
}

impl fmt::Debug for McpOAuthRefreshRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthRefreshRequest")
            .field("client_id", &self.client_id)
            .field("refresh_token", &"[REDACTED]")
            .field("resource", &self.resource)
            .finish()
    }
}

/// Standard OAuth token-type hint supplied to a trusted revocation endpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpOAuthRevocationTokenType {
    /// The request carries an access token because no refresh token was issued.
    AccessToken,
    /// The request carries the refresh token so future grants can be revoked at their root.
    RefreshToken,
}

impl McpOAuthRevocationTokenType {
    const fn as_str(self) -> &'static str {
        match self {
            Self::AccessToken => "access_token",
            Self::RefreshToken => "refresh_token",
        }
    }
}

/// An explicit token-revocation grant bound to one configured MCP resource.
#[derive(Clone)]
pub struct McpOAuthRevocationRequest {
    client_id: String,
    token: String,
    token_type_hint: McpOAuthRevocationTokenType,
    resource: Url,
}

impl McpOAuthRevocationRequest {
    /// Returns the pre-registered public client ID.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the local MCP resource binding used for application policy and audit routing.
    #[must_use]
    pub const fn resource(&self) -> &Url {
        &self.resource
    }

    /// Returns the standardized hint for the token passed to the revocation endpoint.
    #[must_use]
    pub const fn token_type_hint(&self) -> McpOAuthRevocationTokenType {
        self.token_type_hint
    }

    /// Exposes the token only to a trusted revocation adapter implementation.
    #[must_use]
    pub fn token(&self) -> &str {
        &self.token
    }
}

impl fmt::Debug for McpOAuthRevocationRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthRevocationRequest")
            .field("client_id", &self.client_id)
            .field("token", &"[REDACTED]")
            .field("token_type_hint", &self.token_type_hint)
            .field("resource", &self.resource)
            .finish()
    }
}

/// Trusted adapter for an explicit OAuth token revocation request.
pub trait McpOAuthTokenRevoker: Clone + Send + Sync + 'static {
    /// Revoker-specific failure type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Revokes the request token at a selected authorization-server endpoint.
    fn revoke(
        &self,
        endpoint: Url,
        request: McpOAuthRevocationRequest,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// Trusted adapter for authorization-code and refresh-token calls to a selected token endpoint.
pub trait McpOAuthTokenExchanger: Clone + Send + Sync + 'static {
    /// Exchanger-specific failure type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Performs one authorization-code + PKCE token request.
    fn exchange(
        &self,
        endpoint: Url,
        request: McpOAuthTokenExchangeRequest,
    ) -> BoxFuture<'static, Result<McpOAuthTokenSet, Self::Error>>;

    /// Performs one explicit refresh-token request. It never retries an MCP request.
    fn refresh(
        &self,
        endpoint: Url,
        request: McpOAuthRefreshRequest,
    ) -> BoxFuture<'static, Result<McpOAuthTokenSet, Self::Error>>;
}

/// Bounded HTTP exchanger for a pre-registered public OAuth client.
#[derive(Clone)]
pub struct HttpMcpOAuthTokenExchanger {
    client: Client,
}

impl HttpMcpOAuthTokenExchanger {
    /// Creates a token exchanger with the configured finite HTTP timeout.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthError::HttpClient`] if the HTTP client cannot be initialized.
    pub fn new(config: &McpOAuthClientConfig) -> Result<Self, McpOAuthError> {
        let client = Client::builder()
            .timeout(config.http_timeout)
            .build()
            .map_err(|_| McpOAuthError::HttpClient)?;
        Ok(Self { client })
    }
}

impl fmt::Debug for HttpMcpOAuthTokenExchanger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpMcpOAuthTokenExchanger")
            .finish_non_exhaustive()
    }
}

impl McpOAuthTokenExchanger for HttpMcpOAuthTokenExchanger {
    type Error = McpOAuthError;

    fn exchange(
        &self,
        endpoint: Url,
        request: McpOAuthTokenExchangeRequest,
    ) -> BoxFuture<'static, Result<McpOAuthTokenSet, Self::Error>> {
        let client = self.client.clone();
        Box::pin(async move {
            if !valid_resource_url(&endpoint) {
                return Err(McpOAuthError::InvalidMetadata);
            }
            let resource = request.resource.clone();
            let form = vec![
                ("grant_type", "authorization_code".to_owned()),
                ("client_id", request.client_id),
                ("code", request.code),
                ("redirect_uri", request.redirect_uri.into()),
                ("code_verifier", request.code_verifier),
                ("resource", resource.to_string()),
            ];
            fetch_token_set(client, endpoint, form, resource, None).await
        })
    }

    fn refresh(
        &self,
        endpoint: Url,
        request: McpOAuthRefreshRequest,
    ) -> BoxFuture<'static, Result<McpOAuthTokenSet, Self::Error>> {
        let client = self.client.clone();
        Box::pin(async move {
            if !valid_resource_url(&endpoint) {
                return Err(McpOAuthError::InvalidMetadata);
            }
            let resource = request.resource.clone();
            let fallback_refresh_token = request.refresh_token.clone();
            let form = vec![
                ("grant_type", "refresh_token".to_owned()),
                ("client_id", request.client_id),
                ("refresh_token", request.refresh_token),
                ("resource", resource.to_string()),
            ];
            fetch_token_set(
                client,
                endpoint,
                form,
                resource,
                Some(fallback_refresh_token),
            )
            .await
        })
    }
}

impl McpOAuthTokenRevoker for HttpMcpOAuthTokenExchanger {
    type Error = McpOAuthError;

    fn revoke(
        &self,
        endpoint: Url,
        request: McpOAuthRevocationRequest,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let client = self.client.clone();
        Box::pin(async move {
            if !valid_resource_url(&endpoint) {
                return Err(McpOAuthError::InvalidMetadata);
            }
            let form = vec![
                ("client_id", request.client_id),
                ("token", request.token),
                (
                    "token_type_hint",
                    request.token_type_hint.as_str().to_owned(),
                ),
            ];
            let response = client
                .post(endpoint)
                .header(ACCEPT, "application/json")
                .form(&form)
                .send()
                .await
                .map_err(|_| McpOAuthError::RevocationUnavailable)?;
            response
                .status()
                .is_success()
                .then_some(())
                .ok_or(McpOAuthError::RevocationUnavailable)
        })
    }
}

#[derive(Deserialize)]
struct TokenResponseWire {
    access_token: String,
    token_type: String,
    #[serde(default)]
    expires_in: Option<u64>,
    #[serde(default)]
    refresh_token: Option<String>,
}

async fn fetch_token_set(
    client: Client,
    endpoint: Url,
    form: Vec<(&'static str, String)>,
    resource: Url,
    fallback_refresh_token: Option<String>,
) -> Result<McpOAuthTokenSet, McpOAuthError> {
    let response = client
        .post(endpoint)
        .header(ACCEPT, "application/json")
        .form(&form)
        .send()
        .await
        .map_err(|_| McpOAuthError::TokenExchangeUnavailable)?;
    if !response.status().is_success()
        || response
            .content_length()
            .is_some_and(|bytes| bytes > MAX_TOKEN_RESPONSE_BYTES as u64)
        || !response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.to_ascii_lowercase().starts_with("application/json"))
    {
        return Err(McpOAuthError::TokenExchangeUnavailable);
    }
    let body = response
        .bytes()
        .await
        .map_err(|_| McpOAuthError::TokenExchangeUnavailable)?;
    if body.len() > MAX_TOKEN_RESPONSE_BYTES {
        return Err(McpOAuthError::TokenExchangeUnavailable);
    }
    let response: TokenResponseWire =
        serde_json::from_slice(&body).map_err(|_| McpOAuthError::TokenExchangeUnavailable)?;
    if !response.token_type.eq_ignore_ascii_case("bearer") {
        return Err(McpOAuthError::TokenExchangeUnavailable);
    }
    let expires_at = response
        .expires_in
        .and_then(|seconds| SystemTime::now().checked_add(Duration::from_secs(seconds)));
    let access_token = McpOAuthAccessToken::new(response.access_token, expires_at)
        .map_err(|_| McpOAuthError::TokenExchangeUnavailable)?;
    McpOAuthTokenSet::new(
        resource,
        access_token,
        response.refresh_token.or(fallback_refresh_token),
    )
    .map_err(|_| McpOAuthError::TokenExchangeUnavailable)
}

/// Explicit authorization completion and token lifecycle for one selected MCP resource.
///
/// This orchestrator creates no browser session and never replays an MCP action. Applications
/// call [`Self::begin`], route the callback to [`Self::complete`], then persist the result under
/// an application-owned tenant/user key. [`Self::refresh`] is explicit and locally single-flight;
/// distributed stores must provide their own cross-instance refresh serialization.
#[derive(Clone)]
pub struct McpOAuthAuthorizationFlow<S, E, G> {
    config: McpOAuthClientConfig,
    provider: McpOAuthAuthorizationServerMetadata,
    transactions: S,
    exchanger: E,
    generator: G,
    refresh_gate: Arc<tokio::sync::Mutex<()>>,
}

impl<S, E, G> McpOAuthAuthorizationFlow<S, E, G>
where
    S: McpOAuthTransactionStore,
    E: McpOAuthTokenExchanger,
    G: McpOAuthValueGenerator,
{
    /// Creates a flow using explicitly selected, validated authorization-server metadata.
    #[must_use]
    pub fn new(
        config: McpOAuthClientConfig,
        provider: McpOAuthAuthorizationServerMetadata,
        transactions: S,
        exchanger: E,
        generator: G,
    ) -> Self {
        Self {
            config,
            provider,
            transactions,
            exchanger,
            generator,
            refresh_gate: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Stores one state/PKCE transaction and returns the user-consent redirect URL.
    ///
    /// # Errors
    ///
    /// Returns a sanitized failure when the value generator or transaction store is unsuitable.
    pub async fn begin(&self) -> Result<McpOAuthAuthorizationRedirect, McpOAuthError> {
        let state = self.generator.generate();
        let code_verifier = self.generator.generate();
        if !valid_authorization_value(&state) || !valid_authorization_value(&code_verifier) {
            return Err(McpOAuthError::CallbackRejected);
        }
        let transaction = McpOAuthPendingAuthorization {
            state: state.clone(),
            code_verifier: code_verifier.clone(),
            token_endpoint: self.provider.token_endpoint.clone(),
            resource: self.config.resource.clone(),
            expires_at_unix_seconds: unix_seconds()
                .saturating_add(self.config.transaction_ttl.as_secs()),
        };
        self.transactions
            .save(transaction)
            .await
            .map_err(|_| McpOAuthError::TransactionStoreUnavailable)?;

        let mut location = self.provider.authorization_endpoint.clone();
        let scope = self.config.scopes().collect::<Vec<_>>().join(" ");
        let mut query = location.query_pairs_mut();
        query.append_pair("response_type", "code");
        query.append_pair("client_id", &self.config.client_id);
        query.append_pair("redirect_uri", self.config.redirect_uri.as_str());
        query.append_pair("resource", self.config.resource.as_str());
        if !scope.is_empty() {
            query.append_pair("scope", &scope);
        }
        query.append_pair("state", &state);
        query.append_pair("code_challenge", &pkce_challenge(&code_verifier));
        query.append_pair("code_challenge_method", "S256");
        drop(query);
        Ok(McpOAuthAuthorizationRedirect { location })
    }

    /// Atomically consumes one callback state and exchanges its code with PKCE proof.
    ///
    /// # Errors
    ///
    /// Returns a sanitized state, callback, provider, store, or token-exchange failure. A failed
    /// exchange consumes the transaction, so callers must start a new authorization flow.
    pub async fn complete(
        &self,
        callback: McpOAuthAuthorizationCallback,
    ) -> Result<McpOAuthTokenSet, McpOAuthError> {
        let state = callback.state.ok_or(McpOAuthError::CallbackRejected)?;
        if !valid_authorization_value(&state) {
            return Err(McpOAuthError::CallbackRejected);
        }
        let transaction = self
            .transactions
            .take(state)
            .await
            .map_err(|_| McpOAuthError::TransactionStoreUnavailable)?
            .ok_or(McpOAuthError::StateRejected)?;
        if transaction.is_expired() {
            return Err(McpOAuthError::TransactionExpired);
        }
        if callback
            .error_description
            .as_deref()
            .is_some_and(|description| description.len() > MAX_PROVIDER_ERROR_BYTES)
        {
            return Err(McpOAuthError::CallbackRejected);
        }
        if callback
            .error
            .as_deref()
            .is_some_and(|error| !valid_provider_error(error))
        {
            return Err(McpOAuthError::CallbackRejected);
        }
        if callback.error.is_some() {
            return Err(McpOAuthError::ProviderRejected);
        }
        let code = callback.code.ok_or(McpOAuthError::CallbackRejected)?;
        if !valid_authorization_code(&code) {
            return Err(McpOAuthError::CallbackRejected);
        }
        if canonical_resource(&transaction.resource) != canonical_resource(&self.config.resource) {
            return Err(McpOAuthError::StateRejected);
        }
        self.exchanger
            .exchange(
                transaction.token_endpoint,
                McpOAuthTokenExchangeRequest {
                    client_id: self.config.client_id.clone(),
                    code,
                    redirect_uri: self.config.redirect_uri.clone(),
                    code_verifier: transaction.code_verifier,
                    resource: transaction.resource,
                },
            )
            .await
            .map_err(|_| McpOAuthError::TokenExchangeUnavailable)
    }

    /// Persists an authorization result after checking it remains bound to this MCP resource.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthError::ResourceMismatch`] for another resource or
    /// [`McpOAuthError::TokenStoreUnavailable`] when the application store fails.
    pub async fn save<T>(
        &self,
        store: &T,
        key: McpOAuthTokenStoreKey,
        tokens: McpOAuthTokenSet,
    ) -> Result<(), McpOAuthError>
    where
        T: McpOAuthTokenStore,
    {
        self.ensure_resource(tokens.resource())?;
        store
            .save(key, tokens)
            .await
            .map_err(|_| McpOAuthError::TokenStoreUnavailable)
    }

    /// Loads a current token without implicitly refreshing it.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthError::TokenUnavailable`] when the key has no token,
    /// [`McpOAuthError::TokenExpired`] when its access token is expired,
    /// [`McpOAuthError::ResourceMismatch`] for another resource, or
    /// [`McpOAuthError::TokenStoreUnavailable`] when the application store fails.
    pub async fn load_current<T>(
        &self,
        store: &T,
        key: McpOAuthTokenStoreKey,
        now: SystemTime,
    ) -> Result<McpOAuthTokenSet, McpOAuthError>
    where
        T: McpOAuthTokenStore,
    {
        let tokens = store
            .load(key)
            .await
            .map_err(|_| McpOAuthError::TokenStoreUnavailable)?
            .ok_or(McpOAuthError::TokenUnavailable)?;
        self.ensure_resource(tokens.resource())?;
        if tokens.access_token.is_expired_at(now) {
            return Err(McpOAuthError::TokenExpired);
        }
        Ok(tokens)
    }

    /// Explicitly refreshes and atomically replaces one stored token set.
    ///
    /// This method is single-flight only within this flow instance. It does not infer scopes,
    /// retry an MCP action, or decide whether a remote 401/403 should trigger refresh.
    ///
    /// # Errors
    ///
    /// Returns a sanitized unavailable, missing-token, missing-refresh-token, resource-mismatch,
    /// or token-exchange failure. A failed refresh preserves the stored token set.
    pub async fn refresh<T>(
        &self,
        store: &T,
        key: McpOAuthTokenStoreKey,
    ) -> Result<McpOAuthTokenSet, McpOAuthError>
    where
        T: McpOAuthTokenStore,
    {
        let _refresh_guard = self.refresh_gate.lock().await;
        let current = store
            .load(key.clone())
            .await
            .map_err(|_| McpOAuthError::TokenStoreUnavailable)?
            .ok_or(McpOAuthError::TokenUnavailable)?;
        self.ensure_resource(current.resource())?;
        let request = current.refresh_request(&self.config.client_id)?;
        let refreshed = self
            .exchanger
            .refresh(self.provider.token_endpoint.clone(), request)
            .await
            .map_err(|_| McpOAuthError::TokenExchangeUnavailable)?;
        self.ensure_resource(refreshed.resource())?;
        store
            .save(key, refreshed.clone())
            .await
            .map_err(|_| McpOAuthError::TokenStoreUnavailable)?;
        Ok(refreshed)
    }

    /// Explicitly revokes a stored token at a published endpoint, then removes its local record.
    ///
    /// The token remains stored if remote revocation cannot be confirmed, so the application can
    /// surface a retryable disconnect outcome or deliberately remove it under its own incident
    /// policy. This method never retries an MCP action.
    ///
    /// # Errors
    ///
    /// Returns a sanitized missing-token, resource, local-store, unsupported-endpoint, or remote
    /// revocation failure. A successful remote revocation followed by a failed local delete returns
    /// [`McpOAuthError::TokenStoreUnavailable`] because the application must reconcile the record.
    pub async fn revoke_and_remove<T, R>(
        &self,
        store: &T,
        key: McpOAuthTokenStoreKey,
        revoker: &R,
    ) -> Result<(), McpOAuthError>
    where
        T: McpOAuthTokenStore,
        R: McpOAuthTokenRevoker,
    {
        let endpoint = self
            .provider
            .revocation_endpoint
            .clone()
            .ok_or(McpOAuthError::RevocationUnsupported)?;
        let tokens = store
            .load(key.clone())
            .await
            .map_err(|_| McpOAuthError::TokenStoreUnavailable)?
            .ok_or(McpOAuthError::TokenUnavailable)?;
        self.ensure_resource(tokens.resource())?;
        let request = tokens.revocation_request(&self.config.client_id);
        revoker
            .revoke(endpoint, request)
            .await
            .map_err(|_| McpOAuthError::RevocationUnavailable)?;
        store
            .remove(key)
            .await
            .map_err(|_| McpOAuthError::TokenStoreUnavailable)
    }

    fn ensure_resource(&self, resource: &Url) -> Result<(), McpOAuthError> {
        (canonical_resource(resource) == canonical_resource(&self.config.resource))
            .then_some(())
            .ok_or(McpOAuthError::ResourceMismatch)
    }
}

impl<S, E, G> fmt::Debug for McpOAuthAuthorizationFlow<S, E, G> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthAuthorizationFlow")
            .field("config", &self.config)
            .field("provider", &self.provider)
            .field("transaction_store", &std::any::type_name::<S>())
            .field("exchanger", &std::any::type_name::<E>())
            .finish_non_exhaustive()
    }
}

/// Bounded HTTP discovery adapter for MCP protected-resource and authorization-server metadata.
#[derive(Clone)]
pub struct HttpMcpOAuthDiscovery {
    client: Client,
    resource: Url,
}

impl HttpMcpOAuthDiscovery {
    /// Creates discovery for one configured MCP protected resource.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthError::HttpClient`] if the finite HTTP client cannot be created.
    pub fn new(config: &McpOAuthClientConfig) -> Result<Self, McpOAuthError> {
        let client = Client::builder()
            .timeout(config.http_timeout)
            .build()
            .map_err(|_| McpOAuthError::HttpClient)?;
        Ok(Self {
            client,
            resource: config.resource.clone(),
        })
    }

    /// Discovers protected-resource metadata from an optional 401 challenge or well-known URIs.
    ///
    /// A provided `WWW-Authenticate` value is used only for its bounded Bearer
    /// `resource_metadata` parameter. No scopes are automatically accepted or requested.
    ///
    /// # Errors
    ///
    /// Returns a sanitized challenge, transport, HTTP-status, response-bound, or metadata error.
    pub async fn discover_resource_metadata(
        &self,
        www_authenticate: Option<&str>,
    ) -> Result<McpOAuthResourceMetadata, McpOAuthError> {
        let candidates = resource_metadata_urls(&self.resource, www_authenticate)?;
        for url in candidates {
            match self.fetch_json::<ResourceMetadataWire>(&url).await {
                Ok(metadata) => return metadata.into_public(&self.resource),
                Err(McpOAuthError::HttpStatus(StatusCode::NOT_FOUND)) => {}
                Err(error) => return Err(error),
            }
        }
        Err(McpOAuthError::InvalidMetadata)
    }

    /// Discovers OAuth authorization-server metadata, then `OpenID` Connect metadata as fallback.
    ///
    /// The result is accepted only when it declares PKCE `S256` support and HTTPS (or loopback
    /// test) authorization and token endpoints.
    ///
    /// # Errors
    ///
    /// Returns a sanitized transport, HTTP-status, response-bound, or metadata error.
    pub async fn discover_authorization_server(
        &self,
        issuer: &Url,
    ) -> Result<McpOAuthAuthorizationServerMetadata, McpOAuthError> {
        for url in authorization_server_metadata_urls(issuer)? {
            match self
                .fetch_json::<AuthorizationServerMetadataWire>(&url)
                .await
            {
                Ok(metadata) => return metadata.into_public(issuer),
                Err(McpOAuthError::HttpStatus(StatusCode::NOT_FOUND)) => {}
                Err(error) => return Err(error),
            }
        }
        Err(McpOAuthError::InvalidMetadata)
    }

    async fn fetch_json<T: DeserializeOwned>(&self, url: &Url) -> Result<T, McpOAuthError> {
        let response = self
            .client
            .get(url.clone())
            .header(ACCEPT, "application/json")
            .send()
            .await
            .map_err(|_| McpOAuthError::Transport)?;
        if !response.status().is_success() {
            return Err(McpOAuthError::HttpStatus(response.status()));
        }
        let is_json = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.to_ascii_lowercase().starts_with("application/json"));
        if !is_json
            || response
                .content_length()
                .is_some_and(|bytes| bytes > MAX_DISCOVERY_RESPONSE_BYTES as u64)
        {
            return Err(McpOAuthError::InvalidMetadata);
        }
        let body = response
            .bytes()
            .await
            .map_err(|_| McpOAuthError::Transport)?;
        if body.len() > MAX_DISCOVERY_RESPONSE_BYTES {
            return Err(McpOAuthError::InvalidMetadata);
        }
        serde_json::from_slice(&body).map_err(|_| McpOAuthError::InvalidMetadata)
    }
}

impl fmt::Debug for HttpMcpOAuthDiscovery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpMcpOAuthDiscovery")
            .field("resource", &self.resource)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
struct ResourceMetadataWire {
    resource: Url,
    authorization_servers: Vec<Url>,
    #[serde(default)]
    scopes_supported: Vec<String>,
}

impl ResourceMetadataWire {
    fn into_public(
        self,
        expected_resource: &Url,
    ) -> Result<McpOAuthResourceMetadata, McpOAuthError> {
        if canonical_resource(&self.resource) != canonical_resource(expected_resource)
            || !valid_resource_url(&self.resource)
            || self.authorization_servers.is_empty()
            || self.authorization_servers.len() > MAX_DISCOVERY_URLS
            || self
                .authorization_servers
                .iter()
                .any(|url| !valid_resource_url(url))
            || self
                .scopes_supported
                .iter()
                .any(|scope| !valid_scope(scope))
        {
            return Err(McpOAuthError::InvalidMetadata);
        }
        Ok(McpOAuthResourceMetadata {
            resource: self.resource,
            authorization_servers: self.authorization_servers,
            scopes_supported: self.scopes_supported,
        })
    }
}

#[derive(Deserialize)]
struct AuthorizationServerMetadataWire {
    issuer: Url,
    authorization_endpoint: Url,
    token_endpoint: Url,
    #[serde(default)]
    revocation_endpoint: Option<Url>,
    #[serde(default)]
    code_challenge_methods_supported: Vec<String>,
}

impl AuthorizationServerMetadataWire {
    fn into_public(
        self,
        expected_issuer: &Url,
    ) -> Result<McpOAuthAuthorizationServerMetadata, McpOAuthError> {
        if canonical_resource(&self.issuer) != canonical_resource(expected_issuer)
            || !valid_resource_url(&self.issuer)
            || !valid_resource_url(&self.authorization_endpoint)
            || !valid_resource_url(&self.token_endpoint)
            || self
                .revocation_endpoint
                .as_ref()
                .is_some_and(|endpoint| !valid_resource_url(endpoint))
            || !self
                .code_challenge_methods_supported
                .iter()
                .any(|method| method == "S256")
        {
            return Err(McpOAuthError::InvalidMetadata);
        }
        Ok(McpOAuthAuthorizationServerMetadata {
            issuer: self.issuer,
            authorization_endpoint: self.authorization_endpoint,
            token_endpoint: self.token_endpoint,
            revocation_endpoint: self.revocation_endpoint,
        })
    }
}

fn resource_metadata_urls(
    resource: &Url,
    www_authenticate: Option<&str>,
) -> Result<Vec<Url>, McpOAuthError> {
    if let Some(header) = www_authenticate {
        if header.len() > MAX_WWW_AUTHENTICATE_BYTES {
            return Err(McpOAuthError::InvalidChallenge);
        }
        if let Some(url) = bearer_parameter(header, "resource_metadata") {
            let url = Url::parse(url).map_err(|_| McpOAuthError::InvalidChallenge)?;
            if !valid_resource_url(&url) {
                return Err(McpOAuthError::InvalidChallenge);
            }
            return Ok(vec![url]);
        }
    }
    let path = resource.path().trim_start_matches('/');
    let mut path_specific = resource.clone();
    path_specific.set_path(&format!("/.well-known/oauth-protected-resource/{path}"));
    path_specific.set_query(None);
    path_specific.set_fragment(None);
    let mut root = resource.clone();
    root.set_path("/.well-known/oauth-protected-resource");
    root.set_query(None);
    root.set_fragment(None);
    Ok((path_specific != root)
        .then_some(vec![path_specific, root.clone()])
        .unwrap_or_else(|| vec![root]))
}

fn authorization_server_metadata_urls(issuer: &Url) -> Result<Vec<Url>, McpOAuthError> {
    if !valid_resource_url(issuer) {
        return Err(McpOAuthError::InvalidMetadata);
    }
    let path = issuer.path().trim_matches('/');
    let oauth_path = if path.is_empty() {
        "/.well-known/oauth-authorization-server".to_owned()
    } else {
        format!("/.well-known/oauth-authorization-server/{path}")
    };
    let mut oauth = issuer.clone();
    oauth.set_path(&oauth_path);
    oauth.set_query(None);
    oauth.set_fragment(None);
    let oidc_inserted_path = if path.is_empty() {
        "/.well-known/openid-configuration".to_owned()
    } else {
        format!("/.well-known/openid-configuration/{path}")
    };
    let mut oidc_inserted = issuer.clone();
    oidc_inserted.set_path(&oidc_inserted_path);
    oidc_inserted.set_query(None);
    oidc_inserted.set_fragment(None);
    if path.is_empty() {
        return Ok(vec![oauth, oidc_inserted]);
    }
    let mut oidc_appended = issuer.clone();
    oidc_appended.set_path(&format!("/{path}/.well-known/openid-configuration"));
    oidc_appended.set_query(None);
    oidc_appended.set_fragment(None);
    Ok(vec![oauth, oidc_inserted, oidc_appended])
}

fn bearer_parameter<'a>(header: &'a str, name: &str) -> Option<&'a str> {
    let bearer = header.split(',').map(str::trim).find(|part| {
        part.eq_ignore_ascii_case("bearer") || part.to_ascii_lowercase().starts_with("bearer ")
    })?;
    let parameters = bearer
        .strip_prefix("Bearer")
        .or_else(|| bearer.strip_prefix("bearer"))?;
    parameters.split(',').find_map(|parameter| {
        let (key, value) = parameter.trim().split_once('=')?;
        (key.trim().eq_ignore_ascii_case(name))
            .then(|| value.trim().strip_prefix('"')?.strip_suffix('"'))
            .flatten()
    })
}

fn canonical_resource(value: &Url) -> String {
    let mut value = value.clone();
    value.set_fragment(None);
    value.set_query(None);
    value.to_string()
}

fn valid_resource_url(value: &Url) -> bool {
    matches!(value.scheme(), "http" | "https")
        && value.host().is_some()
        && value.username().is_empty()
        && value.password().is_none()
        && value.query().is_none()
        && value.fragment().is_none()
        && (value.scheme() == "https" || is_loopback_host(value.host().as_ref()))
}

fn valid_redirect_uri(value: &Url) -> bool {
    matches!(value.scheme(), "http" | "https")
        && value.host().is_some()
        && value.username().is_empty()
        && value.password().is_none()
        && value.fragment().is_none()
        && (value.scheme() == "https" || is_loopback_host(value.host().as_ref()))
}

fn is_loopback_host(host: Option<&Host<&str>>) -> bool {
    match host {
        Some(Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(host)) => host.is_loopback(),
        Some(Host::Ipv6(host)) => host.is_loopback(),
        None => false,
    }
}

fn valid_client_id(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_CLIENT_ID_BYTES
        && value.bytes().all(|byte| !byte.is_ascii_control())
}

fn valid_scope(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SCOPE_BYTES
        && value
            .bytes()
            .all(|byte| !byte.is_ascii_whitespace() && !byte.is_ascii_control())
}

fn valid_token(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_TOKEN_BYTES
        && value.bytes().all(|byte| !byte.is_ascii_control())
}

fn valid_authorization_value(value: &str) -> bool {
    (43..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-._~".contains(&byte))
}

fn valid_authorization_code(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_AUTHORIZATION_CODE_BYTES
        && value.bytes().all(|byte| !byte.is_ascii_control())
}

fn valid_provider_error(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_PROVIDER_ERROR_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-._".contains(&byte))
}

fn pkce_challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn system_time_to_unix_seconds(value: Option<SystemTime>) -> Option<u64> {
    value.and_then(|time| {
        time.duration_since(UNIX_EPOCH)
            .ok()
            .map(|value| value.as_secs())
    })
}

fn unix_seconds_to_system_time(value: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(value)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
        sync::{
            Arc, Mutex as StdMutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, SystemTime},
    };

    use futures_util::future::BoxFuture;
    use rustee_ai_mcp::McpHttpConfig;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::Mutex as AsyncMutex,
    };
    use url::Url;

    use super::{
        AuthorizationServerMetadataWire, HttpMcpOAuthTokenExchanger, InMemoryMcpOAuthTokenStore,
        InMemoryMcpOAuthTransactionStore, McpOAuthAccessToken, McpOAuthAuthorizationCallback,
        McpOAuthAuthorizationFlow, McpOAuthAuthorizationServerMetadata, McpOAuthClientConfig,
        McpOAuthConfigError, McpOAuthError, McpOAuthRefreshRequest, McpOAuthRevocationRequest,
        McpOAuthRevocationTokenType, McpOAuthTokenExchangeRequest, McpOAuthTokenExchanger,
        McpOAuthTokenRevoker, McpOAuthTokenSet, McpOAuthTokenStore, McpOAuthTokenStoreKey,
        McpOAuthValueGenerator, UuidMcpOAuthValueGenerator, authorization_server_metadata_urls,
        pkce_challenge, resource_metadata_urls,
    };

    const RESOURCE: &str = "https://mcp.example.test/mcp";
    const CLIENT_ID: &str = "rustee-mcp-client";
    const REDIRECT_URI: &str = "https://app.example.test/mcp/callback";
    const ISSUER: &str = "https://auth.example.test";
    const AUTHORIZATION_ENDPOINT: &str = "https://auth.example.test/authorize";
    const TOKEN_ENDPOINT: &str = "https://auth.example.test/token";

    #[derive(Clone, Debug, thiserror::Error)]
    #[error("test token service failure")]
    struct TestError;

    #[derive(Clone)]
    struct SequenceGenerator(Arc<StdMutex<VecDeque<String>>>);

    impl SequenceGenerator {
        fn new(values: impl IntoIterator<Item = String>) -> Self {
            Self(Arc::new(StdMutex::new(values.into_iter().collect())))
        }
    }

    impl McpOAuthValueGenerator for SequenceGenerator {
        fn generate(&self) -> String {
            self.0
                .lock()
                .expect("test OAuth value generator lock must not be poisoned")
                .pop_front()
                .expect("test OAuth values must be available")
        }
    }

    #[derive(Clone, Default)]
    struct RecordingExchanger {
        exchange_requests: Arc<AsyncMutex<Vec<McpOAuthTokenExchangeRequest>>>,
        refresh_requests: Arc<AsyncMutex<Vec<McpOAuthRefreshRequest>>>,
        exchange_calls: Arc<AtomicUsize>,
        refresh_calls: Arc<AtomicUsize>,
    }

    #[derive(Clone, Default)]
    struct RecordingRevoker {
        requests: Arc<AsyncMutex<Vec<McpOAuthRevocationRequest>>>,
        calls: Arc<AtomicUsize>,
    }

    impl McpOAuthTokenRevoker for RecordingRevoker {
        type Error = TestError;

        fn revoke(
            &self,
            endpoint: Url,
            request: McpOAuthRevocationRequest,
        ) -> BoxFuture<'static, Result<(), Self::Error>> {
            let requests = Arc::clone(&self.requests);
            let calls = Arc::clone(&self.calls);
            Box::pin(async move {
                assert_eq!(endpoint.as_str(), "https://auth.example.test/revoke");
                calls.fetch_add(1, Ordering::SeqCst);
                requests.lock().await.push(request);
                Ok(())
            })
        }
    }

    impl McpOAuthTokenExchanger for RecordingExchanger {
        type Error = TestError;

        fn exchange(
            &self,
            endpoint: Url,
            request: McpOAuthTokenExchangeRequest,
        ) -> BoxFuture<'static, Result<McpOAuthTokenSet, Self::Error>> {
            let requests = Arc::clone(&self.exchange_requests);
            let calls = Arc::clone(&self.exchange_calls);
            Box::pin(async move {
                assert_eq!(endpoint.as_str(), TOKEN_ENDPOINT);
                let resource = request.resource().clone();
                calls.fetch_add(1, Ordering::SeqCst);
                requests.lock().await.push(request);
                token_set(
                    resource,
                    "initial-access-token",
                    Some("initial-refresh-token".to_owned()),
                )
            })
        }

        fn refresh(
            &self,
            endpoint: Url,
            request: McpOAuthRefreshRequest,
        ) -> BoxFuture<'static, Result<McpOAuthTokenSet, Self::Error>> {
            let requests = Arc::clone(&self.refresh_requests);
            let calls = Arc::clone(&self.refresh_calls);
            Box::pin(async move {
                assert_eq!(endpoint.as_str(), TOKEN_ENDPOINT);
                let resource = request.resource().clone();
                calls.fetch_add(1, Ordering::SeqCst);
                requests.lock().await.push(request);
                token_set(
                    resource,
                    "refreshed-access-token",
                    Some("rotated-refresh-token".to_owned()),
                )
            })
        }
    }

    fn token_set(
        resource: Url,
        access_token: &str,
        refresh_token: Option<String>,
    ) -> Result<McpOAuthTokenSet, TestError> {
        let access_token =
            McpOAuthAccessToken::new(access_token, None).expect("test access token must be valid");
        McpOAuthTokenSet::new(resource, access_token, refresh_token).map_err(|_| TestError)
    }

    fn config() -> McpOAuthClientConfig {
        McpOAuthClientConfig::new(
            Url::parse(RESOURCE).expect("test resource must parse"),
            CLIENT_ID,
            Url::parse(REDIRECT_URI).expect("test redirect URI must parse"),
        )
        .expect("test client configuration must be valid")
        .with_scope("orders:read")
        .expect("test scope must be valid")
    }

    fn provider() -> McpOAuthAuthorizationServerMetadata {
        McpOAuthAuthorizationServerMetadata::new(
            Url::parse(ISSUER).expect("test issuer must parse"),
            Url::parse(AUTHORIZATION_ENDPOINT).expect("test authorization endpoint must parse"),
            Url::parse(TOKEN_ENDPOINT).expect("test token endpoint must parse"),
        )
        .expect("test provider metadata must be valid")
    }

    fn provider_with_revocation() -> McpOAuthAuthorizationServerMetadata {
        provider()
            .with_revocation_endpoint(
                Url::parse("https://auth.example.test/revoke")
                    .expect("test revocation endpoint must parse"),
            )
            .expect("test revocation endpoint must be valid")
    }

    fn flow(
        exchanger: RecordingExchanger,
    ) -> McpOAuthAuthorizationFlow<
        InMemoryMcpOAuthTransactionStore,
        RecordingExchanger,
        SequenceGenerator,
    > {
        McpOAuthAuthorizationFlow::new(
            config(),
            provider(),
            InMemoryMcpOAuthTransactionStore::default(),
            exchanger,
            SequenceGenerator::new(["s".repeat(43), "v".repeat(43)]),
        )
    }

    async fn token_endpoint_once(body: &str) -> (Url, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test token endpoint must bind loopback");
        let address = listener
            .local_addr()
            .expect("test token endpoint must expose its address");
        let body = body.to_owned();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener
                .accept()
                .await
                .expect("test token endpoint must receive one connection");
            let mut bytes = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                let read = socket
                    .read(&mut buffer)
                    .await
                    .expect("test token endpoint must read request");
                assert!(read > 0, "test client must send a complete request");
                bytes.extend_from_slice(&buffer[..read]);
                let Some(header_end) = bytes.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                else {
                    continue;
                };
                let headers = std::str::from_utf8(&bytes[..header_end])
                    .expect("test request headers must be UTF-8");
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':')
                            .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                            .map(|(_, value)| {
                                value
                                    .trim()
                                    .parse::<usize>()
                                    .expect("content length must parse")
                            })
                    })
                    .expect("test token request must contain a content length");
                if bytes.len() >= header_end + 4 + content_length {
                    break;
                }
            }
            let request = String::from_utf8(bytes).expect("test request must be UTF-8");
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("test token endpoint must respond");
            request
        });
        (
            Url::parse(&format!("http://{address}/token"))
                .expect("loopback token endpoint URL must parse"),
            task,
        )
    }

    #[test]
    fn client_configuration_requires_safe_resource_redirect_and_scopes() {
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
            config.clone().with_scope("orders read").unwrap_err(),
            McpOAuthConfigError::InvalidScope
        );
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
    }

    #[test]
    fn access_token_is_redacted_resource_bound_and_expiry_aware() {
        let resource = Url::parse("https://mcp.example.test/mcp").unwrap();
        let token = McpOAuthAccessToken::new(
            "mcp-access-token",
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(10)),
        )
        .unwrap();
        assert!(!format!("{token:?}").contains("mcp-access-token"));
        assert!(token.is_expired_at(SystemTime::UNIX_EPOCH + Duration::from_secs(11)));

        let config = McpHttpConfig::new(resource.clone()).unwrap();
        let config = token.apply_to_http_config(config, &resource).unwrap();
        assert!(!format!("{config:?}").contains("mcp-access-token"));

        let other = Url::parse("https://other.example.test/mcp").unwrap();
        assert_eq!(
            token
                .apply_to_http_config(McpHttpConfig::new(other.clone()).unwrap(), &resource)
                .unwrap_err(),
            McpOAuthError::ResourceMismatch
        );
    }

    #[test]
    fn discovery_urls_follow_mcp_protected_resource_and_issuer_priority() {
        let resource = Url::parse("https://mcp.example.test/public/mcp").unwrap();
        let resource_urls = resource_metadata_urls(&resource, None).unwrap();
        assert_eq!(
            resource_urls.iter().map(Url::as_str).collect::<Vec<_>>(),
            vec![
                "https://mcp.example.test/.well-known/oauth-protected-resource/public/mcp",
                "https://mcp.example.test/.well-known/oauth-protected-resource",
            ]
        );
        let challenge_url = resource_metadata_urls(
            &resource,
            Some("Bearer resource_metadata=\"https://mcp.example.test/metadata\", scope=\"orders:read\""),
        )
        .unwrap();
        assert_eq!(
            challenge_url[0].as_str(),
            "https://mcp.example.test/metadata"
        );

        let issuer = Url::parse("https://auth.example.test/tenant-a").unwrap();
        let issuer_urls = authorization_server_metadata_urls(&issuer).unwrap();
        assert_eq!(
            issuer_urls.iter().map(Url::as_str).collect::<Vec<_>>(),
            vec![
                "https://auth.example.test/.well-known/oauth-authorization-server/tenant-a",
                "https://auth.example.test/.well-known/openid-configuration/tenant-a",
                "https://auth.example.test/tenant-a/.well-known/openid-configuration",
            ]
        );
    }

    #[test]
    fn authorization_server_metadata_validates_optional_revocation_endpoint() {
        let issuer = Url::parse(ISSUER).unwrap();
        let wire: AuthorizationServerMetadataWire = serde_json::from_value(serde_json::json!({
            "issuer": ISSUER,
            "authorization_endpoint": AUTHORIZATION_ENDPOINT,
            "token_endpoint": TOKEN_ENDPOINT,
            "revocation_endpoint": "https://auth.example.test/revoke",
            "code_challenge_methods_supported": ["S256"],
        }))
        .unwrap();
        let metadata = wire.into_public(&issuer).unwrap();
        assert_eq!(
            metadata.revocation_endpoint().map(Url::as_str),
            Some("https://auth.example.test/revoke")
        );

        let unsafe_wire: AuthorizationServerMetadataWire =
            serde_json::from_value(serde_json::json!({
                "issuer": ISSUER,
                "authorization_endpoint": AUTHORIZATION_ENDPOINT,
                "token_endpoint": TOKEN_ENDPOINT,
                "revocation_endpoint": "http://auth.example.test/revoke",
                "code_challenge_methods_supported": ["S256"],
            }))
            .unwrap();
        assert_eq!(
            unsafe_wire.into_public(&issuer).unwrap_err(),
            McpOAuthError::InvalidMetadata
        );
    }

    #[tokio::test]
    async fn pkce_redirect_binds_resource_and_callback_state_is_single_use() {
        let exchanger = RecordingExchanger::default();
        let flow = flow(exchanger.clone());

        let redirect = flow.begin().await.expect("authorization must begin");
        let pairs = redirect
            .location()
            .query_pairs()
            .into_owned()
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            redirect.location().as_str().split('?').next(),
            Some(AUTHORIZATION_ENDPOINT)
        );
        assert_eq!(pairs.get("response_type"), Some(&"code".to_owned()));
        assert_eq!(pairs.get("client_id"), Some(&CLIENT_ID.to_owned()));
        assert_eq!(pairs.get("redirect_uri"), Some(&REDIRECT_URI.to_owned()));
        assert_eq!(pairs.get("resource"), Some(&RESOURCE.to_owned()));
        assert_eq!(pairs.get("scope"), Some(&"orders:read".to_owned()));
        assert_eq!(pairs.get("state"), Some(&"s".repeat(43)));
        assert_eq!(pairs.get("code_challenge_method"), Some(&"S256".to_owned()));
        assert_eq!(
            pairs.get("code_challenge"),
            Some(&pkce_challenge(&"v".repeat(43)))
        );

        let tokens = flow
            .complete(McpOAuthAuthorizationCallback {
                code: Some("one-time-code".to_owned()),
                state: Some("s".repeat(43)),
                error: None,
                error_description: None,
            })
            .await
            .expect("valid callback must exchange a code");
        assert!(tokens.has_refresh_token());
        assert!(!format!("{tokens:?}").contains("initial-access-token"));
        let requests = exchanger.exchange_requests.lock().await;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].code(), "one-time-code");
        assert_eq!(requests[0].code_verifier(), "v".repeat(43));
        assert_eq!(requests[0].resource().as_str(), RESOURCE);
        drop(requests);

        let replay = flow
            .complete(McpOAuthAuthorizationCallback {
                code: Some("one-time-code".to_owned()),
                state: Some("s".repeat(43)),
                error: None,
                error_description: None,
            })
            .await;
        assert_eq!(replay.unwrap_err(), McpOAuthError::StateRejected);
        assert_eq!(exchanger.exchange_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn provider_rejection_consumes_state_without_exchanging_a_code() {
        let exchanger = RecordingExchanger::default();
        let flow = flow(exchanger.clone());
        flow.begin().await.expect("authorization must begin");

        let result = flow
            .complete(McpOAuthAuthorizationCallback {
                code: None,
                state: Some("s".repeat(43)),
                error: Some("access_denied".to_owned()),
                error_description: Some("provider-only diagnostic".to_owned()),
            })
            .await;
        assert_eq!(result.unwrap_err(), McpOAuthError::ProviderRejected);
        assert_eq!(exchanger.exchange_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn token_refresh_is_explicit_resource_bound_and_replaces_the_stored_set() {
        let exchanger = RecordingExchanger::default();
        let flow = flow(exchanger.clone());
        let store = InMemoryMcpOAuthTokenStore::default();
        let key = McpOAuthTokenStoreKey::new("tenant-a:user-a:connection-a")
            .expect("test token-store key must be valid");
        let initial = token_set(
            Url::parse(RESOURCE).unwrap(),
            "old-access-token",
            Some("old-refresh-token".to_owned()),
        )
        .unwrap();
        flow.save(&store, key.clone(), initial)
            .await
            .expect("initial token must be persisted");

        let refreshed = flow
            .refresh(&store, key.clone())
            .await
            .expect("explicit refresh must succeed");
        assert!(refreshed.has_refresh_token());
        let persisted = store
            .load(key)
            .await
            .expect("local store must load")
            .expect("token must remain stored");
        let secrets = persisted.into_secrets();
        assert_eq!(
            secrets.access_token_for_encryption(),
            "refreshed-access-token"
        );
        assert_eq!(
            secrets.refresh_token_for_encryption(),
            Some("rotated-refresh-token")
        );
        let refresh_requests = exchanger.refresh_requests.lock().await;
        assert_eq!(refresh_requests.len(), 1);
        assert_eq!(refresh_requests[0].refresh_token(), "old-refresh-token");
        assert_eq!(refresh_requests[0].resource().as_str(), RESOURCE);
    }

    #[tokio::test]
    async fn expired_tokens_do_not_trigger_an_implicit_refresh() {
        let exchanger = RecordingExchanger::default();
        let flow = flow(exchanger.clone());
        let store = InMemoryMcpOAuthTokenStore::default();
        let key = McpOAuthTokenStoreKey::new("tenant-a:user-a:expired")
            .expect("test token-store key must be valid");
        let expired_access_token =
            McpOAuthAccessToken::new("expired-access-token", Some(SystemTime::UNIX_EPOCH)).unwrap();
        let expired = McpOAuthTokenSet::new(
            Url::parse(RESOURCE).unwrap(),
            expired_access_token,
            Some("refresh-token".to_owned()),
        )
        .unwrap();
        flow.save(&store, key.clone(), expired)
            .await
            .expect("expired token may be stored");

        assert_eq!(
            flow.load_current(&store, key.clone(), SystemTime::now())
                .await
                .unwrap_err(),
            McpOAuthError::TokenExpired
        );
        assert_eq!(exchanger.refresh_calls.load(Ordering::SeqCst), 0);

        flow.refresh(&store, key)
            .await
            .expect("application-selected refresh must succeed");
        assert_eq!(exchanger.refresh_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn explicit_revocation_prefers_refresh_token_and_removes_only_after_success() {
        let flow = McpOAuthAuthorizationFlow::new(
            config(),
            provider_with_revocation(),
            InMemoryMcpOAuthTransactionStore::default(),
            RecordingExchanger::default(),
            SequenceGenerator::new(Vec::new()),
        );
        let store = InMemoryMcpOAuthTokenStore::default();
        let key = McpOAuthTokenStoreKey::new("tenant-a:user-a:disconnect")
            .expect("test token-store key must be valid");
        let tokens = token_set(
            Url::parse(RESOURCE).unwrap(),
            "disconnect-access-token",
            Some("disconnect-refresh-token".to_owned()),
        )
        .unwrap();
        flow.save(&store, key.clone(), tokens)
            .await
            .expect("token must be stored before revocation");
        let revoker = RecordingRevoker::default();

        flow.revoke_and_remove(&store, key.clone(), &revoker)
            .await
            .expect("successful revocation must remove the local record");
        assert!(store.load(key).await.unwrap().is_none());
        assert_eq!(revoker.calls.load(Ordering::SeqCst), 1);
        let requests = revoker.requests.lock().await;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].token(), "disconnect-refresh-token");
        assert_eq!(
            requests[0].token_type_hint(),
            McpOAuthRevocationTokenType::RefreshToken
        );
        assert_eq!(requests[0].resource().as_str(), RESOURCE);
    }

    #[tokio::test]
    async fn absent_revocation_endpoint_leaves_the_stored_token_untouched() {
        let flow = flow(RecordingExchanger::default());
        let store = InMemoryMcpOAuthTokenStore::default();
        let key = McpOAuthTokenStoreKey::new("tenant-a:user-a:no-revoke")
            .expect("test token-store key must be valid");
        flow.save(
            &store,
            key.clone(),
            token_set(
                Url::parse(RESOURCE).unwrap(),
                "access-token",
                Some("refresh-token".to_owned()),
            )
            .unwrap(),
        )
        .await
        .expect("token must be stored");

        assert_eq!(
            flow.revoke_and_remove(&store, key.clone(), &RecordingRevoker::default())
                .await
                .unwrap_err(),
            McpOAuthError::RevocationUnsupported
        );
        assert!(store.load(key).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn http_revoker_posts_the_selected_secret_without_a_resource_parameter() {
        let (endpoint, request_task) = token_endpoint_once("{}").await;
        let revoker =
            HttpMcpOAuthTokenExchanger::new(&config()).expect("HTTP revoker must initialize");
        revoker
            .revoke(
                endpoint,
                McpOAuthRevocationRequest {
                    client_id: CLIENT_ID.to_owned(),
                    token: "revocation-refresh-token".to_owned(),
                    token_type_hint: McpOAuthRevocationTokenType::RefreshToken,
                    resource: Url::parse(RESOURCE).unwrap(),
                },
            )
            .await
            .expect("bounded loopback revocation must succeed");
        let request = request_task
            .await
            .expect("test revocation endpoint task must complete");
        let (_, body) = request
            .split_once("\r\n\r\n")
            .expect("HTTP revocation request must contain a body");
        let form = url::form_urlencoded::parse(body.as_bytes())
            .into_owned()
            .collect::<BTreeMap<_, _>>();
        assert_eq!(form.get("client_id"), Some(&CLIENT_ID.to_owned()));
        assert_eq!(
            form.get("token"),
            Some(&"revocation-refresh-token".to_owned())
        );
        assert_eq!(
            form.get("token_type_hint"),
            Some(&"refresh_token".to_owned())
        );
        assert!(!form.contains_key("resource"));
    }

    #[tokio::test]
    async fn http_exchanger_posts_pkce_and_resource_to_the_selected_token_endpoint() {
        let (endpoint, request_task) = token_endpoint_once(
            r#"{"access_token":"http-access-token","token_type":"Bearer","expires_in":60,"refresh_token":"http-refresh-token"}"#,
        )
        .await;
        let exchanger = HttpMcpOAuthTokenExchanger::new(&config())
            .expect("HTTP token exchanger must initialize");
        let token_set = exchanger
            .exchange(
                endpoint,
                McpOAuthTokenExchangeRequest {
                    client_id: CLIENT_ID.to_owned(),
                    code: "issued-code".to_owned(),
                    redirect_uri: Url::parse(REDIRECT_URI).unwrap(),
                    code_verifier: "v".repeat(43),
                    resource: Url::parse(RESOURCE).unwrap(),
                },
            )
            .await
            .expect("bounded loopback token request must succeed");
        let request = request_task
            .await
            .expect("test token endpoint task must complete");
        let (_, body) = request
            .split_once("\r\n\r\n")
            .expect("HTTP request must contain a body");
        let form = url::form_urlencoded::parse(body.as_bytes())
            .into_owned()
            .collect::<BTreeMap<_, _>>();
        assert!(
            request
                .to_ascii_lowercase()
                .contains("accept: application/json")
        );
        assert_eq!(
            form.get("grant_type"),
            Some(&"authorization_code".to_owned())
        );
        assert_eq!(form.get("client_id"), Some(&CLIENT_ID.to_owned()));
        assert_eq!(form.get("code"), Some(&"issued-code".to_owned()));
        assert_eq!(form.get("redirect_uri"), Some(&REDIRECT_URI.to_owned()));
        assert_eq!(form.get("code_verifier"), Some(&"v".repeat(43)));
        assert_eq!(form.get("resource"), Some(&RESOURCE.to_owned()));
        let secrets = token_set.into_secrets();
        assert_eq!(secrets.access_token_for_encryption(), "http-access-token");
        assert_eq!(
            secrets.refresh_token_for_encryption(),
            Some("http-refresh-token")
        );
    }

    #[test]
    fn uuid_generator_creates_pkce_safe_values() {
        let value = UuidMcpOAuthValueGenerator.generate();
        assert!((43..=128).contains(&value.len()));
        assert!(value.bytes().all(|byte| byte.is_ascii_alphanumeric()));
    }
}
