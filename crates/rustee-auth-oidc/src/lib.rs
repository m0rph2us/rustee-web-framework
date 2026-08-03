//! Remote JWKS-backed OIDC resource-server authentication.
//!
//! Tokens must carry a `kid`. Only signature-verification JWKs that explicitly declare the
//! configured asymmetric algorithm are cached. The verifier refreshes for a missing key and on
//! cache expiry, while a small refresh interval prevents untrusted `kid` values from causing a
//! fetch storm.

mod browser_login;
mod introspection;

pub use browser_login::{
    AuthorizationCallback, AuthorizationRedirect, AuthorizationTransactionStore,
    AuthorizationValueGenerator, HttpOidcDiscovery, HttpOidcTokenExchanger,
    InMemoryAuthorizationTransactionStore, OidcBrowserConfig, OidcBrowserConfigError,
    OidcBrowserLogin, OidcClientAuthentication, OidcClientSecret, OidcDiscovery, OidcLoginError,
    OidcLoginResult, OidcProviderMetadata, OidcTokenExchangeRequest, OidcTokenExchanger,
    OidcTokenResponse, PendingAuthorization, UuidAuthorizationValueGenerator,
};
pub use introspection::{
    HttpOpaqueTokenIntrospector, OpaqueIntrospectionConfig, OpaqueIntrospectionConfigError,
    OpaqueTokenAuthenticator, OpaqueTokenIntrospection, OpaqueTokenIntrospectionRequest,
    OpaqueTokenIntrospector,
};

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use futures_util::future::BoxFuture;
use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode, decode_header,
    jwk::{Jwk, JwkSet, KeyAlgorithm, KeyOperations, PublicKeyUse},
};
use reqwest::Client;
use rustee_auth::{AuthError, BearerAuthenticator, Principal};
use serde::Deserialize;
use tokio::sync::{Mutex, RwLock};
use url::Url;

const DEFAULT_CACHE_TTL: Duration = Duration::from_mins(5);
const DEFAULT_MINIMUM_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// OIDC resource-server settings for one trusted issuer and JWKS endpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OidcResourceServerConfig {
    algorithm: Algorithm,
    issuer: String,
    audience: String,
    jwks_url: Url,
    leeway_seconds: u64,
    cache_ttl: Duration,
    minimum_refresh_interval: Duration,
}

impl OidcResourceServerConfig {
    /// Creates settings that accept one asymmetric JWT algorithm from an HTTPS JWKS endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`OidcConfigError`] for blank issuer/audience, invalid JWKS URLs, or symmetric
    /// algorithms that must not be trusted from a remote OIDC key set.
    pub fn new(
        algorithm: Algorithm,
        issuer: impl Into<String>,
        audience: impl Into<String>,
        jwks_url: Url,
    ) -> Result<Self, OidcConfigError> {
        let issuer = issuer.into();
        let audience = audience.into();
        if issuer.trim().is_empty() || audience.trim().is_empty() {
            return Err(OidcConfigError::BlankField);
        }
        if !is_asymmetric(algorithm) {
            return Err(OidcConfigError::SymmetricAlgorithm);
        }
        if !is_valid_jwks_url(&jwks_url) {
            return Err(OidcConfigError::InvalidJwksUrl);
        }
        Ok(Self {
            algorithm,
            issuer,
            audience,
            jwks_url,
            leeway_seconds: 0,
            cache_ttl: DEFAULT_CACHE_TTL,
            minimum_refresh_interval: DEFAULT_MINIMUM_REFRESH_INTERVAL,
        })
    }

    /// Allows explicitly configured clock skew for expiry and not-before validation.
    #[must_use]
    pub const fn with_leeway_seconds(mut self, leeway_seconds: u64) -> Self {
        self.leeway_seconds = leeway_seconds;
        self
    }

    /// Sets how long a successful JWKS response can be used before it must be refreshed.
    ///
    /// A zero duration is useful for deterministic tests, but production callers should retain a
    /// bounded cache duration to avoid making every token validation depend on the identity
    /// provider.
    #[must_use]
    pub const fn with_cache_ttl(mut self, cache_ttl: Duration) -> Self {
        self.cache_ttl = cache_ttl;
        self
    }

    /// Sets the minimum time between remote refresh attempts.
    ///
    /// This also bounds refreshes caused by arbitrary unknown `kid` values. Set it to zero only
    /// when deterministic tests or an explicitly managed upstream cache make that appropriate.
    #[must_use]
    pub const fn with_minimum_refresh_interval(mut self, interval: Duration) -> Self {
        self.minimum_refresh_interval = interval;
        self
    }

    /// Returns the only token algorithm accepted by this verifier.
    #[must_use]
    pub const fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    /// Returns the required token issuer.
    #[must_use]
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Returns the required token audience.
    #[must_use]
    pub fn audience(&self) -> &str {
        &self.audience
    }

    /// Returns the configured HTTPS JWKS endpoint.
    #[must_use]
    pub const fn jwks_url(&self) -> &Url {
        &self.jwks_url
    }

    /// Returns the successful JWKS cache lifetime.
    #[must_use]
    pub const fn cache_ttl(&self) -> Duration {
        self.cache_ttl
    }

    /// Returns the minimum interval between refresh attempts.
    #[must_use]
    pub const fn minimum_refresh_interval(&self) -> Duration {
        self.minimum_refresh_interval
    }

    fn validation(&self) -> Validation {
        let mut validation = Validation::new(self.algorithm);
        validation.leeway = self.leeway_seconds;
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.validate_aud = true;
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.set_audience(&[self.audience.as_str()]);
        validation.set_required_spec_claims(&["sub", "iss", "aud", "exp", "nbf"]);
        validation
    }

    fn id_token_validation(&self) -> Validation {
        let mut validation = Validation::new(self.algorithm);
        validation.leeway = self.leeway_seconds;
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.validate_aud = true;
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.set_audience(&[self.audience.as_str()]);
        validation.set_required_spec_claims(&["sub", "iss", "aud", "exp"]);
        validation
    }
}

/// Invalid OIDC resource-server settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OidcConfigError {
    /// An issuer or audience was blank.
    #[error("OIDC issuer and audience must not be blank")]
    BlankField,
    /// The configured algorithm was symmetric.
    #[error("remote OIDC JWKS verification requires an asymmetric algorithm")]
    SymmetricAlgorithm,
    /// The configured JWKS endpoint was not a valid HTTPS endpoint.
    #[error("OIDC JWKS URL must be an absolute HTTPS URL without credentials or a fragment")]
    InvalidJwksUrl,
    /// The HTTP client timeout was zero.
    #[error("OIDC JWKS HTTP timeout must be greater than zero")]
    ZeroFetchTimeout,
    /// The HTTP client could not be initialized.
    #[error("OIDC JWKS HTTP client could not be initialized")]
    HttpClientInitialization,
}

/// Fetches a JSON Web Key Set for one trusted configuration endpoint.
pub trait JwksFetcher: Clone + Send + Sync + 'static {
    /// Fetcher-specific failure type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Retrieves the current key set.
    fn fetch(&self) -> BoxFuture<'static, Result<JwkSet, Self::Error>>;
}

/// Verifies an OIDC ID token and proves that it belongs to the browser login transaction.
pub trait IdTokenVerifier: Clone + Send + Sync + 'static {
    /// Validates one ID token and its nonce.
    fn verify_id_token(
        &self,
        token: &str,
        expected_nonce: &str,
    ) -> BoxFuture<'static, Result<Principal, AuthError>>;
}

/// HTTPS-capable JWKS fetcher for production OIDC deployments.
#[derive(Clone)]
pub struct HttpJwksFetcher {
    client: Client,
    url: Url,
}

impl HttpJwksFetcher {
    /// Creates a JWKS fetcher with a finite timeout and an HTTPS endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`OidcConfigError`] if the endpoint is not valid for a remote JWKS or the client
    /// timeout is zero.
    pub fn new(url: Url, timeout: Duration) -> Result<Self, OidcConfigError> {
        if !is_valid_jwks_url(&url) {
            return Err(OidcConfigError::InvalidJwksUrl);
        }
        if timeout.is_zero() {
            return Err(OidcConfigError::ZeroFetchTimeout);
        }
        let client = Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|_| OidcConfigError::HttpClientInitialization)?;
        Ok(Self { client, url })
    }
}

impl fmt::Debug for HttpJwksFetcher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpJwksFetcher")
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

impl JwksFetcher for HttpJwksFetcher {
    type Error = reqwest::Error;

    fn fetch(&self) -> BoxFuture<'static, Result<JwkSet, Self::Error>> {
        let client = self.client.clone();
        let url = self.url.clone();
        Box::pin(async move {
            client
                .get(url)
                .send()
                .await?
                .error_for_status()?
                .json()
                .await
        })
    }
}

/// JWKS-backed bearer verifier with cache expiry and key-rotation behavior.
#[derive(Clone)]
pub struct JwksAuthenticator<F> {
    config: OidcResourceServerConfig,
    fetcher: F,
    cache: Arc<RwLock<JwksCache>>,
    refresh_gate: Arc<Mutex<()>>,
}

impl<F> JwksAuthenticator<F>
where
    F: JwksFetcher,
{
    /// Creates an empty verifier.
    ///
    /// Call [`Self::refresh`] during startup when an application must fail fast if its identity
    /// provider is unavailable. Otherwise, the first token validation loads the JWKS lazily.
    #[must_use]
    pub fn new(config: OidcResourceServerConfig, fetcher: F) -> Self {
        Self {
            config,
            fetcher,
            cache: Arc::new(RwLock::new(JwksCache::default())),
            refresh_gate: Arc::new(Mutex::new(())),
        }
    }

    /// Fetches and replaces the cached keys with compatible entries from the configured JWKS.
    ///
    /// Duplicate key IDs and JWKs with an incompatible algorithm, key use, or key operation are
    /// excluded rather than selecting an arbitrary remote key.
    ///
    /// # Errors
    ///
    /// Returns the fetcher's error when the remote JWKS cannot be retrieved or decoded.
    pub async fn refresh(&self) -> Result<(), F::Error> {
        let _refresh_guard = self.refresh_gate.lock().await;
        self.fetch_and_store(RefreshReason::Explicit).await
    }

    async fn key_for(&self, kid: &str) -> Result<Option<DecodingKey>, KeyLookupError<F::Error>> {
        match self.cache_lookup(kid).await {
            CacheLookup::Key(key) => return Ok(Some(key)),
            CacheLookup::Missing => return Ok(None),
            CacheLookup::Unavailable => return Err(KeyLookupError::Unavailable),
            CacheLookup::Refresh(_) => {}
        }

        let _refresh_guard = self.refresh_gate.lock().await;
        match self.cache_lookup(kid).await {
            CacheLookup::Key(key) => Ok(Some(key)),
            CacheLookup::Missing => Ok(None),
            CacheLookup::Unavailable => Err(KeyLookupError::Unavailable),
            CacheLookup::Refresh(reason) => {
                self.fetch_and_store(reason)
                    .await
                    .map_err(KeyLookupError::Fetch)?;
                Ok(self.cache.read().await.keys.get(kid).cloned())
            }
        }
    }

    async fn verification_key(&self, token: &str) -> Result<DecodingKey, AuthError> {
        let header = decode_header(token).map_err(|_| AuthError::RejectedBearerToken)?;
        if header.alg != self.config.algorithm {
            return Err(AuthError::RejectedBearerToken);
        }
        let kid = header.kid.ok_or(AuthError::RejectedBearerToken)?;
        self.key_for(&kid)
            .await
            .map_err(|_| AuthError::ProviderUnavailable)?
            .ok_or(AuthError::RejectedBearerToken)
    }

    async fn cache_lookup(&self, kid: &str) -> CacheLookup {
        let cache = self.cache.read().await;
        let key = cache.keys.get(kid).cloned();
        let has_key = key.is_some();
        let fresh = cache
            .last_successful_refresh
            .is_some_and(|refreshed_at| refreshed_at.elapsed() < self.config.cache_ttl);

        if fresh && let Some(key) = key {
            return CacheLookup::Key(key);
        }
        if !has_key && !cache.unknown_key_refresh_allowed(self.config.minimum_refresh_interval) {
            return if cache.last_refresh_failed {
                CacheLookup::Unavailable
            } else {
                CacheLookup::Missing
            };
        }
        if has_key && !cache.refresh_allowed(self.config.minimum_refresh_interval) {
            return CacheLookup::Unavailable;
        }
        if has_key {
            CacheLookup::Refresh(RefreshReason::ExpiredCache)
        } else {
            CacheLookup::Refresh(RefreshReason::UnknownKey)
        }
    }

    async fn fetch_and_store(&self, reason: RefreshReason) -> Result<(), F::Error> {
        {
            let mut cache = self.cache.write().await;
            let now = Instant::now();
            cache.last_refresh_attempt = Some(now);
            if matches!(reason, RefreshReason::UnknownKey) {
                cache.last_unknown_key_refresh = Some(now);
            }
        }

        let result = self.fetcher.fetch().await;
        let mut cache = self.cache.write().await;
        match result {
            Ok(set) => {
                cache.keys = compatible_keys(set, self.config.algorithm);
                cache.last_successful_refresh = Some(Instant::now());
                cache.last_refresh_failed = false;
                Ok(())
            }
            Err(error) => {
                cache.last_refresh_failed = true;
                Err(error)
            }
        }
    }
}

impl<F> fmt::Debug for JwksAuthenticator<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JwksAuthenticator")
            .field("algorithm", &self.config.algorithm)
            .field("issuer", &self.config.issuer)
            .field("cache_ttl", &self.config.cache_ttl)
            .finish_non_exhaustive()
    }
}

impl<F> BearerAuthenticator for JwksAuthenticator<F>
where
    F: JwksFetcher,
{
    fn authenticate(&self, token: &str) -> BoxFuture<'static, Result<Principal, AuthError>> {
        let this = self.clone();
        let token = token.to_owned();
        Box::pin(async move {
            let key = this.verification_key(&token).await?;
            let claims = decode::<Claims>(&token, &key, &this.config.validation())
                .map_err(|_| AuthError::RejectedBearerToken)?
                .claims;
            claims.into_principal()
        })
    }
}

impl<F> IdTokenVerifier for JwksAuthenticator<F>
where
    F: JwksFetcher,
{
    fn verify_id_token(
        &self,
        token: &str,
        expected_nonce: &str,
    ) -> BoxFuture<'static, Result<Principal, AuthError>> {
        let this = self.clone();
        let token = token.to_owned();
        let expected_nonce = expected_nonce.to_owned();
        Box::pin(async move {
            if expected_nonce.is_empty() {
                return Err(AuthError::RejectedBearerToken);
            }
            let key = this.verification_key(&token).await?;
            let claims = decode::<IdTokenClaims>(&token, &key, &this.config.id_token_validation())
                .map_err(|_| AuthError::RejectedBearerToken)?
                .claims;
            if claims
                .aud
                .as_array()
                .is_some_and(|audiences| audiences.len() > 1)
                && !claims
                    .authorized_party
                    .as_deref()
                    .is_some_and(|party| constant_time_eq(party, &this.config.audience))
            {
                return Err(AuthError::RejectedBearerToken);
            }
            if !constant_time_eq(&claims.nonce, &expected_nonce) {
                return Err(AuthError::RejectedBearerToken);
            }
            claims.into_principal()
        })
    }
}

#[derive(Default)]
struct JwksCache {
    keys: BTreeMap<String, DecodingKey>,
    last_successful_refresh: Option<Instant>,
    last_refresh_attempt: Option<Instant>,
    last_unknown_key_refresh: Option<Instant>,
    last_refresh_failed: bool,
}

impl JwksCache {
    fn refresh_allowed(&self, minimum_refresh_interval: Duration) -> bool {
        self.last_refresh_attempt
            .is_none_or(|attempted_at| attempted_at.elapsed() >= minimum_refresh_interval)
    }

    fn unknown_key_refresh_allowed(&self, minimum_refresh_interval: Duration) -> bool {
        self.last_unknown_key_refresh
            .is_none_or(|attempted_at| attempted_at.elapsed() >= minimum_refresh_interval)
    }
}

enum CacheLookup {
    Key(DecodingKey),
    Missing,
    Refresh(RefreshReason),
    Unavailable,
}

#[derive(Clone, Copy)]
enum RefreshReason {
    Explicit,
    ExpiredCache,
    UnknownKey,
}

enum KeyLookupError<E> {
    Fetch(E),
    Unavailable,
}

fn compatible_keys(set: JwkSet, algorithm: Algorithm) -> BTreeMap<String, DecodingKey> {
    let mut keys = BTreeMap::new();
    let mut seen = BTreeSet::new();
    let mut duplicates = BTreeSet::new();

    for jwk in set.keys {
        let Some(kid) = jwk.common.key_id.clone() else {
            continue;
        };
        if !seen.insert(kid.clone()) {
            duplicates.insert(kid);
            continue;
        }
        if is_compatible_jwk(&jwk, algorithm)
            && let Ok(key) = DecodingKey::from_jwk(&jwk)
        {
            keys.insert(kid, key);
        }
    }
    for duplicate in duplicates {
        keys.remove(&duplicate);
    }
    keys
}

fn is_compatible_jwk(jwk: &Jwk, algorithm: Algorithm) -> bool {
    matches!(
        (algorithm, jwk.common.key_algorithm),
        (Algorithm::RS256, Some(KeyAlgorithm::RS256))
            | (Algorithm::RS384, Some(KeyAlgorithm::RS384))
            | (Algorithm::RS512, Some(KeyAlgorithm::RS512))
            | (Algorithm::PS256, Some(KeyAlgorithm::PS256))
            | (Algorithm::PS384, Some(KeyAlgorithm::PS384))
            | (Algorithm::PS512, Some(KeyAlgorithm::PS512))
            | (Algorithm::ES256, Some(KeyAlgorithm::ES256))
            | (Algorithm::ES384, Some(KeyAlgorithm::ES384))
            | (Algorithm::EdDSA, Some(KeyAlgorithm::EdDSA))
    ) && matches!(
        jwk.common.public_key_use,
        None | Some(PublicKeyUse::Signature)
    ) && jwk
        .common
        .key_operations
        .as_ref()
        .is_none_or(|operations| operations.contains(&KeyOperations::Verify))
}

#[derive(Debug, Deserialize)]
struct Claims {
    sub: String,
    iss: String,
    #[serde(rename = "aud")]
    _audience: serde_json::Value,
    #[serde(rename = "exp")]
    _expiration: serde_json::Value,
    #[serde(rename = "nbf")]
    _not_before: serde_json::Value,
    #[serde(default)]
    tenant: Option<String>,
    #[serde(default)]
    scope: Option<ScopeClaim>,
    #[serde(default)]
    roles: Option<StringSetClaim>,
    #[serde(default)]
    permissions: Option<StringSetClaim>,
}

impl Claims {
    fn into_principal(self) -> Result<Principal, AuthError> {
        let mut principal = Principal::new(self.sub)
            .and_then(|principal| principal.with_issuer(self.iss))
            .map_err(|_| AuthError::RejectedBearerToken)?;
        if let Some(tenant) = self.tenant {
            principal = principal
                .with_tenant(tenant)
                .map_err(|_| AuthError::RejectedBearerToken)?;
        }
        for scope in self.scope.into_iter().flat_map(ScopeClaim::into_scopes) {
            principal = principal
                .with_scope(scope)
                .map_err(|_| AuthError::RejectedBearerToken)?;
        }
        for role in self.roles.into_iter().flat_map(StringSetClaim::into_values) {
            principal = principal
                .with_role(role)
                .map_err(|_| AuthError::RejectedBearerToken)?;
        }
        for permission in self
            .permissions
            .into_iter()
            .flat_map(StringSetClaim::into_values)
        {
            principal = principal
                .with_permission(permission)
                .map_err(|_| AuthError::RejectedBearerToken)?;
        }
        Ok(principal)
    }
}

#[derive(Debug, Deserialize)]
struct IdTokenClaims {
    sub: String,
    iss: String,
    aud: serde_json::Value,
    #[serde(rename = "exp")]
    _expiration: serde_json::Value,
    #[serde(default, rename = "nbf")]
    _not_before: Option<serde_json::Value>,
    #[serde(rename = "iat")]
    _issued_at: u64,
    nonce: String,
    #[serde(default, rename = "azp")]
    authorized_party: Option<String>,
    #[serde(default)]
    tenant: Option<String>,
    #[serde(default)]
    scope: Option<ScopeClaim>,
    #[serde(default)]
    roles: Option<StringSetClaim>,
    #[serde(default)]
    permissions: Option<StringSetClaim>,
}

impl IdTokenClaims {
    fn into_principal(self) -> Result<Principal, AuthError> {
        let mut principal = Principal::new(self.sub)
            .and_then(|principal| principal.with_issuer(self.iss))
            .map_err(|_| AuthError::RejectedBearerToken)?;
        if let Some(tenant) = self.tenant {
            principal = principal
                .with_tenant(tenant)
                .map_err(|_| AuthError::RejectedBearerToken)?;
        }
        for scope in self.scope.into_iter().flat_map(ScopeClaim::into_scopes) {
            principal = principal
                .with_scope(scope)
                .map_err(|_| AuthError::RejectedBearerToken)?;
        }
        for role in self.roles.into_iter().flat_map(StringSetClaim::into_values) {
            principal = principal
                .with_role(role)
                .map_err(|_| AuthError::RejectedBearerToken)?;
        }
        for permission in self
            .permissions
            .into_iter()
            .flat_map(StringSetClaim::into_values)
        {
            principal = principal
                .with_permission(permission)
                .map_err(|_| AuthError::RejectedBearerToken)?;
        }
        Ok(principal)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum ScopeClaim {
    SpaceDelimited(String),
    Values(Vec<String>),
}

impl ScopeClaim {
    fn into_scopes(self) -> Vec<String> {
        match self {
            Self::SpaceDelimited(scopes) => scopes
                .split_ascii_whitespace()
                .map(ToOwned::to_owned)
                .collect(),
            Self::Values(scopes) => scopes,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum StringSetClaim {
    One(String),
    Values(Vec<String>),
}

impl StringSetClaim {
    fn into_values(self) -> Vec<String> {
        match self {
            Self::One(value) => vec![value],
            Self::Values(values) => values,
        }
    }
}

fn is_valid_jwks_url(url: &Url) -> bool {
    url.scheme() == "https"
        && url.host().is_some()
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
}

fn constant_time_eq(left: &str, right: &str) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.bytes()
        .zip(right.bytes())
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

const fn is_asymmetric(algorithm: Algorithm) -> bool {
    matches!(
        algorithm,
        Algorithm::RS256
            | Algorithm::RS384
            | Algorithm::RS512
            | Algorithm::PS256
            | Algorithm::PS384
            | Algorithm::PS512
            | Algorithm::ES256
            | Algorithm::ES384
            | Algorithm::EdDSA
    )
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use futures_util::future::BoxFuture;
    use jsonwebtoken::{
        Algorithm, EncodingKey, Header, encode,
        jwk::{Jwk, JwkSet, KeyAlgorithm, PublicKeyUse},
    };
    use rustee_auth::{AuthError, BearerAuthenticator};
    use serde::Serialize;
    use tokio::sync::Mutex;
    use url::Url;

    use super::{
        IdTokenVerifier, JwksAuthenticator, JwksFetcher, OidcConfigError, OidcResourceServerConfig,
    };

    const ISSUER: &str = "https://issuer.example.test";
    const AUDIENCE: &str = "rustee-api";
    const TEST_RSA_PRIVATE_KEY: &str = r"-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDJETqse41HRBsc
7cfcq3ak4oZWFCoZlcic525A3FfO4qW9BMtRO/iXiyCCHn8JhiL9y8j5JdVP2Q9Z
IpfElcFd3/guS9w+5RqQGgCR+H56IVUyHZWtTJbKPcwWXQdNUX0rBFcsBzCRESJL
eelOEdHIjG7LRkx5l/FUvlqsyHDVJEQsHwegZ8b8C0fz0EgT2MMEdn10t6Ur1rXz
jMB/wvCg8vG8lvciXmedyo9xJ8oMOh0wUEgxziVDMMovmC+aJctcHUAYubwoGN8T
yzcvnGqL7JSh36Pwy28iPzXZ2RLhAyJFU39vLaHdljwthUaupldlNyCfa6Ofy4qN
ctlUPlN1AgMBAAECggEAdESTQjQ70O8QIp1ZSkCYXeZjuhj081CK7jhhp/4ChK7J
GlFQZMwiBze7d6K84TwAtfQGZhQ7km25E1kOm+3hIDCoKdVSKch/oL54f/BK6sKl
qlIzQEAenho4DuKCm3I4yAw9gEc0DV70DuMTR0LEpYyXcNJY3KNBOTjN5EYQAR9s
2MeurpgK2MdJlIuZaIbzSGd+diiz2E6vkmcufJLtmYUT/k/ddWvEtz+1DnO6bRHh
xuuDMeJA/lGB/EYloSLtdyCF6sII6C6slJJtgfb0bPy7l8VtL5iDyz46IKyzdyzW
tKAn394dm7MYR1RlUBEfqFUyNK7C+pVMVoTwCC2V4QKBgQD64syfiQ2oeUlLYDm4
CcKSP3RnES02bcTyEDFSuGyyS1jldI4A8GXHJ/lG5EYgiYa1RUivge4lJrlNfjyf
dV230xgKms7+JiXqag1FI+3mqjAgg4mYiNjaao8N8O3/PD59wMPeWYImsWXNyeHS
55rUKiHERtCcvdzKl4u35ZtTqQKBgQDNKnX2bVqOJ4WSqCgHRhOm386ugPHfy+8j
m6cicmUR46ND6ggBB03bCnEG9OtGisxTo/TuYVRu3WP4KjoJs2LD5fwdwJqpgtHl
yVsk45Y1Hfo+7M6lAuR8rzCi6kHHNb0HyBmZjysHWZsn79ZM+sQnLpgaYgQGRbKV
DZWlbw7g7QKBgQCl1u+98UGXAP1jFutwbPsx40IVszP4y5ypCe0gqgon3UiY/G+1
zTLp79GGe/SjI2VpQ7AlW7TI2A0bXXvDSDi3/5Dfya9ULnFXv9yfvH1QwWToySpW
Kvd1gYSoiX84/WCtjZOr0e0HmLIb0vw0hqZA4szJSqoxQgvF22EfIWaIaQKBgQCf
34+OmMYw8fEvSCPxDxVvOwW2i7pvV14hFEDYIeZKW2W1HWBhVMzBfFB5SE8yaCQy
pRfOzj9aKOCm2FjjiErVNpkQoi6jGtLvScnhZAt/lr2TXTrl8OwVkPrIaN0bG/AS
aUYxmBPCpXu3UjhfQiWqFq/mFyzlqlgvuCc9g95HPQKBgAscKP8mLxdKwOgX8yFW
GcZ0izY/30012ajdHY+/QK5lsMoxTnn0skdS+spLxaS5ZEO4qvPVb8RAoCkWMMal
2pOhmquJQVDPDLuZHdrIiKiDM20dy9sMfHygWcZjQ4WSxf/J7T9canLZIXFhHAZT
3wc9h4G8BBCtWN2TN/LsGZdB
-----END PRIVATE KEY-----";

    #[derive(Clone, Debug, thiserror::Error)]
    #[error("test JWKS endpoint is unavailable")]
    struct FetchError;

    #[derive(Clone)]
    struct FakeFetcher {
        replies: Arc<Mutex<VecDeque<Result<JwkSet, FetchError>>>>,
        calls: Arc<AtomicUsize>,
    }

    impl FakeFetcher {
        fn new(replies: impl IntoIterator<Item = Result<JwkSet, FetchError>>) -> Self {
            Self {
                replies: Arc::new(Mutex::new(replies.into_iter().collect())),
                calls: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl JwksFetcher for FakeFetcher {
        type Error = FetchError;

        fn fetch(&self) -> BoxFuture<'static, Result<JwkSet, Self::Error>> {
            let replies = Arc::clone(&self.replies);
            let calls = Arc::clone(&self.calls);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                replies.lock().await.pop_front().unwrap_or(Err(FetchError))
            })
        }
    }

    #[derive(Serialize)]
    struct TestClaims<'a> {
        sub: &'a str,
        iss: &'a str,
        aud: &'a str,
        exp: u64,
        nbf: u64,
        tenant: &'a str,
        scope: &'a str,
        roles: &'a [&'a str],
        permissions: &'a [&'a str],
    }

    #[derive(Serialize)]
    struct TestIdTokenClaims<'a> {
        sub: &'a str,
        iss: &'a str,
        aud: &'a str,
        exp: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        nbf: Option<u64>,
        iat: u64,
        nonce: &'a str,
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock must be after the Unix epoch")
            .as_secs()
    }

    fn token(kid: Option<&str>) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = kid.map(ToOwned::to_owned);
        encode(
            &header,
            &TestClaims {
                sub: "alice",
                iss: ISSUER,
                aud: AUDIENCE,
                exp: now() + 300,
                nbf: now() - 1,
                tenant: "acme",
                scope: "profile:read profile:write",
                roles: &["project-viewer"],
                permissions: &["project:read"],
            },
            &EncodingKey::from_rsa_pem(TEST_RSA_PRIVATE_KEY.as_bytes())
                .expect("embedded test key must be valid"),
        )
        .expect("test claims must encode")
    }

    fn id_token(kid: &str, nonce: &str, nbf: Option<u64>) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(kid.to_owned());
        let current = now();
        encode(
            &header,
            &TestIdTokenClaims {
                sub: "alice",
                iss: ISSUER,
                aud: AUDIENCE,
                exp: current + 300,
                nbf,
                iat: current,
                nonce,
            },
            &EncodingKey::from_rsa_pem(TEST_RSA_PRIVATE_KEY.as_bytes())
                .expect("embedded test key must be valid"),
        )
        .expect("test ID token claims must encode")
    }

    fn jwk(kid: &str) -> Jwk {
        let encoding_key = EncodingKey::from_rsa_pem(TEST_RSA_PRIVATE_KEY.as_bytes())
            .expect("embedded test key must be valid");
        let mut jwk = Jwk::from_encoding_key(&encoding_key, Algorithm::RS256)
            .expect("test key must make a JWK");
        jwk.common.key_id = Some(kid.to_owned());
        jwk.common.public_key_use = Some(PublicKeyUse::Signature);
        jwk
    }

    fn jwks(kid: &str) -> JwkSet {
        JwkSet {
            keys: vec![jwk(kid)],
        }
    }

    fn config() -> OidcResourceServerConfig {
        OidcResourceServerConfig::new(
            Algorithm::RS256,
            ISSUER,
            AUDIENCE,
            Url::parse("https://issuer.example.test/.well-known/jwks.json")
                .expect("test URL must be valid"),
        )
        .expect("test configuration must be valid")
    }

    #[tokio::test]
    async fn verifies_a_remote_jwks_token_and_reuses_a_fresh_key() {
        let fetcher = FakeFetcher::new([Ok(jwks("primary"))]);
        let authenticator = JwksAuthenticator::new(config(), fetcher.clone());

        let principal = authenticator
            .authenticate(&token(Some("primary")))
            .await
            .expect("matching JWK must validate the signature");
        authenticator
            .authenticate(&token(Some("primary")))
            .await
            .expect("fresh JWK must be cached");

        assert_eq!(principal.subject(), "alice");
        assert_eq!(principal.issuer(), Some(ISSUER));
        assert_eq!(principal.tenant(), Some("acme"));
        assert!(principal.has_scope("profile:read"));
        assert!(principal.has_role("project-viewer"));
        assert!(principal.has_permission("project:read"));
        assert_eq!(fetcher.calls(), 1);
    }

    #[tokio::test]
    async fn unknown_kid_refreshes_once_and_accepts_a_rotated_key() {
        let fetcher = FakeFetcher::new([Ok(jwks("old")), Ok(jwks("rotated"))]);
        let authenticator = JwksAuthenticator::new(config(), fetcher.clone());

        authenticator
            .refresh()
            .await
            .expect("initial JWKS fetch must work");
        let principal = authenticator
            .authenticate(&token(Some("rotated")))
            .await
            .expect("unknown rotated kid must trigger one refresh");

        assert_eq!(principal.subject(), "alice");
        assert_eq!(fetcher.calls(), 2);

        let unknown = authenticator
            .authenticate(&token(Some("unrecognized")))
            .await;
        assert_eq!(unknown.unwrap_err(), AuthError::RejectedBearerToken);
        assert_eq!(fetcher.calls(), 2);
    }

    #[tokio::test]
    async fn rejects_missing_or_untrusted_jwk_keys_without_accepting_the_token() {
        let fetcher = FakeFetcher::new([Ok(jwks("primary"))]);
        let authenticator = JwksAuthenticator::new(config(), fetcher.clone());
        let missing_kid = authenticator.authenticate(&token(None)).await;

        assert_eq!(missing_kid.unwrap_err(), AuthError::RejectedBearerToken);
        assert_eq!(fetcher.calls(), 0);

        let mut encryption_key = jwk("encryption");
        encryption_key.common.public_key_use = Some(PublicKeyUse::Encryption);
        encryption_key.common.key_algorithm = Some(KeyAlgorithm::RS256);
        let untrusted = JwksAuthenticator::new(
            config(),
            FakeFetcher::new([Ok(JwkSet {
                keys: vec![encryption_key],
            })]),
        )
        .authenticate(&token(Some("encryption")))
        .await;

        assert_eq!(untrusted.unwrap_err(), AuthError::RejectedBearerToken);
    }

    #[tokio::test]
    async fn rejects_duplicate_key_ids_and_tampered_signatures() {
        let duplicate = JwksAuthenticator::new(
            config(),
            FakeFetcher::new([Ok(JwkSet {
                keys: vec![jwk("primary"), jwk("primary")],
            })]),
        )
        .authenticate(&token(Some("primary")))
        .await;
        assert_eq!(duplicate.unwrap_err(), AuthError::RejectedBearerToken);

        let authenticator =
            JwksAuthenticator::new(config(), FakeFetcher::new([Ok(jwks("primary"))]));
        let valid = token(Some("primary"));
        let signature_start = valid.rfind('.').expect("JWT has a signature") + 1;
        let mut tampered = valid.into_bytes();
        tampered[signature_start] = if tampered[signature_start] == b'a' {
            b'b'
        } else {
            b'a'
        };
        let tampered = String::from_utf8(tampered).expect("JWT remains ASCII after tampering");

        let rejected = authenticator.authenticate(&tampered).await;
        assert_eq!(rejected.unwrap_err(), AuthError::RejectedBearerToken);
    }

    #[tokio::test]
    async fn id_token_verification_requires_the_transaction_nonce() {
        let fetcher = FakeFetcher::new([Ok(jwks("primary"))]);
        let authenticator = JwksAuthenticator::new(config(), fetcher.clone());
        let token = id_token("primary", "browser-transaction-nonce", Some(now() - 1));

        let principal = authenticator
            .verify_id_token(&token, "browser-transaction-nonce")
            .await
            .expect("matching nonce and signed ID token must validate");
        let wrong_nonce = authenticator
            .verify_id_token(&token, "a-different-transaction-nonce")
            .await;

        assert_eq!(principal.subject(), "alice");
        assert_eq!(wrong_nonce.unwrap_err(), AuthError::RejectedBearerToken);
        assert_eq!(fetcher.calls(), 1);
    }

    #[tokio::test]
    async fn id_token_without_not_before_claim_remains_valid() {
        let authenticator =
            JwksAuthenticator::new(config(), FakeFetcher::new([Ok(jwks("primary"))]));

        let principal = authenticator
            .verify_id_token(
                &id_token("primary", "browser-transaction-nonce", None),
                "browser-transaction-nonce",
            )
            .await
            .expect("OIDC ID tokens may omit nbf");

        assert_eq!(principal.subject(), "alice");
    }

    #[tokio::test]
    async fn cache_expiry_rechecks_the_jwks_in_a_controlled_test_configuration() {
        let fetcher = FakeFetcher::new([Ok(jwks("primary")), Ok(jwks("primary"))]);
        let authenticator = JwksAuthenticator::new(
            config()
                .with_cache_ttl(Duration::ZERO)
                .with_minimum_refresh_interval(Duration::ZERO),
            fetcher.clone(),
        );

        authenticator
            .authenticate(&token(Some("primary")))
            .await
            .expect("first token must validate");
        authenticator
            .authenticate(&token(Some("primary")))
            .await
            .expect("expired cache must refresh before validating");

        assert_eq!(fetcher.calls(), 2);
    }

    #[tokio::test]
    async fn jwks_transport_failure_is_not_reported_as_an_invalid_token() {
        let authenticator = JwksAuthenticator::new(config(), FakeFetcher::new([Err(FetchError)]));

        let error = authenticator
            .authenticate(&token(Some("primary")))
            .await
            .expect_err("failed key fetch must fail closed");

        assert_eq!(error, AuthError::ProviderUnavailable);
    }

    #[test]
    fn config_rejects_symmetric_algorithms_and_non_https_endpoints() {
        let hmac = OidcResourceServerConfig::new(
            Algorithm::HS256,
            ISSUER,
            AUDIENCE,
            Url::parse("https://issuer.example.test/jwks").expect("test URL must be valid"),
        );
        let insecure = OidcResourceServerConfig::new(
            Algorithm::RS256,
            ISSUER,
            AUDIENCE,
            Url::parse("http://issuer.example.test/jwks").expect("test URL must be valid"),
        );

        assert_eq!(hmac.unwrap_err(), OidcConfigError::SymmetricAlgorithm);
        assert_eq!(insecure.unwrap_err(), OidcConfigError::InvalidJwksUrl);
    }
}
