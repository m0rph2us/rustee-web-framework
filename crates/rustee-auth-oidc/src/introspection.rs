//! OAuth 2.0 opaque access-token introspection support.
//!
//! The configured introspection endpoint is a trusted authentication dependency. A successful
//! response is still checked for its active state, issuer, audience, and any supplied time
//! bounds before it becomes a [`Principal`]. Only a SHA-256 fingerprint, never the raw token,
//! is used as a bounded in-memory cache key.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use futures_util::future::BoxFuture;
use reqwest::Client;
use rustee_auth::{AuthError, BearerAuthenticator, Principal};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use url::Url;

use crate::{OidcClientAuthentication, ScopeClaim, StringSetClaim, constant_time_eq};

const DEFAULT_CACHE_TTL: Duration = Duration::from_mins(1);
const DEFAULT_MAX_CACHE_ENTRIES: usize = 1_024;

/// Trusted settings for one OAuth 2.0 opaque access-token introspection endpoint.
#[derive(Clone, Eq, PartialEq)]
pub struct OpaqueIntrospectionConfig {
    issuer: String,
    audience: String,
    endpoint: Url,
    client_id: String,
    authentication: OidcClientAuthentication,
    leeway_seconds: u64,
    cache_ttl: Duration,
    max_cache_entries: usize,
}

impl OpaqueIntrospectionConfig {
    /// Creates an opaque-token configuration bound to one HTTPS issuer and endpoint.
    ///
    /// The client authentication setting is sent only to this configured endpoint. `None` is
    /// available for providers that explicitly support public or network-authenticated resource
    /// servers; confidential client authentication is normally the production choice.
    ///
    /// # Errors
    ///
    /// Returns [`OpaqueIntrospectionConfigError`] when a required string is blank or the
    /// endpoint is not an absolute HTTPS URL without credentials or a fragment.
    pub fn new(
        issuer: impl Into<String>,
        audience: impl Into<String>,
        endpoint: Url,
        client_id: impl Into<String>,
        authentication: OidcClientAuthentication,
    ) -> Result<Self, OpaqueIntrospectionConfigError> {
        let issuer = issuer.into();
        let audience = audience.into();
        let client_id = client_id.into();
        if issuer.trim().is_empty() || audience.trim().is_empty() || client_id.trim().is_empty() {
            return Err(OpaqueIntrospectionConfigError::BlankField);
        }
        if !is_valid_introspection_url(&endpoint) {
            return Err(OpaqueIntrospectionConfigError::InvalidEndpoint);
        }
        Ok(Self {
            issuer,
            audience,
            endpoint,
            client_id,
            authentication,
            leeway_seconds: 0,
            cache_ttl: DEFAULT_CACHE_TTL,
            max_cache_entries: DEFAULT_MAX_CACHE_ENTRIES,
        })
    }

    /// Allows explicitly configured clock skew for supplied `exp` and `nbf` values.
    #[must_use]
    pub const fn with_leeway_seconds(mut self, leeway_seconds: u64) -> Self {
        self.leeway_seconds = leeway_seconds;
        self
    }

    /// Sets the maximum successful-response cache lifetime.
    ///
    /// A zero duration disables caching. A cached entry is always additionally capped at its
    /// introspection response's `exp` value, so it cannot outlive the token itself.
    #[must_use]
    pub const fn with_cache_ttl(mut self, cache_ttl: Duration) -> Self {
        self.cache_ttl = cache_ttl;
        self
    }

    /// Limits the number of cached token fingerprints retained in memory.
    ///
    /// A zero limit disables caching. When a full cache contains only live entries, new tokens
    /// are verified remotely instead of evicting a still-valid entry unexpectedly.
    #[must_use]
    pub const fn with_max_cache_entries(mut self, max_cache_entries: usize) -> Self {
        self.max_cache_entries = max_cache_entries;
        self
    }

    /// Returns the required introspection response issuer.
    #[must_use]
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Returns the audience required in an active response.
    #[must_use]
    pub fn audience(&self) -> &str {
        &self.audience
    }

    /// Returns the configured HTTPS introspection endpoint.
    #[must_use]
    pub const fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    /// Returns the OAuth resource-server client ID.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the configured introspection client authentication method.
    #[must_use]
    pub const fn authentication(&self) -> &OidcClientAuthentication {
        &self.authentication
    }

    /// Returns the maximum duration a successful response may be cached.
    #[must_use]
    pub const fn cache_ttl(&self) -> Duration {
        self.cache_ttl
    }

    /// Returns the maximum number of token fingerprints cached in memory.
    #[must_use]
    pub const fn max_cache_entries(&self) -> usize {
        self.max_cache_entries
    }
}

impl fmt::Debug for OpaqueIntrospectionConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpaqueIntrospectionConfig")
            .field("issuer", &self.issuer)
            .field("audience", &self.audience)
            .field("endpoint", &self.endpoint)
            .field("client_id", &self.client_id)
            .field("authentication", &self.authentication)
            .field("leeway_seconds", &self.leeway_seconds)
            .field("cache_ttl", &self.cache_ttl)
            .field("max_cache_entries", &self.max_cache_entries)
            .finish()
    }
}

/// Invalid opaque-token introspection settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OpaqueIntrospectionConfigError {
    /// An issuer, audience, or client ID was blank.
    #[error("opaque introspection issuer, audience, and client ID must not be blank")]
    BlankField,
    /// The introspection endpoint was not a valid HTTPS endpoint.
    #[error(
        "opaque introspection endpoint must be an absolute HTTPS URL without credentials or a fragment"
    )]
    InvalidEndpoint,
    /// The HTTP client timeout was zero.
    #[error("opaque introspection HTTP timeout must be greater than zero")]
    ZeroHttpTimeout,
    /// The HTTP client could not be initialized.
    #[error("opaque introspection HTTP client could not be initialized")]
    HttpClientInitialization,
}

/// A raw opaque credential and the trusted client identity used to introspect it.
///
/// This request deliberately has a redacted [`Debug`] implementation. Custom introspectors must
/// use the token only for the outgoing provider request and must never log or persist it.
#[derive(Clone)]
pub struct OpaqueTokenIntrospectionRequest {
    token: String,
    client_id: String,
    authentication: OidcClientAuthentication,
}

impl OpaqueTokenIntrospectionRequest {
    /// Returns the raw credential for a trusted custom introspection adapter.
    ///
    /// The value is a bearer credential and must not be logged, serialized, or returned in an
    /// application response.
    #[must_use]
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Returns the resource-server client ID.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns client authentication to use exclusively at the trusted endpoint.
    #[must_use]
    pub const fn authentication(&self) -> &OidcClientAuthentication {
        &self.authentication
    }
}

impl fmt::Debug for OpaqueTokenIntrospectionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpaqueTokenIntrospectionRequest")
            .field("token", &"[REDACTED]")
            .field("client_id", &self.client_id)
            .field("authentication", &self.authentication)
            .finish()
    }
}

/// An adapter that retrieves a provider's opaque-token introspection response.
pub trait OpaqueTokenIntrospector: Clone + Send + Sync + 'static {
    /// Adapter-specific failure type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Retrieves one response from the configured trusted endpoint.
    fn introspect(
        &self,
        endpoint: Url,
        request: OpaqueTokenIntrospectionRequest,
    ) -> BoxFuture<'static, Result<OpaqueTokenIntrospection, Self::Error>>;
}

/// HTTPS `application/x-www-form-urlencoded` introspection adapter for production use.
#[derive(Clone)]
pub struct HttpOpaqueTokenIntrospector {
    client: Client,
}

impl HttpOpaqueTokenIntrospector {
    /// Creates an introspection adapter with a finite request timeout.
    ///
    /// # Errors
    ///
    /// Returns [`OpaqueIntrospectionConfigError`] when the timeout is zero or the client cannot
    /// be built.
    pub fn new(timeout: Duration) -> Result<Self, OpaqueIntrospectionConfigError> {
        if timeout.is_zero() {
            return Err(OpaqueIntrospectionConfigError::ZeroHttpTimeout);
        }
        let client = Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|_| OpaqueIntrospectionConfigError::HttpClientInitialization)?;
        Ok(Self { client })
    }
}

impl fmt::Debug for HttpOpaqueTokenIntrospector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpOpaqueTokenIntrospector")
            .finish_non_exhaustive()
    }
}

impl OpaqueTokenIntrospector for HttpOpaqueTokenIntrospector {
    type Error = reqwest::Error;

    fn introspect(
        &self,
        endpoint: Url,
        request: OpaqueTokenIntrospectionRequest,
    ) -> BoxFuture<'static, Result<OpaqueTokenIntrospection, Self::Error>> {
        let client = self.client.clone();
        Box::pin(async move {
            let mut form = vec![
                ("token", request.token),
                ("token_type_hint", "access_token".to_owned()),
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
            request
                .header("accept", "application/json")
                .form(&form)
                .send()
                .await?
                .error_for_status()?
                .json()
                .await
        })
    }
}

/// A provider response for one opaque bearer credential.
///
/// The deserializer defaults absent `active` to `false`, so malformed successful HTTP responses
/// never become authenticated identities.
#[derive(Clone, Debug, Deserialize)]
pub struct OpaqueTokenIntrospection {
    #[serde(default)]
    active: bool,
    #[serde(default)]
    sub: Option<String>,
    #[serde(default)]
    iss: Option<String>,
    #[serde(default)]
    aud: Option<AudienceClaim>,
    #[serde(default)]
    exp: Option<u64>,
    #[serde(default)]
    nbf: Option<u64>,
    #[serde(default)]
    tenant: Option<String>,
    #[serde(default)]
    scope: Option<ScopeClaim>,
    #[serde(default)]
    roles: Option<StringSetClaim>,
    #[serde(default)]
    permissions: Option<StringSetClaim>,
}

impl OpaqueTokenIntrospection {
    /// Creates an inactive response for custom-adapter tests.
    #[must_use]
    pub const fn inactive() -> Self {
        Self {
            active: false,
            sub: None,
            iss: None,
            aud: None,
            exp: None,
            nbf: None,
            tenant: None,
            scope: None,
            roles: None,
            permissions: None,
        }
    }

    /// Creates an active response with the three identity claims Rustee requires.
    #[must_use]
    pub fn active(
        subject: impl Into<String>,
        issuer: impl Into<String>,
        audience: impl Into<String>,
    ) -> Self {
        Self {
            active: true,
            sub: Some(subject.into()),
            iss: Some(issuer.into()),
            aud: Some(AudienceClaim::One(audience.into())),
            exp: None,
            nbf: None,
            tenant: None,
            scope: None,
            roles: None,
            permissions: None,
        }
    }

    /// Adds a provider expiration time expressed as Unix seconds.
    #[must_use]
    pub const fn with_expiration(mut self, expiration_unix_seconds: u64) -> Self {
        self.exp = Some(expiration_unix_seconds);
        self
    }

    /// Adds a provider not-before time expressed as Unix seconds.
    #[must_use]
    pub const fn with_not_before(mut self, not_before_unix_seconds: u64) -> Self {
        self.nbf = Some(not_before_unix_seconds);
        self
    }

    /// Adds a provider-confirmed tenant context.
    #[must_use]
    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    /// Adds OAuth scopes as a space-delimited string.
    #[must_use]
    pub fn with_scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(ScopeClaim::SpaceDelimited(scope.into()));
        self
    }

    /// Adds one provider-confirmed role.
    #[must_use]
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.roles = Some(StringSetClaim::One(role.into()));
        self
    }

    /// Adds one provider-confirmed direct permission.
    #[must_use]
    pub fn with_permission(mut self, permission: impl Into<String>) -> Self {
        self.permissions = Some(StringSetClaim::One(permission.into()));
        self
    }

    fn validated_principal(
        self,
        config: &OpaqueIntrospectionConfig,
    ) -> Result<(Principal, Option<Duration>), AuthError> {
        if !self.active
            || self.sub.as_deref().is_none_or(str::is_empty)
            || !self
                .iss
                .as_deref()
                .is_some_and(|issuer| constant_time_eq(issuer, &config.issuer))
            || !self
                .aud
                .as_ref()
                .is_some_and(|audience| audience.contains(&config.audience))
        {
            return Err(AuthError::RejectedBearerToken);
        }

        let now = unix_seconds();
        if self
            .exp
            .is_some_and(|expiration| expiration.saturating_add(config.leeway_seconds) <= now)
            || self
                .nbf
                .is_some_and(|not_before| not_before > now.saturating_add(config.leeway_seconds))
        {
            return Err(AuthError::RejectedBearerToken);
        }

        let subject = self.sub.expect("validated subject must be present");
        let issuer = self.iss.expect("validated issuer must be present");
        let mut principal = Principal::new(subject)
            .and_then(|principal| principal.with_issuer(issuer))
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

        let cache_ttl = self.exp.and_then(|expiration| {
            let remaining = expiration.saturating_sub(now);
            let bounded = config.cache_ttl.min(Duration::from_secs(remaining));
            (!bounded.is_zero()).then_some(bounded)
        });
        Ok((principal, cache_ttl))
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum AudienceClaim {
    One(String),
    Many(Vec<String>),
}

impl AudienceClaim {
    fn contains(&self, expected: &str) -> bool {
        match self {
            Self::One(audience) => constant_time_eq(audience, expected),
            Self::Many(audiences) => audiences
                .iter()
                .any(|audience| constant_time_eq(audience, expected)),
        }
    }
}

/// Opaque bearer authenticator with a bounded cache of active, expiring token fingerprints.
#[derive(Clone)]
pub struct OpaqueTokenAuthenticator<I> {
    config: OpaqueIntrospectionConfig,
    introspector: I,
    cache: Arc<Mutex<BTreeMap<String, CachedPrincipal>>>,
}

impl<I> OpaqueTokenAuthenticator<I>
where
    I: OpaqueTokenIntrospector,
{
    /// Creates an opaque bearer authenticator with an empty response cache.
    #[must_use]
    pub fn new(config: OpaqueIntrospectionConfig, introspector: I) -> Self {
        Self {
            config,
            introspector,
            cache: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn cached_principal(&self, cache_key: &str) -> Option<Principal> {
        let mut cache = self
            .cache
            .lock()
            .expect("opaque token cache lock must not be poisoned");
        let now = Instant::now();
        cache.retain(|_, entry| entry.expires_at > now);
        cache.get(cache_key).map(|entry| entry.principal.clone())
    }

    fn cache_principal(&self, cache_key: String, principal: Principal, ttl: Option<Duration>) {
        let Some(ttl) = ttl else {
            return;
        };
        if self.config.max_cache_entries == 0 {
            return;
        }

        let mut cache = self
            .cache
            .lock()
            .expect("opaque token cache lock must not be poisoned");
        let now = Instant::now();
        cache.retain(|_, entry| entry.expires_at > now);
        if cache.len() >= self.config.max_cache_entries && !cache.contains_key(&cache_key) {
            return;
        }
        cache.insert(
            cache_key,
            CachedPrincipal {
                principal,
                expires_at: now + ttl,
            },
        );
    }
}

impl<I> fmt::Debug for OpaqueTokenAuthenticator<I> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpaqueTokenAuthenticator")
            .field("config", &self.config)
            .field("introspector", &std::any::type_name::<I>())
            .finish_non_exhaustive()
    }
}

impl<I> BearerAuthenticator for OpaqueTokenAuthenticator<I>
where
    I: OpaqueTokenIntrospector,
{
    fn authenticate(&self, token: &str) -> BoxFuture<'static, Result<Principal, AuthError>> {
        let this = self.clone();
        let token = token.to_owned();
        Box::pin(async move {
            let cache_key = token_fingerprint(&token);
            if let Some(principal) = this.cached_principal(&cache_key) {
                return Ok(principal);
            }

            let request = OpaqueTokenIntrospectionRequest {
                token,
                client_id: this.config.client_id.clone(),
                authentication: this.config.authentication.clone(),
            };
            let result = this
                .introspector
                .introspect(this.config.endpoint.clone(), request)
                .await
                .map_err(|_| AuthError::ProviderUnavailable)?;
            let (principal, cache_ttl) = result.validated_principal(&this.config)?;
            this.cache_principal(cache_key, principal.clone(), cache_ttl);
            Ok(principal)
        })
    }
}

#[derive(Clone)]
struct CachedPrincipal {
    principal: Principal,
    expires_at: Instant,
}

fn is_valid_introspection_url(url: &Url) -> bool {
    url.scheme() == "https"
        && url.host().is_some()
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
}

fn token_fingerprint(token: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(token.as_bytes()))
}

fn form_encode_component(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
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
        collections::VecDeque,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use futures_util::future::BoxFuture;
    use rustee_auth::{AuthError, BearerAuthenticator};
    use tokio::sync::Mutex;
    use url::Url;

    use super::{
        HttpOpaqueTokenIntrospector, OidcClientAuthentication, OpaqueIntrospectionConfig,
        OpaqueIntrospectionConfigError, OpaqueTokenAuthenticator, OpaqueTokenIntrospection,
        OpaqueTokenIntrospectionRequest, OpaqueTokenIntrospector, unix_seconds,
    };

    const ISSUER: &str = "https://issuer.example.test";
    const AUDIENCE: &str = "rustee-api";

    #[derive(Clone, Debug, thiserror::Error)]
    #[error("test introspection endpoint is unavailable")]
    struct IntrospectionError;

    #[derive(Clone)]
    struct FakeIntrospector {
        replies: Arc<Mutex<VecDeque<Result<OpaqueTokenIntrospection, IntrospectionError>>>>,
        calls: Arc<AtomicUsize>,
    }

    impl FakeIntrospector {
        fn new(
            replies: impl IntoIterator<Item = Result<OpaqueTokenIntrospection, IntrospectionError>>,
        ) -> Self {
            Self {
                replies: Arc::new(Mutex::new(replies.into_iter().collect())),
                calls: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl OpaqueTokenIntrospector for FakeIntrospector {
        type Error = IntrospectionError;

        fn introspect(
            &self,
            endpoint: Url,
            request: OpaqueTokenIntrospectionRequest,
        ) -> BoxFuture<'static, Result<OpaqueTokenIntrospection, Self::Error>> {
            let replies = Arc::clone(&self.replies);
            let calls = Arc::clone(&self.calls);
            Box::pin(async move {
                assert_eq!(
                    endpoint.as_str(),
                    "https://issuer.example.test/oauth2/introspect"
                );
                assert_eq!(request.client_id(), "rustee-resource-server");
                assert!(
                    matches!(request.token(), "opaque-token" | "another-opaque-token"),
                    "test must use one known opaque credential"
                );
                calls.fetch_add(1, Ordering::SeqCst);
                replies
                    .lock()
                    .await
                    .pop_front()
                    .expect("test introspector needs a queued reply")
            })
        }
    }

    fn config() -> OpaqueIntrospectionConfig {
        OpaqueIntrospectionConfig::new(
            ISSUER,
            AUDIENCE,
            Url::parse("https://issuer.example.test/oauth2/introspect")
                .expect("test URL must be valid"),
            "rustee-resource-server",
            OidcClientAuthentication::None,
        )
        .expect("test configuration must be valid")
    }

    fn active_response() -> OpaqueTokenIntrospection {
        OpaqueTokenIntrospection::active("alice", ISSUER, AUDIENCE)
            .with_expiration(unix_seconds() + 300)
            .with_tenant("acme")
            .with_scope("profile:read profile:write")
            .with_role("project-viewer")
            .with_permission("project:read")
    }

    #[tokio::test]
    async fn validates_active_response_and_caches_only_a_fingerprint() {
        let introspector = FakeIntrospector::new([Ok(active_response())]);
        let authenticator = OpaqueTokenAuthenticator::new(config(), introspector.clone());

        let principal = authenticator
            .authenticate("opaque-token")
            .await
            .expect("active matching response must authenticate");
        authenticator
            .authenticate("opaque-token")
            .await
            .expect("unexpired response must be served from cache");

        assert_eq!(principal.subject(), "alice");
        assert_eq!(principal.issuer(), Some(ISSUER));
        assert_eq!(principal.tenant(), Some("acme"));
        assert!(principal.has_scope("profile:read"));
        assert!(principal.has_role("project-viewer"));
        assert!(principal.has_permission("project:read"));
        assert_eq!(introspector.calls(), 1);
        assert!(!format!("{authenticator:?}").contains("opaque-token"));
    }

    #[tokio::test]
    async fn rejects_inactive_or_mismatched_identity_responses() {
        let inactive = OpaqueTokenAuthenticator::new(
            config(),
            FakeIntrospector::new([Ok(OpaqueTokenIntrospection::inactive())]),
        );
        assert_eq!(
            inactive.authenticate("opaque-token").await.unwrap_err(),
            AuthError::RejectedBearerToken
        );

        let wrong_issuer = OpaqueTokenAuthenticator::new(
            config(),
            FakeIntrospector::new([Ok(OpaqueTokenIntrospection::active(
                "alice",
                "https://other.example.test",
                AUDIENCE,
            )
            .with_expiration(unix_seconds() + 300))]),
        );
        assert_eq!(
            wrong_issuer.authenticate("opaque-token").await.unwrap_err(),
            AuthError::RejectedBearerToken
        );

        let expired = OpaqueTokenAuthenticator::new(
            config(),
            FakeIntrospector::new([Ok(OpaqueTokenIntrospection::active(
                "alice", ISSUER, AUDIENCE,
            )
            .with_expiration(unix_seconds().saturating_sub(1)))]),
        );
        assert_eq!(
            expired.authenticate("opaque-token").await.unwrap_err(),
            AuthError::RejectedBearerToken
        );
    }

    #[tokio::test]
    async fn provider_failure_is_fail_closed_and_cache_requires_an_expiration() {
        let unavailable = OpaqueTokenAuthenticator::new(
            config(),
            FakeIntrospector::new([Err(IntrospectionError)]),
        );
        assert_eq!(
            unavailable.authenticate("opaque-token").await.unwrap_err(),
            AuthError::ProviderUnavailable
        );

        let no_expiration = OpaqueTokenAuthenticator::new(
            config(),
            FakeIntrospector::new([
                Ok(OpaqueTokenIntrospection::active("alice", ISSUER, AUDIENCE)),
                Ok(OpaqueTokenIntrospection::active("alice", ISSUER, AUDIENCE)),
            ]),
        );
        no_expiration
            .authenticate("opaque-token")
            .await
            .expect("active response without expiration still works");
        no_expiration
            .authenticate("opaque-token")
            .await
            .expect("unbounded response must be checked again");
        assert_eq!(no_expiration.introspector.calls(), 2);
    }

    #[tokio::test]
    async fn cache_capacity_limits_retained_fingerprints() {
        let introspector = FakeIntrospector::new([
            Ok(active_response()),
            Ok(active_response()),
            Ok(active_response()),
        ]);
        let authenticator =
            OpaqueTokenAuthenticator::new(config().with_max_cache_entries(1), introspector.clone());

        authenticator
            .authenticate("opaque-token")
            .await
            .expect("first token must authenticate");
        authenticator
            .authenticate("another-opaque-token")
            .await
            .expect("second token must authenticate without evicting first");
        authenticator
            .authenticate("another-opaque-token")
            .await
            .expect("uncached second token must be remotely checked again");

        assert_eq!(introspector.calls(), 3);
    }

    #[test]
    fn rejects_invalid_config_and_zero_http_timeout() {
        let endpoint =
            Url::parse("http://issuer.example.test/introspect").expect("test URL must parse");
        assert_eq!(
            OpaqueIntrospectionConfig::new(
                ISSUER,
                AUDIENCE,
                endpoint,
                "rustee-resource-server",
                OidcClientAuthentication::None,
            )
            .unwrap_err(),
            OpaqueIntrospectionConfigError::InvalidEndpoint
        );
        assert_eq!(
            HttpOpaqueTokenIntrospector::new(Duration::ZERO).unwrap_err(),
            OpaqueIntrospectionConfigError::ZeroHttpTimeout
        );
    }

    #[test]
    fn request_debug_redacts_the_bearer_credential() {
        let request = OpaqueTokenIntrospectionRequest {
            token: "opaque-token".to_owned(),
            client_id: "rustee-resource-server".to_owned(),
            authentication: OidcClientAuthentication::None,
        };
        assert!(!format!("{request:?}").contains("opaque-token"));
    }
}
