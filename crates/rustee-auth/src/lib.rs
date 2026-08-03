//! Provider-neutral authentication principal, bearer middleware, and scope policy.
//!
//! Token verification belongs in a provider crate. This crate accepts only the verified identity
//! result and never stores a raw bearer token in a request extension.

use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    fmt,
    task::{Context, Poll},
};

use futures_util::future::BoxFuture;
use hmac::{Hmac, Mac};
use http::{
    HeaderMap, HeaderValue, StatusCode,
    header::{AUTHORIZATION, HOST, HeaderName},
};
use rustee_core::{Error, FromRequest, IntoResponse, Request, Response, RouteParams, StateStore};
use sha2::Sha256;
use tower::{Layer, Service, util::BoxCloneService};
use zeroize::Zeroize;

pub use rustee_tenant::{TenantContext, TenantContextError as TenantPolicyError};

/// A validated identity made available to application handlers.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Principal {
    subject: String,
    issuer: Option<String>,
    tenant: Option<String>,
    scopes: BTreeSet<String>,
    #[serde(default)]
    roles: BTreeSet<String>,
    #[serde(default)]
    permissions: BTreeSet<String>,
}

impl Principal {
    /// Creates a principal with a non-blank subject identifier.
    ///
    /// # Errors
    ///
    /// Returns [`PrincipalError::BlankField`] when `subject` is blank.
    pub fn new(subject: impl Into<String>) -> Result<Self, PrincipalError> {
        let subject = subject.into();
        ensure_not_blank(&subject, "subject")?;
        Ok(Self {
            subject,
            issuer: None,
            tenant: None,
            scopes: BTreeSet::new(),
            roles: BTreeSet::new(),
            permissions: BTreeSet::new(),
        })
    }

    /// Adds the issuer that validated this principal.
    ///
    /// # Errors
    ///
    /// Returns [`PrincipalError::BlankField`] when `issuer` is blank.
    pub fn with_issuer(mut self, issuer: impl Into<String>) -> Result<Self, PrincipalError> {
        let issuer = issuer.into();
        ensure_not_blank(&issuer, "issuer")?;
        self.issuer = Some(issuer);
        Ok(self)
    }

    /// Adds the verified tenant context for this principal.
    ///
    /// # Errors
    ///
    /// Returns [`PrincipalError::BlankField`] when `tenant` is blank.
    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Result<Self, PrincipalError> {
        let tenant = tenant.into();
        ensure_not_blank(&tenant, "tenant")?;
        self.tenant = Some(tenant);
        Ok(self)
    }

    /// Adds a verified scope.
    ///
    /// # Errors
    ///
    /// Returns [`PrincipalError::BlankField`] when `scope` is blank.
    pub fn with_scope(mut self, scope: impl Into<String>) -> Result<Self, PrincipalError> {
        let scope = scope.into();
        ensure_not_blank(&scope, "scope")?;
        self.scopes.insert(scope);
        Ok(self)
    }

    /// Adds a role supplied by a trusted identity verifier or server-side identity mapping.
    ///
    /// # Errors
    ///
    /// Returns [`PrincipalError::BlankField`] when `role` is blank.
    pub fn with_role(mut self, role: impl Into<String>) -> Result<Self, PrincipalError> {
        let role = role.into();
        ensure_not_blank(&role, "role")?;
        self.roles.insert(role);
        Ok(self)
    }

    /// Adds a direct permission supplied by a trusted identity verifier or server-side mapping.
    ///
    /// # Errors
    ///
    /// Returns [`PrincipalError::BlankField`] when `permission` is blank.
    pub fn with_permission(
        mut self,
        permission: impl Into<String>,
    ) -> Result<Self, PrincipalError> {
        let permission = permission.into();
        ensure_not_blank(&permission, "permission")?;
        self.permissions.insert(permission);
        Ok(self)
    }

    /// Returns the authenticated subject identifier.
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Returns the issuer when the verifier provided one.
    #[must_use]
    pub fn issuer(&self) -> Option<&str> {
        self.issuer.as_deref()
    }

    /// Returns the verified tenant when the verifier provided one.
    #[must_use]
    pub fn tenant(&self) -> Option<&str> {
        self.tenant.as_deref()
    }

    /// Returns the verified scopes in deterministic order.
    pub fn scopes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.scopes.iter().map(String::as_str)
    }

    /// Returns whether this principal includes the supplied scope.
    #[must_use]
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.contains(scope)
    }

    /// Returns verified roles in deterministic order.
    pub fn roles(&self) -> impl ExactSizeIterator<Item = &str> {
        self.roles.iter().map(String::as_str)
    }

    /// Returns whether the principal has one verified role.
    #[must_use]
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.contains(role)
    }

    /// Returns direct verified permissions in deterministic order.
    pub fn permissions(&self) -> impl ExactSizeIterator<Item = &str> {
        self.permissions.iter().map(String::as_str)
    }

    /// Returns whether the principal has one direct verified permission.
    #[must_use]
    pub fn has_permission(&self, permission: &str) -> bool {
        self.permissions.contains(permission)
    }
}

/// Invalid principal content rejected before it reaches request extensions.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PrincipalError {
    /// A required identity field was blank.
    #[error("{field} must not be blank")]
    BlankField {
        /// The invalid field name.
        field: &'static str,
    },
}

fn ensure_not_blank(value: &str, field: &'static str) -> Result<(), PrincipalError> {
    if value.trim().is_empty() {
        return Err(PrincipalError::BlankField { field });
    }
    Ok(())
}

/// A failure that is safe to render as an authentication rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AuthError {
    /// No bearer credential was supplied.
    #[error("missing bearer token")]
    MissingBearerToken,
    /// The authorization header did not contain one well-formed bearer credential.
    #[error("invalid bearer token")]
    InvalidBearerToken,
    /// A provider rejected a syntactically valid bearer credential.
    #[error("bearer token was rejected")]
    RejectedBearerToken,
    /// Required authentication infrastructure could not be reached safely.
    #[error("authentication provider is unavailable")]
    ProviderUnavailable,
}

/// A provider-specific verifier of a bearer credential.
pub trait BearerAuthenticator: Clone + Send + Sync + 'static {
    /// Verifies a raw bearer credential and returns only a validated principal.
    fn authenticate(&self, token: &str) -> BoxFuture<'static, Result<Principal, AuthError>>;
}

/// A deliberately simple static authenticator intended for tests and local examples only.
#[derive(Clone, Default)]
pub struct StaticTokenAuthenticator {
    tokens: BTreeMap<String, Principal>,
}

impl StaticTokenAuthenticator {
    /// Creates an empty authenticator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one local token-to-principal mapping.
    ///
    /// # Errors
    ///
    /// Returns [`StaticTokenError::BlankToken`] when `token` is blank.
    pub fn insert(
        &mut self,
        token: impl Into<String>,
        principal: Principal,
    ) -> Result<(), StaticTokenError> {
        let token = token.into();
        if token.trim().is_empty() {
            return Err(StaticTokenError::BlankToken);
        }
        self.tokens.insert(token, principal);
        Ok(())
    }
}

impl fmt::Debug for StaticTokenAuthenticator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StaticTokenAuthenticator")
            .field("registered_tokens", &self.tokens.len())
            .finish()
    }
}

impl BearerAuthenticator for StaticTokenAuthenticator {
    fn authenticate(&self, token: &str) -> BoxFuture<'static, Result<Principal, AuthError>> {
        let principal = self.tokens.get(token).cloned();
        Box::pin(async move { principal.ok_or(AuthError::RejectedBearerToken) })
    }
}

/// Invalid static authenticator configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StaticTokenError {
    /// A local static token was blank.
    #[error("static bearer token must not be blank")]
    BlankToken,
}

const MAX_API_KEY_BYTES: usize = 4 * 1024;
const API_KEY_PEPPER_BYTES: usize = 32;

/// Secret material used to derive API-key lookup fingerprints.
///
/// Load this value from a secret manager or a deployment-owned protected configuration source.
/// It is deliberately not serializable or printable.
pub struct ApiKeyPepper([u8; API_KEY_PEPPER_BYTES]);

impl ApiKeyPepper {
    /// Creates a pepper from exactly 256 bits of deployment-owned secret material.
    ///
    /// # Errors
    ///
    /// Returns [`ApiKeyPepperError::AllZero`] for the all-zero value, which would not provide a
    /// deployment-held secret for the keyed derivation.
    pub fn new(bytes: [u8; API_KEY_PEPPER_BYTES]) -> Result<Self, ApiKeyPepperError> {
        if bytes.iter().all(|byte| *byte == 0) {
            return Err(ApiKeyPepperError::AllZero);
        }
        Ok(Self(bytes))
    }

    /// Derives a bounded, keyed lookup fingerprint without exposing the API key to a store.
    ///
    /// # Errors
    ///
    /// Returns [`ApiKeyError::InvalidApiKey`] when `api_key` cannot appear as a valid API-key
    /// header value. A failure to initialize the HMAC implementation is mapped to
    /// [`ApiKeyError::ProviderUnavailable`].
    pub fn fingerprint(&self, api_key: &str) -> Result<ApiKeyFingerprint, ApiKeyError> {
        if !is_valid_api_key_value(api_key) {
            return Err(ApiKeyError::InvalidApiKey);
        }

        let mut mac = Hmac::<Sha256>::new_from_slice(&self.0)
            .map_err(|_| ApiKeyError::ProviderUnavailable)?;
        mac.update(api_key.as_bytes());
        Ok(ApiKeyFingerprint(mac.finalize().into_bytes().into()))
    }
}

impl Clone for ApiKeyPepper {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

impl Drop for ApiKeyPepper {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl fmt::Debug for ApiKeyPepper {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApiKeyPepper([redacted])")
    }
}

/// Invalid deployment-owned API-key pepper material.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ApiKeyPepperError {
    /// The all-zero value is not a deployment-held secret.
    #[error("API-key pepper must not be all zero")]
    AllZero,
}

/// Opaque HMAC-SHA-256 lookup value for one API key.
///
/// The value is not the API key and has no text rendering. It can be used as the primary lookup
/// key in a provider store, alongside that provider's active/revoked state and audit transaction.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiKeyFingerprint([u8; 32]);

impl ApiKeyFingerprint {
    /// Returns the fixed-size binary value for a protected provider lookup.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for ApiKeyFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApiKeyFingerprint([redacted])")
    }
}

/// A production-facing API-key store that receives only a keyed fingerprint.
///
/// The store owns persistence and maps unknown, revoked, expired, or disabled keys to
/// [`ApiKeyError::RejectedApiKey`]. It may atomically update last-used/audit records with a
/// successful lookup, but must not record the raw API key or fingerprint in general logs.
pub trait ApiKeyFingerprintStore: Clone + Send + Sync + 'static {
    /// Resolves one keyed fingerprint to a validated principal.
    fn authenticate(
        &self,
        fingerprint: ApiKeyFingerprint,
    ) -> BoxFuture<'static, Result<Principal, ApiKeyError>>;
}

/// [`ApiKeyAuthenticator`] implementation that sends only a keyed fingerprint to its store.
///
/// Rotation and revocation are represented by store records: deployments can keep multiple active
/// fingerprints for one principal during client-key rotation and reject a revoked record without
/// changing the HTTP layer.
#[derive(Clone)]
pub struct KeyedApiKeyAuthenticator<S> {
    pepper: ApiKeyPepper,
    store: S,
}

impl<S> KeyedApiKeyAuthenticator<S> {
    /// Creates an API-key authenticator that derives HMAC-SHA-256 lookup fingerprints.
    #[must_use]
    pub fn new(pepper: ApiKeyPepper, store: S) -> Self {
        Self { pepper, store }
    }
}

impl<S> fmt::Debug for KeyedApiKeyAuthenticator<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KeyedApiKeyAuthenticator")
            .field("store", &std::any::type_name::<S>())
            .finish_non_exhaustive()
    }
}

/// A failure that is safe to render as an API-key authentication rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ApiKeyError {
    /// The configured API-key header was not present.
    #[error("missing API key")]
    MissingApiKey,
    /// The API-key header was malformed, repeated, or outside the accepted bound.
    #[error("invalid API key")]
    InvalidApiKey,
    /// A provider rejected a syntactically valid API key.
    #[error("API key was rejected")]
    RejectedApiKey,
    /// Required API-key authentication infrastructure could not be reached safely.
    #[error("API-key authentication provider is unavailable")]
    ProviderUnavailable,
}

/// A provider-specific verifier of one API-key header value.
///
/// Implementations receive a bounded printable ASCII value from [`ApiKeyLayer`] and return only a
/// validated [`Principal`]. They must not log the key and should use a constant-time comparison or
/// a secret-safe lookup when handling production credentials.
pub trait ApiKeyAuthenticator: Clone + Send + Sync + 'static {
    /// Verifies a raw API-key header value and returns only a validated principal.
    fn authenticate(&self, api_key: &str) -> BoxFuture<'static, Result<Principal, ApiKeyError>>;
}

impl<S> ApiKeyAuthenticator for KeyedApiKeyAuthenticator<S>
where
    S: ApiKeyFingerprintStore,
{
    fn authenticate(&self, api_key: &str) -> BoxFuture<'static, Result<Principal, ApiKeyError>> {
        let fingerprint = self.pepper.fingerprint(api_key);
        let store = self.store.clone();
        Box::pin(async move { store.authenticate(fingerprint?).await })
    }
}

/// A deliberately simple static API-key authenticator for tests and local examples only.
///
/// Production applications should keep only a derived lookup value in their identity provider and
/// make its comparison and rotation policy explicit.
#[derive(Clone, Default)]
pub struct StaticApiKeyAuthenticator {
    keys: BTreeMap<String, Principal>,
}

impl StaticApiKeyAuthenticator {
    /// Creates an empty local API-key authenticator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one local API-key-to-principal mapping.
    ///
    /// # Errors
    ///
    /// Returns [`StaticApiKeyError::InvalidKey`] when `api_key` cannot appear as a bounded
    /// printable API-key header value.
    pub fn insert(
        &mut self,
        api_key: impl Into<String>,
        principal: Principal,
    ) -> Result<(), StaticApiKeyError> {
        let api_key = api_key.into();
        if !is_valid_api_key_value(&api_key) {
            return Err(StaticApiKeyError::InvalidKey);
        }
        self.keys.insert(api_key, principal);
        Ok(())
    }
}

impl fmt::Debug for StaticApiKeyAuthenticator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StaticApiKeyAuthenticator")
            .field("registered_keys", &self.keys.len())
            .finish()
    }
}

impl ApiKeyAuthenticator for StaticApiKeyAuthenticator {
    fn authenticate(&self, api_key: &str) -> BoxFuture<'static, Result<Principal, ApiKeyError>> {
        let principal = self.keys.get(api_key).cloned();
        Box::pin(async move { principal.ok_or(ApiKeyError::RejectedApiKey) })
    }
}

/// Invalid local static API-key authenticator configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StaticApiKeyError {
    /// A static key was blank, too large, or not printable ASCII.
    #[error("static API key must be printable ASCII and at most {MAX_API_KEY_BYTES} bytes")]
    InvalidKey,
}

/// Invalid [`ApiKeyLayer`] configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ApiKeyLayerError {
    /// The configured credential header was not a valid HTTP field name.
    #[error("API-key header name must be a valid HTTP field name")]
    InvalidHeaderName,
}

/// Tower layer that authenticates every request from one explicit API-key header.
///
/// The layer rejects missing, repeated, non-ASCII, blank, or oversized values before a provider
/// sees them. It does not read query or cookie credentials, infer a key header from `OpenAPI`, or
/// expose a raw key to handlers.
#[derive(Clone)]
#[must_use = "an API-key authentication layer must be applied to a service to have an effect"]
pub struct ApiKeyLayer<A> {
    header_name: HeaderName,
    authenticator: A,
}

impl<A> ApiKeyLayer<A> {
    /// Creates an API-key layer for one explicit request header.
    ///
    /// # Errors
    ///
    /// Returns [`ApiKeyLayerError::InvalidHeaderName`] when `header_name` is not an HTTP field
    /// name.
    pub fn header(
        header_name: impl AsRef<str>,
        authenticator: A,
    ) -> Result<Self, ApiKeyLayerError> {
        let header_name = HeaderName::from_bytes(header_name.as_ref().as_bytes())
            .map_err(|_| ApiKeyLayerError::InvalidHeaderName)?;
        Ok(Self {
            header_name,
            authenticator,
        })
    }

    /// Returns the normalized HTTP field name carrying the API key.
    #[must_use]
    pub fn header_name(&self) -> &HeaderName {
        &self.header_name
    }
}

impl<A> fmt::Debug for ApiKeyLayer<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApiKeyLayer")
            .field("header_name", &self.header_name)
            .field("authenticator", &std::any::type_name::<A>())
            .finish()
    }
}

/// Service produced by [`ApiKeyLayer`].
#[derive(Clone)]
pub struct ApiKeyService<A> {
    inner: BoxCloneService<Request, Response, Infallible>,
    header_name: HeaderName,
    authenticator: A,
}

impl<A> fmt::Debug for ApiKeyService<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApiKeyService")
            .field("header_name", &self.header_name)
            .field("authenticator", &std::any::type_name::<A>())
            .finish_non_exhaustive()
    }
}

impl<S, A> Layer<S> for ApiKeyLayer<A>
where
    A: ApiKeyAuthenticator,
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = ApiKeyService<A>;

    fn layer(&self, inner: S) -> Self::Service {
        ApiKeyService {
            inner: BoxCloneService::new(inner),
            header_name: self.header_name.clone(),
            authenticator: self.authenticator.clone(),
        }
    }
}

impl<A> Service<Request> for ApiKeyService<A>
where
    A: ApiKeyAuthenticator,
{
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, mut request: Request) -> Self::Future {
        let header_name = self.header_name.clone();
        let authenticator = self.authenticator.clone();
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let api_key = match api_key_header_value(request.headers(), &header_name) {
                Ok(api_key) => api_key.to_owned(),
                Err(error) => return Ok(api_key_authentication_response(error)),
            };
            let principal = match authenticator.authenticate(&api_key).await {
                Ok(principal) => principal,
                Err(error) => return Ok(api_key_authentication_response(error)),
            };
            request.extensions_mut().insert(principal);
            inner.call(request).await
        })
    }
}

/// Tower layer that authenticates every request with a bearer credential.
#[derive(Clone)]
#[must_use = "an authentication layer must be applied to a service to have an effect"]
pub struct AuthLayer<A> {
    authenticator: A,
}

impl<A> AuthLayer<A> {
    /// Creates a bearer authentication layer from a provider verifier.
    pub fn bearer(authenticator: A) -> Self {
        Self { authenticator }
    }
}

impl<A> fmt::Debug for AuthLayer<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthLayer")
            .field("authenticator", &std::any::type_name::<A>())
            .finish()
    }
}

/// Service produced by [`AuthLayer`].
#[derive(Clone)]
pub struct AuthService<A> {
    inner: BoxCloneService<Request, Response, Infallible>,
    authenticator: A,
}

impl<A> fmt::Debug for AuthService<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthService")
            .field("authenticator", &std::any::type_name::<A>())
            .finish()
    }
}

impl<S, A> Layer<S> for AuthLayer<A>
where
    A: BearerAuthenticator,
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = AuthService<A>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthService {
            inner: BoxCloneService::new(inner),
            authenticator: self.authenticator.clone(),
        }
    }
}

impl<A> Service<Request> for AuthService<A>
where
    A: BearerAuthenticator,
{
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, mut request: Request) -> Self::Future {
        let authenticator = self.authenticator.clone();
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let token = match bearer_token(request.headers().get(AUTHORIZATION)) {
                Ok(token) => token.to_owned(),
                Err(error) => return Ok(authentication_response(error)),
            };
            let principal = match authenticator.authenticate(&token).await {
                Ok(principal) => principal,
                Err(error) => return Ok(authentication_response(error)),
            };
            request.extensions_mut().insert(principal);
            inner.call(request).await
        })
    }
}

/// Extracts the authenticated principal or returns a 401 response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthUser(pub Principal);

impl FromRequest for AuthUser {
    fn from_request<'a>(
        request: &'a mut Request,
        _params: &'a RouteParams,
        _state: &'a StateStore,
    ) -> BoxFuture<'a, rustee_core::Result<Self>> {
        Box::pin(async move { required_principal(request).map(Self) })
    }
}

/// Extracts an authenticated principal with an explicit route-level requirement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequireAuth(pub Principal);

impl FromRequest for RequireAuth {
    fn from_request<'a>(
        request: &'a mut Request,
        _params: &'a RouteParams,
        _state: &'a StateStore,
    ) -> BoxFuture<'a, rustee_core::Result<Self>> {
        Box::pin(async move { required_principal(request).map(Self) })
    }
}

/// Extracts an authenticated principal when present without requiring one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptionalAuthUser(pub Option<Principal>);

impl FromRequest for OptionalAuthUser {
    fn from_request<'a>(
        request: &'a mut Request,
        _params: &'a RouteParams,
        _state: &'a StateStore,
    ) -> BoxFuture<'a, rustee_core::Result<Self>> {
        Box::pin(async move { Ok(Self(request.extensions().get::<Principal>().cloned())) })
    }
}

/// A layer that requires all configured scopes from an authenticated principal.
#[derive(Clone, Debug, Eq, PartialEq)]
#[must_use = "a scope policy must be applied to a service to have an effect"]
pub struct RequireScopesLayer {
    required: BTreeSet<String>,
}

impl RequireScopesLayer {
    /// Creates a policy that requires every supplied scope.
    ///
    /// # Errors
    ///
    /// Returns [`ScopePolicyError`] for an empty requirement or blank scope.
    pub fn new(
        scopes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, ScopePolicyError> {
        let required = scopes.into_iter().map(Into::into).collect::<BTreeSet<_>>();
        if required.is_empty() {
            return Err(ScopePolicyError::EmptyRequirement);
        }
        if required.iter().any(|scope| scope.trim().is_empty()) {
            return Err(ScopePolicyError::BlankScope);
        }
        Ok(Self { required })
    }
}

/// Invalid scope policy configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ScopePolicyError {
    /// No scopes were required.
    #[error("a scope policy must require at least one scope")]
    EmptyRequirement,
    /// A supplied scope was blank.
    #[error("a required scope must not be blank")]
    BlankScope,
}

/// Service produced by [`RequireScopesLayer`].
#[derive(Clone, Debug)]
pub struct RequireScopes {
    inner: BoxCloneService<Request, Response, Infallible>,
    required: BTreeSet<String>,
}

impl<S> Layer<S> for RequireScopesLayer
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = RequireScopes;

    fn layer(&self, inner: S) -> Self::Service {
        RequireScopes {
            inner: BoxCloneService::new(inner),
            required: self.required.clone(),
        }
    }
}

impl Service<Request> for RequireScopes {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let required = self.required.clone();
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let Some(principal) = request.extensions().get::<Principal>() else {
                return Ok(authentication_response(AuthError::MissingBearerToken));
            };
            if !required.iter().all(|scope| principal.has_scope(scope)) {
                return Ok(Error::new(
                    StatusCode::FORBIDDEN,
                    "insufficient_scope",
                    "the authenticated principal lacks a required scope",
                )
                .into_response());
            }
            inner.call(request).await
        })
    }
}

/// A server-side mapping from trusted role names to granted permissions.
///
/// This policy deliberately lives in application configuration rather than token parsing. Each
/// deployment stays in control of what an IdP-provided role is allowed to do.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RolePolicy {
    grants: BTreeMap<String, BTreeSet<String>>,
}

impl RolePolicy {
    /// Creates an empty policy that grants no role-derived permissions.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds every supplied permission to one role.
    ///
    /// # Errors
    ///
    /// Returns [`RolePolicyError`] when the role is blank, no permissions are supplied, or one
    /// permission is blank.
    pub fn grant(
        &mut self,
        role: impl Into<String>,
        permissions: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<(), RolePolicyError> {
        let role = role.into();
        if role.trim().is_empty() {
            return Err(RolePolicyError::BlankRole);
        }
        let permissions = permissions
            .into_iter()
            .map(Into::into)
            .collect::<BTreeSet<_>>();
        if permissions.is_empty() {
            return Err(RolePolicyError::EmptyPermissions);
        }
        if permissions
            .iter()
            .any(|permission| permission.trim().is_empty())
        {
            return Err(RolePolicyError::BlankPermission);
        }
        self.grants.entry(role).or_default().extend(permissions);
        Ok(())
    }

    /// Returns whether a direct permission or any principal role grants `permission`.
    #[must_use]
    pub fn permits(&self, principal: &Principal, permission: &str) -> bool {
        principal.has_permission(permission)
            || principal.roles().any(|role| {
                self.grants
                    .get(role)
                    .is_some_and(|permissions| permissions.contains(permission))
            })
    }
}

/// Invalid role-to-permission policy settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RolePolicyError {
    /// The configured role was blank.
    #[error("a role policy role must not be blank")]
    BlankRole,
    /// The configured role had no permissions.
    #[error("a role policy must grant at least one permission")]
    EmptyPermissions,
    /// A configured permission was blank.
    #[error("a role policy permission must not be blank")]
    BlankPermission,
}

/// A layer that requires every configured permission from direct grants or [`RolePolicy`] roles.
#[derive(Clone, Debug, Eq, PartialEq)]
#[must_use = "a permission policy must be applied to a service to have an effect"]
pub struct RequirePermissionsLayer {
    required: BTreeSet<String>,
    policy: RolePolicy,
}

impl RequirePermissionsLayer {
    /// Creates a policy that requires every supplied permission.
    ///
    /// # Errors
    ///
    /// Returns [`PermissionPolicyError`] for empty or blank permission requirements.
    pub fn new(
        permissions: impl IntoIterator<Item = impl Into<String>>,
        policy: RolePolicy,
    ) -> Result<Self, PermissionPolicyError> {
        let required = permissions
            .into_iter()
            .map(Into::into)
            .collect::<BTreeSet<_>>();
        if required.is_empty() {
            return Err(PermissionPolicyError::EmptyRequirement);
        }
        if required
            .iter()
            .any(|permission| permission.trim().is_empty())
        {
            return Err(PermissionPolicyError::BlankPermission);
        }
        Ok(Self { required, policy })
    }
}

/// Invalid permission policy configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PermissionPolicyError {
    /// No permissions were required.
    #[error("a permission policy must require at least one permission")]
    EmptyRequirement,
    /// A supplied permission was blank.
    #[error("a required permission must not be blank")]
    BlankPermission,
}

/// Service produced by [`RequirePermissionsLayer`].
#[derive(Clone, Debug)]
pub struct RequirePermissions {
    inner: BoxCloneService<Request, Response, Infallible>,
    required: BTreeSet<String>,
    policy: RolePolicy,
}

impl<S> Layer<S> for RequirePermissionsLayer
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = RequirePermissions;

    fn layer(&self, inner: S) -> Self::Service {
        RequirePermissions {
            inner: BoxCloneService::new(inner),
            required: self.required.clone(),
            policy: self.policy.clone(),
        }
    }
}

impl Service<Request> for RequirePermissions {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let required = self.required.clone();
        let policy = self.policy.clone();
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let Some(principal) = request.extensions().get::<Principal>() else {
                return Ok(authentication_response(AuthError::MissingBearerToken));
            };
            if !required
                .iter()
                .all(|permission| policy.permits(principal, permission))
            {
                return Ok(Error::new(
                    StatusCode::FORBIDDEN,
                    "insufficient_permission",
                    "the authenticated principal lacks a required permission",
                )
                .into_response());
            }
            inner.call(request).await
        })
    }
}

/// Resolves a tenant from a server-controlled routing, host-mapping, or session source.
///
/// Resolvers receive an authenticated principal but must not treat an arbitrary client header as a
/// tenant authority. [`TenantResolutionLayer`] verifies the resolved context against that
/// principal before the inner service can observe it.
pub trait TenantResolver: Clone + Send + Sync + 'static {
    /// Provider-specific infrastructure failure.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Resolves the context for this request, or returns `None` for an unmapped tenant.
    fn resolve(
        &self,
        request: &Request,
        principal: &Principal,
    ) -> BoxFuture<'static, Result<Option<TenantContext>, Self::Error>>;
}

/// A server-configured, exact-authority tenant resolver.
///
/// The request `Host` selects only a configured routing scope. It does not grant access: the
/// resolution layer still requires the authenticated principal to have the same tenant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostTenantResolver {
    tenants: BTreeMap<String, TenantContext>,
}

impl HostTenantResolver {
    /// Creates a resolver from one or more configured host authority to tenant mappings.
    ///
    /// Host matching is ASCII case-insensitive and exact after HTTP authority parsing. A mapping
    /// must not use userinfo, whitespace, quotes, or a duplicate authority.
    ///
    /// # Errors
    ///
    /// Returns [`HostTenantResolverError`] for an empty map or an invalid host mapping.
    pub fn new<I, H>(hosts: I) -> Result<Self, HostTenantResolverError>
    where
        I: IntoIterator<Item = (H, TenantContext)>,
        H: Into<String>,
    {
        let mut tenants = BTreeMap::new();
        for (host, tenant) in hosts {
            let host = canonical_host(&host.into())?;
            if tenants.insert(host.clone(), tenant).is_some() {
                return Err(HostTenantResolverError::DuplicateHost);
            }
        }
        if tenants.is_empty() {
            return Err(HostTenantResolverError::EmptyMapping);
        }
        Ok(Self { tenants })
    }
}

impl TenantResolver for HostTenantResolver {
    type Error = Infallible;

    fn resolve(
        &self,
        request: &Request,
        _principal: &Principal,
    ) -> BoxFuture<'static, Result<Option<TenantContext>, Self::Error>> {
        let tenant =
            request_host(request.headers()).and_then(|host| self.tenants.get(&host).cloned());
        Box::pin(async move { Ok(tenant) })
    }
}

/// Invalid server-configured host-to-tenant mapping.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum HostTenantResolverError {
    /// No host mapping was configured.
    #[error("at least one tenant host mapping is required")]
    EmptyMapping,
    /// One configured authority was blank or structurally invalid.
    #[error("tenant host mapping must be one valid HTTP authority without userinfo")]
    InvalidHost,
    /// More than one configured mapping normalized to the same authority.
    #[error("tenant host mapping contains a duplicate authority")]
    DuplicateHost,
}

fn canonical_host(host: &str) -> Result<String, HostTenantResolverError> {
    if host.trim().is_empty() || host.contains([' ', '\"', '@']) {
        return Err(HostTenantResolverError::InvalidHost);
    }
    host.parse::<http::uri::Authority>()
        .map(|authority| authority.as_str().to_ascii_lowercase())
        .map_err(|_| HostTenantResolverError::InvalidHost)
}

fn request_host(headers: &HeaderMap) -> Option<String> {
    let values = headers.get_all(HOST);
    let mut values = values.iter();
    let host = values.next()?;
    if values.next().is_some() {
        return None;
    }
    host.to_str()
        .ok()
        .and_then(|host| canonical_host(host).ok())
}

/// A layer that resolves a trusted tenant, checks it against the principal, and inserts it.
///
/// Place this layer inside [`AuthLayer`] so the resolver receives a verified [`Principal`]. It
/// returns 404 for an unmapped tenant, 403 for a principal mismatch, and a sanitized 503 when its
/// resolver fails.
#[derive(Clone)]
#[must_use = "a tenant resolution layer must be applied to a service to have an effect"]
pub struct TenantResolutionLayer<R> {
    resolver: R,
}

impl<R> TenantResolutionLayer<R> {
    /// Creates a tenant-resolution boundary from a trusted resolver.
    pub const fn new(resolver: R) -> Self {
        Self { resolver }
    }
}

impl<R> fmt::Debug for TenantResolutionLayer<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TenantResolutionLayer")
            .field("resolver", &std::any::type_name::<R>())
            .finish()
    }
}

/// Service produced by [`TenantResolutionLayer`].
#[derive(Clone)]
pub struct TenantResolution<R> {
    inner: BoxCloneService<Request, Response, Infallible>,
    resolver: R,
}

impl<R> fmt::Debug for TenantResolution<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TenantResolution")
            .field("resolver", &std::any::type_name::<R>())
            .finish_non_exhaustive()
    }
}

impl<S, R> Layer<S> for TenantResolutionLayer<R>
where
    R: TenantResolver,
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = TenantResolution<R>;

    fn layer(&self, inner: S) -> Self::Service {
        TenantResolution {
            inner: BoxCloneService::new(inner),
            resolver: self.resolver.clone(),
        }
    }
}

impl<R> Service<Request> for TenantResolution<R>
where
    R: TenantResolver,
{
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, mut request: Request) -> Self::Future {
        let resolver = self.resolver.clone();
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let Some(principal) = request.extensions().get::<Principal>().cloned() else {
                return Ok(authentication_response(AuthError::MissingBearerToken));
            };
            let context = match resolver.resolve(&request, &principal).await {
                Ok(Some(context)) => context,
                Ok(None) => {
                    return Ok(Error::new(
                        StatusCode::NOT_FOUND,
                        "tenant_not_found",
                        "the requested tenant is not available",
                    )
                    .into_response());
                }
                Err(_) => {
                    return Ok(Error::new(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "tenant_resolution_unavailable",
                        "tenant resolution is unavailable",
                    )
                    .into_response());
                }
            };
            if principal.tenant() != Some(context.tenant()) {
                return Ok(Error::new(
                    StatusCode::FORBIDDEN,
                    "tenant_mismatch",
                    "the authenticated principal does not belong to this tenant",
                )
                .into_response());
            }
            request.extensions_mut().insert(context);
            inner.call(request).await
        })
    }
}

/// A layer that requires the authenticated principal to match request [`TenantContext`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[must_use = "a tenant policy must be applied to a service to have an effect"]
pub struct RequireTenantMatchLayer;

impl RequireTenantMatchLayer {
    /// Creates a tenant isolation layer.
    pub const fn new() -> Self {
        Self
    }
}

/// Service produced by [`RequireTenantMatchLayer`].
#[derive(Clone, Debug)]
pub struct RequireTenantMatch {
    inner: BoxCloneService<Request, Response, Infallible>,
}

impl<S> Layer<S> for RequireTenantMatchLayer
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = RequireTenantMatch;

    fn layer(&self, inner: S) -> Self::Service {
        RequireTenantMatch {
            inner: BoxCloneService::new(inner),
        }
    }
}

impl Service<Request> for RequireTenantMatch {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let Some(principal) = request.extensions().get::<Principal>() else {
                return Ok(authentication_response(AuthError::MissingBearerToken));
            };
            let Some(context) = request.extensions().get::<TenantContext>() else {
                return Ok(Error::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "tenant_context_missing",
                    "tenant context is required for this route",
                )
                .into_response());
            };
            if principal.tenant() != Some(context.tenant()) {
                return Ok(Error::new(
                    StatusCode::FORBIDDEN,
                    "tenant_mismatch",
                    "the authenticated principal does not belong to this tenant",
                )
                .into_response());
            }
            inner.call(request).await
        })
    }
}

fn bearer_token(value: Option<&HeaderValue>) -> Result<&str, AuthError> {
    let value = value.ok_or(AuthError::MissingBearerToken)?;
    let value = value.to_str().map_err(|_| AuthError::InvalidBearerToken)?;
    let token = value
        .strip_prefix("Bearer ")
        .filter(|token| !token.is_empty() && !token.chars().any(char::is_whitespace))
        .ok_or(AuthError::InvalidBearerToken)?;
    Ok(token)
}

fn api_key_header_value<'a>(
    headers: &'a HeaderMap,
    header_name: &HeaderName,
) -> Result<&'a str, ApiKeyError> {
    let mut values = headers.get_all(header_name).iter();
    let value = values.next().ok_or(ApiKeyError::MissingApiKey)?;
    if values.next().is_some() {
        return Err(ApiKeyError::InvalidApiKey);
    }
    let value = value.to_str().map_err(|_| ApiKeyError::InvalidApiKey)?;
    if !is_valid_api_key_value(value) {
        return Err(ApiKeyError::InvalidApiKey);
    }
    Ok(value)
}

fn is_valid_api_key_value(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_API_KEY_BYTES
        && value.bytes().all(|byte| (b'!'..=b'~').contains(&byte))
}

fn required_principal(request: &Request) -> rustee_core::Result<Principal> {
    request
        .extensions()
        .get::<Principal>()
        .cloned()
        .ok_or_else(|| {
            Error::new(
                StatusCode::UNAUTHORIZED,
                "unauthenticated",
                "authentication is required",
            )
        })
}

fn authentication_response(error: AuthError) -> Response {
    let (code, message) = match error {
        AuthError::MissingBearerToken => ("missing_bearer_token", "a bearer token is required"),
        AuthError::InvalidBearerToken | AuthError::RejectedBearerToken => {
            ("invalid_bearer_token", "the bearer token is invalid")
        }
        AuthError::ProviderUnavailable => {
            let response = Error::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "authentication_unavailable",
                "authentication service is unavailable",
            )
            .into_response();
            return response;
        }
    };
    let mut response = Error::new(StatusCode::UNAUTHORIZED, code, message).into_response();
    response.headers_mut().insert(
        http::header::WWW_AUTHENTICATE,
        HeaderValue::from_static("Bearer"),
    );
    response
}

fn api_key_authentication_response(error: ApiKeyError) -> Response {
    let (code, message) = match error {
        ApiKeyError::MissingApiKey => ("missing_api_key", "an API key is required"),
        ApiKeyError::InvalidApiKey | ApiKeyError::RejectedApiKey => {
            ("invalid_api_key", "the API key is invalid")
        }
        ApiKeyError::ProviderUnavailable => {
            return Error::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "authentication_unavailable",
                "authentication service is unavailable",
            )
            .into_response();
        }
    };
    let mut response = Error::new(StatusCode::UNAUTHORIZED, code, message).into_response();
    response.headers_mut().insert(
        http::header::WWW_AUTHENTICATE,
        HeaderValue::from_static("ApiKey"),
    );
    response
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use http::{HeaderValue, Request as HttpRequest, StatusCode, header::WWW_AUTHENTICATE};
    use rustee_core::empty_body;
    use rustee_router::App;
    use rustee_server::{ServerOptions, serve_service_listener_with_options};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::oneshot,
        time::timeout,
    };
    use tower::{Layer, ServiceExt};

    use super::{
        ApiKeyAuthenticator, ApiKeyError, ApiKeyFingerprint, ApiKeyFingerprintStore, ApiKeyLayer,
        ApiKeyLayerError, ApiKeyPepper, ApiKeyPepperError, AuthError, AuthLayer, AuthUser,
        BearerAuthenticator, HostTenantResolver, HostTenantResolverError, KeyedApiKeyAuthenticator,
        PermissionPolicyError, Principal, RequireAuth, RequirePermissionsLayer, RequireScopesLayer,
        RequireTenantMatchLayer, RolePolicy, RolePolicyError, ScopePolicyError,
        StaticApiKeyAuthenticator, StaticApiKeyError, StaticTokenAuthenticator, TenantContext,
        TenantPolicyError, TenantResolutionLayer, TenantResolver,
    };
    use futures_util::future;

    #[derive(Clone, Copy)]
    struct UnavailableAuthenticator;

    impl BearerAuthenticator for UnavailableAuthenticator {
        fn authenticate(
            &self,
            _: &str,
        ) -> futures_util::future::BoxFuture<'static, Result<Principal, AuthError>> {
            Box::pin(future::ready(Err(AuthError::ProviderUnavailable)))
        }
    }

    #[derive(Clone, Copy)]
    struct UnavailableApiKeyAuthenticator;

    impl ApiKeyAuthenticator for UnavailableApiKeyAuthenticator {
        fn authenticate(
            &self,
            _: &str,
        ) -> futures_util::future::BoxFuture<'static, Result<Principal, ApiKeyError>> {
            Box::pin(future::ready(Err(ApiKeyError::ProviderUnavailable)))
        }
    }

    #[derive(Clone)]
    struct FingerprintStore {
        expected: ApiKeyFingerprint,
        principal: Principal,
    }

    impl ApiKeyFingerprintStore for FingerprintStore {
        fn authenticate(
            &self,
            fingerprint: ApiKeyFingerprint,
        ) -> futures_util::future::BoxFuture<'static, Result<Principal, ApiKeyError>> {
            let principal = (fingerprint == self.expected).then(|| self.principal.clone());
            Box::pin(future::ready(principal.ok_or(ApiKeyError::RejectedApiKey)))
        }
    }

    #[derive(Clone, Copy)]
    struct UnavailableTenantResolver;

    impl TenantResolver for UnavailableTenantResolver {
        type Error = std::io::Error;

        fn resolve(
            &self,
            _: &rustee_core::Request,
            _: &Principal,
        ) -> futures_util::future::BoxFuture<'static, Result<Option<TenantContext>, Self::Error>>
        {
            Box::pin(future::ready(Err(std::io::Error::other("not reachable"))))
        }
    }

    fn authenticator() -> StaticTokenAuthenticator {
        let principal = Principal::new("alice")
            .unwrap()
            .with_scope("profile:read")
            .unwrap();
        let mut authenticator = StaticTokenAuthenticator::new();
        authenticator.insert("local-token", principal).unwrap();
        authenticator
    }

    fn request(token: Option<&str>) -> rustee_core::Request {
        let mut builder = HttpRequest::builder().method("GET").uri("/me");
        if let Some(token) = token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        builder.body(empty_body()).unwrap()
    }

    fn tenant_authenticator() -> StaticTokenAuthenticator {
        let mut authenticator = StaticTokenAuthenticator::new();
        authenticator
            .insert(
                "tenant-token",
                Principal::new("alice")
                    .unwrap()
                    .with_tenant("acme")
                    .unwrap(),
            )
            .unwrap();
        authenticator
    }

    fn api_key_authenticator() -> StaticApiKeyAuthenticator {
        let mut authenticator = StaticApiKeyAuthenticator::new();
        authenticator
            .insert(
                "local-api-key",
                Principal::new("service-client")
                    .unwrap()
                    .with_scope("profile:read")
                    .unwrap(),
            )
            .unwrap();
        authenticator
    }

    fn api_key_request(values: &[&str]) -> rustee_core::Request {
        let mut request = HttpRequest::builder()
            .method("GET")
            .uri("/me")
            .body(empty_body())
            .unwrap();
        for value in values {
            request.headers_mut().append(
                "x-api-key",
                HeaderValue::from_str(value).expect("test API key header must be valid"),
            );
        }
        request
    }

    async fn raw_http_request(address: std::net::SocketAddr, request: &[u8]) -> String {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(request).await.unwrap();
        let mut response = Vec::new();
        timeout(Duration::from_secs(2), stream.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        String::from_utf8(response).unwrap()
    }

    fn tenant_request(token: Option<&str>, host: &str) -> rustee_core::Request {
        let mut request = request(token);
        request.headers_mut().insert("host", host.parse().unwrap());
        request
    }

    #[tokio::test]
    async fn bearer_layer_rejects_missing_credentials_with_a_challenge() {
        let service = AuthLayer::bearer(authenticator())
            .layer(App::new().get("/me", || async { "unexpected" }));

        let response = service.oneshot(request(None)).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(response.headers()[WWW_AUTHENTICATE], "Bearer");
    }

    #[tokio::test]
    async fn bearer_layer_returns_a_sanitized_503_when_a_provider_is_unavailable() {
        let service = AuthLayer::bearer(UnavailableAuthenticator)
            .layer(App::new().get("/me", || async { "unexpected" }));

        let response = service.oneshot(request(Some("token"))).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response.headers().get(WWW_AUTHENTICATE).is_none());
    }

    #[tokio::test]
    async fn api_key_layer_authenticates_one_explicit_header_without_exposing_the_key() {
        let service = ApiKeyLayer::header("X-API-Key", api_key_authenticator())
            .unwrap()
            .layer(
                App::new().get("/me", |AuthUser(principal): AuthUser| async move {
                    principal.subject().to_owned()
                }),
            );

        let response = service
            .oneshot(api_key_request(&["local-api-key"]))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn keyed_api_key_authenticator_uses_only_a_keyed_fingerprint_for_lookup() {
        let pepper = ApiKeyPepper::new([7; 32]).unwrap();
        let expected = pepper.fingerprint("local-api-key").unwrap();
        let authenticator = KeyedApiKeyAuthenticator::new(
            pepper,
            FingerprintStore {
                expected,
                principal: Principal::new("service-client").unwrap(),
            },
        );
        let service = ApiKeyLayer::header("x-api-key", authenticator)
            .unwrap()
            .layer(
                App::new().get("/me", |AuthUser(principal): AuthUser| async move {
                    principal.subject().to_owned()
                }),
            );

        let response = service
            .oneshot(api_key_request(&["local-api-key"]))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn api_key_pepper_fingerprint_is_stable_bounded_and_redacted() {
        let pepper = ApiKeyPepper::new([7; 32]).unwrap();
        let fingerprint = pepper.fingerprint("local-api-key").unwrap();
        assert_eq!(fingerprint, pepper.fingerprint("local-api-key").unwrap());
        assert_ne!(fingerprint, pepper.fingerprint("other-api-key").unwrap());
        assert_eq!(fingerprint.as_bytes().len(), 32);
        assert_eq!(format!("{pepper:?}"), "ApiKeyPepper([redacted])");
        assert_eq!(format!("{fingerprint:?}"), "ApiKeyFingerprint([redacted])");
        assert_eq!(
            pepper.fingerprint("not a valid key").unwrap_err(),
            ApiKeyError::InvalidApiKey
        );
        assert_eq!(
            ApiKeyPepper::new([0; 32]).unwrap_err(),
            ApiKeyPepperError::AllZero
        );
    }

    #[tokio::test]
    async fn api_key_layer_rejects_missing_duplicate_or_malformed_values() {
        let service = ApiKeyLayer::header("x-api-key", api_key_authenticator())
            .unwrap()
            .layer(App::new().get("/me", || async { "unexpected" }));

        let missing = service.clone().oneshot(api_key_request(&[])).await.unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(missing.headers()[WWW_AUTHENTICATE], "ApiKey");

        let duplicate = service
            .clone()
            .oneshot(api_key_request(&["local-api-key", "other-key"]))
            .await
            .unwrap();
        assert_eq!(duplicate.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(duplicate.headers()[WWW_AUTHENTICATE], "ApiKey");

        let malformed = service
            .oneshot(api_key_request(&["local api key"]))
            .await
            .unwrap();
        assert_eq!(malformed.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(malformed.headers()[WWW_AUTHENTICATE], "ApiKey");
    }

    #[tokio::test]
    async fn api_key_layer_rejects_duplicate_headers_over_real_tcp_without_echoing_them() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let service = ApiKeyLayer::header("x-api-key", api_key_authenticator())
            .unwrap()
            .layer(App::new().get("/me", || async { "unexpected" }));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            serve_service_listener_with_options(
                listener,
                service,
                ServerOptions::default(),
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
            .unwrap();
        });

        let response = raw_http_request(
            address,
            b"GET /me HTTP/1.1\r\nHost: localhost\r\nX-API-Key: local-api-key\r\nX-API-Key: duplicate-api-key\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 401 Unauthorized\r\n"));
        assert!(response.contains("www-authenticate: ApiKey\r\n"));
        assert!(response.contains("\"code\":\"invalid_api_key\""));
        assert!(!response.contains("local-api-key"));
        assert!(!response.contains("duplicate-api-key"));

        let _ = shutdown_tx.send(());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn api_key_layer_returns_a_sanitized_503_when_a_provider_is_unavailable() {
        let service = ApiKeyLayer::header("x-api-key", UnavailableApiKeyAuthenticator)
            .unwrap()
            .layer(App::new().get("/me", || async { "unexpected" }));

        let response = service
            .oneshot(api_key_request(&["local-api-key"]))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response.headers().get(WWW_AUTHENTICATE).is_none());
    }

    #[test]
    fn api_key_configuration_is_validated_without_logging_credentials() {
        assert_eq!(
            ApiKeyLayer::header("x api key", api_key_authenticator()).unwrap_err(),
            ApiKeyLayerError::InvalidHeaderName
        );
        assert_eq!(
            StaticApiKeyAuthenticator::new()
                .insert("contains space", Principal::new("service-client").unwrap())
                .unwrap_err(),
            StaticApiKeyError::InvalidKey
        );
        assert_eq!(
            format!("{:?}", api_key_authenticator()),
            "StaticApiKeyAuthenticator { registered_keys: 1 }"
        );
    }

    #[tokio::test]
    async fn auth_user_receives_only_the_validated_principal() {
        let service = AuthLayer::bearer(authenticator()).layer(
            App::new().get("/me", |AuthUser(principal): AuthUser| async move {
                principal.subject().to_owned()
            }),
        );

        let response = service.oneshot(request(Some("local-token"))).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn require_auth_rejects_a_route_without_an_authenticated_principal() {
        let app = App::new().get("/me", |RequireAuth(principal): RequireAuth| async move {
            principal.subject().to_owned()
        });

        let response = app.oneshot(request(None)).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn scope_layer_rejects_an_authenticated_principal_without_every_scope() {
        let policy = RequireScopesLayer::new(["profile:read", "profile:write"]).unwrap();
        let service = AuthLayer::bearer(authenticator())
            .layer(policy.layer(App::new().get("/me", || async { "unexpected" })));

        let response = service.oneshot(request(Some("local-token"))).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn scope_policy_rejects_an_empty_requirement() {
        let error = RequireScopesLayer::new(Vec::<String>::new()).unwrap_err();
        assert_eq!(error, ScopePolicyError::EmptyRequirement);
    }

    #[tokio::test]
    async fn permission_layer_accepts_direct_permissions_and_server_configured_roles() {
        let direct_principal = Principal::new("direct")
            .unwrap()
            .with_permission("project:read")
            .unwrap();
        let role_principal = Principal::new("role")
            .unwrap()
            .with_role("project-viewer")
            .unwrap();
        let mut authenticator = StaticTokenAuthenticator::new();
        authenticator
            .insert("direct-token", direct_principal)
            .unwrap();
        authenticator.insert("role-token", role_principal).unwrap();

        let mut roles = RolePolicy::new();
        roles.grant("project-viewer", ["project:read"]).unwrap();
        let policy = RequirePermissionsLayer::new(["project:read"], roles).unwrap();
        let service = AuthLayer::bearer(authenticator)
            .layer(policy.layer(App::new().get("/me", || async { "allowed" })));

        assert_eq!(
            service
                .clone()
                .oneshot(request(Some("direct-token")))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            service
                .oneshot(request(Some("role-token")))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn permission_layer_rejects_a_principal_without_a_grant() {
        let policy = RequirePermissionsLayer::new(["project:write"], RolePolicy::new()).unwrap();
        let service = AuthLayer::bearer(authenticator())
            .layer(policy.layer(App::new().get("/me", || async { "unexpected" })));

        let response = service.oneshot(request(Some("local-token"))).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn tenant_layer_allows_only_the_server_context_matching_the_principal() {
        let tenant_principal = Principal::new("alice")
            .unwrap()
            .with_tenant("acme")
            .unwrap();
        let mut authenticator = StaticTokenAuthenticator::new();
        authenticator
            .insert("tenant-token", tenant_principal)
            .unwrap();
        let service = AuthLayer::bearer(authenticator).layer(
            RequireTenantMatchLayer::new().layer(App::new().get("/me", || async { "allowed" })),
        );

        let mut matching = request(Some("tenant-token"));
        matching
            .extensions_mut()
            .insert(TenantContext::new("acme").unwrap());
        assert_eq!(
            service.clone().oneshot(matching).await.unwrap().status(),
            StatusCode::OK
        );

        let mut mismatched = request(Some("tenant-token"));
        mismatched
            .extensions_mut()
            .insert(TenantContext::new("other").unwrap());
        assert_eq!(
            service.clone().oneshot(mismatched).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );

        assert_eq!(
            service
                .oneshot(request(Some("tenant-token")))
                .await
                .unwrap()
                .status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[tokio::test]
    async fn host_tenant_resolution_isolates_the_verified_principal() {
        let resolver = HostTenantResolver::new([
            ("acme.example.test", TenantContext::new("acme").unwrap()),
            ("other.example.test", TenantContext::new("other").unwrap()),
        ])
        .unwrap();
        let service = AuthLayer::bearer(tenant_authenticator()).layer(
            TenantResolutionLayer::new(resolver).layer(
                App::new().get("/me", |context: TenantContext| async move {
                    context.tenant().to_owned()
                }),
            ),
        );

        let response = service
            .clone()
            .oneshot(tenant_request(Some("tenant-token"), "ACME.EXAMPLE.TEST"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        assert_eq!(
            service
                .clone()
                .oneshot(tenant_request(Some("tenant-token"), "other.example.test"))
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            service
                .clone()
                .oneshot(tenant_request(Some("tenant-token"), "unknown.example.test"))
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );

        let mut duplicate = tenant_request(Some("tenant-token"), "acme.example.test");
        duplicate
            .headers_mut()
            .append("host", "other.example.test".parse().unwrap());
        assert_eq!(
            service.oneshot(duplicate).await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn tenant_resolution_failure_is_sanitized_and_fail_closed() {
        let service = AuthLayer::bearer(tenant_authenticator()).layer(
            TenantResolutionLayer::new(UnavailableTenantResolver)
                .layer(App::new().get("/me", || async { "unexpected" })),
        );
        let response = service
            .oneshot(tenant_request(Some("tenant-token"), "acme.example.test"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn role_and_tenant_policy_reject_invalid_configuration() {
        let mut roles = RolePolicy::new();
        assert_eq!(
            roles.grant("", ["project:read"]).unwrap_err(),
            RolePolicyError::BlankRole
        );
        assert_eq!(
            roles.grant("viewer", Vec::<String>::new()).unwrap_err(),
            RolePolicyError::EmptyPermissions
        );
        assert_eq!(
            RequirePermissionsLayer::new(Vec::<String>::new(), roles).unwrap_err(),
            PermissionPolicyError::EmptyRequirement
        );
        assert_eq!(
            TenantContext::new(" ").unwrap_err(),
            TenantPolicyError::BlankTenant
        );
        assert_eq!(
            HostTenantResolver::new(Vec::<(String, TenantContext)>::new()).unwrap_err(),
            HostTenantResolverError::EmptyMapping
        );
        assert_eq!(
            HostTenantResolver::new([
                ("ACME.EXAMPLE.TEST", TenantContext::new("acme").unwrap()),
                ("acme.example.test", TenantContext::new("other").unwrap()),
            ])
            .unwrap_err(),
            HostTenantResolverError::DuplicateHost
        );
        assert_eq!(
            HostTenantResolver::new([(
                "https://acme.example.test",
                TenantContext::new("acme").unwrap(),
            )])
            .unwrap_err(),
            HostTenantResolverError::InvalidHost
        );
    }
}
