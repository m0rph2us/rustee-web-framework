//! OAuth protected-resource metadata and bearer authorization for Rustee MCP servers.
//!
//! This optional adapter exposes the public metadata required for an MCP HTTP resource and
//! applies a verified bearer principal to a mounted [`rustee_mcp::McpServer`]. Token signature,
//! issuer, and audience validation remain with a configured [`BearerAuthenticator`]. Applications
//! must configure that verifier with the exact `resource` URI from
//! [`McpOAuthResourceServerConfig`], then keep tool, context, tenant, and approval policy in their
//! existing Rustee boundaries.

use std::{
    collections::BTreeSet,
    convert::Infallible,
    fmt,
    task::{Context, Poll},
};

use futures_util::future::BoxFuture;
use http::{
    HeaderValue, Method, StatusCode,
    header::{ALLOW, AUTHORIZATION, CONTENT_TYPE, WWW_AUTHENTICATE},
};
use rustee_auth::{AuthError, BearerAuthenticator};
use rustee_core::{Error, IntoResponse, Request, Response, full_body, response};
use serde::Serialize;
use tower::{Layer, Service, util::BoxCloneService};
use url::{Host, Url};

const MAX_AUTHORIZATION_SERVERS: usize = 8;
const MAX_REQUIRED_SCOPES: usize = 32;
const MAX_SCOPE_BYTES: usize = 256;
const MAX_URL_BYTES: usize = 2048;

#[derive(Serialize)]
struct ProtectedResourceMetadata<'a> {
    resource: &'a str,
    authorization_servers: Vec<&'a str>,
    scopes_supported: Vec<&'a str>,
    bearer_methods_supported: [&'static str; 1],
}

/// Public configuration for one MCP OAuth protected resource.
///
/// `resource` is the canonical, externally visible MCP endpoint URL. The application must
/// configure its JWT or introspection verifier to require it as the access-token audience; this
/// type deliberately does not parse, retain, or inspect raw access tokens itself.
#[derive(Clone, Eq, PartialEq)]
pub struct McpOAuthResourceServerConfig {
    resource: Url,
    protected_resource_metadata: Url,
    authorization_servers: BTreeSet<String>,
    required_scopes: BTreeSet<String>,
}

impl McpOAuthResourceServerConfig {
    /// Creates public metadata for one protected MCP HTTP endpoint.
    ///
    /// `protected_resource_metadata` is the exact public metadata URL advertised in a Bearer
    /// challenge. Every URL must use HTTPS, except loopback HTTP used for local development and
    /// contract tests. At least one explicitly trusted authorization-server issuer is required.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthResourceServerConfigError`] when a URL is unsafe, no authorization
    /// server is configured, or the configured server limit is exceeded.
    pub fn new<I>(
        resource: Url,
        protected_resource_metadata: Url,
        authorization_servers: I,
    ) -> Result<Self, McpOAuthResourceServerConfigError>
    where
        I: IntoIterator<Item = Url>,
    {
        if !valid_public_url(&resource) {
            return Err(McpOAuthResourceServerConfigError::InvalidResourceUrl);
        }
        if !valid_public_url(&protected_resource_metadata) {
            return Err(McpOAuthResourceServerConfigError::InvalidProtectedResourceMetadataUrl);
        }

        let mut servers = BTreeSet::new();
        for server in authorization_servers {
            if !valid_public_url(&server) {
                return Err(McpOAuthResourceServerConfigError::InvalidAuthorizationServerUrl);
            }
            servers.insert(server.to_string());
        }
        if servers.is_empty() {
            return Err(McpOAuthResourceServerConfigError::EmptyAuthorizationServers);
        }
        if servers.len() > MAX_AUTHORIZATION_SERVERS {
            return Err(McpOAuthResourceServerConfigError::TooManyAuthorizationServers);
        }

        Ok(Self {
            resource,
            protected_resource_metadata,
            authorization_servers: servers,
            required_scopes: BTreeSet::new(),
        })
    }

    /// Requires every supplied scope after the bearer token is cryptographically verified.
    ///
    /// A resource without scoped authorization can omit this call, but a server that advertises
    /// scopes should normally require the same scopes here rather than relying on token presence.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthResourceServerConfigError`] for an empty requirement, an invalid scope,
    /// or more than 32 scopes.
    pub fn with_required_scopes<I, S>(
        mut self,
        scopes: I,
    ) -> Result<Self, McpOAuthResourceServerConfigError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let scopes = scopes.into_iter().map(Into::into).collect::<BTreeSet<_>>();
        if scopes.is_empty() {
            return Err(McpOAuthResourceServerConfigError::EmptyScopeRequirement);
        }
        if scopes.len() > MAX_REQUIRED_SCOPES {
            return Err(McpOAuthResourceServerConfigError::TooManyRequiredScopes);
        }
        if scopes.iter().any(|scope| !valid_scope(scope)) {
            return Err(McpOAuthResourceServerConfigError::InvalidScope);
        }
        self.required_scopes = scopes;
        Ok(self)
    }

    /// Returns the exact externally visible MCP resource URL.
    #[must_use]
    pub fn resource(&self) -> &Url {
        &self.resource
    }

    /// Returns the public protected-resource metadata URL advertised in challenges.
    #[must_use]
    pub fn protected_resource_metadata(&self) -> &Url {
        &self.protected_resource_metadata
    }

    /// Returns configured authorization-server issuers in deterministic order.
    pub fn authorization_servers(&self) -> impl ExactSizeIterator<Item = &str> {
        self.authorization_servers.iter().map(String::as_str)
    }

    /// Returns required verified OAuth scopes in deterministic order.
    pub fn required_scopes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.required_scopes.iter().map(String::as_str)
    }

    fn scope_parameter(&self) -> Option<String> {
        (!self.required_scopes.is_empty()).then(|| {
            self.required_scopes
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(" ")
        })
    }

    fn accepts_issuer(&self, issuer: Option<&str>) -> bool {
        issuer
            .and_then(|issuer| Url::parse(issuer).ok())
            .is_some_and(|issuer| self.authorization_servers.contains(issuer.as_str()))
    }
}

impl fmt::Debug for McpOAuthResourceServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthResourceServerConfig")
            .field("resource", &self.resource)
            .field(
                "protected_resource_metadata",
                &self.protected_resource_metadata,
            )
            .field("authorization_servers", &self.authorization_servers)
            .field("required_scopes", &self.required_scopes)
            .finish()
    }
}

/// Invalid public protected-resource configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum McpOAuthResourceServerConfigError {
    /// The canonical MCP resource URL was not a safe public HTTP(S) URL.
    #[error(
        "MCP OAuth resource URL must be an absolute HTTPS URL or loopback HTTP URL without credentials, query, or fragment"
    )]
    InvalidResourceUrl,
    /// The advertised protected-resource metadata URL was not a safe public HTTP(S) URL.
    #[error(
        "MCP OAuth protected-resource metadata URL must be an absolute HTTPS URL or loopback HTTP URL without credentials, query, or fragment"
    )]
    InvalidProtectedResourceMetadataUrl,
    /// An authorization-server issuer URL was unsafe or malformed.
    #[error(
        "MCP OAuth authorization-server URL must be an absolute HTTPS URL or loopback HTTP URL without credentials, query, or fragment"
    )]
    InvalidAuthorizationServerUrl,
    /// Public metadata needs at least one explicitly configured authorization server.
    #[error("MCP OAuth resource server needs at least one authorization server")]
    EmptyAuthorizationServers,
    /// Public metadata intentionally has a small issuer allowlist.
    #[error("MCP OAuth resource server supports at most eight authorization servers")]
    TooManyAuthorizationServers,
    /// A caller passed no scopes to the explicit scope-requirement builder.
    #[error("MCP OAuth scope requirement must not be empty")]
    EmptyScopeRequirement,
    /// A configured scope contained whitespace or unsafe header characters.
    #[error(
        "MCP OAuth scope must be a bounded visible ASCII token without whitespace, quotes, or backslashes"
    )]
    InvalidScope,
    /// Public metadata intentionally has a bounded scope list.
    #[error("MCP OAuth resource server supports at most 32 required scopes")]
    TooManyRequiredScopes,
}

/// Public service for one protected-resource metadata document.
///
/// Mount this service at the exact `protected_resource_metadata` path. The metadata document is
/// intentionally unauthenticated: it discloses only public resource and issuer configuration,
/// never accepted tokens, principals, tenant mappings, tool inventory, or authorization policy.
#[derive(Clone, Debug)]
pub struct McpOAuthProtectedResourceMetadata {
    config: McpOAuthResourceServerConfig,
}

impl McpOAuthProtectedResourceMetadata {
    /// Creates the metadata service for a validated resource-server configuration.
    #[must_use]
    pub const fn new(config: McpOAuthResourceServerConfig) -> Self {
        Self { config }
    }

    /// Handles one prefix-stripped request.
    pub fn handle(&self, request: &Request) -> Response {
        if request.uri().path() != "/" {
            return Error::not_found(
                "the requested protected-resource metadata endpoint was not found",
            )
            .into_response();
        }
        if request.method() != Method::GET {
            let mut response = response(StatusCode::METHOD_NOT_ALLOWED, rustee_core::empty_body());
            response
                .headers_mut()
                .insert(ALLOW, HeaderValue::from_static("GET"));
            return response;
        }

        let document = ProtectedResourceMetadata {
            resource: self.config.resource().as_str(),
            authorization_servers: self.config.authorization_servers().collect(),
            scopes_supported: self.config.required_scopes().collect(),
            bearer_methods_supported: ["header"],
        };
        let Ok(encoded) = serde_json::to_vec(&document) else {
            return Error::internal().into_response();
        };
        let mut response = response(StatusCode::OK, full_body(encoded));
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        );
        response
    }
}

impl Service<Request> for McpOAuthProtectedResourceMetadata {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let metadata = self.clone();
        Box::pin(async move { Ok(metadata.handle(&request)) })
    }
}

/// Tower layer that verifies MCP bearer access tokens and exposes the resulting [`Principal`].
///
/// Wrap a prefix-stripped [`rustee_mcp::McpServer`] with this layer. It never grants tool or
/// context visibility by itself: the existing MCP access/context policies still receive the
/// inserted principal and must make tenant and action-level decisions.
#[derive(Clone)]
#[must_use = "the MCP OAuth layer must be applied to a service to have an effect"]
pub struct McpOAuthResourceServerLayer<A> {
    config: McpOAuthResourceServerConfig,
    authenticator: A,
}

impl<A> McpOAuthResourceServerLayer<A> {
    /// Creates a protected-resource bearer layer from a validated configuration and trusted
    /// authenticator. Configure the authenticator to validate the exact resource URL as audience.
    pub const fn new(config: McpOAuthResourceServerConfig, authenticator: A) -> Self {
        Self {
            config,
            authenticator,
        }
    }
}

impl<A> fmt::Debug for McpOAuthResourceServerLayer<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthResourceServerLayer")
            .field("config", &self.config)
            .field("authenticator", &std::any::type_name::<A>())
            .finish()
    }
}

/// Service produced by [`McpOAuthResourceServerLayer`].
#[derive(Clone)]
pub struct McpOAuthResourceServer<A> {
    inner: BoxCloneService<Request, Response, Infallible>,
    config: McpOAuthResourceServerConfig,
    authenticator: A,
}

impl<A> fmt::Debug for McpOAuthResourceServer<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthResourceServer")
            .field("config", &self.config)
            .field("authenticator", &std::any::type_name::<A>())
            .finish_non_exhaustive()
    }
}

impl<S, A> Layer<S> for McpOAuthResourceServerLayer<A>
where
    A: BearerAuthenticator,
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = McpOAuthResourceServer<A>;

    fn layer(&self, inner: S) -> Self::Service {
        McpOAuthResourceServer {
            inner: BoxCloneService::new(inner),
            config: self.config.clone(),
            authenticator: self.authenticator.clone(),
        }
    }
}

impl<A> Service<Request> for McpOAuthResourceServer<A>
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
        let config = self.config.clone();
        let authenticator = self.authenticator.clone();
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let token = match bearer_token(&request) {
                Ok(token) => token.to_owned(),
                Err(error) => return Ok(authentication_response(&config, error)),
            };
            let principal = match authenticator.authenticate(&token).await {
                Ok(principal) => principal,
                Err(error) => return Ok(authentication_response(&config, error)),
            };
            if !config.accepts_issuer(principal.issuer()) {
                return Ok(authentication_response(
                    &config,
                    AuthError::RejectedBearerToken,
                ));
            }
            if !config
                .required_scopes()
                .all(|scope| principal.has_scope(scope))
            {
                return Ok(insufficient_scope_response(&config));
            }
            request.extensions_mut().insert(principal);
            inner.call(request).await
        })
    }
}

fn bearer_token(request: &Request) -> Result<&str, AuthError> {
    let mut values = request.headers().get_all(AUTHORIZATION).iter();
    let value = values.next().ok_or(AuthError::MissingBearerToken)?;
    if values.next().is_some() {
        return Err(AuthError::InvalidBearerToken);
    }
    let value = value.to_str().map_err(|_| AuthError::InvalidBearerToken)?;
    let Some((scheme, token)) = value.split_once(' ') else {
        return Err(AuthError::InvalidBearerToken);
    };
    if !scheme.eq_ignore_ascii_case("Bearer")
        || token.is_empty()
        || token.chars().any(char::is_whitespace)
    {
        return Err(AuthError::InvalidBearerToken);
    }
    Ok(token)
}

fn authentication_response(config: &McpOAuthResourceServerConfig, error: AuthError) -> Response {
    match error {
        AuthError::ProviderUnavailable => Error::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "mcp_oauth_authentication_unavailable",
            "MCP OAuth authentication is unavailable",
        )
        .into_response(),
        AuthError::MissingBearerToken => challenge_response(
            config,
            StatusCode::UNAUTHORIZED,
            "mcp_oauth_authentication_required",
            "MCP OAuth bearer authentication is required",
            None,
        ),
        AuthError::InvalidBearerToken | AuthError::RejectedBearerToken => challenge_response(
            config,
            StatusCode::UNAUTHORIZED,
            "mcp_oauth_invalid_token",
            "MCP OAuth bearer token is invalid",
            Some("invalid_token"),
        ),
    }
}

fn insufficient_scope_response(config: &McpOAuthResourceServerConfig) -> Response {
    challenge_response(
        config,
        StatusCode::FORBIDDEN,
        "mcp_oauth_insufficient_scope",
        "MCP OAuth bearer token lacks a required scope",
        Some("insufficient_scope"),
    )
}

fn challenge_response(
    config: &McpOAuthResourceServerConfig,
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    error: Option<&'static str>,
) -> Response {
    let mut response = Error::new(status, code, message).into_response();
    response
        .headers_mut()
        .insert(WWW_AUTHENTICATE, challenge_header(config, error));
    response
}

fn challenge_header(config: &McpOAuthResourceServerConfig, error: Option<&str>) -> HeaderValue {
    let mut value = format!(
        "Bearer resource_metadata=\"{}\"",
        config.protected_resource_metadata()
    );
    if let Some(scope) = config.scope_parameter() {
        value.push_str(", scope=\"");
        value.push_str(&scope);
        value.push('"');
    }
    if let Some(error) = error {
        value.push_str(", error=\"");
        value.push_str(error);
        value.push('"');
    }
    HeaderValue::from_str(&value)
        .expect("validated MCP OAuth metadata and scope values fit HTTP headers")
}

fn valid_public_url(url: &Url) -> bool {
    if url.as_str().len() > MAX_URL_BYTES
        || url.username() != ""
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return false;
    }
    match (url.scheme(), url.host()) {
        ("https", Some(_)) => true,
        ("http", Some(Host::Ipv4(address))) => address.is_loopback(),
        ("http", Some(Host::Ipv6(address))) => address.is_loopback(),
        ("http", Some(Host::Domain(domain))) => domain.eq_ignore_ascii_case("localhost"),
        _ => false,
    }
}

fn valid_scope(scope: &str) -> bool {
    !scope.is_empty()
        && scope.len() <= MAX_SCOPE_BYTES
        && scope
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && byte != b'"' && byte != b'\\')
}

#[cfg(test)]
mod tests {
    use http::{Request as HttpRequest, StatusCode, header::WWW_AUTHENTICATE};
    use http_body_util::BodyExt;
    use rustee_ai::{
        DenyAllToolApproval, ToolApprovalAuditEvent, ToolApprovalAuditSink,
        ToolExecutionAuditEvent, ToolExecutionAuditSink, ToolRegistry,
    };
    use rustee_auth::{AuthUser, StaticTokenAuthenticator};
    use rustee_core::{empty_body, full_body};
    use rustee_mcp::{DenyAllMcpToolAccess, MCP_PROTOCOL_VERSION, McpServer, McpServerConfig};
    use rustee_router::App;
    use serde_json::json;
    use tower::{Layer, ServiceExt};
    use url::Url;

    use super::{
        McpOAuthProtectedResourceMetadata, McpOAuthResourceServerConfig,
        McpOAuthResourceServerConfigError, McpOAuthResourceServerLayer,
    };

    fn config() -> McpOAuthResourceServerConfig {
        McpOAuthResourceServerConfig::new(
            Url::parse("https://api.example.test/mcp").unwrap(),
            Url::parse("https://api.example.test/.well-known/oauth-protected-resource/mcp")
                .unwrap(),
            [Url::parse("https://issuer.example.test").unwrap()],
        )
        .unwrap()
        .with_required_scopes(["mcp:tools", "mcp:resources"])
        .unwrap()
    }

    fn request(method: http::Method, uri: &str, token: Option<&str>) -> rustee_core::Request {
        let mut builder = HttpRequest::builder().method(method).uri(uri);
        if let Some(token) = token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        builder.body(empty_body()).unwrap()
    }

    fn authenticator() -> StaticTokenAuthenticator {
        let mut authenticator = StaticTokenAuthenticator::new();
        authenticator
            .insert(
                "full-access",
                rustee_auth::Principal::new("alice")
                    .unwrap()
                    .with_issuer("https://issuer.example.test")
                    .unwrap()
                    .with_scope("mcp:tools")
                    .unwrap()
                    .with_scope("mcp:resources")
                    .unwrap(),
            )
            .unwrap();
        authenticator
            .insert(
                "limited-access",
                rustee_auth::Principal::new("bob")
                    .unwrap()
                    .with_issuer("https://issuer.example.test")
                    .unwrap()
                    .with_scope("mcp:tools")
                    .unwrap(),
            )
            .unwrap();
        authenticator
    }

    #[derive(Clone, Copy, Debug, Default)]
    struct NoopAudit;

    impl ToolApprovalAuditSink for NoopAudit {
        type Error = std::convert::Infallible;

        fn record_approved(
            &self,
            _: ToolApprovalAuditEvent,
        ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
            Box::pin(async { Ok(()) })
        }
    }

    impl ToolExecutionAuditSink for NoopAudit {
        fn record_outcome(
            &self,
            _: ToolExecutionAuditEvent,
        ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn mcp_initialize_request(token: Option<&str>) -> rustee_core::Request {
        let mut builder = HttpRequest::builder()
            .method(http::Method::POST)
            .uri("/mcp")
            .header(http::header::CONTENT_TYPE, "application/json");
        if let Some(token) = token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        builder
            .body(full_body(
                serde_json::to_vec(&json!({
                    "jsonrpc":"2.0",
                    "id":1,
                    "method":"initialize",
                    "params":{"protocolVersion":MCP_PROTOCOL_VERSION}
                }))
                .unwrap(),
            ))
            .unwrap()
    }

    #[tokio::test]
    async fn metadata_is_public_and_only_exposes_validated_public_configuration() {
        let app = App::new().nest(
            "/.well-known/oauth-protected-resource/mcp",
            McpOAuthProtectedResourceMetadata::new(config()),
        );

        let response = app
            .oneshot(request(
                http::Method::GET,
                "/.well-known/oauth-protected-resource/mcp",
                None,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[http::header::CONTENT_TYPE],
            "application/json; charset=utf-8"
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["resource"], "https://api.example.test/mcp");
        assert_eq!(
            value["authorization_servers"],
            serde_json::json!(["https://issuer.example.test/"])
        );
        assert_eq!(
            value["scopes_supported"],
            serde_json::json!(["mcp:resources", "mcp:tools"])
        );
        assert_eq!(
            value["bearer_methods_supported"],
            serde_json::json!(["header"])
        );
    }

    #[tokio::test]
    async fn metadata_rejects_non_get_requests_without_a_challenge() {
        let response = McpOAuthProtectedResourceMetadata::new(config())
            .oneshot(request(http::Method::POST, "/", None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(response.headers()[http::header::ALLOW], "GET");
        assert!(response.headers().get(WWW_AUTHENTICATE).is_none());
    }

    #[tokio::test]
    async fn layer_challenges_missing_or_invalid_bearer_tokens_with_metadata() {
        let service = McpOAuthResourceServerLayer::new(config(), authenticator())
            .layer(App::new().get("/", || async { "unexpected" }));

        let missing = service
            .clone()
            .oneshot(request(http::Method::POST, "/", None))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            missing.headers()[WWW_AUTHENTICATE],
            "Bearer resource_metadata=\"https://api.example.test/.well-known/oauth-protected-resource/mcp\", scope=\"mcp:resources mcp:tools\""
        );

        let invalid = service
            .oneshot(request(http::Method::POST, "/", Some("unknown")))
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            invalid.headers()[WWW_AUTHENTICATE],
            "Bearer resource_metadata=\"https://api.example.test/.well-known/oauth-protected-resource/mcp\", scope=\"mcp:resources mcp:tools\", error=\"invalid_token\""
        );
    }

    #[tokio::test]
    async fn layer_rejects_authenticated_principals_missing_a_required_scope() {
        let service = McpOAuthResourceServerLayer::new(config(), authenticator())
            .layer(App::new().get("/", || async { "unexpected" }));
        let response = service
            .oneshot(request(http::Method::POST, "/", Some("limited-access")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            response.headers()[WWW_AUTHENTICATE],
            "Bearer resource_metadata=\"https://api.example.test/.well-known/oauth-protected-resource/mcp\", scope=\"mcp:resources mcp:tools\", error=\"insufficient_scope\""
        );
    }

    #[tokio::test]
    async fn layer_rejects_a_verified_principal_from_an_unadvertised_issuer() {
        let mut authenticator = StaticTokenAuthenticator::new();
        authenticator
            .insert(
                "wrong-issuer",
                rustee_auth::Principal::new("mallory")
                    .unwrap()
                    .with_issuer("https://other-issuer.example.test")
                    .unwrap()
                    .with_scope("mcp:tools")
                    .unwrap()
                    .with_scope("mcp:resources")
                    .unwrap(),
            )
            .unwrap();
        let service = McpOAuthResourceServerLayer::new(config(), authenticator)
            .layer(App::new().get("/", || async { "unexpected" }));

        let response = service
            .oneshot(request(http::Method::GET, "/", Some("wrong-issuer")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers()[WWW_AUTHENTICATE],
            "Bearer resource_metadata=\"https://api.example.test/.well-known/oauth-protected-resource/mcp\", scope=\"mcp:resources mcp:tools\", error=\"invalid_token\""
        );
    }

    #[tokio::test]
    async fn layer_inserts_only_the_verified_principal_for_the_existing_mcp_policy() {
        let service = McpOAuthResourceServerLayer::new(config(), authenticator()).layer(
            App::new().get("/", |AuthUser(principal): AuthUser| async move {
                principal.subject().to_owned()
            }),
        );
        let response = service
            .oneshot(request(http::Method::GET, "/", Some("full-access")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "alice"
        );
    }

    #[tokio::test]
    async fn layer_composes_with_a_mounted_mcp_server_without_bypassing_the_challenge() {
        let mcp = McpServer::new(
            McpServerConfig::new("protected-mcp", "0.1.0").unwrap(),
            ToolRegistry::new(),
            DenyAllMcpToolAccess,
            DenyAllToolApproval,
            NoopAudit,
        );
        let app = App::new().nest(
            "/mcp",
            McpOAuthResourceServerLayer::new(config(), authenticator()).layer(mcp),
        );

        let missing = app
            .clone()
            .oneshot(mcp_initialize_request(None))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
        assert!(
            missing.headers()[WWW_AUTHENTICATE]
                .to_str()
                .unwrap()
                .contains("resource_metadata")
        );

        let accepted = app
            .oneshot(mcp_initialize_request(Some("full-access")))
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::OK);
        let body = accepted.into_body().collect().await.unwrap().to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["result"]["serverInfo"]["name"], "protected-mcp");
    }

    #[test]
    fn configuration_rejects_unsafe_urls_and_scope_header_injection() {
        let error = McpOAuthResourceServerConfig::new(
            Url::parse("http://api.example.test/mcp").unwrap(),
            Url::parse("https://api.example.test/.well-known/oauth-protected-resource/mcp")
                .unwrap(),
            [Url::parse("https://issuer.example.test").unwrap()],
        )
        .unwrap_err();
        assert_eq!(error, McpOAuthResourceServerConfigError::InvalidResourceUrl);

        let error = McpOAuthResourceServerConfig::new(
            Url::parse("https://api.example.test/mcp").unwrap(),
            Url::parse("https://api.example.test/.well-known/oauth-protected-resource/mcp")
                .unwrap(),
            [Url::parse("https://issuer.example.test").unwrap()],
        )
        .unwrap()
        .with_required_scopes(["mcp:tools\r\nother"])
        .unwrap_err();
        assert_eq!(error, McpOAuthResourceServerConfigError::InvalidScope);
    }

    #[derive(Clone, Copy)]
    struct UnavailableAuthenticator;

    impl rustee_auth::BearerAuthenticator for UnavailableAuthenticator {
        fn authenticate(
            &self,
            _: &str,
        ) -> futures_util::future::BoxFuture<
            'static,
            Result<rustee_auth::Principal, rustee_auth::AuthError>,
        > {
            Box::pin(async { Err(rustee_auth::AuthError::ProviderUnavailable) })
        }
    }

    #[tokio::test]
    async fn unavailable_verifier_is_a_sanitized_503_without_an_oauth_challenge() {
        let service = McpOAuthResourceServerLayer::new(config(), UnavailableAuthenticator)
            .layer(App::new().get("/", || async { "unexpected" }));
        let response = service
            .oneshot(request(http::Method::POST, "/", Some("anything")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response.headers().get(WWW_AUTHENTICATE).is_none());
    }
}
