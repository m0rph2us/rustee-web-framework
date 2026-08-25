//! Bearer principal admission and protected-resource challenge middleware.

use std::{
    convert::Infallible,
    fmt,
    task::{Context, Poll},
};

use futures_util::future::BoxFuture;
use http::{HeaderValue, StatusCode, header::WWW_AUTHENTICATE};
use rustee_auth::{AuthError, BearerAuthenticator, extract_bearer_token};
use rustee_core::{BoxCloneServiceExt, Error, IntoResponse, Request, Response};
use tower::{Layer, Service, util::BoxCloneService};

use crate::config::McpOAuthResourceServerConfig;

/// Tower layer that verifies MCP bearer access tokens and exposes the resulting `Principal`.
///
/// Wrap a prefix-stripped `rustee_mcp::McpServer` with this layer. It never grants tool or
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
        let inner = self.inner.clone();
        Box::pin(async move {
            let token = match extract_bearer_token(request.headers()) {
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
            inner.call_ready(request).await
        })
    }
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
