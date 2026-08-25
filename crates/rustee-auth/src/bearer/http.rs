//! Tower bearer middleware and sanitized challenge or availability responses.

use std::{
    convert::Infallible,
    fmt,
    task::{Context, Poll},
};

use futures_util::future::BoxFuture;
use http::{HeaderValue, StatusCode};
use rustee_core::{BoxCloneServiceExt, Error, IntoResponse, Request, Response};
use tower::{Layer, Service, util::BoxCloneService};

use super::{
    authenticator::{AuthError, BearerAuthenticator},
    token::extract_bearer_token,
};

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
        let inner = self.inner.clone();
        Box::pin(async move {
            let token = match extract_bearer_token(request.headers()) {
                Ok(token) => token.to_owned(),
                Err(error) => return Ok(authentication_response(error)),
            };
            let principal = match authenticator.authenticate(&token).await {
                Ok(principal) => principal,
                Err(error) => return Ok(authentication_response(error)),
            };
            request.extensions_mut().insert(principal);
            inner.call_ready(request).await
        })
    }
}

pub(crate) fn authentication_response(error: AuthError) -> Response {
    let (code, message) = match error {
        AuthError::MissingBearerToken => ("missing_bearer_token", "a bearer token is required"),
        AuthError::InvalidBearerToken | AuthError::RejectedBearerToken => {
            ("invalid_bearer_token", "the bearer token is invalid")
        }
        AuthError::ProviderUnavailable => {
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
        HeaderValue::from_static("Bearer"),
    );
    response
}
