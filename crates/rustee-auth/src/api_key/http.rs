//! Tower API-key header admission and sanitized HTTP responses.

use std::{
    convert::Infallible,
    fmt,
    task::{Context, Poll},
};

use futures_util::future::BoxFuture;
use http::{HeaderMap, HeaderValue, StatusCode, header::HeaderName};
use rustee_core::{BoxCloneServiceExt, Error, IntoResponse, Request, Response};
use tower::{Layer, Service, util::BoxCloneService};

use super::authenticator::{ApiKeyAuthenticator, ApiKeyError, is_valid_api_key_value};

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
        let inner = self.inner.clone();
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
            inner.call_ready(request).await
        })
    }
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
