//! Explicit CORS policy, browser preflight handling, and response header application.

use std::{
    convert::Infallible,
    task::{Context, Poll},
};

use futures_util::future::BoxFuture;
use http::{
    HeaderMap, HeaderValue, Method, StatusCode,
    header::{
        ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_HEADERS,
        ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_REQUEST_METHOD,
        ORIGIN, VARY,
    },
};
use rustee_core::{BoxCloneServiceExt, Request, Response, empty_body, response};
use tower::{Layer, Service, util::BoxCloneService};

/// A conservative CORS layer for a single explicit allowed origin.
///
/// CORS response headers are applied only when the request carries exactly one `Origin` header
/// equal to the configured origin. Other requests continue to the inner service unchanged.
#[derive(Clone, Debug)]
#[must_use = "a CORS builder must be layered onto an application to have an effect"]
pub struct CorsLayer {
    allowed_origin: HeaderValue,
    allowed_methods: HeaderValue,
    allowed_headers: HeaderValue,
    allow_credentials: bool,
}

impl CorsLayer {
    /// Creates a CORS policy for one explicit origin.
    pub fn new(allowed_origin: HeaderValue) -> Self {
        Self {
            allowed_origin,
            allowed_methods: HeaderValue::from_static("GET, POST, PUT, PATCH, DELETE, OPTIONS"),
            allowed_headers: HeaderValue::from_static("content-type, authorization"),
            allow_credentials: false,
        }
    }

    /// Sets the allowed request methods advertised to browsers.
    pub fn allow_methods(mut self, methods: HeaderValue) -> Self {
        self.allowed_methods = methods;
        self
    }

    /// Sets the allowed request headers advertised to browsers.
    pub fn allow_headers(mut self, headers: HeaderValue) -> Self {
        self.allowed_headers = headers;
        self
    }

    /// Allows credentials only for the explicitly configured origin.
    pub fn allow_credentials(mut self, allow_credentials: bool) -> Self {
        self.allow_credentials = allow_credentials;
        self
    }

    fn accepts_origin(&self, headers: &HeaderMap) -> bool {
        let mut origins = headers.get_all(ORIGIN).iter();
        matches!(
            (origins.next(), origins.next()),
            (Some(origin), None) if origin == self.allowed_origin
        )
    }

    fn apply_actual(&self, response: &mut Response) {
        self.apply_origin(response);
    }

    fn apply_preflight(&self, response: &mut Response) {
        self.apply_origin(response);
        response
            .headers_mut()
            .insert(ACCESS_CONTROL_ALLOW_METHODS, self.allowed_methods.clone());
        response
            .headers_mut()
            .insert(ACCESS_CONTROL_ALLOW_HEADERS, self.allowed_headers.clone());
    }

    fn apply_origin(&self, response: &mut Response) {
        response
            .headers_mut()
            .insert(ACCESS_CONTROL_ALLOW_ORIGIN, self.allowed_origin.clone());
        add_vary_origin(response.headers_mut());
        if self.allow_credentials {
            response.headers_mut().insert(
                ACCESS_CONTROL_ALLOW_CREDENTIALS,
                HeaderValue::from_static("true"),
            );
        }
    }
}

/// Service produced by [`CorsLayer`].
#[derive(Clone, Debug)]
pub struct Cors {
    inner: BoxCloneService<Request, Response, Infallible>,
    policy: CorsLayer,
}

impl<S> Layer<S> for CorsLayer
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = Cors;

    fn layer(&self, inner: S) -> Self::Service {
        Cors {
            inner: BoxCloneService::new(inner),
            policy: self.clone(),
        }
    }
}

impl Service<Request> for Cors {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let cors_request = classify_request(&request, &self.policy);
        if matches!(cors_request, CorsRequest::Preflight) {
            let policy = self.policy.clone();
            return Box::pin(async move {
                let mut response = response(StatusCode::NO_CONTENT, empty_body());
                policy.apply_preflight(&mut response);
                Ok(response)
            });
        }

        let inner = self.inner.clone();
        let policy = self.policy.clone();
        Box::pin(async move {
            let mut response = inner.call_ready(request).await?;
            if matches!(cors_request, CorsRequest::Actual) {
                policy.apply_actual(&mut response);
            }
            Ok(response)
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CorsRequest {
    None,
    Actual,
    Preflight,
}

fn classify_request(request: &Request, policy: &CorsLayer) -> CorsRequest {
    if !policy.accepts_origin(request.headers()) {
        return CorsRequest::None;
    }
    if request.method() == Method::OPTIONS
        && request
            .headers()
            .contains_key(ACCESS_CONTROL_REQUEST_METHOD)
    {
        return if has_one_header(request.headers(), ACCESS_CONTROL_REQUEST_METHOD) {
            CorsRequest::Preflight
        } else {
            CorsRequest::None
        };
    }
    CorsRequest::Actual
}

fn has_one_header(headers: &HeaderMap, name: http::header::HeaderName) -> bool {
    let mut values = headers.get_all(name).iter();
    values.next().is_some() && values.next().is_none()
}

fn add_vary_origin(headers: &mut HeaderMap) {
    let already_varies = headers.get_all(VARY).iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value
                .split(',')
                .map(str::trim)
                .any(|field| field == "*" || field.eq_ignore_ascii_case(ORIGIN.as_str()))
        })
    });
    if !already_varies {
        headers.append(VARY, HeaderValue::from_static("Origin"));
    }
}
