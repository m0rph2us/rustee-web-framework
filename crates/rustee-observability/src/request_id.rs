//! Request correlation IDs and their tracing span lifecycle.

use std::{
    convert::Infallible,
    fmt,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use futures_util::{FutureExt, future::BoxFuture};
use http::{HeaderValue, header::HeaderName};
use rustee_core::{
    BoxCloneServiceExt, Error, FromRequest, Request, Response, RouteClassification, RouteParams,
    StateStore,
};
use tower::{Layer, Service, util::BoxCloneService};
use tracing::{Instrument, info};
use uuid::Uuid;

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

/// A generated request correlation identifier.
///
/// Rustee always generates this value at the application boundary. It deliberately does not
/// reuse a client-provided header until a future trusted-proxy policy proves its provenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestId(String);

impl RequestId {
    /// Creates a new unpredictable request ID.
    #[must_use]
    pub fn generate() -> Self {
        Self(Uuid::new_v4().simple().to_string())
    }

    /// Returns the identifier as an HTTP-safe ASCII string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromRequest for RequestId {
    fn from_request<'a>(
        request: &'a mut Request,
        _params: &'a RouteParams,
        _state: &'a StateStore,
    ) -> BoxFuture<'a, rustee_core::Result<Self>> {
        async move {
            request.extensions().get::<Self>().cloned().ok_or_else(|| {
                Error::new(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    "request_id_missing",
                    "request correlation middleware is required",
                )
            })
        }
        .boxed()
    }
}

/// Tower layer that injects a generated [`RequestId`] and emits one completion event per request.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[must_use = "a request ID layer must be applied to a service to have an effect"]
pub struct RequestIdLayer;

impl RequestIdLayer {
    /// Creates a request-correlation layer with the default `X-Request-Id` response header.
    pub const fn new() -> Self {
        Self
    }
}

/// Internal integration hook for assigning a parent before a Rustee request span starts.
///
/// Optional observability adapters place [`RequestSpanParent`] in the request extensions before
/// this layer. The hook runs after the request span is created but before it is entered, so an
/// adapter can associate it with a remote trace without making this crate depend on a telemetry
/// SDK.
#[doc(hidden)]
pub trait RequestSpanParentHook: Send + Sync + 'static {
    /// Associates `span` with one optional integration-specific parent.
    fn apply(&self, span: &tracing::Span);
}

/// Request extension carrying one [`RequestSpanParentHook`] for [`RequestIdLayer`].
///
/// This is public only so optional observability integration crates can install it. Applications
/// should use the integration crate's request layer instead of constructing this value directly.
#[doc(hidden)]
#[derive(Clone)]
pub struct RequestSpanParent(Arc<dyn RequestSpanParentHook>);

impl RequestSpanParent {
    /// Wraps an optional integration-specific parent hook.
    #[must_use]
    pub fn new(parent: impl RequestSpanParentHook) -> Self {
        Self(Arc::new(parent))
    }

    /// Applies the contained parent before a request span is entered.
    pub fn apply(&self, span: &tracing::Span) {
        self.0.apply(span);
    }
}

impl fmt::Debug for RequestSpanParent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequestSpanParent")
            .finish_non_exhaustive()
    }
}

/// Service produced by [`RequestIdLayer`].
#[derive(Clone, Debug)]
pub struct RequestIdService {
    inner: BoxCloneService<Request, Response, Infallible>,
}

impl<S> Layer<S> for RequestIdLayer
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = RequestIdService;

    fn layer(&self, inner: S) -> Self::Service {
        RequestIdService {
            inner: BoxCloneService::new(inner),
        }
    }
}

impl Service<Request> for RequestIdService {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, mut request: Request) -> Self::Future {
        let span_parent = request.extensions().get::<RequestSpanParent>().cloned();
        let request_id = RequestId::generate();
        let method = request.method().clone();
        request.extensions_mut().insert(request_id.clone());
        let inner = self.inner.clone();
        let span = tracing::info_span!(
            "rustee.request",
            otel.name = "HTTP request",
            otel.kind = "server",
            otel.status_code = tracing::field::Empty,
            request_id = %request_id,
            method = %method,
            http.request.method = %method,
            status = tracing::field::Empty,
            http.response.status_code = tracing::field::Empty,
            duration_ms = tracing::field::Empty,
            route = tracing::field::Empty,
        );
        if let Some(span_parent) = span_parent {
            span_parent.apply(&span);
        }
        let completion_span = span.clone();

        async move {
            let started_at = Instant::now();
            let mut response = inner.call_ready(request).await?;
            let status = response.status();
            let duration_ms = started_at.elapsed().as_millis();
            completion_span.record("status", tracing::field::display(status.as_u16()));
            completion_span.record(
                "http.response.status_code",
                tracing::field::display(status.as_u16()),
            );
            completion_span.record(
                "otel.status_code",
                tracing::field::display(if status.is_server_error() {
                    "ERROR"
                } else {
                    "UNSET"
                }),
            );
            completion_span.record("duration_ms", tracing::field::display(duration_ms));
            if let Some(route) = response.extensions().get::<RouteClassification>() {
                completion_span.record("route", tracing::field::display(route.as_str()));
            }
            info!(parent: &completion_span, "request completed");
            response.headers_mut().insert(
                REQUEST_ID_HEADER,
                HeaderValue::from_str(request_id.as_str())
                    .expect("generated UUID request ID must be a valid header value"),
            );
            Ok(response)
        }
        .instrument(span)
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use http::{HeaderValue, Request as HttpRequest, StatusCode, header::HeaderName};
    use rustee_core::{empty_body, response};
    use rustee_router::App;
    use tower::{Layer, ServiceExt};

    use super::{RequestId, RequestIdLayer};

    #[tokio::test]
    async fn generates_and_returns_a_request_id_without_trusting_client_input() {
        let service = RequestIdLayer::new().layer(
            App::new().get("/health", |request_id: RequestId| async move {
                request_id.to_string()
            }),
        );
        let request = HttpRequest::builder()
            .method("GET")
            .uri("/health")
            .header(HeaderName::from_static("x-request-id"), "client-controlled")
            .body(empty_body())
            .expect("test request must build");

        let response = service
            .oneshot(request)
            .await
            .expect("service is infallible");
        assert_eq!(response.status(), StatusCode::OK);
        let request_id = response
            .headers()
            .get("x-request-id")
            .expect("response must include a request ID")
            .to_str()
            .expect("generated ID must be ASCII");
        assert_ne!(request_id, "client-controlled");
        assert_eq!(request_id.len(), 32);
        assert!(request_id.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }

    #[tokio::test]
    async fn response_header_matches_the_handler_id_and_overrides_application_values() {
        let observed_handler_id = Arc::new(Mutex::new(None));
        let observed_by_handler = Arc::clone(&observed_handler_id);
        let service =
            RequestIdLayer::new().layer(App::new().get("/health", move |request_id: RequestId| {
                let observed_by_handler = Arc::clone(&observed_by_handler);
                async move {
                    *observed_by_handler
                        .lock()
                        .expect("test request ID state must not be poisoned") =
                        Some(request_id.to_string());
                    let mut response = response(StatusCode::OK, empty_body());
                    response.headers_mut().insert(
                        "x-request-id",
                        HeaderValue::from_static("application-controlled"),
                    );
                    response
                }
            }));
        let request = HttpRequest::builder()
            .uri("/health")
            .body(empty_body())
            .expect("test request must build");

        let response = service
            .oneshot(request)
            .await
            .expect("service is infallible");
        let header_id = response
            .headers()
            .get("x-request-id")
            .expect("response must include a request ID")
            .to_str()
            .expect("generated ID must be ASCII")
            .to_owned();
        let handler_id = observed_handler_id
            .lock()
            .expect("test request ID state must not be poisoned")
            .clone()
            .expect("handler must receive a request ID");

        assert_ne!(header_id, "application-controlled");
        assert_eq!(header_id, handler_id);
    }
}
