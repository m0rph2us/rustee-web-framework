//! Optional OpenTelemetry export and W3C Trace Context propagation for Rustee `tracing` spans.
//!
//! This crate does not construct an exporter, read endpoint credentials, or own SDK shutdown.
//! Applications build their tracer provider, choose an exporter and sampling policy, then install
//! the returned tracer with [`init`]. Rustee request spans already contain only the generated
//! request ID, method, status, duration, and configured route classification. W3C propagation is
//! opt-in through [`TraceContextLayer`] and never writes trace headers to HTTP responses.

use std::task::{Context as TaskContext, Poll};

use http::{HeaderMap, HeaderName, HeaderValue};
use opentelemetry::{
    Context,
    propagation::{Extractor, Injector, TextMapPropagator},
    trace::TraceContextExt,
};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use rustee_core::Request;
use rustee_observability::{RequestSpanParent, RequestSpanParentHook};
use tower::{Layer, Service};
use tracing::Subscriber;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{
    EnvFilter, layer::SubscriberExt, registry::LookupSpan, util::SubscriberInitExt,
};

use opentelemetry::trace::Tracer;

pub use opentelemetry;
pub use tracing_opentelemetry;

/// Creates a composable `tracing` layer backed by one application-owned tracer.
///
/// Use this when the application already owns its subscriber assembly. The layer exports Rustee's
/// existing request span without adding request paths, hosts, credentials, or payloads.
#[must_use]
pub fn layer<S, T>(tracer: T) -> tracing_opentelemetry::OpenTelemetryLayer<S, T>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
    T: Tracer + Send + Sync + 'static,
    T::Span: Send + Sync,
{
    tracing_opentelemetry::layer().with_tracer(tracer)
}

/// Installs formatted `tracing` output and OpenTelemetry export for one application tracer.
///
/// The function reads `RUST_LOG` using the same fallback as `rustee-observability::init`. It is
/// process-global: call it once, retain the tracer provider in application state, and shut that
/// provider down during the application's graceful shutdown sequence.
///
/// Returns `true` when this call installed the global subscriber. A `false` result means another
/// subscriber was already installed; no exporter or span policy is changed.
#[must_use]
pub fn init<T>(tracer: T) -> bool
where
    T: Tracer + Send + Sync + 'static,
    T::Span: Send + Sync,
{
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .with(layer(tracer))
        .try_init()
        .is_ok()
}

const TRACEPARENT: &str = "traceparent";
const TRACESTATE: &str = "tracestate";
const MAX_TRACEPARENT_LEN: usize = 512;
const MAX_TRACESTATE_LEN: usize = 512;

/// Opt-in Tower layer that makes a valid inbound W3C trace context the parent of Rustee's request
/// span.
///
/// Place this outside [`rustee_observability::RequestIdLayer`] so the request span sees the parsed
/// parent before it is entered. The layer accepts one bounded ASCII `traceparent`, combines bounded
/// `tracestate` fields in wire order, and silently starts a new root trace for invalid, duplicate,
/// or oversized input. It does not trust a remote sampled flag over the application's tracer
/// provider sampler, record raw propagation headers, or add tracing headers to HTTP responses.
#[derive(Clone, Debug, Default)]
#[must_use = "a trace context layer must be applied to a service to have an effect"]
pub struct TraceContextLayer {
    propagator: TraceContextPropagator,
}

impl TraceContextLayer {
    /// Creates a layer using the W3C `traceparent` and `tracestate` propagator only.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Service produced by [`TraceContextLayer`].
#[derive(Clone, Debug)]
pub struct TraceContextService<S> {
    inner: S,
    propagator: TraceContextPropagator,
}

impl<S> Layer<S> for TraceContextLayer {
    type Service = TraceContextService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TraceContextService {
            inner,
            propagator: self.propagator.clone(),
        }
    }
}

impl<S> Service<Request> for TraceContextService<S>
where
    S: Service<Request>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, context: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, mut request: Request) -> Self::Future {
        if let Some(parent) = remote_parent(&self.propagator, request.headers()) {
            request
                .extensions_mut()
                .insert(RequestSpanParent::new(parent));
        }
        self.inner.call(request)
    }
}

/// Injects the current Rustee `tracing` span context into an outbound HTTP header map.
///
/// Existing `traceparent` and `tracestate` values are removed first. With no active OpenTelemetry
/// span, the headers remain absent rather than forwarding a caller-supplied value. This helper is
/// explicit: Rustee does not inject propagation headers into response headers or arbitrary clients.
pub fn inject_current_context(headers: &mut HeaderMap) {
    inject_context(&tracing::Span::current().context(), headers);
}

/// Injects one OpenTelemetry context into an outbound HTTP header map.
///
/// Existing W3C trace headers are removed before injection. Applications commonly use this from a
/// client request created inside a Rustee request handler.
pub fn inject_context(context: &Context, headers: &mut HeaderMap) {
    headers.remove(TRACEPARENT);
    headers.remove(TRACESTATE);
    let mut injector = HeaderInjector(headers);
    TraceContextPropagator::new().inject_context(context, &mut injector);
}

#[derive(Clone, Debug)]
struct RemoteTraceParent(Context);

impl RequestSpanParentHook for RemoteTraceParent {
    fn apply(&self, span: &tracing::Span) {
        let _ = span.set_parent(self.0.clone());
    }
}

fn remote_parent(
    propagator: &TraceContextPropagator,
    headers: &HeaderMap,
) -> Option<RemoteTraceParent> {
    let carrier = IncomingHeaders::from_headers(headers)?;
    let context = propagator.extract(&carrier);
    context
        .span()
        .span_context()
        .is_valid()
        .then_some(RemoteTraceParent(context))
}

#[derive(Debug)]
struct IncomingHeaders {
    traceparent: String,
    tracestate: Option<String>,
}

impl IncomingHeaders {
    fn from_headers(headers: &HeaderMap) -> Option<Self> {
        let traceparent = one_bounded_header(headers, TRACEPARENT, MAX_TRACEPARENT_LEN)?;
        let tracestate = joined_bounded_headers(headers, TRACESTATE, MAX_TRACESTATE_LEN);
        Some(Self {
            traceparent,
            tracestate,
        })
    }
}

impl Extractor for IncomingHeaders {
    fn get(&self, key: &str) -> Option<&str> {
        if key.eq_ignore_ascii_case(TRACEPARENT) {
            Some(&self.traceparent)
        } else if key.eq_ignore_ascii_case(TRACESTATE) {
            self.tracestate.as_deref()
        } else {
            None
        }
    }

    fn keys(&self) -> Vec<&str> {
        let mut keys = vec![TRACEPARENT];
        if self.tracestate.is_some() {
            keys.push(TRACESTATE);
        }
        keys
    }
}

struct HeaderInjector<'a>(&'a mut HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if key.eq_ignore_ascii_case(TRACESTATE) && value.is_empty() {
            self.0.remove(TRACESTATE);
            return;
        }
        let Ok(name) = HeaderName::from_bytes(key.as_bytes()) else {
            return;
        };
        let Ok(value) = HeaderValue::from_str(&value) else {
            return;
        };
        self.0.insert(name, value);
    }
}

fn one_bounded_header(headers: &HeaderMap, name: &str, maximum_len: usize) -> Option<String> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?.to_str().ok()?;
    if values.next().is_some() || value.len() > maximum_len || !value.is_ascii() {
        return None;
    }
    Some(value.to_owned())
}

fn joined_bounded_headers(headers: &HeaderMap, name: &str, maximum_len: usize) -> Option<String> {
    let mut joined = String::new();
    for value in &headers.get_all(name) {
        let value = value.to_str().ok()?;
        let separator_len = usize::from(!joined.is_empty());
        if !value.is_ascii()
            || joined
                .len()
                .saturating_add(separator_len)
                .saturating_add(value.len())
                > maximum_len
        {
            return None;
        }
        if !joined.is_empty() {
            joined.push(',');
        }
        joined.push_str(value);
    }
    (!joined.is_empty()).then_some(joined)
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        sync::{Arc, Mutex},
    };

    use http::{HeaderMap, Request as HttpRequest, StatusCode, header::HeaderName};
    use opentelemetry::{trace::SpanKind, trace::TracerProvider};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use rustee_core::{empty_body, response};
    use rustee_observability::RequestIdLayer;
    use rustee_router::App;
    use tower::{Layer, ServiceExt, service_fn};
    use tracing_subscriber::layer::SubscriberExt;

    use super::{TRACEPARENT, TRACESTATE, TraceContextLayer, inject_current_context, layer};

    #[tokio::test]
    async fn request_spans_export_only_correlation_and_bounded_http_metadata() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry()
            .with(layer(provider.tracer("rustee-observability-contract")));
        let dispatch = tracing::Dispatch::new(subscriber);
        let guard = tracing::dispatcher::set_default(&dispatch);

        let service = RequestIdLayer::new().layer(App::new().get("/health", || async { "ok" }));
        let response = service
            .oneshot(
                HttpRequest::builder()
                    .method("GET")
                    .uri("/health?secret=never-exported")
                    .body(empty_body())
                    .unwrap(),
            )
            .await
            .unwrap();
        drop(guard);
        let request_id = response
            .headers()
            .get(HeaderName::from_static("x-request-id"))
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();

        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        let span = spans
            .iter()
            .find(|span| span.name == "HTTP request")
            .unwrap_or_else(|| panic!("request span must be exported: {spans:#?}"));
        assert_eq!(span.span_kind, SpanKind::Server);
        let attributes = format!("{:?}", span.attributes);
        assert!(attributes.contains(&request_id));
        assert!(attributes.contains("http.request.method"));
        assert!(attributes.contains("http.response.status_code"));
        assert!(attributes.contains("/health"));
        assert!(!attributes.contains("secret=never-exported"));
        provider.shutdown().unwrap();
    }

    #[tokio::test]
    async fn trace_context_layer_parents_request_and_injects_the_current_child_context() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry()
            .with(layer(provider.tracer("rustee-trace-context-contract")));
        let dispatch = tracing::Dispatch::new(subscriber);
        let guard = tracing::dispatcher::set_default(&dispatch);
        let outgoing = Arc::new(Mutex::new(None::<HeaderMap>));
        let captured = outgoing.clone();
        let service =
            TraceContextLayer::new().layer(RequestIdLayer::new().layer(service_fn(move |_| {
                let captured = captured.clone();
                async move {
                    let mut headers = HeaderMap::new();
                    headers.insert(TRACEPARENT, "stale-parent".parse().unwrap());
                    headers.insert(TRACESTATE, "stale=state".parse().unwrap());
                    inject_current_context(&mut headers);
                    *captured.lock().unwrap() = Some(headers);
                    Ok::<_, Infallible>(response(StatusCode::OK, empty_body()))
                }
            })));
        let response = service
            .oneshot(
                HttpRequest::builder()
                    .method("GET")
                    .uri("/health")
                    .header(
                        TRACEPARENT,
                        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                    )
                    .header(TRACESTATE, "vendor=one")
                    .body(empty_body())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        drop(guard);

        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        let span = spans
            .iter()
            .find(|span| span.name == "HTTP request")
            .unwrap_or_else(|| panic!("request span must be exported: {spans:#?}"));
        assert_eq!(span.span_kind, SpanKind::Server);
        assert!(span.parent_span_is_remote);
        assert_eq!(
            span.span_context.trace_id().to_string(),
            "0af7651916cd43dd8448eb211c80319c"
        );
        assert_eq!(span.parent_span_id.to_string(), "b7ad6b7169203331");

        let outgoing = outgoing.lock().unwrap().take().unwrap();
        let traceparent = outgoing.get(TRACEPARENT).unwrap().to_str().unwrap();
        let parts = traceparent.split('-').collect::<Vec<_>>();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0], "00");
        assert_eq!(parts[1], span.span_context.trace_id().to_string());
        assert_eq!(parts[2], span.span_context.span_id().to_string());
        assert_eq!(outgoing.get(TRACESTATE).unwrap(), "vendor=one");
        provider.shutdown().unwrap();
    }

    #[tokio::test]
    async fn duplicate_traceparent_starts_a_new_root_and_does_not_forward_tracestate() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry()
            .with(layer(provider.tracer("rustee-trace-context-rejection")));
        let dispatch = tracing::Dispatch::new(subscriber);
        let guard = tracing::dispatcher::set_default(&dispatch);
        let outgoing = Arc::new(Mutex::new(None::<HeaderMap>));
        let captured = outgoing.clone();
        let service =
            TraceContextLayer::new().layer(RequestIdLayer::new().layer(service_fn(move |_| {
                let captured = captured.clone();
                async move {
                    let mut headers = HeaderMap::new();
                    headers.insert(TRACEPARENT, "stale-parent".parse().unwrap());
                    headers.insert(TRACESTATE, "stale=state".parse().unwrap());
                    inject_current_context(&mut headers);
                    *captured.lock().unwrap() = Some(headers);
                    Ok::<_, Infallible>(response(StatusCode::OK, empty_body()))
                }
            })));
        let request = HttpRequest::builder()
            .method("GET")
            .uri("/health")
            .header(
                TRACEPARENT,
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            )
            .header(TRACESTATE, "secret=never-forwarded")
            .body(empty_body())
            .unwrap();
        let (mut parts, body) = request.into_parts();
        parts.headers.append(
            HeaderName::from_static(TRACEPARENT),
            "00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01"
                .parse()
                .unwrap(),
        );
        let response = service
            .oneshot(HttpRequest::from_parts(parts, body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        drop(guard);

        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        let span = spans
            .iter()
            .find(|span| span.name == "HTTP request")
            .unwrap_or_else(|| panic!("request span must be exported: {spans:#?}"));
        assert!(!span.parent_span_is_remote);
        assert_ne!(
            span.span_context.trace_id().to_string(),
            "0af7651916cd43dd8448eb211c80319c"
        );

        let outgoing = outgoing.lock().unwrap().take().unwrap();
        assert!(outgoing.get(TRACEPARENT).is_some());
        assert!(outgoing.get(TRACESTATE).is_none());
        provider.shutdown().unwrap();
    }
}
