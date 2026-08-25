use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
};

use http::{HeaderMap, Request as HttpRequest, StatusCode, header::HeaderName};
use opentelemetry::{trace::SpanKind, trace::TracerProvider};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use rustee_core::{empty_body, response};
use rustee_observability::RequestIdLayer;
use tower::{Layer, ServiceExt, service_fn};
use tracing_subscriber::layer::SubscriberExt;

use crate::{TraceContextLayer, inject_current_context};

use super::super::w3c::{TRACEPARENT, TRACESTATE};

struct LeakyService;

impl std::fmt::Debug for LeakyService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("LeakyService(private-trace-service-configuration)")
    }
}

#[test]
fn trace_context_service_debug_does_not_delegate_to_the_inner_service() {
    let service = TraceContextLayer::new().layer(LeakyService);

    let debug = format!("{service:?}");

    assert!(debug.contains("inner_type"));
    assert!(!debug.contains("private-trace-service-configuration"));
}

#[tokio::test]
async fn trace_context_layer_parents_request_and_injects_the_current_child_context() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let subscriber = tracing_subscriber::registry().with(crate::layer(
        provider.tracer("rustee-trace-context-contract"),
    ));
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
    let subscriber = tracing_subscriber::registry().with(crate::layer(
        provider.tracer("rustee-trace-context-rejection"),
    ));
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
