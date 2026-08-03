//! Opt-in verification against a real OTLP/HTTP Collector.

use std::time::Duration;

use http::Request as HttpRequest;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::SdkTracerProvider;
use rustee_core::empty_body;
use rustee_observability::RequestIdLayer;
use rustee_observability_opentelemetry::layer;
use rustee_router::App;
use tower::{Layer, ServiceExt};
use tracing_subscriber::layer::SubscriberExt;

const DEFAULT_OTLP_ENDPOINT: &str = "http://127.0.0.1:4318";

#[tokio::test]
#[ignore = "requires an OTLP/HTTP Collector; CI provisions one"]
async fn otlp_http_collector_accepts_a_rustee_server_span() {
    let endpoint =
        std::env::var("RUSTEE_OTLP_ENDPOINT").unwrap_or_else(|_| DEFAULT_OTLP_ENDPOINT.to_owned());
    let mut last_error = None;

    for _ in 0..10 {
        match export_once(&endpoint).await {
            Ok(()) => return,
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    panic!(
        "OTLP Collector did not accept the Rustee request span: {}",
        last_error.unwrap_or_else(|| "no export attempt completed".to_owned())
    );
}

async fn export_once(endpoint: &str) -> Result<(), String> {
    let exporter = SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(endpoint)
        .with_timeout(Duration::from_secs(1))
        .build()
        .map_err(|error| format!("cannot build OTLP exporter: {error}"))?;
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .build();
    let subscriber =
        tracing_subscriber::registry().with(layer(provider.tracer("rustee-otlp-contract")));
    let dispatch = tracing::Dispatch::new(subscriber);
    let guard = tracing::dispatcher::set_default(&dispatch);
    let service = RequestIdLayer::new().layer(App::new().get("/health", || async { "ok" }));
    let response = service
        .oneshot(
            HttpRequest::builder()
                .uri("/health")
                .body(empty_body())
                .map_err(|error| format!("cannot build contract request: {error}"))?,
        )
        .await
        .map_err(|error| format!("request service unexpectedly failed: {error}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "request span source returned HTTP {}",
            response.status()
        ));
    }
    drop(guard);

    let flush = provider
        .force_flush()
        .map_err(|error| format!("OTLP force flush failed: {error}"));
    let shutdown = provider
        .shutdown()
        .map_err(|error| format!("OTLP provider shutdown failed: {error}"));
    flush?;
    shutdown
}
