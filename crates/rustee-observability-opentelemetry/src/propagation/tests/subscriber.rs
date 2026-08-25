use http::{Request as HttpRequest, header::HeaderName};
use opentelemetry::{trace::SpanKind, trace::TracerProvider};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use rustee_core::empty_body;
use rustee_observability::RequestIdLayer;
use rustee_router::App;
use tower::{Layer, ServiceExt};
use tracing_subscriber::layer::SubscriberExt;

#[tokio::test]
async fn request_spans_export_only_correlation_and_bounded_http_metadata() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let subscriber = tracing_subscriber::registry().with(crate::layer(
        provider.tracer("rustee-observability-contract"),
    ));
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
