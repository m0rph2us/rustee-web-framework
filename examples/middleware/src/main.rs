//! Compile-checked middleware composition example for a Rustee HTTP service.

use std::{convert::Infallible, net::SocketAddr};

use http::HeaderValue;
use rustee::{App, Json, Request, Response, ServerOptions};
use rustee_middleware::{CompressionLayer, CorsLayer, PanicCatchLayer};
use rustee_observability::{RequestId, RequestIdLayer};
use serde::Serialize;
use tokio::net::TcpListener;
use tower::{Layer, util::BoxCloneService};

#[derive(Serialize)]
struct Health {
    status: &'static str,
    request_id: String,
}

async fn health(request_id: RequestId) -> Json<Health> {
    Json(Health {
        status: "ok",
        request_id: request_id.to_string(),
    })
}

fn service() -> BoxCloneService<Request, Response, Infallible> {
    let app = App::new().get("/health", health);
    BoxCloneService::new(RequestIdLayer::new().layer(
        PanicCatchLayer::new().layer(
            CompressionLayer::new().layer(
                CorsLayer::new(HeaderValue::from_static("http://localhost:3000")).layer(app),
            ),
        ),
    ))
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 3002))).await?;
    rustee::serve_service_listener_with_options(
        listener,
        service(),
        ServerOptions::default(),
        std::future::pending(),
    )
    .await
}

#[cfg(test)]
mod tests {
    use http::{Request as HttpRequest, StatusCode, header};
    use rustee::empty_body;
    use tower::ServiceExt;

    use super::service;

    #[tokio::test]
    async fn middleware_generates_correlation_and_applies_the_explicit_cors_policy() {
        let response = service()
            .oneshot(
                HttpRequest::builder()
                    .uri("/health")
                    .header(header::ORIGIN, "http://localhost:3000")
                    .header("x-request-id", "untrusted-client-value")
                    .body(empty_body())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "http://localhost:3000"
        );
        assert_ne!(response.headers()["x-request-id"], "untrusted-client-value");
    }
}
