use http::{Method, Request as HttpRequest, StatusCode};
use http_body_util::BodyExt;
use rustee_core::empty_body;
use rustee_router::App;
use tower::{Layer, ServiceExt};

use crate::PanicCatchLayer;

async fn panic_handler() -> &'static str {
    panic!("private panic detail must not reach an HTTP response");
}

#[tokio::test]
async fn panic_catch_layer_returns_a_redacted_internal_response() {
    let service = PanicCatchLayer::new().layer(App::new().get("/panic", panic_handler));
    let request = HttpRequest::builder()
        .method(Method::GET)
        .uri("/panic")
        .body(empty_body())
        .unwrap();

    let response = service.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        body,
        r#"{"error":{"code":"internal_error","message":"an internal server error occurred"}}"#
    );
    assert!(!String::from_utf8_lossy(&body).contains("private panic detail"));
}
