use http::{
    HeaderValue, Method, Request as HttpRequest, StatusCode,
    header::{
        ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_REQUEST_METHOD,
        ORIGIN, VARY,
    },
};
use rustee_core::{IntoResponse, empty_body};
use rustee_router::App;
use tower::{Layer, ServiceExt};

use crate::CorsLayer;

#[tokio::test]
async fn cors_preflight_does_not_reach_the_application() {
    let service = CorsLayer::new("https://app.example.test".parse().unwrap())
        .layer(App::new().get("/resource", || async { "unexpected" }));
    let request = HttpRequest::builder()
        .method("OPTIONS")
        .uri("/resource")
        .header(ORIGIN, "https://app.example.test")
        .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
        .body(empty_body())
        .unwrap();

    let response = service.oneshot(request).await.unwrap();
    assert_eq!(response.status(), 204);
    assert_eq!(
        response.headers()[ACCESS_CONTROL_ALLOW_ORIGIN],
        "https://app.example.test"
    );
    assert_eq!(response.headers()[VARY], "Origin");
}

#[tokio::test]
async fn cors_applies_only_to_one_matching_origin() {
    let service = CorsLayer::new("https://app.example.test".parse().unwrap())
        .layer(App::new().fallback(|| async { "application" }));
    let allowed = HttpRequest::builder()
        .method(Method::GET)
        .uri("/resource")
        .header(ORIGIN, "https://app.example.test")
        .body(empty_body())
        .unwrap();
    let response = service.clone().oneshot(allowed).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[ACCESS_CONTROL_ALLOW_ORIGIN],
        "https://app.example.test"
    );
    assert_eq!(response.headers()[VARY], "Origin");
    assert!(
        response
            .headers()
            .get(ACCESS_CONTROL_ALLOW_METHODS)
            .is_none()
    );

    let rejected = HttpRequest::builder()
        .method(Method::OPTIONS)
        .uri("/resource")
        .header(ORIGIN, "https://other.example.test")
        .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
        .body(empty_body())
        .unwrap();
    let response = service.clone().oneshot(rejected).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .headers()
            .get(ACCESS_CONTROL_ALLOW_ORIGIN)
            .is_none()
    );
    assert!(response.headers().get(VARY).is_none());

    let mut duplicate = HttpRequest::builder()
        .method(Method::GET)
        .uri("/resource")
        .header(ORIGIN, "https://app.example.test")
        .body(empty_body())
        .unwrap();
    duplicate
        .headers_mut()
        .append(ORIGIN, HeaderValue::from_static("https://app.example.test"));
    let response = service.oneshot(duplicate).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .headers()
            .get(ACCESS_CONTROL_ALLOW_ORIGIN)
            .is_none()
    );
}

#[tokio::test]
async fn cors_preserves_existing_vary_values_without_duplicating_origin() {
    let service = CorsLayer::new("https://app.example.test".parse().unwrap()).layer(
        App::new().get("/resource", || async {
            let mut response = "ok".into_response();
            response
                .headers_mut()
                .append(VARY, HeaderValue::from_static("Accept-Encoding"));
            response
                .headers_mut()
                .append(VARY, HeaderValue::from_static("Origin"));
            response
        }),
    );
    let request = HttpRequest::builder()
        .method(Method::GET)
        .uri("/resource")
        .header(ORIGIN, "https://app.example.test")
        .body(empty_body())
        .unwrap();

    let response = service.oneshot(request).await.unwrap();
    let vary_values = response
        .headers()
        .get_all(VARY)
        .iter()
        .map(|value| value.to_str().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(vary_values, ["Accept-Encoding", "Origin"]);
}
