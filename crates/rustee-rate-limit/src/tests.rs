use std::{
    collections::VecDeque,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use http::{Request as HttpRequest, StatusCode};
use http_body_util::BodyExt;
use rustee_core::empty_body;
use rustee_router::App;
use tower::{Layer, ServiceExt};

use super::{
    FixedWindow, RateLimitConfigError, RateLimitDecision, RateLimitKey, RateLimitLayer,
    RateLimitStore, StoreFailurePolicy,
};

#[derive(Clone)]
struct ScriptedStore {
    decisions: Arc<Mutex<VecDeque<Result<RateLimitDecision, io::Error>>>>,
}

impl ScriptedStore {
    fn new(decisions: impl IntoIterator<Item = Result<RateLimitDecision, io::Error>>) -> Self {
        Self {
            decisions: Arc::new(Mutex::new(decisions.into_iter().collect())),
        }
    }
}

impl RateLimitStore for ScriptedStore {
    type Error = io::Error;

    fn check(
        &self,
        _: RateLimitKey,
        _: FixedWindow,
    ) -> futures_util::future::BoxFuture<'static, Result<RateLimitDecision, Self::Error>> {
        let decision = self.decisions.lock().unwrap().pop_front().unwrap();
        Box::pin(async move { decision })
    }
}

fn policy() -> FixedWindow {
    FixedWindow::new(3, Duration::from_secs(30)).unwrap()
}

fn request() -> rustee_core::Request {
    HttpRequest::builder().uri("/").body(empty_body()).unwrap()
}

fn verified_principal_key(_: &rustee_core::Request) -> Option<RateLimitKey> {
    RateLimitKey::new("verified-principal").ok()
}

#[test]
fn rate_limit_key_debug_output_redacts_the_key_value() {
    let key = RateLimitKey::new("credential-fingerprint-value").unwrap();

    let output = format!("{key:?}");

    assert!(output.contains("byte_len"));
    assert!(!output.contains(key.as_str()));
}

#[test]
fn fixed_window_requires_a_positive_representable_millisecond_duration() {
    assert_eq!(
        FixedWindow::new(1, Duration::from_nanos(999_999)),
        Err(RateLimitConfigError::InvalidWindow)
    );
    assert_eq!(
        FixedWindow::new(1, Duration::from_millis(1) + Duration::from_nanos(1)),
        Err(RateLimitConfigError::InvalidWindow)
    );

    let window = FixedWindow::new(1, Duration::from_millis(1))
        .expect("one millisecond is a valid storage window");
    assert_eq!(window.window_millis(), 1);

    let largest = FixedWindow::new(1, Duration::from_millis(i64::MAX as u64))
        .expect("the largest signed millisecond value is valid");
    assert_eq!(largest.window_millis(), i64::MAX as u64);
    assert_eq!(
        FixedWindow::new(1, Duration::from_millis(i64::MAX as u64 + 1)),
        Err(RateLimitConfigError::InvalidWindow)
    );
}

#[tokio::test]
async fn allowed_response_includes_bounded_rate_limit_headers() {
    let store = ScriptedStore::new([Ok(RateLimitDecision::allowed(
        policy(),
        2,
        Duration::from_millis(1_250),
    ))]);
    let service = RateLimitLayer::new(
        store,
        policy(),
        verified_principal_key,
        StoreFailurePolicy::FailClosed,
    )
    .layer(App::new().get("/", || async { "ok" }));

    let response = service.oneshot(request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["ratelimit-limit"], "3");
    assert_eq!(response.headers()["ratelimit-remaining"], "2");
    assert_eq!(response.headers()["ratelimit-reset"], "2");
    assert!(response.headers().get("retry-after").is_none());
}

#[tokio::test]
async fn denied_request_does_not_reach_the_application() {
    let store = ScriptedStore::new([Ok(RateLimitDecision::denied(
        policy(),
        Duration::from_millis(1),
    ))]);
    let service = RateLimitLayer::new(
        store,
        policy(),
        verified_principal_key,
        StoreFailurePolicy::FailClosed,
    )
    .layer(App::new().get("/", || async { "unexpected" }));

    let response = service.oneshot(request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()["ratelimit-remaining"], "0");
    assert_eq!(response.headers()["retry-after"], "1");
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert!(
        std::str::from_utf8(&body)
            .unwrap()
            .contains("rate_limit_exceeded")
    );
}

#[tokio::test]
async fn store_failure_is_sanitized_when_fail_closed() {
    let store = ScriptedStore::new([Err(io::Error::other("redis endpoint and key are secret"))]);
    let service = RateLimitLayer::new(
        store,
        policy(),
        verified_principal_key,
        StoreFailurePolicy::FailClosed,
    )
    .layer(App::new().get("/", || async { "unexpected" }));

    let response = service.oneshot(request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("rate_limit_unavailable"));
    assert!(!body.contains("redis endpoint"));
}

#[tokio::test]
async fn store_failure_passes_through_only_when_fail_open_is_explicit() {
    let store = ScriptedStore::new([Err(io::Error::other("unavailable"))]);
    let service = RateLimitLayer::new(
        store,
        policy(),
        verified_principal_key,
        StoreFailurePolicy::FailOpen,
    )
    .layer(App::new().get("/", || async { "allowed" }));

    let response = service.oneshot(request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get("ratelimit-limit").is_none());
}
