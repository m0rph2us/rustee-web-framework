//! Opt-in Redis contract tests. Run with a disposable Redis instance at `RUSTEE_REDIS_URL`.

use http::{Request as HttpRequest, StatusCode, header::SET_COOKIE};
use rustee_auth::Principal;
use rustee_auth_session::{SessionCookieConfig, SessionLayer, SessionManager, SessionUser};
use rustee_auth_session_redis::{RedisSessionStore, RedisSessionStoreError};
use rustee_core::{Response, empty_body};
use rustee_redis::{RedisConfig, connect};
use rustee_router::App;
use tower::{Layer, ServiceExt};
use uuid::Uuid;

fn redis_url() -> String {
    std::env::var("RUSTEE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/15".to_owned())
}

#[tokio::test]
#[ignore = "requires a Redis server; CI provisions one"]
async fn redis_store_restores_a_session_through_the_http_layer() {
    let connection = connect(&RedisConfig::new(redis_url())).await.unwrap();
    let store = RedisSessionStore::with_namespace(
        connection,
        format!("rustee:test:session:{}", Uuid::new_v4()),
    )
    .unwrap();
    let cookie = SessionCookieConfig::new("rustee_session", 60)
        .unwrap()
        .with_secure(false)
        .unwrap();
    let manager = SessionManager::new(store.clone(), cookie.clone());
    let issued = manager
        .establish(Principal::new("redis-alice").unwrap())
        .await
        .unwrap();
    let mut login_response = Response::new(empty_body());
    issued.apply_to(&mut login_response);
    let session_cookie = login_response
        .headers()
        .get(SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    let service = SessionLayer::new(store.clone(), cookie).layer(App::new().post(
        "/profile",
        |user: SessionUser| async move {
            assert_eq!(user.principal().subject(), "redis-alice");
            "updated"
        },
    ));
    let response = service
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri("/profile")
                .header("cookie", session_cookie)
                .body(empty_body())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let oversized_cookie = SessionCookieConfig::new("rustee_oversized", u64::MAX)
        .unwrap()
        .with_secure(false)
        .unwrap();
    let oversized_manager = SessionManager::new(store, oversized_cookie);
    assert!(matches!(
        oversized_manager
            .establish(Principal::new("redis-oversized").unwrap())
            .await,
        Err(RedisSessionStoreError::TtlOutOfRange)
    ));
}
