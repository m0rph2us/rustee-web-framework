use std::convert::Infallible;

use futures_util::future::BoxFuture;
use http::{HeaderValue, Request as HttpRequest, StatusCode, header::SET_COOKIE};
use rustee_auth::Principal;
use rustee_core::{empty_body, response};
use rustee_router::App;
use tower::{Layer, ServiceExt};

use super::{
    CookieConfigError, CsrfLayer, InMemorySessionStore, IssuedSession, MAX_COOKIE_NAME_BYTES,
    SameSite, Session, SessionCookieConfig, SessionId, SessionLayer, SessionManager, SessionStore,
    SessionUser,
};

#[derive(Clone)]
struct MismatchedSessionStore {
    session: Session,
}

impl SessionStore for MismatchedSessionStore {
    type Error = Infallible;

    fn save(&self, _session: Session) -> BoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(async { Ok(()) })
    }

    fn load(&self, _id: SessionId) -> BoxFuture<'static, Result<Option<Session>, Self::Error>> {
        let session = self.session.clone();
        Box::pin(async move { Ok(Some(session)) })
    }

    fn delete(&self, _id: SessionId) -> BoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(async { Ok(()) })
    }
}

fn session_cookie(issued: &IssuedSession) -> HeaderValue {
    let mut response = response(StatusCode::NO_CONTENT, empty_body());
    issued.apply_to(&mut response);
    response
        .headers()
        .get(SET_COOKIE)
        .expect("issued session must add a cookie")
        .clone()
}

#[tokio::test]
async fn session_layer_restores_a_principal_and_csrf_layer_protects_session_posts() {
    let store = InMemorySessionStore::default();
    let cookie = SessionCookieConfig::new("rustee_session", 60)
        .unwrap()
        .with_secure(false)
        .unwrap();
    let manager = SessionManager::new(store.clone(), cookie.clone());
    let issued = manager
        .establish(Principal::new("alice").unwrap())
        .await
        .unwrap();
    let session_cookie = session_cookie(&issued);
    let session_cookie = session_cookie.to_str().unwrap();
    // Session restoration wraps CSRF so the CSRF layer can distinguish session-authenticated
    // requests from public or bearer-authenticated endpoints.
    let service = SessionLayer::new(store, cookie).layer(
        CsrfLayer.layer(
            App::new()
                .post("/profile", |user: SessionUser| async move {
                    assert_eq!(user.principal().subject(), "alice");
                    "updated"
                })
                .post("/webhook", || async { "accepted" }),
        ),
    );

    let rejected = service
        .clone()
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
    let accepted = service
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri("/profile")
                .header("cookie", session_cookie)
                .header("x-csrf-token", issued.csrf_token())
                .body(empty_body())
                .unwrap(),
        )
        .await
        .unwrap();
    let public_post = service
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri("/webhook")
                .body(empty_body())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(rejected.status(), StatusCode::FORBIDDEN);
    assert_eq!(accepted.status(), StatusCode::OK);
    assert_eq!(public_post.status(), StatusCode::OK);
}

#[tokio::test]
async fn session_layer_rejects_a_store_record_bound_to_another_session_id() {
    let backing_store = InMemorySessionStore::default();
    let cookie = SessionCookieConfig::new("rustee_session", 60)
        .unwrap()
        .with_secure(false)
        .unwrap();
    let manager = SessionManager::new(backing_store.clone(), cookie.clone());
    let alice = manager
        .establish(Principal::new("alice").unwrap())
        .await
        .unwrap();
    let bob = manager
        .establish(Principal::new("bob").unwrap())
        .await
        .unwrap();
    let bob_id = session_cookie(&bob)
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .and_then(|cookie| cookie.split_once('='))
        .and_then(|(_, value)| SessionId::parse(value))
        .unwrap();
    let bob_session = backing_store.load(bob_id).await.unwrap().unwrap();
    let service = SessionLayer::new(
        MismatchedSessionStore {
            session: bob_session,
        },
        cookie,
    )
    .layer(App::new().get("/profile", |_: SessionUser| async { "not reached" }));

    let response = service
        .oneshot(
            HttpRequest::builder()
                .uri("/profile")
                .header("cookie", session_cookie(&alice))
                .body(empty_body())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[test]
fn cross_site_cookies_cannot_disable_secure_transport() {
    let config = SessionCookieConfig::new("session", 60)
        .unwrap()
        .with_secure(false)
        .unwrap();

    assert_eq!(
        config.with_same_site(SameSite::None),
        Err(CookieConfigError::SameSiteNoneRequiresSecure)
    );
}

#[test]
fn session_cookie_name_has_a_bounded_header_footprint() {
    let maximum_name = "s".repeat(MAX_COOKIE_NAME_BYTES);
    assert!(SessionCookieConfig::new(maximum_name, 60).is_ok());

    let oversized_name = "s".repeat(MAX_COOKIE_NAME_BYTES + 1);
    assert_eq!(
        SessionCookieConfig::new(oversized_name, 60),
        Err(CookieConfigError::InvalidName)
    );
}
