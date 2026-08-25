use bytes::Bytes;
use http::{
    HeaderMap, HeaderValue, StatusCode,
    header::{COOKIE, SET_COOKIE},
};
use rustee_core::{empty_body, full_body, response};
use rustee_router::App;

use crate::{DEFAULT_MAX_COOKIE_BYTES, DEFAULT_MAX_COOKIE_COUNT, TestApp, TestResponseError};

#[tokio::test]
async fn opt_in_cookie_jar_carries_session_style_cookies_and_honors_manual_override() {
    let app = App::new()
        .get("/login", || async {
            let mut response = response(StatusCode::NO_CONTENT, empty_body());
            response.headers_mut().append(
                SET_COOKIE,
                HeaderValue::from_static("session=opaque; Path=/; HttpOnly; SameSite=Lax"),
            );
            response
                .headers_mut()
                .append(SET_COOKIE, HeaderValue::from_static("theme=dark; Path=/"));
            response
        })
        .get("/profile", |headers: HeaderMap| async move {
            headers
                .get(COOKIE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("<missing>")
                .to_owned()
        })
        .get("/logout", || async {
            let mut response = response(StatusCode::NO_CONTENT, empty_body());
            response.headers_mut().append(
                SET_COOKIE,
                HeaderValue::from_static("session=; Path=/; Max-Age=0; HttpOnly"),
            );
            response
        });
    let client = TestApp::new(app).with_cookie_jar();
    let jar = client.cookie_jar().unwrap();

    client.get("/login").unwrap().send().await.unwrap();
    assert_eq!(jar.len(), 2);
    assert_eq!(
        client
            .get("/profile")
            .unwrap()
            .send()
            .await
            .unwrap()
            .text()
            .unwrap(),
        "session=opaque; theme=dark"
    );
    assert_eq!(
        client
            .get("/profile")
            .unwrap()
            .header("cookie", "session=manual")
            .unwrap()
            .send()
            .await
            .unwrap()
            .text()
            .unwrap(),
        "session=manual"
    );

    client.get("/logout").unwrap().send().await.unwrap();
    assert_eq!(jar.len(), 1);
    assert_eq!(
        client
            .get("/profile")
            .unwrap()
            .send()
            .await
            .unwrap()
            .text()
            .unwrap(),
        "theme=dark"
    );
    jar.clear();
    assert!(jar.is_empty());
}

#[tokio::test]
async fn cookie_jar_rejects_malformed_set_cookie_without_retaining_it() {
    let app = App::new().get("/invalid", || async {
        let mut response = response(StatusCode::NO_CONTENT, empty_body());
        response
            .headers_mut()
            .append(SET_COOKIE, HeaderValue::from_static("not-a-cookie"));
        response
    });
    let client = TestApp::new(app).with_cookie_jar();
    let jar = client.cookie_jar().unwrap();

    let error = client.get("/invalid").unwrap().send().await.unwrap_err();

    assert_eq!(error, TestResponseError::InvalidSetCookie);
    assert!(jar.is_empty());
}

#[tokio::test]
async fn cookie_jar_rejects_an_over_capacity_response_atomically() {
    let app = App::new().get("/many", || async {
        let mut response = response(StatusCode::NO_CONTENT, empty_body());
        for index in 0..=DEFAULT_MAX_COOKIE_COUNT {
            response.headers_mut().append(
                SET_COOKIE,
                HeaderValue::try_from(format!("cookie{index}=value")).unwrap(),
            );
        }
        response
    });
    let client = TestApp::new(app).with_cookie_jar();
    let jar = client.cookie_jar().unwrap();

    let error = client.get("/many").unwrap().send().await.unwrap_err();

    assert_eq!(error, TestResponseError::CookieJarLimitExceeded);
    assert!(jar.is_empty());
}

#[tokio::test]
async fn cookie_jar_rejects_an_over_byte_bound_response_atomically() {
    let app = App::new().get("/large-cookie", || async {
        let mut response = response(StatusCode::NO_CONTENT, empty_body());
        response.headers_mut().append(
            SET_COOKIE,
            HeaderValue::try_from(format!("session={}", "x".repeat(DEFAULT_MAX_COOKIE_BYTES)))
                .unwrap(),
        );
        response
    });
    let client = TestApp::new(app).with_cookie_jar();
    let jar = client.cookie_jar().unwrap();

    let error = client
        .get("/large-cookie")
        .unwrap()
        .send()
        .await
        .unwrap_err();

    assert_eq!(error, TestResponseError::CookieJarLimitExceeded);
    assert!(jar.is_empty());
}

#[tokio::test]
async fn cookie_jar_bounds_the_emitted_header_including_pair_separators() {
    let app = App::new()
        .get("/exact", || async {
            let mut response = response(StatusCode::NO_CONTENT, empty_body());
            response.headers_mut().append(
                SET_COOKIE,
                HeaderValue::try_from(format!("a={}", "x".repeat(DEFAULT_MAX_COOKIE_BYTES - 2)))
                    .expect("boundary cookie header must be valid"),
            );
            response
        })
        .get("/separator-overflow", || async {
            let mut response = response(StatusCode::NO_CONTENT, empty_body());
            response
                .headers_mut()
                .append(SET_COOKIE, HeaderValue::from_static("a="));
            response.headers_mut().append(
                SET_COOKIE,
                HeaderValue::try_from(format!("b={}", "x".repeat(DEFAULT_MAX_COOKIE_BYTES - 2)))
                    .expect("separator-overflow cookie header must be valid"),
            );
            response
        });
    let client = TestApp::new(app).with_cookie_jar();
    let jar = client.cookie_jar().expect("cookie jar must be attached");

    client
        .get("/exact")
        .expect("test URI must be valid")
        .send()
        .await
        .expect("a cookie that exactly fills the emitted header budget must be retained");
    assert_eq!(jar.len(), 1);
    jar.clear();

    let error = client
        .get("/separator-overflow")
        .expect("test URI must be valid")
        .send()
        .await
        .expect_err("cookie separators must count toward the emitted header budget");
    assert_eq!(error, TestResponseError::CookieJarLimitExceeded);
    assert!(jar.is_empty());
}

#[tokio::test]
async fn cookie_jar_updates_only_after_the_bounded_response_is_read() {
    let app = App::new().get("/large", || async {
        let mut response = response(StatusCode::OK, full_body(Bytes::from_static(b"oversized")));
        response.headers_mut().append(
            SET_COOKIE,
            HeaderValue::from_static("session=opaque; Path=/"),
        );
        response
    });
    let client = TestApp::with_max_response_bytes(app, 4)
        .unwrap()
        .with_cookie_jar();
    let jar = client.cookie_jar().unwrap();

    let error = client.get("/large").unwrap().send().await.unwrap_err();

    assert_eq!(error, TestResponseError::ResponseTooLarge);
    assert!(jar.is_empty());
}
