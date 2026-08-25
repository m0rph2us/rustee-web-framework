use bytes::Bytes;
use http::{HeaderName, HeaderValue, StatusCode};
use rustee_core::{full_body, response};
use rustee_router::App;

use crate::{TestApp, TestResponseError};

#[tokio::test]
async fn response_bound_stops_before_retaining_an_oversized_body() {
    let app = App::new().get("/large", || async {
        response(StatusCode::OK, full_body(Bytes::from_static(b"oversized")))
    });
    let error = TestApp::with_max_response_bytes(app, 4)
        .unwrap()
        .get("/large")
        .unwrap()
        .send()
        .await
        .unwrap_err();
    assert_eq!(error, TestResponseError::ResponseTooLarge);
}

#[tokio::test]
async fn assertions_do_not_render_response_body_or_header_values() {
    let app = App::new().get("/", || async {
        let mut response = response(
            StatusCode::ACCEPTED,
            full_body(Bytes::from_static(b"secret")),
        );
        response
            .headers_mut()
            .insert("x-private", HeaderValue::from_static("secret"));
        response
    });
    let response = TestApp::new(app).get("/").unwrap().send().await.unwrap();
    assert_eq!(
        response
            .assert_status(StatusCode::OK)
            .unwrap_err()
            .to_string(),
        "expected HTTP status 200, received 202"
    );
    assert_eq!(
        response
            .assert_header(
                &HeaderName::from_static("x-private"),
                &HeaderValue::from_static("different"),
            )
            .unwrap_err()
            .to_string(),
        "response header did not match the expected value"
    );
}
