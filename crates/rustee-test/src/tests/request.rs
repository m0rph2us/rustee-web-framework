use bytes::Bytes;
use http::{HeaderMap, StatusCode, header::CONTENT_TYPE};
use rustee_core::Json;
use rustee_router::App;

use super::support::Greeting;
use crate::{TestApp, TestAppError, TestRequestError};

#[tokio::test]
async fn request_bound_matches_server_extractor_behavior() {
    let app = App::new().post("/greeting", |Json(_greeting): Json<Greeting>| async {
        StatusCode::NO_CONTENT
    });
    let response = TestApp::with_max_request_bytes(app, 4)
        .unwrap()
        .post("/greeting")
        .unwrap()
        .header(CONTENT_TYPE.as_str(), "application/json")
        .unwrap()
        .body(Bytes::from_static(br#"{"name":"Ada"}"#))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[test]
fn request_json_encoding_stops_at_the_configured_body_limit() {
    let error = TestApp::with_max_request_bytes(App::new(), 4)
        .unwrap()
        .post("/")
        .unwrap()
        .json(&Greeting {
            name: "Ada".to_owned(),
        })
        .unwrap_err();

    assert_eq!(error, TestRequestError::JsonTooLarge);
}

#[test]
fn request_and_configuration_validation_are_sanitized() {
    assert_eq!(
        TestApp::with_max_response_bytes(App::new(), 0).unwrap_err(),
        TestAppError::ZeroResponseBodyLimit
    );
    assert_eq!(
        TestApp::with_max_request_bytes(App::new(), 0).unwrap_err(),
        TestAppError::ZeroRequestBodyLimit
    );
    assert_eq!(
        TestApp::new(App::new()).get("not a URI").unwrap_err(),
        TestRequestError::InvalidUri
    );
    assert_eq!(
        TestApp::new(App::new())
            .get("/")
            .unwrap()
            .header("bad header", "value")
            .unwrap_err(),
        TestRequestError::InvalidHeaderName
    );
}

#[test]
fn string_requests_accept_only_absolute_path_uris() {
    let client = TestApp::new(App::new());

    for uri in ["relative", "http://example.test/path", "*"] {
        assert_eq!(client.get(uri).unwrap_err(), TestRequestError::InvalidUri);
    }
}

#[tokio::test]
async fn appended_headers_preserve_existing_values_for_ambiguous_header_tests() {
    let app = App::new().get("/headers", |headers: HeaderMap| async move {
        let value_count = headers.get_all("x-test-mode").iter().count();
        if value_count == 2 {
            StatusCode::NO_CONTENT
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    });

    let response = TestApp::new(app)
        .get("/headers")
        .unwrap()
        .header("x-test-mode", "first")
        .unwrap()
        .append_header("x-test-mode", "second")
        .unwrap()
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}
