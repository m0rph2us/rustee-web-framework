use bytes::Bytes;
use http::{
    HeaderValue, StatusCode,
    header::{AUTHORIZATION, CONTENT_TYPE, SET_COOKIE},
};
use rustee_core::{Json, full_body, response};
use rustee_router::App;

use super::support::Greeting;
use crate::TestApp;

#[tokio::test]
async fn test_app_sends_json_and_decodes_a_bounded_json_response() {
    let app = App::new().post("/greeting", |Json(greeting): Json<Greeting>| async move {
        (
            StatusCode::CREATED,
            Json(Greeting {
                name: greeting.name,
            }),
        )
    });
    let response = TestApp::new(app)
        .post("/greeting")
        .unwrap()
        .header("x-test-id", "request-1")
        .unwrap()
        .json(&Greeting {
            name: "Ada".to_owned(),
        })
        .unwrap()
        .send()
        .await
        .unwrap();

    response.assert_status(StatusCode::CREATED).unwrap();
    response
        .assert_header(
            &CONTENT_TYPE,
            &HeaderValue::from_static("application/json; charset=utf-8"),
        )
        .unwrap();
    assert_eq!(response.json::<Greeting>().unwrap().name, "Ada");
}

#[tokio::test]
async fn request_and_response_debug_redact_headers_queries_and_bodies() {
    let request = TestApp::new(App::new())
        .post("/profile?token=query-secret")
        .unwrap()
        .header(AUTHORIZATION.as_str(), "Bearer header-secret")
        .unwrap()
        .header("x-private", "private-header")
        .unwrap()
        .body("request-body-secret");
    let request_debug = format!("{request:?}");
    assert!(request_debug.contains("method: POST"));
    assert!(request_debug.contains("has_query: true"));
    assert!(request_debug.contains("header_count: 2"));
    assert!(request_debug.contains("has_authorization: true"));
    assert!(!request_debug.contains("/profile"));
    assert!(!request_debug.contains("query-secret"));
    assert!(!request_debug.contains("header-secret"));
    assert!(!request_debug.contains("private-header"));
    assert!(!request_debug.contains("request-body-secret"));

    let app = App::new().get("/debug", || async {
        let mut response = response(
            StatusCode::CREATED,
            full_body(Bytes::from_static(b"response-body-secret")),
        );
        response
            .headers_mut()
            .insert(SET_COOKIE, "session=response-secret".parse().unwrap());
        response
            .headers_mut()
            .insert("x-private", "private-header".parse().unwrap());
        response
    });
    let response = TestApp::new(app)
        .get("/debug")
        .unwrap()
        .send()
        .await
        .unwrap();
    let response_debug = format!("{response:?}");
    assert!(response_debug.contains("status: 201"));
    assert!(response_debug.contains("header_count: 2"));
    assert!(response_debug.contains("has_set_cookie: true"));
    assert!(!response_debug.contains("response-secret"));
    assert!(!response_debug.contains("private-header"));
    assert!(!response_debug.contains("response-body-secret"));
}
