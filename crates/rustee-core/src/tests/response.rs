use http::{
    StatusCode,
    header::{CONTENT_LENGTH, CONTENT_TYPE, TRANSFER_ENCODING},
};
use http_body_util::BodyExt;

use crate::*;

#[test]
fn error_response_is_json_and_sanitized() {
    let response = Error::bad_request("invalid input").into_response();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response.headers()[CONTENT_TYPE],
        "application/json; charset=utf-8"
    );
}

#[tokio::test]
async fn bounded_json_response_sets_content_type_and_rejects_oversize_values() {
    let response = json_response_bounded(
        StatusCode::CREATED,
        &serde_json::json!({"created": true}),
        32,
    )
    .expect("small JSON response is accepted");
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(
        response.headers()[CONTENT_TYPE],
        "application/json; charset=utf-8"
    );
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body is infallible")
        .to_bytes();
    assert_eq!(body.as_ref(), br#"{"created":true}"#);

    let error = json_response_bounded(
        StatusCode::OK,
        &serde_json::json!({"body": "x".repeat(64)}),
        16,
    )
    .expect_err("oversized JSON response is rejected");
    assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(error.code(), "response_too_large");
    assert_eq!(
        error.to_string(),
        "response_too_large: response body exceeds configured limit"
    );
}

#[tokio::test]
async fn bodyless_statuses_discard_response_content_and_payload_metadata() {
    for status in [
        StatusCode::CONTINUE,
        StatusCode::NO_CONTENT,
        StatusCode::RESET_CONTENT,
        StatusCode::NOT_MODIFIED,
    ] {
        let direct = response(status, full_body("private response body"));
        let direct_body = direct
            .into_body()
            .collect()
            .await
            .expect("replacement body is infallible")
            .to_bytes();
        assert!(direct_body.is_empty());

        let mut value = response(StatusCode::OK, full_body("private response body"));
        value.headers_mut().insert(
            CONTENT_LENGTH,
            "21".parse().expect("valid test content length"),
        );
        value.headers_mut().insert(
            TRANSFER_ENCODING,
            "chunked".parse().expect("valid test transfer encoding"),
        );
        let combined = (status, value).into_response();
        assert!(!combined.headers().contains_key(CONTENT_LENGTH));
        assert!(!combined.headers().contains_key(TRANSFER_ENCODING));
        let combined_body = combined
            .into_body()
            .collect()
            .await
            .expect("replacement body is infallible")
            .to_bytes();
        assert!(combined_body.is_empty());
    }
}
