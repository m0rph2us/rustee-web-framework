use http::{Request as HttpRequest, StatusCode, header::CONTENT_TYPE};

use crate::*;

#[derive(Debug, Eq, PartialEq, serde::Deserialize)]
struct SearchQuery {
    term: String,
    page: u16,
}

#[derive(Debug)]
struct RejectWithPrivateDetail;

impl<'de> serde::Deserialize<'de> for RejectWithPrivateDetail {
    fn deserialize<D>(_: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Err(serde::de::Error::custom("private-parser-detail"))
    }
}

#[tokio::test]
async fn query_extractor_decodes_typed_values() {
    let mut request = HttpRequest::builder()
        .uri("/?term=rustee%20framework&page=2")
        .body(empty_body())
        .expect("test request is valid");

    let query = Query::<SearchQuery>::from_request(
        &mut request,
        &RouteParams::default(),
        &StateStore::default(),
    )
    .await
    .expect("query is decoded");

    assert_eq!(
        query.0,
        SearchQuery {
            term: "rustee framework".to_owned(),
            page: 2,
        }
    );
}

#[tokio::test]
async fn json_extractor_maps_invalid_json_to_bad_request() {
    let mut request = HttpRequest::builder()
        .header(CONTENT_TYPE, "application/json")
        .body(full_body("{not-json}"))
        .expect("test request is valid");

    let error = Json::<serde_json::Value>::from_request(
        &mut request,
        &RouteParams::default(),
        &StateStore::default(),
    )
    .await
    .expect_err("invalid JSON is rejected");

    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error.code(), "bad_request");
}

#[tokio::test]
async fn json_extractor_accepts_only_standard_application_json_media_types() {
    for (content_type, accepted) in [
        ("application/json; charset=utf-8", true),
        ("APPLICATION/PROBLEM+JSON; charset=utf-8", true),
        ("text/problem+json", false),
        ("application/jsonp", false),
        ("application/+json", false),
    ] {
        let mut request = HttpRequest::builder()
            .header(CONTENT_TYPE, content_type)
            .body(full_body("{}"))
            .expect("test request is valid");
        let result = Json::<serde_json::Value>::from_request(
            &mut request,
            &RouteParams::default(),
            &StateStore::default(),
        )
        .await;

        assert_eq!(result.is_ok(), accepted, "{content_type}");
    }
}

#[tokio::test]
async fn json_extractor_rejects_duplicate_content_type_before_decoding() {
    let mut request = HttpRequest::builder()
        .header(CONTENT_TYPE, "application/json")
        .header(CONTENT_TYPE, "text/plain")
        .body(full_body("{not-json}"))
        .expect("test request is valid");

    let error = Json::<serde_json::Value>::from_request(
        &mut request,
        &RouteParams::default(),
        &StateStore::default(),
    )
    .await
    .expect_err("duplicate content types are rejected before decoding");

    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        error.to_string(),
        "bad_request: duplicate Content-Type header"
    );
}

#[tokio::test]
async fn decoder_errors_do_not_render_deserializer_details() {
    let state = StateStore::default();
    let params = RouteParams::new(vec![("value".to_owned(), "private-path".to_owned())]);

    let mut json_request = HttpRequest::builder()
        .header(CONTENT_TYPE, "application/json")
        .body(full_body("\"input\""))
        .expect("test request is valid");
    let json_error =
        Json::<RejectWithPrivateDetail>::from_request(&mut json_request, &params, &state)
            .await
            .expect_err("custom JSON decoder rejects input");

    let mut query_request = HttpRequest::builder()
        .uri("/?value=private-query")
        .body(empty_body())
        .expect("test request is valid");
    let query_error =
        Query::<RejectWithPrivateDetail>::from_request(&mut query_request, &params, &state)
            .await
            .expect_err("custom query decoder rejects input");

    let mut path_request = HttpRequest::builder()
        .body(empty_body())
        .expect("test request is valid");
    let path_error =
        Path::<RejectWithPrivateDetail>::from_request(&mut path_request, &params, &state)
            .await
            .expect_err("custom path decoder rejects input");

    for (error, expected) in [
        (json_error, "bad_request: invalid JSON body"),
        (query_error, "bad_request: invalid query string"),
        (path_error, "bad_request: invalid path parameters"),
    ] {
        assert_eq!(error.to_string(), expected);
        assert!(!error.to_string().contains("private-parser-detail"));
    }
}
