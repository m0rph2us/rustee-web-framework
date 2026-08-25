use http::{HeaderValue, Method, Request as HttpRequest, StatusCode, header::CONTENT_TYPE};
use http_body_util::BodyExt;
use rustee_core::{FromHeader, Header, Json, Path, Query, State, empty_body, full_body};
use serde::{Deserialize, Serialize};

use crate::App;

use super::request;

#[derive(Deserialize)]
struct UserPath {
    id: u64,
}

#[derive(Deserialize)]
struct FilePath {
    name: String,
}

#[derive(Serialize)]
struct User {
    id: u64,
}

#[derive(Deserialize)]
struct CreateUser {
    name: String,
}

#[derive(Serialize)]
struct CreatedUser {
    name: String,
}

#[derive(Deserialize)]
struct GreetingQuery {
    name: String,
}

struct GreetingState {
    prefix: String,
}

struct RequestId(String);

impl FromHeader for RequestId {
    const NAME: &'static str = "x-request-id";

    fn from_header(value: &HeaderValue) -> rustee_core::Result<Self> {
        value
            .to_str()
            .map(|value| Self(value.to_owned()))
            .map_err(|_| rustee_core::Error::bad_request("invalid x-request-id header"))
    }
}

#[tokio::test]
async fn path_extractor_deserializes_named_parameters() {
    let app = App::new().get("/users/:id", |Path(path): Path<UserPath>| async move {
        Json(User { id: path.id })
    });

    let response = app.call(request(Method::GET, "/users/42")).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        r#"{"id":42}"#
    );
}

#[tokio::test]
async fn path_extractor_decodes_percent_encoded_segments_without_form_semantics() {
    let app = App::new().get("/files/:name", |Path(path): Path<FilePath>| async move {
        path.name
    });

    let response = app
        .call(request(Method::GET, "/files/Ada%20Lovelace+Co%2B"))
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "Ada Lovelace+Co+"
    );
}

#[tokio::test]
async fn path_extractor_rejects_malformed_or_non_utf8_percent_encoded_segments() {
    let app = App::new().get("/files/:name", |Path(path): Path<FilePath>| async move {
        path.name
    });

    for path in ["/files/bad%2", "/files/bad%XZ", "/files/%FF"] {
        let response = app.call(request(Method::GET, path)).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
    }
}

#[tokio::test]
async fn json_extractor_requires_json_and_returns_typed_response() {
    let app = App::new().post("/users", |Json(user): Json<CreateUser>| async move {
        (StatusCode::CREATED, Json(CreatedUser { name: user.name }))
    });
    let request = HttpRequest::builder()
        .method(Method::POST)
        .uri("/users")
        .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
        .body(full_body(r#"{"name":"Ada"}"#))
        .unwrap();

    let response = app.call(request).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        r#"{"name":"Ada"}"#
    );
}

#[tokio::test]
async fn json_extractor_rejects_missing_content_type() {
    let app = App::new().post("/users", |_user: Json<CreateUser>| async { "created" });
    let request = HttpRequest::builder()
        .method(Method::POST)
        .uri("/users")
        .body(full_body(r#"{"name":"Ada"}"#))
        .unwrap();

    assert_eq!(
        app.call(request).await.status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
}

#[tokio::test]
async fn query_and_typed_state_are_available_to_handlers() {
    let app = App::new()
        .get(
            "/greeting",
            |State(state): State<GreetingState>, Query(query): Query<GreetingQuery>| async move {
                format!("{}, {}", state.prefix, query.name)
            },
        )
        .with_state(GreetingState {
            prefix: String::from("Hello"),
        });

    let response = app
        .call(request(Method::GET, "/greeting?name=Rustee"))
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "Hello, Rustee"
    );
}

#[tokio::test]
async fn typed_header_extractor_uses_the_declared_header_name() {
    let app = App::new().get("/request-id", |Header(id): Header<RequestId>| async move {
        id.0
    });
    let request = HttpRequest::builder()
        .method(Method::GET)
        .uri("/request-id")
        .header("x-request-id", "request-123")
        .body(empty_body())
        .unwrap();

    let response = app.call(request).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "request-123"
    );
}

#[tokio::test]
async fn typed_header_extractor_rejects_duplicate_values_before_parsing() {
    let app = App::new().get("/request-id", |Header(id): Header<RequestId>| async move {
        id.0
    });
    let request = HttpRequest::builder()
        .method(Method::GET)
        .uri("/request-id")
        .header("x-request-id", "first")
        .header("x-request-id", "second")
        .body(empty_body())
        .expect("test request must be valid");

    let response = app.call(request).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        r#"{"error":{"code":"bad_request","message":"duplicate x-request-id header"}}"#
    );
}
