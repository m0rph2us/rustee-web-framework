use http::{Method, StatusCode, header::WWW_AUTHENTICATE};
use http_body_util::BodyExt;
use rustee_router::App;
use tower::ServiceExt;

use crate::McpOAuthProtectedResourceMetadata;

use super::support::{config, request};

#[tokio::test]
async fn metadata_is_public_and_only_exposes_validated_public_configuration() {
    let app = App::new().nest(
        "/.well-known/oauth-protected-resource/mcp",
        McpOAuthProtectedResourceMetadata::new(config()),
    );

    let response = app
        .oneshot(request(
            Method::GET,
            "/.well-known/oauth-protected-resource/mcp",
            None,
        ))
        .await
        .expect("metadata request must complete");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[http::header::CONTENT_TYPE],
        "application/json; charset=utf-8"
    );
    let body = response
        .into_body()
        .collect()
        .await
        .expect("metadata response body must be readable")
        .to_bytes();
    let value: serde_json::Value =
        serde_json::from_slice(&body).expect("metadata response must be JSON");
    assert_eq!(value["resource"], "https://api.example.test/mcp");
    assert_eq!(
        value["authorization_servers"],
        serde_json::json!(["https://issuer.example.test/"])
    );
    assert_eq!(
        value["scopes_supported"],
        serde_json::json!(["mcp:resources", "mcp:tools"])
    );
    assert_eq!(
        value["bearer_methods_supported"],
        serde_json::json!(["header"])
    );
}

#[tokio::test]
async fn metadata_rejects_non_get_requests_without_a_challenge() {
    let response = McpOAuthProtectedResourceMetadata::new(config())
        .oneshot(request(Method::POST, "/", None))
        .await
        .expect("metadata request must complete");
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(response.headers()[http::header::ALLOW], "GET");
    assert!(response.headers().get(WWW_AUTHENTICATE).is_none());
}
