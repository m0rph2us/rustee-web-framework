use http::{Method, StatusCode, header::WWW_AUTHENTICATE};
use http_body_util::BodyExt;
use rustee_auth::{AuthUser, MAX_BEARER_TOKEN_BYTES, StaticTokenAuthenticator};
use rustee_router::App;
use tower::{Layer, ServiceExt};

use crate::McpOAuthResourceServerLayer;

use super::support::{authenticator, config, request};

#[tokio::test]
async fn layer_challenges_missing_or_invalid_bearer_tokens_with_metadata() {
    let service = McpOAuthResourceServerLayer::new(config(), authenticator())
        .layer(App::new().get("/", || async { "unexpected" }));

    let missing = service
        .clone()
        .oneshot(request(Method::POST, "/", None))
        .await
        .expect("missing-token request must complete");
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        missing.headers()[WWW_AUTHENTICATE],
        "Bearer resource_metadata=\"https://api.example.test/.well-known/oauth-protected-resource/mcp\", scope=\"mcp:resources mcp:tools\""
    );

    let invalid = service
        .oneshot(request(Method::POST, "/", Some("unknown")))
        .await
        .expect("invalid-token request must complete");
    assert_eq!(invalid.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        invalid.headers()[WWW_AUTHENTICATE],
        "Bearer resource_metadata=\"https://api.example.test/.well-known/oauth-protected-resource/mcp\", scope=\"mcp:resources mcp:tools\", error=\"invalid_token\""
    );
}

#[tokio::test]
async fn layer_rejects_authenticated_principals_missing_a_required_scope() {
    let service = McpOAuthResourceServerLayer::new(config(), authenticator())
        .layer(App::new().get("/", || async { "unexpected" }));
    let response = service
        .oneshot(request(Method::POST, "/", Some("limited-access")))
        .await
        .expect("scope-limited request must complete");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        response.headers()[WWW_AUTHENTICATE],
        "Bearer resource_metadata=\"https://api.example.test/.well-known/oauth-protected-resource/mcp\", scope=\"mcp:resources mcp:tools\", error=\"insufficient_scope\""
    );
}

#[tokio::test]
async fn layer_rejects_a_verified_principal_from_an_unadvertised_issuer() {
    let mut authenticator = StaticTokenAuthenticator::new();
    authenticator
        .insert(
            "wrong-issuer",
            rustee_auth::Principal::new("mallory")
                .expect("test principal must be valid")
                .with_issuer("https://other-issuer.example.test")
                .expect("test issuer must be valid")
                .with_scope("mcp:tools")
                .expect("test scope must be valid")
                .with_scope("mcp:resources")
                .expect("test scope must be valid"),
        )
        .expect("test token must be valid");
    let service = McpOAuthResourceServerLayer::new(config(), authenticator)
        .layer(App::new().get("/", || async { "unexpected" }));

    let response = service
        .oneshot(request(Method::GET, "/", Some("wrong-issuer")))
        .await
        .expect("wrong-issuer request must complete");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers()[WWW_AUTHENTICATE],
        "Bearer resource_metadata=\"https://api.example.test/.well-known/oauth-protected-resource/mcp\", scope=\"mcp:resources mcp:tools\", error=\"invalid_token\""
    );
}

#[tokio::test]
async fn layer_inserts_only_the_verified_principal_for_the_existing_mcp_policy() {
    let service = McpOAuthResourceServerLayer::new(config(), authenticator()).layer(
        App::new().get("/", |AuthUser(principal): AuthUser| async move {
            principal.subject().to_owned()
        }),
    );
    let response = service
        .oneshot(request(Method::GET, "/", Some("full-access")))
        .await
        .expect("authenticated request must complete");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .into_body()
            .collect()
            .await
            .expect("authenticated response body must be readable")
            .to_bytes(),
        "alice"
    );
}

#[derive(Clone, Copy)]
struct UnavailableAuthenticator;

impl rustee_auth::BearerAuthenticator for UnavailableAuthenticator {
    fn authenticate(
        &self,
        _: &str,
    ) -> futures_util::future::BoxFuture<
        'static,
        Result<rustee_auth::Principal, rustee_auth::AuthError>,
    > {
        Box::pin(async { Err(rustee_auth::AuthError::ProviderUnavailable) })
    }
}

#[tokio::test]
async fn unavailable_verifier_is_a_sanitized_503_without_an_oauth_challenge() {
    let service = McpOAuthResourceServerLayer::new(config(), UnavailableAuthenticator)
        .layer(App::new().get("/", || async { "unexpected" }));
    let response = service
        .oneshot(request(Method::POST, "/", Some("anything")))
        .await
        .expect("unavailable-verifier request must complete");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(response.headers().get(WWW_AUTHENTICATE).is_none());
}

#[tokio::test]
async fn oversized_bearer_token_is_rejected_before_the_verifier_runs() {
    let service = McpOAuthResourceServerLayer::new(config(), UnavailableAuthenticator)
        .layer(App::new().get("/", || async { "unexpected" }));
    let token = "a".repeat(MAX_BEARER_TOKEN_BYTES + 1);

    let response = service
        .oneshot(request(Method::POST, "/", Some(&token)))
        .await
        .expect("oversized-token request must complete");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers()[WWW_AUTHENTICATE],
        "Bearer resource_metadata=\"https://api.example.test/.well-known/oauth-protected-resource/mcp\", scope=\"mcp:resources mcp:tools\", error=\"invalid_token\""
    );
}
