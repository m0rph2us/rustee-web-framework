use super::*;
use crate::extract_bearer_token;
use http::{HeaderMap, HeaderValue, header::AUTHORIZATION};

#[derive(Clone, Copy)]
struct UnavailableAuthenticator;

impl BearerAuthenticator for UnavailableAuthenticator {
    fn authenticate(
        &self,
        _: &str,
    ) -> futures_util::future::BoxFuture<'static, Result<Principal, AuthError>> {
        Box::pin(future::ready(Err(AuthError::ProviderUnavailable)))
    }
}

#[tokio::test]
async fn bearer_layer_rejects_missing_credentials_with_a_challenge() {
    let service =
        AuthLayer::bearer(authenticator()).layer(App::new().get("/me", || async { "unexpected" }));

    let response = service.oneshot(request(None)).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers()[WWW_AUTHENTICATE], "Bearer");
}

#[tokio::test]
async fn bearer_layer_returns_a_sanitized_503_when_a_provider_is_unavailable() {
    let service = AuthLayer::bearer(UnavailableAuthenticator)
        .layer(App::new().get("/me", || async { "unexpected" }));

    let response = service.oneshot(request(Some("token"))).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(response.headers().get(WWW_AUTHENTICATE).is_none());
}

#[tokio::test]
async fn bearer_layer_rejects_an_oversized_credential_before_provider_authentication() {
    let token = "a".repeat(MAX_BEARER_TOKEN_BYTES + 1);
    let service = AuthLayer::bearer(UnavailableAuthenticator)
        .layer(App::new().get("/me", || async { "unexpected" }));

    let response = service.oneshot(request(Some(&token))).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers()[WWW_AUTHENTICATE], "Bearer");
}

#[tokio::test]
async fn bearer_layer_rejects_non_b64token_credentials_before_provider_authentication() {
    let service = AuthLayer::bearer(UnavailableAuthenticator)
        .layer(App::new().get("/me", || async { "unexpected" }));

    let response = service
        .oneshot(request(Some("opaque:token")))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers()[WWW_AUTHENTICATE], "Bearer");
}

#[tokio::test]
async fn bearer_layer_accepts_a_case_insensitive_bearer_scheme() {
    let service =
        AuthLayer::bearer(authenticator()).layer(App::new().get("/me", || async { "allowed" }));
    let mut request = request(Some("local-token"));
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_static("bearer local-token"),
    );

    let response = service.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn bearer_layer_rejects_duplicate_authorization_headers_before_provider_authentication() {
    let service = AuthLayer::bearer(UnavailableAuthenticator)
        .layer(App::new().get("/me", || async { "unexpected" }));
    let mut request = request(Some("first-token"));
    request.headers_mut().append(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer second-token"),
    );

    let response = service.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers()[WWW_AUTHENTICATE], "Bearer");
}

#[test]
fn bearer_token_extractor_enforces_one_bounded_header_value() {
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("bearer local-token"),
    );
    assert_eq!(extract_bearer_token(&headers), Ok("local-token"));

    headers.append(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer another-token"),
    );
    assert_eq!(
        extract_bearer_token(&headers),
        Err(AuthError::InvalidBearerToken)
    );

    let mut oversized = HeaderMap::new();
    oversized.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!(
            "Bearer {}",
            "a".repeat(MAX_BEARER_TOKEN_BYTES + 1)
        ))
        .unwrap(),
    );
    assert_eq!(
        extract_bearer_token(&oversized),
        Err(AuthError::InvalidBearerToken)
    );

    let mut allowed = HeaderMap::new();
    allowed.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer alpha-._~+/=="),
    );
    assert_eq!(extract_bearer_token(&allowed), Ok("alpha-._~+/=="));

    for value in [
        "Bearer opaque:token",
        "Bearer token=with-padding",
        "Bearer ===",
        "Bearer token,another",
    ] {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static(value));
        assert_eq!(
            extract_bearer_token(&headers),
            Err(AuthError::InvalidBearerToken),
            "{value}"
        );
    }
}

#[test]
fn static_authenticator_rejects_tokens_that_http_bearer_admission_cannot_use() {
    let principal = Principal::new("local-user").unwrap();
    let mut authenticator = StaticTokenAuthenticator::new();

    assert_eq!(
        authenticator.insert(" ", principal.clone()).unwrap_err(),
        StaticTokenError::BlankToken
    );
    assert_eq!(
        authenticator
            .insert("contains whitespace", principal.clone())
            .unwrap_err(),
        StaticTokenError::InvalidToken
    );
    assert_eq!(
        authenticator
            .insert("opaque:token", principal.clone())
            .unwrap_err(),
        StaticTokenError::InvalidToken
    );
    assert_eq!(
        authenticator
            .insert("a".repeat(MAX_BEARER_TOKEN_BYTES + 1), principal)
            .unwrap_err(),
        StaticTokenError::InvalidToken
    );
}

#[test]
fn static_authenticator_rejects_duplicate_tokens() {
    let mut authenticator = StaticTokenAuthenticator::new();
    authenticator
        .insert("local-token", Principal::new("first").unwrap())
        .unwrap();

    assert_eq!(
        authenticator
            .insert("local-token", Principal::new("replacement").unwrap())
            .unwrap_err(),
        StaticTokenError::DuplicateToken
    );
}
