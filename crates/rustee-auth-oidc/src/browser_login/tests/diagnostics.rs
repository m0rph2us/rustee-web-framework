//! Browser-login configuration and redacted diagnostic regression coverage.

use super::*;

#[tokio::test]
async fn login_errors_render_sanitized_http_responses() {
    let rejected = OidcLoginError::ProviderRejected.into_response();
    let unavailable = OidcLoginError::TokenExchangeUnavailable.into_response();

    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &rejected.into_body().collect().await.unwrap().to_bytes(),
        )
        .unwrap(),
        serde_json::json!({
            "error": {
                "code": "oidc_login_rejected",
                "message": "OIDC login request was rejected",
            }
        })
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &unavailable.into_body().collect().await.unwrap().to_bytes(),
        )
        .unwrap(),
        serde_json::json!({
            "error": {
                "code": "oidc_unavailable",
                "message": "OIDC login service is unavailable",
            }
        })
    );
}

#[test]
fn config_rejects_insecure_urls_and_invalid_scopes() {
    let insecure = OidcBrowserConfig::new(
        Url::parse("http://issuer.example.test").expect("URL must parse"),
        CLIENT_ID,
        Url::parse(REDIRECT_URI).expect("URL must parse"),
        Url::parse(JWKS_URL).expect("URL must parse"),
        OidcClientAuthentication::None,
    );
    let invalid_scope = config().with_scope("profile email");

    assert_eq!(
        insecure.unwrap_err(),
        OidcBrowserConfigError::InvalidIssuerUrl
    );
    assert_eq!(
        invalid_scope.unwrap_err(),
        OidcBrowserConfigError::InvalidScope
    );
}

#[test]
fn browser_login_debug_redacts_configuration_and_callback_values() {
    let config = OidcBrowserConfig::new(
        Url::parse("https://private-browser-issuer.example.test")
            .expect("test issuer URL must parse"),
        "private-browser-client-id",
        Url::parse("https://private-browser-callback.example.test/auth/callback")
            .expect("test redirect URL must parse"),
        Url::parse("https://private-jwks.example.test/keys").expect("test URL must parse"),
        OidcClientAuthentication::None,
    )
    .expect("test configuration must be valid")
    .with_scope("private-browser-scope")
    .expect("test scope must be valid");
    let provider = OidcProviderMetadata {
        issuer: "https://private-provider-issuer.example.test".to_owned(),
        authorization_endpoint: Url::parse("https://private-authorize.example.test/authorize")
            .expect("test URL must parse"),
        token_endpoint: Url::parse("https://private-token.example.test/token")
            .expect("test URL must parse"),
        jwks_url: Url::parse("https://private-discovered-jwks.example.test/keys")
            .expect("test URL must parse"),
    };
    let pending = super::PendingAuthorization {
        state: "private-state".to_owned(),
        nonce: "private-nonce".to_owned(),
        code_verifier: "private-code-verifier".to_owned(),
        token_endpoint: Url::parse("https://private-pending-token.example.test/token")
            .expect("test URL must parse"),
        expires_at_unix_seconds: super::unix_seconds() + 60,
    };
    let redirect = super::AuthorizationRedirect::new(
        Url::parse(
            "https://private-redirect.example.test/authorize?state=private-state&nonce=private-nonce",
        )
        .expect("test URL must parse"),
    )
    .expect("test redirect must be header-safe");
    let exchange = OidcTokenExchangeRequest {
        client_id: "private-token-client-id".to_owned(),
        authentication: OidcClientAuthentication::None,
        code: "private-code".to_owned(),
        redirect_uri: Url::parse("https://private-callback.example.test/auth/callback")
            .expect("test URL must parse"),
        code_verifier: "private-code-verifier".to_owned(),
    };
    let callback = AuthorizationCallback {
        code: Some("private-callback-code".to_owned()),
        state: Some("private-callback-state".to_owned()),
        error: Some("private-provider-error".to_owned()),
        error_description: Some("private-provider-error-description".to_owned()),
    };

    let debug = format!("{config:?}{provider:?}{pending:?}{redirect:?}{exchange:?}{callback:?}");
    for value in [
        "private-browser-issuer.example.test",
        "private-browser-client-id",
        "private-browser-callback.example.test",
        "private-jwks.example.test",
        "private-browser-scope",
        "private-provider-issuer.example.test",
        "private-authorize.example.test",
        "private-token.example.test",
        "private-discovered-jwks.example.test",
        "private-pending-token.example.test",
        "private-redirect.example.test",
        "private-callback.example.test",
        "private-state",
        "private-nonce",
        "private-code",
        "private-code-verifier",
        "private-token-client-id",
        "private-callback-code",
        "private-callback-state",
        "private-provider-error",
        "private-provider-error-description",
    ] {
        assert!(
            !debug.contains(value),
            "Debug output must not include {value:?}"
        );
    }
}
