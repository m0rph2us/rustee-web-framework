use std::time::{Duration, SystemTime};

use rustee_ai_mcp::McpHttpConfig;
use url::Url;

use crate::{
    HttpMcpOAuthDiscovery, McpOAuthAccessToken, McpOAuthAuthorizationCallback,
    McpOAuthAuthorizationRedirect, McpOAuthAuthorizationServerMetadata, McpOAuthClientConfig,
    McpOAuthError, McpOAuthPendingAuthorization, McpOAuthTokenExchangeRequest,
};

use super::token_set;

#[test]
fn access_token_is_redacted_resource_bound_and_expiry_aware() {
    let resource = Url::parse("https://mcp.example.test/mcp").unwrap();
    let token = McpOAuthAccessToken::new(
        "mcp-access-token",
        Some(SystemTime::UNIX_EPOCH + Duration::from_secs(10)),
    )
    .unwrap();
    assert!(!format!("{token:?}").contains("mcp-access-token"));
    assert!(token.is_expired_at(SystemTime::UNIX_EPOCH + Duration::from_secs(11)));

    let config = McpHttpConfig::new(resource.clone()).unwrap();
    let config = token.apply_to_http_config(config, &resource).unwrap();
    assert!(!format!("{config:?}").contains("mcp-access-token"));

    let opaque_token = McpOAuthAccessToken::new("provider:opaque-token", None).unwrap();
    assert!(
        opaque_token
            .apply_to_http_config(McpHttpConfig::new(resource.clone()).unwrap(), &resource)
            .is_ok()
    );

    let other = Url::parse("https://other.example.test/mcp").unwrap();
    assert_eq!(
        token
            .apply_to_http_config(McpHttpConfig::new(other.clone()).unwrap(), &resource)
            .unwrap_err(),
        McpOAuthError::ResourceMismatch
    );
}

#[test]
fn access_token_rejects_values_that_cannot_form_an_http_bearer_header() {
    assert_eq!(
        McpOAuthAccessToken::new("provider-token\r\n", None),
        Err(McpOAuthError::InvalidToken)
    );
}

#[test]
fn oauth_debug_output_redacts_capabilities_and_connection_identifiers() {
    let resource =
        Url::parse("https://private-mcp.example.test/mcp").expect("test resource must parse");
    let redirect_uri = Url::parse("https://private-app.example.test/callback")
        .expect("test redirect URI must parse");
    let token_endpoint = Url::parse("https://private-auth.example.test/token")
        .expect("test token endpoint must parse");
    let config =
        McpOAuthClientConfig::new(resource.clone(), "private-client-id", redirect_uri.clone())
            .expect("test configuration must be valid")
            .with_scope("private:scope")
            .expect("test scope must be valid");
    let provider = McpOAuthAuthorizationServerMetadata::new(
        Url::parse("https://private-auth.example.test/issuer").expect("test issuer must parse"),
        Url::parse("https://private-auth.example.test/authorize")
            .expect("test authorization endpoint must parse"),
        token_endpoint.clone(),
    )
    .expect("test provider must be valid")
    .with_revocation_endpoint(
        Url::parse("https://private-auth.example.test/revoke")
            .expect("test revocation endpoint must parse"),
    )
    .expect("test revocation endpoint must be valid");
    let discovery = HttpMcpOAuthDiscovery::new(&config).expect("test discovery must build");
    let pending = McpOAuthPendingAuthorization {
        state: "private-state".to_owned(),
        code_verifier: "private-verifier".to_owned(),
        token_endpoint: token_endpoint.clone(),
        resource: resource.clone(),
        expires_at_unix_seconds: 123,
    };
    let redirect = McpOAuthAuthorizationRedirect {
        location: Url::parse(
            "https://private-auth.example.test/authorize?state=private-state&code_challenge=private-verifier",
        )
        .expect("test authorization URL must parse"),
    };
    let callback = McpOAuthAuthorizationCallback {
        code: Some("private-code".to_owned()),
        state: Some("private-state".to_owned()),
        error: Some("private-provider-error".to_owned()),
        error_description: Some("private-provider-diagnostic".to_owned()),
    };
    let exchange = McpOAuthTokenExchangeRequest {
        client_id: "private-client-id".to_owned(),
        code: "private-code".to_owned(),
        redirect_uri,
        code_verifier: "private-verifier".to_owned(),
        resource: resource.clone(),
    };
    let token_set = token_set(
        resource,
        "private-access-token",
        Some("private-refresh-token".to_owned()),
    )
    .expect("test token set must be valid");
    let token_secrets = token_set.clone().into_secrets();
    let refresh = token_set
        .refresh_request("private-client-id")
        .expect("test refresh request must be available");
    let revocation = token_set.revocation_request("private-client-id");
    let diagnostics = [
        format!("{config:?}"),
        format!("{provider:?}"),
        format!("{discovery:?}"),
        format!("{pending:?}"),
        format!("{redirect:?}"),
        format!("{callback:?}"),
        format!("{exchange:?}"),
        format!("{token_set:?}"),
        format!("{token_secrets:?}"),
        format!("{refresh:?}"),
        format!("{revocation:?}"),
    ];

    for diagnostic in diagnostics {
        for sensitive in [
            "private-mcp.example.test",
            "private-app.example.test",
            "private-auth.example.test",
            "private-client-id",
            "private:scope",
            "private-state",
            "private-code",
            "private-provider-error",
            "private-provider-diagnostic",
            "private-verifier",
            "private-access-token",
            "private-refresh-token",
        ] {
            assert!(
                !diagnostic.contains(sensitive),
                "diagnostic leaked {sensitive}: {diagnostic}"
            );
        }
    }
}
