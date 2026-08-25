use reqwest::header::{HeaderMap, HeaderValue, WWW_AUTHENTICATE};
use url::Url;

use crate::{
    AuthorizationServerMetadataWire, MAX_WWW_AUTHENTICATE_BYTES, McpOAuthClientConfig,
    McpOAuthError, authorization_server_metadata_urls, resource_metadata_urls,
    www_authenticate_value,
};

use super::{AUTHORIZATION_ENDPOINT, CLIENT_ID, ISSUER, REDIRECT_URI, TOKEN_ENDPOINT};

#[test]
fn discovery_urls_never_exceed_the_trusted_url_budget() {
    let resource_prefix = "https://mcp.example.test/";
    let resource = Url::parse(&format!(
        "{resource_prefix}{}",
        "p".repeat(crate::config::MAX_URL_BYTES - resource_prefix.len())
    ))
    .unwrap();
    assert_eq!(resource.as_str().len(), crate::config::MAX_URL_BYTES);
    McpOAuthClientConfig::new(
        resource.clone(),
        CLIENT_ID,
        Url::parse(REDIRECT_URI).unwrap(),
    )
    .expect("resource URL at the trusted length limit must be valid");
    assert_eq!(
        resource_metadata_urls(&resource, None),
        Err(McpOAuthError::InvalidMetadata)
    );

    let issuer_prefix = "https://auth.example.test/";
    let issuer = Url::parse(&format!(
        "{issuer_prefix}{}",
        "p".repeat(crate::config::MAX_URL_BYTES - issuer_prefix.len())
    ))
    .unwrap();
    assert_eq!(
        authorization_server_metadata_urls(&issuer),
        Err(McpOAuthError::InvalidMetadata)
    );
}

#[test]
fn discovery_urls_follow_mcp_protected_resource_and_issuer_priority() {
    let resource = Url::parse("https://mcp.example.test/public/mcp").unwrap();
    let resource_urls = resource_metadata_urls(&resource, None).unwrap();
    assert_eq!(
        resource_urls.iter().map(Url::as_str).collect::<Vec<_>>(),
        vec![
            "https://mcp.example.test/.well-known/oauth-protected-resource/public/mcp",
            "https://mcp.example.test/.well-known/oauth-protected-resource",
        ]
    );
    let challenge_url = resource_metadata_urls(
        &resource,
        Some(
            "Bearer resource_metadata=\"https://mcp.example.test/metadata\", scope=\"orders:read\"",
        ),
    )
    .unwrap();
    assert_eq!(
        challenge_url[0].as_str(),
        "https://mcp.example.test/metadata"
    );

    let issuer = Url::parse("https://auth.example.test/tenant-a").unwrap();
    let issuer_urls = authorization_server_metadata_urls(&issuer).unwrap();
    assert_eq!(
        issuer_urls.iter().map(Url::as_str).collect::<Vec<_>>(),
        vec![
            "https://auth.example.test/.well-known/oauth-authorization-server/tenant-a",
            "https://auth.example.test/.well-known/openid-configuration/tenant-a",
            "https://auth.example.test/tenant-a/.well-known/openid-configuration",
        ]
    );
}

#[test]
fn bearer_challenge_reads_later_metadata_parameter_without_crossing_challenge_boundaries() {
    let resource = Url::parse("https://mcp.example.test/public/mcp").unwrap();
    let challenge_url = resource_metadata_urls(
        &resource,
        Some(
            "Basic realm=\"legacy\", Bearer realm=\"MCP, remote\", resource_metadata=\"https://mcp.example.test/metadata\", scope=\"orders:read\"",
        ),
    )
    .unwrap();
    assert_eq!(
        challenge_url[0].as_str(),
        "https://mcp.example.test/metadata"
    );

    let fallback_urls = resource_metadata_urls(
        &resource,
        Some(
            "Basic realm=\"legacy\", resource_metadata=\"https://untrusted.example.test/metadata\"",
        ),
    )
    .unwrap();
    assert_eq!(
        fallback_urls[0].as_str(),
        "https://mcp.example.test/.well-known/oauth-protected-resource/public/mcp"
    );
}

#[test]
fn bearer_challenge_rejects_ambiguous_metadata_parameters() {
    let resource = Url::parse("https://mcp.example.test/public/mcp").unwrap();

    assert_eq!(
        resource_metadata_urls(
            &resource,
            Some(
                "Bearer resource_metadata=\"https://mcp.example.test/first\", resource_metadata=\"https://mcp.example.test/second\"",
            ),
        ),
        Err(McpOAuthError::InvalidChallenge)
    );
}

#[test]
fn challenge_headers_are_combined_without_arbitrary_selection() {
    let resource = Url::parse("https://mcp.example.test/public/mcp").unwrap();
    let mut headers = HeaderMap::new();
    headers.append(
        WWW_AUTHENTICATE,
        HeaderValue::from_static("Basic realm=\"legacy\""),
    );
    headers.append(
        WWW_AUTHENTICATE,
        HeaderValue::from_static("Bearer resource_metadata=\"https://mcp.example.test/metadata\""),
    );

    let challenge = www_authenticate_value(&headers).unwrap();
    let urls = resource_metadata_urls(&resource, challenge.as_deref()).unwrap();
    assert_eq!(urls[0].as_str(), "https://mcp.example.test/metadata");

    headers.clear();
    headers.insert(
        WWW_AUTHENTICATE,
        HeaderValue::from_str(&"x".repeat(MAX_WWW_AUTHENTICATE_BYTES + 1)).unwrap(),
    );
    assert_eq!(
        www_authenticate_value(&headers),
        Err(McpOAuthError::InvalidChallenge)
    );

    headers.insert(WWW_AUTHENTICATE, HeaderValue::from_bytes(b"\x80").unwrap());
    assert_eq!(
        www_authenticate_value(&headers),
        Err(McpOAuthError::InvalidChallenge)
    );
}

#[test]
fn authorization_server_metadata_validates_optional_revocation_endpoint() {
    let issuer = Url::parse(ISSUER).unwrap();
    let wire: AuthorizationServerMetadataWire = serde_json::from_value(serde_json::json!({
        "issuer": ISSUER,
        "authorization_endpoint": AUTHORIZATION_ENDPOINT,
        "token_endpoint": TOKEN_ENDPOINT,
        "revocation_endpoint": "https://auth.example.test/revoke",
        "code_challenge_methods_supported": ["S256"],
    }))
    .unwrap();
    let metadata = wire.into_public(&issuer).unwrap();
    assert_eq!(
        metadata.revocation_endpoint().map(Url::as_str),
        Some("https://auth.example.test/revoke")
    );

    let unsafe_wire: AuthorizationServerMetadataWire = serde_json::from_value(serde_json::json!({
        "issuer": ISSUER,
        "authorization_endpoint": AUTHORIZATION_ENDPOINT,
        "token_endpoint": TOKEN_ENDPOINT,
        "revocation_endpoint": "http://auth.example.test/revoke",
        "code_challenge_methods_supported": ["S256"],
    }))
    .unwrap();
    assert_eq!(
        unsafe_wire.into_public(&issuer).unwrap_err(),
        McpOAuthError::InvalidMetadata
    );
}
