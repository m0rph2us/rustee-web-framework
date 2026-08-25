use url::Url;

use crate::{McpOAuthResourceServerConfig, McpOAuthResourceServerConfigError};

#[test]
fn configuration_rejects_unsafe_urls_and_scope_header_injection() {
    let error = McpOAuthResourceServerConfig::new(
        Url::parse("http://api.example.test/mcp").expect("test resource URL must parse"),
        Url::parse("https://api.example.test/.well-known/oauth-protected-resource/mcp")
            .expect("test metadata URL must parse"),
        [Url::parse("https://issuer.example.test").expect("test issuer URL must parse")],
    )
    .expect_err("insecure resource URL must be rejected");
    assert_eq!(error, McpOAuthResourceServerConfigError::InvalidResourceUrl);

    let error = McpOAuthResourceServerConfig::new(
        Url::parse("https://api.example.test/mcp").expect("test resource URL must parse"),
        Url::parse("https://api.example.test/.well-known/oauth-protected-resource/mcp")
            .expect("test metadata URL must parse"),
        [Url::parse("https://issuer.example.test").expect("test issuer URL must parse")],
    )
    .expect("test configuration must be valid")
    .with_required_scopes(["mcp:tools\r\nother"])
    .expect_err("unsafe scope must be rejected");
    assert_eq!(error, McpOAuthResourceServerConfigError::InvalidScope);

    for scope in ["mcp\"tools", "mcp\\tools", "mcp:\u{00e9}"] {
        let error = McpOAuthResourceServerConfig::new(
            Url::parse("https://api.example.test/mcp").expect("test resource URL must parse"),
            Url::parse("https://api.example.test/.well-known/oauth-protected-resource/mcp")
                .expect("test metadata URL must parse"),
            [Url::parse("https://issuer.example.test").expect("test issuer URL must parse")],
        )
        .expect("test configuration must be valid")
        .with_required_scopes([scope])
        .expect_err("unsafe scope must be rejected");
        assert_eq!(error, McpOAuthResourceServerConfigError::InvalidScope);
    }

    let scopes = (0..32).map(|index| format!("scope-{index:02}-{}", "x".repeat(247)));
    let error = McpOAuthResourceServerConfig::new(
        Url::parse("https://api.example.test/mcp").expect("test resource URL must parse"),
        Url::parse("https://api.example.test/.well-known/oauth-protected-resource/mcp")
            .expect("test metadata URL must parse"),
        [Url::parse("https://issuer.example.test").expect("test issuer URL must parse")],
    )
    .expect("test configuration must be valid")
    .with_required_scopes(scopes)
    .expect_err("oversized scope header must be rejected");
    assert_eq!(
        error,
        McpOAuthResourceServerConfigError::ScopeParameterTooLong
    );
}

#[test]
fn required_scope_limit_stops_before_consuming_the_remaining_iterator() {
    let scopes = (0..33)
        .map(|index| format!("scope-{index}"))
        .chain(std::iter::once_with(|| {
            panic!("scope limit must reject before reading the iterator tail")
        }));

    let error = McpOAuthResourceServerConfig::new(
        Url::parse("https://api.example.test/mcp").expect("test resource URL must parse"),
        Url::parse("https://api.example.test/.well-known/oauth-protected-resource/mcp")
            .expect("test metadata URL must parse"),
        [Url::parse("https://issuer.example.test").expect("test issuer URL must parse")],
    )
    .expect("test configuration must be valid")
    .with_required_scopes(scopes)
    .expect_err("scope count must be rejected");

    assert_eq!(
        error,
        McpOAuthResourceServerConfigError::TooManyRequiredScopes
    );
}

#[test]
fn authorization_server_limit_stops_before_consuming_the_remaining_iterator() {
    let authorization_servers = (0..9)
        .map(|port| {
            Url::parse(&format!("http://127.0.0.1:{port}")).expect("test issuer URL must parse")
        })
        .chain(std::iter::once_with(|| {
            panic!("authorization-server limit must reject before reading the iterator tail")
        }));

    let error = McpOAuthResourceServerConfig::new(
        Url::parse("https://api.example.test/mcp").expect("test resource URL must parse"),
        Url::parse("https://api.example.test/.well-known/oauth-protected-resource/mcp")
            .expect("test metadata URL must parse"),
        authorization_servers,
    )
    .expect_err("issuer count must be rejected");

    assert_eq!(
        error,
        McpOAuthResourceServerConfigError::TooManyAuthorizationServers
    );
}
