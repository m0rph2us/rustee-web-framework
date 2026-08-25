//! MCP HTTP client configuration regression coverage.

use super::*;

#[test]
fn configuration_requires_secure_endpoints_and_redacts_connection_values() {
    let config = McpHttpConfig::new(url::Url::parse("https://mcp.example.test/tools").unwrap())
        .unwrap()
        .with_bearer_token("mcp-secret")
        .unwrap();
    let debug = format!("{config:?}");
    assert!(!debug.contains("mcp.example.test"));
    assert!(!debug.contains("mcp-secret"));
    assert!(debug.contains("endpoint: \"[REDACTED]\""));
    assert_eq!(
        McpHttpConfig::new(url::Url::parse("http://mcp.example.test/tools").unwrap()).unwrap_err(),
        McpHttpConfigError::InvalidEndpoint
    );
    assert_eq!(
        config.clone().with_max_response_bytes(0).unwrap_err(),
        McpHttpConfigError::ZeroResponseLimit
    );
    assert_eq!(
        config.clone().with_max_request_bytes(0).unwrap_err(),
        McpHttpConfigError::ZeroRequestLimit
    );
    assert_eq!(
        config.clone().with_context_limits(0, 1).unwrap_err(),
        McpHttpConfigError::ZeroContextLimit
    );
}

#[test]
fn configuration_rejects_malformed_or_oversized_bearer_credentials() {
    let config =
        McpHttpConfig::new(url::Url::parse("https://mcp.example.test/tools").unwrap()).unwrap();

    for token in ["token\r\n", "token\u{0000}"] {
        assert_eq!(
            config.clone().with_bearer_token(token).unwrap_err(),
            McpHttpConfigError::InvalidBearerToken
        );
    }
    assert_eq!(
        config.clone().with_bearer_token(" \t").unwrap_err(),
        McpHttpConfigError::BlankBearerToken
    );
    assert_eq!(
        config
            .clone()
            .with_bearer_token("a".repeat(MAX_HTTP_BEARER_TOKEN_BYTES + 1))
            .unwrap_err(),
        McpHttpConfigError::InvalidBearerToken
    );
    assert!(
        config
            .clone()
            .with_bearer_token("provider:opaque-token")
            .is_ok()
    );
    assert!(
        config
            .with_bearer_token(format!("{}=", "a".repeat(MAX_HTTP_BEARER_TOKEN_BYTES - 1)))
            .is_ok()
    );
}

#[test]
fn automatic_recovery_configuration_preserves_operation_specific_errors() {
    let config =
        McpHttpConfig::new(url::Url::parse("https://mcp.example.test/tools").unwrap()).unwrap();
    let cases = [
        (
            0,
            Duration::from_millis(1),
            Duration::from_millis(1),
            McpHttpConfigError::ZeroSessionRecoveryAttempts,
            McpHttpConfigError::ZeroSseResumptionAttempts,
        ),
        (
            9,
            Duration::from_millis(1),
            Duration::from_millis(1),
            McpHttpConfigError::SessionRecoveryAttemptLimit,
            McpHttpConfigError::SseResumptionAttemptLimit,
        ),
        (
            1,
            Duration::ZERO,
            Duration::from_millis(1),
            McpHttpConfigError::ZeroSessionRecoveryBackoff,
            McpHttpConfigError::ZeroSseResumptionBackoff,
        ),
        (
            1,
            Duration::from_millis(2),
            Duration::from_millis(1),
            McpHttpConfigError::InvalidSessionRecoveryBackoff,
            McpHttpConfigError::InvalidSseResumptionBackoff,
        ),
        (
            1,
            Duration::from_millis(1),
            Duration::from_secs(31),
            McpHttpConfigError::SessionRecoveryBackoffLimit,
            McpHttpConfigError::SseResumptionBackoffLimit,
        ),
    ];

    for (max_attempts, initial_backoff, max_backoff, session_error, sse_error) in cases {
        assert_eq!(
            config
                .clone()
                .with_automatic_session_recovery(max_attempts, initial_backoff, max_backoff)
                .unwrap_err(),
            session_error
        );
        assert_eq!(
            config
                .clone()
                .with_automatic_sse_resumption(max_attempts, initial_backoff, max_backoff)
                .unwrap_err(),
            sse_error
        );
    }
}
