mod context;
mod protocol;
mod support;
mod tools;

use super::{MAX_ALLOWED_ORIGINS, McpServerConfig, McpServerConfigError};

#[test]
fn configuration_validates_public_metadata_and_bounds() {
    let config = McpServerConfig::new("rustee-mcp", "0.1.0").unwrap();
    assert_eq!(
        config.clone().with_max_request_bytes(0).unwrap_err(),
        McpServerConfigError::ZeroRequestLimit
    );
    assert_eq!(
        config.clone().with_max_context_items(0).unwrap_err(),
        McpServerConfigError::ZeroContextItemLimit
    );
    assert_eq!(
        config.clone().with_max_tool_items(0).unwrap_err(),
        McpServerConfigError::ZeroToolItemLimit
    );
    assert_eq!(
        McpServerConfig::new(" ", "0.1.0").unwrap_err(),
        McpServerConfigError::InvalidServerInfo
    );
    assert_eq!(
        config
            .clone()
            .with_allowed_origins(["https://console.example/paths-are-not-origins"])
            .unwrap_err(),
        McpServerConfigError::InvalidAllowedOrigin
    );
    assert!(
        config
            .with_allowed_origins(["https://CONSOLE.example:443", "http://localhost:3000"])
            .is_ok()
    );
    assert_eq!(
        McpServerConfig::new("rustee-mcp", "0.1.0")
            .unwrap()
            .with_allowed_origins(
                (0..=MAX_ALLOWED_ORIGINS).map(|index| format!("https://tenant-{index}.example")),
            )
            .unwrap_err(),
        McpServerConfigError::AllowedOriginLimit
    );
}

#[test]
fn origin_configuration_deduplicates_canonical_values_and_redacts_debug_output() {
    let config = McpServerConfig::new("rustee-mcp", "0.1.0")
        .unwrap()
        .with_allowed_origins([
            "https://CONSOLE.example:443",
            "https://console.example",
            "https://admin.example",
        ])
        .unwrap();

    let debug = format!("{config:?}");
    assert!(debug.contains("allowed_origin_count: 2"));
    assert!(!debug.contains("console.example"));
    assert!(!debug.contains("admin.example"));
}
