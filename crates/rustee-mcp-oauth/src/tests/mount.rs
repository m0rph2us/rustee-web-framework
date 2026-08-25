use http::StatusCode;
use http_body_util::BodyExt;
use rustee_ai::{DenyAllToolApproval, ToolRegistry};
use rustee_mcp::{DenyAllMcpToolAccess, McpServer, McpServerConfig};
use rustee_router::App;
use tower::{Layer, ServiceExt};

use crate::McpOAuthResourceServerLayer;

use super::support::{NoopAudit, authenticator, config, mcp_initialize_request};

#[tokio::test]
async fn layer_composes_with_a_mounted_mcp_server_without_bypassing_the_challenge() {
    let mcp = McpServer::new(
        McpServerConfig::new("protected-mcp", "0.1.0").expect("test MCP configuration is valid"),
        ToolRegistry::new(),
        DenyAllMcpToolAccess,
        DenyAllToolApproval,
        NoopAudit,
    );
    let app = App::new().nest(
        "/mcp",
        McpOAuthResourceServerLayer::new(config(), authenticator()).layer(mcp),
    );

    let missing = app
        .clone()
        .oneshot(mcp_initialize_request(None))
        .await
        .expect("missing-token MCP request must complete");
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
    assert!(
        missing.headers()[http::header::WWW_AUTHENTICATE]
            .to_str()
            .expect("challenge header is ASCII")
            .contains("resource_metadata")
    );

    let accepted = app
        .oneshot(mcp_initialize_request(Some("full-access")))
        .await
        .expect("authenticated MCP request must complete");
    assert_eq!(accepted.status(), StatusCode::OK);
    let body = accepted
        .into_body()
        .collect()
        .await
        .expect("MCP response body must be readable")
        .to_bytes();
    let value: serde_json::Value =
        serde_json::from_slice(&body).expect("MCP response must be JSON");
    assert_eq!(value["result"]["serverInfo"]["name"], "protected-mcp");
}
