//! MCP session lifecycle regression coverage.

use super::*;

#[tokio::test]
async fn initialization_rejects_missing_required_server_capabilities() {
    let (endpoint, server) = server(vec![json_reply(
        1,
        &json!({
            "protocolVersion":MCP_PROTOCOL_VERSION,
            "serverInfo":{"name":"fixture","version":"0.1.0"}
        }),
        None,
    )])
    .await;
    let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();

    assert_eq!(
        client.initialize().await.unwrap_err(),
        McpError::MalformedResponse
    );
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains("\"method\":\"initialize\""));
}

#[tokio::test]
async fn session_expiry_requires_reinitialization_without_replaying_the_request() {
    let (endpoint, server) = server(vec![
        json_reply(1, &initialize_result(), Some("session-old")),
        status_reply(202),
        not_found_reply(),
        json_reply(3, &initialize_result(), Some("session-new")),
        status_reply(202),
        json_reply(
            4,
            &json!({"tools":[{"name":"orders.recovered","inputSchema":{"type":"object"}}]}),
            None,
        ),
    ])
    .await;
    let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
    client.initialize().await.unwrap();

    assert_eq!(
        client.list_tools().await.unwrap_err(),
        McpError::SessionExpired
    );
    client.initialize().await.unwrap();
    let tools = client.list_tools().await.unwrap();
    assert_eq!(tools[0].name(), "orders.recovered");

    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 6);
    assert!(!requests[3].contains("mcp-session-id"));
    assert!(requests[4].contains("mcp-session-id: session-new"));
    assert!(requests[5].contains("mcp-session-id: session-new"));
}

#[tokio::test]
async fn automatic_session_recovery_prepares_a_new_session_without_replaying_the_request() {
    let (endpoint, server) = server(vec![
        json_reply(1, &initialize_result(), Some("session-old")),
        status_reply(202),
        not_found_reply(),
        json_reply(1, &initialize_result(), Some("session-new")),
        status_reply(202),
        json_reply(
            2,
            &json!({"tools":[{"name":"orders.recovered","inputSchema":{"type":"object"}}]}),
            None,
        ),
    ])
    .await;
    let config = McpHttpConfig::new(endpoint)
        .unwrap()
        .with_automatic_session_recovery(1, Duration::from_millis(1), Duration::from_millis(1))
        .unwrap();
    let client = McpHttpClient::new(config).unwrap();
    client.initialize().await.unwrap();

    assert_eq!(
        client.list_tools().await.unwrap_err(),
        McpError::SessionExpired
    );
    let recovered = client.list_tools().await.unwrap();
    assert_eq!(recovered[0].name(), "orders.recovered");

    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 6);
    assert!(requests[2].contains("\"method\":\"tools/list\""));
    assert!(requests[2].contains("mcp-session-id: session-old"));
    assert!(requests[3].contains("\"method\":\"initialize\""));
    assert!(!requests[3].contains("mcp-session-id"));
    assert!(requests[4].contains("\"method\":\"notifications/initialized\""));
    assert!(requests[4].contains("mcp-session-id: session-new"));
    assert!(requests[5].contains("\"method\":\"tools/list\""));
    assert!(requests[5].contains("mcp-session-id: session-new"));
}

#[tokio::test]
async fn session_expiry_never_replays_an_approved_remote_tool_call() {
    let (endpoint, server) = server(vec![
        json_reply(1, &initialize_result(), Some("session-old")),
        status_reply(202),
        json_reply(
            2,
            &json!({"tools":[{"name":"orders.expired","inputSchema":{"type":"object"}}]}),
            None,
        ),
        not_found_reply(),
    ])
    .await;
    let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
    client.initialize().await.unwrap();
    let discovered = client.list_tools().await.unwrap().remove(0);
    let mut registry = ToolRegistry::new();
    registry
        .register(McpRemoteTool::from_discovery(
            client,
            discovered,
            ToolRisk::Privileged,
        ))
        .unwrap();

    let error = registry
        .execute(
            tool_context(),
            ToolCall::new("call-expired", "orders.expired", json!({"id":7})).unwrap(),
            &Approve,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, ToolRunError::Execution(_)));

    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 4);
    assert!(requests[3].contains("\"method\":\"tools/call\""));
}

#[tokio::test]
async fn closing_a_session_handles_a_stateless_405_and_clears_local_state() {
    let (endpoint, server) = server(vec![
        json_reply(1, &initialize_result(), Some("session-close")),
        status_reply(202),
        status_reply(405),
    ])
    .await;
    let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
    client.initialize().await.unwrap();
    client.close_session().await.unwrap();
    assert_eq!(
        client.list_tools().await.unwrap_err(),
        McpError::NotInitialized
    );

    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 3);
    assert!(requests[2].starts_with("delete /mcp http/1.1\r\n"));
    assert!(requests[2].contains("mcp-protocol-version: 2025-11-25"));
    assert!(requests[2].contains("mcp-session-id: session-close"));
}

#[test]
fn malformed_mcp_errors_stay_sanitized() {
    assert_eq!(
        McpError::RemoteError.to_string(),
        "MCP endpoint rejected the JSON-RPC request"
    );
}
