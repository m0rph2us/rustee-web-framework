//! Remote-tool discovery and execution regression coverage.

use super::*;

#[tokio::test]
async fn discovery_preserves_dotted_names_but_deny_policy_prevents_remote_call() {
    let (endpoint, server) = server(vec![
        json_reply(1, &initialize_result(), Some("session-a")),
        status_reply(202),
        json_reply(
            2,
            &json!({
                "tools":[{
                    "name":"orders.lookup.v1",
                    "description":"remote description",
                    "inputSchema":{"type":"object"}
                }]
            }),
            None,
        ),
    ])
    .await;
    let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
    client.initialize().await.unwrap();
    let discovered = client.list_tools().await.unwrap();
    assert_eq!(discovered[0].name(), "orders.lookup.v1");
    assert!(!format!("{:?}", discovered[0]).contains("remote description"));

    let mut registry = ToolRegistry::new();
    registry
        .register(McpRemoteTool::from_discovery(
            client,
            discovered.into_iter().next().unwrap(),
            ToolRisk::ReadOnly,
        ))
        .unwrap();
    let error = registry
        .execute(
            tool_context(),
            ToolCall::new("call-1", "orders.lookup.v1", json!({"id":7})).unwrap(),
            &DenyAllToolApproval,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, ToolRunError::Denied { .. }));

    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 3);
    assert!(
        requests
            .iter()
            .all(|request| !request.contains("tools/call"))
    );
    assert!(requests[1].contains("mcp-protocol-version: 2025-11-25"));
}

#[tokio::test]
async fn approved_remote_tool_uses_session_and_returns_bounded_untrusted_result() {
    let (endpoint, server) = server(vec![
        json_reply(1, &initialize_result(), Some("session-b")),
        status_reply(202),
        json_reply(
            2,
            &json!({"tools":[{"name":"orders.lookup","inputSchema":{"type":"object"}}]}),
            None,
        ),
        sse_reply(
            3,
            &json!({
                "content":[{"type":"text","text":"untrusted remote content"}],
                "structuredContent":{"status":"found"}
            }),
            &[],
        ),
    ])
    .await;
    let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
    client.initialize().await.unwrap();
    let discovered = client.list_tools().await.unwrap().remove(0);
    let mut registry = ToolRegistry::new();
    registry
        .register(
            McpRemoteTool::from_discovery(client, discovered, ToolRisk::ReadOnly)
                .with_rustee_idempotency_metadata(),
        )
        .unwrap();
    let result = registry
        .execute(
            tool_context(),
            ToolCall::new("call-2", "orders.lookup", json!({"id":7})).unwrap(),
            &Approve,
        )
        .await
        .unwrap();
    assert_eq!(
        result.content(),
        &json!({"mcp":{
            "content":[{"type":"text","text":"untrusted remote content"}],
            "structured_content":{"status":"found"}
        }})
    );

    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 4);
    assert!(requests[3].contains("\"method\":\"tools/call\""));
    assert!(requests[3].contains("\"io.rustee/idempotency-key\":\"external:order:7\""));
    assert!(requests[3].contains("mcp-session-id: session-b"));
}

#[tokio::test]
async fn remote_error_details_are_not_exposed_through_tool_execution() {
    let (endpoint, server) = server(vec![
        json_reply(1, &initialize_result(), None),
        status_reply(202),
        json_reply(
            2,
            &json!({"tools":[{"name":"orders.fail","inputSchema":{"type":"object"}}]}),
            None,
        ),
        json_reply(
            3,
            &json!({"content":[{"type":"text","text":"private server detail"}],"isError":true}),
            None,
        ),
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
            ToolCall::new("call-3", "orders.fail", json!({})).unwrap(),
            &Approve,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, ToolRunError::Execution(_)));
    assert!(!error.to_string().contains("private server detail"));
    server.await.unwrap();
}

#[tokio::test]
async fn discovery_rejects_oversized_response_before_retaining_remote_data() {
    let (endpoint, server) = server(vec![
        json_reply(
            1,
            &initialize_result(),
            None,
        ),
        status_reply(202),
        json_reply(
            2,
            &json!({
                "tools":[{
                    "name":"orders.large",
                    "description":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                    "inputSchema":{"type":"object"}
                }]
            }),
            None,
        ),
    ])
    .await;
    let config = McpHttpConfig::new(endpoint)
        .unwrap()
        .with_max_response_bytes(256)
        .unwrap();
    let client = McpHttpClient::new(config).unwrap();
    client.initialize().await.unwrap();

    assert_eq!(
        client.list_tools().await.unwrap_err(),
        McpError::ResponseTooLarge
    );
    server.await.unwrap();
}

#[tokio::test]
async fn discovery_stops_at_the_configured_pagination_bound() {
    let (endpoint, server) = server(vec![
        json_reply(1, &initialize_result(), None),
        status_reply(202),
        json_reply(
            2,
            &json!({
                "tools":[{"name":"orders.page","inputSchema":{"type":"object"}}],
                "nextCursor":"next-page"
            }),
            None,
        ),
    ])
    .await;
    let config = McpHttpConfig::new(endpoint)
        .unwrap()
        .with_tool_discovery_limits(1, 8)
        .unwrap();
    let client = McpHttpClient::new(config).unwrap();
    client.initialize().await.unwrap();

    assert_eq!(
        client.list_tools().await.unwrap_err(),
        McpError::ToolDiscoveryLimit
    );
    server.await.unwrap();
}
