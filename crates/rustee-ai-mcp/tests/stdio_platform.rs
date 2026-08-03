use std::{convert::Infallible, path::PathBuf};

use rustee_ai::{
    AiExecutionContext, ToolApprovalDecision, ToolApprovalPolicy, ToolCall, ToolExecutionContext,
    ToolRegistry, ToolRisk,
};
use rustee_ai_mcp::{McpStdioClient, McpStdioConfig, McpStdioRemoteTool};
use serde_json::json;

#[tokio::test]
async fn platform_fixture_handshakes_discovers_executes_and_closes() {
    let client = McpStdioClient::new(McpStdioConfig::new(fixture_program()).unwrap());
    client.initialize().await.unwrap();
    let discovered = client.list_tools().await.unwrap().remove(0);
    assert_eq!(discovered.name(), "orders.platform");

    let mut registry = ToolRegistry::new();
    registry
        .register(McpStdioRemoteTool::from_discovery(
            client.clone(),
            discovered,
            ToolRisk::ReadOnly,
        ))
        .unwrap();
    let result = registry
        .execute(
            ToolExecutionContext::new(
                AiExecutionContext::new("tenant-a", "user-7").unwrap(),
                "platform-stdio-action",
            )
            .unwrap(),
            ToolCall::new("call-1", "orders.platform", json!({"id":7})).unwrap(),
            &Approve,
        )
        .await
        .unwrap();
    assert_eq!(
        result.content()["mcp"]["content"][0]["text"],
        "platform fixture result"
    );
    client.close().await.unwrap();
}

fn fixture_program() -> PathBuf {
    std::env::var_os("CARGO_BIN_EXE_rustee-ai-mcp-stdio-fixture")
        .map(PathBuf::from)
        .expect("Cargo must provide the stdio fixture binary to this integration test")
}

#[derive(Clone, Copy)]
struct Approve;

impl ToolApprovalPolicy for Approve {
    type Error = Infallible;

    fn approve(
        &self,
        _: AiExecutionContext,
        _: ToolCall,
        _: ToolRisk,
    ) -> futures_util::future::BoxFuture<'static, Result<ToolApprovalDecision, Self::Error>> {
        Box::pin(futures_util::future::ready(Ok(
            ToolApprovalDecision::Approved,
        )))
    }
}
