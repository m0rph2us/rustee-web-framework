//! Internal regression coverage for stdio MCP lifecycle and recovery.

use std::{
    collections::BTreeMap,
    convert::Infallible,
    fs,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rustee_ai::{
    AiExecutionContext, ToolApprovalDecision, ToolApprovalPolicy, ToolCall, ToolExecutionContext,
    ToolRegistry, ToolRisk, ToolRunError,
};
use serde_json::{Value, json};

use super::connection::{decode_stdio_message, encode_message};
use super::{
    MAX_STDIO_ARGUMENT_BYTES, MAX_STDIO_ARGUMENT_COUNT, McpStdioClient, McpStdioConfig,
    McpStdioConfigError, McpStdioRemoteTool, should_discard_connection,
};
use crate::{MCP_PROTOCOL_VERSION, McpError, McpPromptContent, McpResourceData};

#[test]
fn configuration_requires_bounded_local_commands() {
    assert_eq!(
        McpStdioConfig::new("").unwrap_err(),
        McpStdioConfigError::BlankProgram
    );
    assert_eq!(
        McpStdioConfig::new("server")
            .unwrap()
            .with_max_message_bytes(0)
            .unwrap_err(),
        McpStdioConfigError::ZeroMessageLimit
    );
    assert_eq!(
        McpStdioConfig::new("server")
            .unwrap()
            .with_context_limits(1, 0)
            .unwrap_err(),
        McpStdioConfigError::ZeroContextLimit
    );
    assert_eq!(
        McpStdioConfig::new("server")
            .unwrap()
            .with_request_timeout(Duration::ZERO)
            .unwrap_err(),
        McpStdioConfigError::ZeroRequestTimeout
    );
    assert_eq!(
        McpStdioConfig::new("server")
            .unwrap()
            .with_shutdown_timeout(Duration::ZERO)
            .unwrap_err(),
        McpStdioConfigError::ZeroShutdownTimeout
    );
    assert_eq!(
        McpStdioConfig::new("server")
            .unwrap()
            .with_automatic_restart(0, Duration::from_millis(1), Duration::from_millis(1))
            .unwrap_err(),
        McpStdioConfigError::ZeroRestartAttempts
    );
    assert_eq!(
        McpStdioConfig::new("server")
            .unwrap()
            .with_automatic_restart(1, Duration::ZERO, Duration::from_millis(1))
            .unwrap_err(),
        McpStdioConfigError::ZeroRestartBackoff
    );
    assert_eq!(
        McpStdioConfig::new("server")
            .unwrap()
            .with_automatic_restart(1, Duration::from_millis(2), Duration::from_millis(1))
            .unwrap_err(),
        McpStdioConfigError::InvalidRestartBackoff
    );
    assert_eq!(
        McpStdioConfig::new("server")
            .unwrap()
            .with_automatic_restart(9, Duration::from_millis(1), Duration::from_millis(1))
            .unwrap_err(),
        McpStdioConfigError::RestartAttemptLimit
    );
    assert_eq!(
        McpStdioConfig::new("server")
            .unwrap()
            .with_automatic_restart(1, Duration::from_secs(1), Duration::from_secs(31))
            .unwrap_err(),
        McpStdioConfigError::RestartBackoffLimit
    );
    assert_eq!(
        McpStdioConfig::new("server")
            .unwrap()
            .with_arguments((0..=MAX_STDIO_ARGUMENT_COUNT).map(|_| "argument"))
            .unwrap_err(),
        McpStdioConfigError::ArgumentCountLimit
    );
    assert_eq!(
        McpStdioConfig::new("server")
            .unwrap()
            .with_arguments(["x".repeat(MAX_STDIO_ARGUMENT_BYTES + 1)])
            .unwrap_err(),
        McpStdioConfigError::ArgumentByteLimit
    );
    assert_eq!(
        McpStdioConfig::new("server")
            .unwrap()
            .with_arguments(["private\0argument"])
            .unwrap_err(),
        McpStdioConfigError::InvalidArgument
    );
}

#[test]
fn configuration_debug_redacts_local_command_values() {
    let config = McpStdioConfig::new("/private/tools/mcp-agent")
        .unwrap()
        .with_arguments(["--credential", "private-command-argument"])
        .unwrap();

    let debug = format!("{config:?}");
    assert!(!debug.contains("/private/tools/mcp-agent"));
    assert!(!debug.contains("private-command-argument"));
    assert!(debug.contains("program: \"[REDACTED]\""));
    assert!(debug.contains("argument_count: 2"));
}

#[test]
fn outbound_message_encoding_stops_at_the_configured_limit() {
    let value = json!({"arguments":"x".repeat(256)});

    assert_eq!(
        encode_message(&value, 64).unwrap_err(),
        McpError::StdioRequestTooLarge
    );
    let encoded = encode_message(&value, 1024).unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&encoded).unwrap(), value);
}

#[test]
fn inbound_stdio_messages_separate_replies_from_notifications() {
    assert_eq!(
        decode_stdio_message(br#"{"jsonrpc":"2.0","id":7,"result":{"tools":[]}}"#, 7,).unwrap(),
        Some(json!({"tools":[]})),
    );
    assert_eq!(
        decode_stdio_message(
            br#"{"jsonrpc":"2.0","method":"notifications/progress","params":{"progress":1}}"#,
            7,
        )
        .unwrap(),
        None,
    );
    assert_eq!(
        decode_stdio_message(br#"{"jsonrpc":"2.0","id":7,"error":{}}"#, 7).unwrap_err(),
        McpError::RemoteError,
    );
    for line in [
        b"not json".as_slice(),
        br#"{"jsonrpc":"2.0","id":8,"result":{}}"#.as_slice(),
        br#"{"jsonrpc":"2.0","params":{}}"#.as_slice(),
    ] {
        assert_eq!(
            decode_stdio_message(line, 7).unwrap_err(),
            McpError::MalformedResponse,
        );
    }
}

#[test]
fn connection_discard_policy_only_marks_unusable_stdio_sessions() {
    for error in [
        McpError::Transport,
        McpError::ResponseTooLarge,
        McpError::StdioMessageLimit,
        McpError::StdioTerminated,
        McpError::StdioTimeout,
        McpError::MalformedResponse,
    ] {
        assert!(should_discard_connection::<()>(&Err(error)));
    }
    for error in [
        McpError::RemoteError,
        McpError::ToolExecutionFailed,
        McpError::UnsupportedCapability,
        McpError::InvalidContextRequest,
        McpError::ToolDiscoveryLimit,
    ] {
        assert!(!should_discard_connection::<()>(&Err(error)));
    }
}

#[cfg(unix)]
#[tokio::test]
async fn timed_out_request_discards_the_subprocess_connection() {
    let script = format!(
        "read line; printf '%s\\n' '{{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{{\"protocolVersion\":\"{MCP_PROTOCOL_VERSION}\",\"capabilities\":{{}},\"serverInfo\":{{\"name\":\"fixture\",\"version\":\"0.1.0\"}}}}}}'; read line; read line; while :; do :; done"
    );
    let client = McpStdioClient::new(
        McpStdioConfig::new("sh")
            .unwrap()
            .with_arguments(["-c", script.as_str()])
            .unwrap()
            .with_request_timeout(Duration::from_millis(10))
            .unwrap()
            .with_shutdown_timeout(Duration::from_millis(10))
            .unwrap(),
    );
    client.initialize().await.unwrap();

    assert_eq!(
        client.list_tools().await.unwrap_err(),
        McpError::StdioTimeout
    );
    assert_eq!(
        client.list_tools().await.unwrap_err(),
        McpError::NotInitialized
    );
}

#[cfg(unix)]
#[tokio::test]
async fn explicit_restart_replaces_the_stdio_process_without_replaying_a_call() {
    let script = format!(
        "read line; printf '%s\\n' '{{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{{\"protocolVersion\":\"{MCP_PROTOCOL_VERSION}\",\"capabilities\":{{}},\"serverInfo\":{{\"name\":\"fixture\",\"version\":\"0.1.0\"}}}}}}'; read line; read line; printf '%s\\n' '{{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{{\"tools\":[{{\"name\":\"orders.restarted\",\"inputSchema\":{{\"type\":\"object\"}}}}]}}}}'"
    );
    let client = McpStdioClient::new(
        McpStdioConfig::new("sh")
            .unwrap()
            .with_arguments(["-c", script.as_str()])
            .unwrap(),
    );
    client.initialize().await.unwrap();
    client.restart().await.unwrap();

    let discovered = client.list_tools().await.unwrap();
    assert_eq!(discovered[0].name(), "orders.restarted");
    client.close().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn automatic_restart_prepares_a_new_process_without_replaying_a_tool_call() {
    let state_path = std::env::temp_dir().join(format!(
        "rustee-ai-mcp-auto-restart-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let state = state_path.to_str().unwrap();
    let initialize = stdio_reply(
        1,
        &json!({
            "protocolVersion":MCP_PROTOCOL_VERSION,
            "capabilities":{},
            "serverInfo":{"name":"fixture","version":"0.1.0"}
        }),
    );
    let discovery = stdio_reply(
        2,
        &json!({"tools":[{"name":"orders.recovered","inputSchema":{"type":"object"}}]}),
    );
    let script = format!(
        "state=$0; if [ -f \"$state\" ]; then restarted=1; else : > \"$state\"; restarted=0; fi; read line; printf '%s\\n' '{initialize}'; read line; if [ \"$restarted\" -eq 0 ]; then read line; printf '%s\\n' '{discovery}'; read line; printf '%s\\n' 'tool-call' >> \"$state\"; exit 0; fi; read line; printf '%s\\n' 'second-request' >> \"$state\"; printf '%s\\n' '{discovery}'"
    );
    let client = McpStdioClient::new(
        McpStdioConfig::new("sh")
            .unwrap()
            .with_arguments(["-c", script.as_str(), state])
            .unwrap()
            .with_automatic_restart(1, Duration::from_millis(1), Duration::from_millis(1))
            .unwrap(),
    );
    client.initialize().await.unwrap();
    let tool = client.list_tools().await.unwrap().remove(0);
    let mut registry = ToolRegistry::new();
    registry
        .register(McpStdioRemoteTool::from_discovery(
            client.clone(),
            tool,
            ToolRisk::ReadOnly,
        ))
        .unwrap();

    let error = registry
        .execute(
            ToolExecutionContext::new(
                AiExecutionContext::new("tenant-a", "user-7").unwrap(),
                "automatic-restart-action",
            )
            .unwrap(),
            ToolCall::new("call-1", "orders.recovered", json!({})).unwrap(),
            &Approve,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, ToolRunError::Execution(_)));
    assert_eq!(fs::read_to_string(&state_path).unwrap(), "tool-call\n");

    let recovered = client.list_tools().await.unwrap();
    assert_eq!(recovered[0].name(), "orders.recovered");
    assert_eq!(
        fs::read_to_string(&state_path).unwrap(),
        "tool-call\nsecond-request\n"
    );
    client.close().await.unwrap();
    fs::remove_file(state_path).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn stdio_context_discovery_and_reads_stay_explicit_and_bounded() {
    let initialize = stdio_reply(
        1,
        &json!({
            "protocolVersion":MCP_PROTOCOL_VERSION,
            "capabilities":{"resources":{},"prompts":{}},
            "serverInfo":{"name":"fixture","version":"0.1.0"}
        }),
    );
    let resources = stdio_reply(
        2,
        &json!({"resources":[{
            "uri":"resource://tenant-a/customer/7",
            "name":"customer-record",
            "mimeType":"text/plain"
        }]}),
    );
    let templates = stdio_reply(
        3,
        &json!({"resourceTemplates":[{
            "uriTemplate":"resource://tenant-a/customer/{id}",
            "name":"customer-by-id"
        }]}),
    );
    let contents = stdio_reply(
        4,
        &json!({"contents":[{
            "uri":"resource://tenant-a/customer/7",
            "text":"private customer context"
        }]}),
    );
    let prompts = stdio_reply(
        5,
        &json!({"prompts":[{
            "name":"customer-summary",
            "arguments":[{"name":"customer_id","required":true}]
        }]}),
    );
    let prompt = stdio_reply(
        6,
        &json!({"messages":[
            {"role":"user","content":{"type":"text","text":"Summarize the selected customer."}},
            {"role":"assistant","content":{"type":"resource_link","uri":"resource://tenant-a/customer/7","name":"customer-record"}}
        ]}),
    );
    let script = format!(
        "read line; printf '%s\\n' '{initialize}'; read line; read line; printf '%s\\n' '{resources}'; read line; printf '%s\\n' '{templates}'; read line; printf '%s\\n' '{contents}'; read line; printf '%s\\n' '{prompts}'; read line; printf '%s\\n' '{prompt}'"
    );
    let client = McpStdioClient::new(
        McpStdioConfig::new("sh")
            .unwrap()
            .with_arguments(["-c", script.as_str()])
            .unwrap(),
    );
    client.initialize().await.unwrap();

    let resources = client.list_resources().await.unwrap();
    assert_eq!(resources[0].name(), "customer-record");
    let templates = client.list_resource_templates().await.unwrap();
    assert_eq!(templates[0].name(), "customer-by-id");
    let contents = client.read_resource(resources[0].uri()).await.unwrap();
    assert!(matches!(
        contents[0].data(),
        McpResourceData::Text(text) if text == "private customer context"
    ));
    let prompts = client.list_prompts().await.unwrap();
    assert!(prompts[0].arguments()[0].required());
    let prompt = client
        .get_prompt(
            "customer-summary",
            &BTreeMap::from([("customer_id".to_owned(), "7".to_owned())]),
        )
        .await
        .unwrap();
    assert!(matches!(
        prompt.messages()[0].content(),
        McpPromptContent::Text(text) if text == "Summarize the selected customer."
    ));
    assert!(!format!("{prompt:?}").contains("Summarize the selected customer."));
    client.close().await.unwrap();
}

fn stdio_reply(id: u64, result: &serde_json::Value) -> String {
    json!({"jsonrpc":"2.0","id":id,"result":result}).to_string()
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
