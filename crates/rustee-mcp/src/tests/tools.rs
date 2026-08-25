use std::sync::atomic::Ordering;

use serde_json::{Value, json};

use super::support::{protocol_request, response_json, server, server_with_approval};

#[tokio::test]
async fn approved_call_runs_through_terminal_audit_and_returns_structured_result() {
    let (server, calls, audit) = server(["orders.lookup"]);
    let response = server
        .handle(protocol_request(&json!({
            "jsonrpc":"2.0",
            "id":"call-1",
            "method":"tools/call",
            "params":{"name":"orders.lookup","arguments":{"id":7}}
        })))
        .await;
    let response = response_json(response).await;
    assert_eq!(response["result"]["isError"], Value::Null);
    assert_eq!(
        response["result"]["structuredContent"],
        json!({"status":"found"})
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(audit.approval_count(), 1);
    assert_eq!(audit.outcome_count(), 1);
}

#[tokio::test]
async fn denied_or_hidden_tool_never_runs_handler_and_redacts_failure() {
    let (denied_server, calls, _) = server_with_approval(["orders.lookup"], false);
    let response = denied_server
        .handle(protocol_request(&json!({
            "jsonrpc":"2.0",
            "id":3,
            "method":"tools/call",
            "params":{"name":"orders.lookup","arguments":{"id":7}}
        })))
        .await;
    let response = response_json(response).await;
    assert_eq!(response["result"]["isError"], true);
    assert!(!response.to_string().contains("handler secret"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let (hidden_server, hidden_calls, _) = server([]);
    let hidden = hidden_server
        .handle(protocol_request(&json!({
            "jsonrpc":"2.0",
            "id":4,
            "method":"tools/call",
            "params":{"name":"orders.lookup","arguments":{"id":7}}
        })))
        .await;
    let hidden = response_json(hidden).await;
    assert_eq!(hidden["result"]["isError"], true);
    assert_eq!(hidden_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn malformed_tool_names_fail_as_invalid_parameters_before_policy_evaluation() {
    let (server, calls, audit) = server(["orders.lookup"]);
    let response = server
        .handle(protocol_request(&json!({
            "jsonrpc":"2.0",
            "id":5,
            "method":"tools/call",
            "params":{"name":"orders lookup","arguments":{"id":7}}
        })))
        .await;
    let response = response_json(response).await;

    assert_eq!(response["error"]["code"], -32602);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(audit.approval_count(), 0);
    assert_eq!(audit.outcome_count(), 0);
}

#[tokio::test]
async fn handler_failure_returns_a_generic_mcp_tool_error_after_terminal_audit() {
    let (server, calls, audit) = server(["orders.lookup"]);
    let response = server
        .handle(protocol_request(&json!({
            "jsonrpc":"2.0",
            "id":13,
            "method":"tools/call",
            "params":{"name":"orders.lookup","arguments":{"id":13}}
        })))
        .await;
    let response = response_json(response).await;
    assert_eq!(response["result"]["isError"], true);
    assert!(!response.to_string().contains("handler secret"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(audit.approval_count(), 1);
    assert_eq!(audit.outcome_count(), 1);
}
