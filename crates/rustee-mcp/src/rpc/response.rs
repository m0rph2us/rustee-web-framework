//! Bounded MCP JSON-RPC response and tool-result encoding.

use http::{HeaderValue, StatusCode, header::CONTENT_TYPE};
use rustee_core::{Response, full_body, response};
use rustee_json::to_vec_bounded;
use serde_json::{Value, json};

pub(crate) const MAX_PROTOCOL_ERROR_RESPONSE_BYTES: usize = 512;

pub(crate) fn tool_success(content: &Value, max_bytes: usize) -> Option<Value> {
    let text = String::from_utf8(bounded_json(content, max_bytes)?).ok()?;
    Some(json!({
        "content":[{"type":"text","text":text}],
        "structuredContent":content,
    }))
}

pub(crate) fn tool_failure() -> Value {
    json!({
        "content":[{"type":"text","text":"tool execution failed"}],
        "isError":true,
    })
}

fn bounded_json(value: &Value, max_bytes: usize) -> Option<Vec<u8>> {
    to_vec_bounded(value, max_bytes).ok()
}

fn json_bytes(encoded: Vec<u8>) -> Response {
    let mut response = response(StatusCode::OK, full_body(encoded));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    response
}

/// Encodes one successful JSON-RPC response within the configured success-response limit.
pub(crate) fn rpc_result_response(id: &Value, result: &Value, max_bytes: usize) -> Response {
    let value = json!({"jsonrpc":"2.0","id":id,"result":result});
    let Some(encoded) = bounded_json(&value, max_bytes) else {
        return response_limit_error();
    };
    json_bytes(encoded)
}

/// Encodes a separately bounded fixed JSON-RPC protocol error.
///
/// Protocol errors remain available even when an application configures a success-response limit
/// too small for a structured result. The echoed ID was admitted by `RpcRequest::parse`.
pub(crate) fn rpc_error_response(id: &Value, code: i64, message: &'static str) -> Response {
    fixed_json_response(&json!({
        "jsonrpc":"2.0",
        "id":id,
        "error":{"code":code,"message":message}
    }))
}

/// Produces the fixed fallback used when a successful JSON-RPC result exceeds its byte limit.
pub(crate) fn response_limit_error() -> Response {
    rpc_error_response(
        &Value::Null,
        -32000,
        "response body exceeds configured limit",
    )
}

fn fixed_json_response(value: &Value) -> Response {
    let Some(encoded) = bounded_json(value, MAX_PROTOCOL_ERROR_RESPONSE_BYTES) else {
        return response(StatusCode::INTERNAL_SERVER_ERROR, rustee_core::empty_body());
    };
    json_bytes(encoded)
}
