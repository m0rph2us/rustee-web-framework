//! Shared bounded JSON-RPC protocol validation and encoding for MCP transports.

use reqwest::{StatusCode, header::HeaderValue};
use rustee_json::{BoundedJsonError, to_vec_bounded};
use serde_json::{Value, json};

pub(super) const MAX_SESSION_ID_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BoundedJsonEncodingError {
    TooLarge,
    Malformed,
}

pub(super) fn encode_bounded_json(
    value: &Value,
    max_bytes: usize,
) -> Result<Vec<u8>, BoundedJsonEncodingError> {
    match to_vec_bounded(value, max_bytes) {
        Ok(encoded) => Ok(encoded),
        Err(BoundedJsonError::TooLarge) => Err(BoundedJsonEncodingError::TooLarge),
        Err(BoundedJsonError::Serialize(_)) => Err(BoundedJsonEncodingError::Malformed),
    }
}

pub(super) struct McpReply {
    pub(super) result: Value,
    pub(super) session_id: Option<McpHeaderValue>,
}

/// A bounded printable remote value that is safe to reflect in an HTTP request header.
///
/// Both MCP session IDs and SSE event IDs cross this boundary before the client retains them for
/// a later request. Retaining the parsed header value prevents each transport path from
/// independently reparsing an untrusted string.
#[derive(Clone, Eq, PartialEq)]
pub(super) struct McpHeaderValue {
    header: HeaderValue,
}

impl McpHeaderValue {
    pub(super) fn from_remote(value: &str, max_bytes: usize) -> Result<Self, McpError> {
        if !valid_printable_header_value(value, max_bytes) {
            return Err(McpError::MalformedResponse);
        }
        let header = HeaderValue::from_str(value).map_err(|_| McpError::MalformedResponse)?;
        Ok(Self { header })
    }

    pub(super) fn as_header_value(&self) -> HeaderValue {
        self.header.clone()
    }
}

/// Sanitized MCP adapter failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum McpError {
    /// The local HTTP client could not be initialized.
    #[error("MCP HTTP client could not be initialized")]
    Client,
    /// The remote endpoint did not respond within the bounded transport contract.
    #[error("MCP transport request failed")]
    Transport,
    /// The remote endpoint returned an unsuccessful HTTP status.
    #[error("MCP endpoint returned HTTP status {0}")]
    HttpStatus(StatusCode),
    /// A request response was neither JSON nor Streamable HTTP SSE.
    #[error("MCP endpoint returned an unsupported response content type")]
    UnsupportedResponseContentType,
    /// The remote body exceeded the configured in-memory response limit.
    #[error("MCP response exceeded the configured byte limit")]
    ResponseTooLarge,
    /// A local HTTP JSON-RPC request exceeded its configured byte limit.
    #[error("MCP HTTP request exceeded the configured byte limit")]
    RequestTooLarge,
    /// An SSE response ended before it returned the matching JSON-RPC result.
    #[error("MCP SSE response ended before the JSON-RPC result")]
    SseStreamTerminated,
    /// A remote SSE retry delay exceeded the caller's configured finite limit.
    #[error("MCP SSE retry delay exceeded the configured limit")]
    SseRetryLimit,
    /// A stateful endpoint returned 404 for the current MCP session.
    #[error("MCP session expired; initialize a new session before retrying")]
    SessionExpired,
    /// The configured stdio MCP server could not be started.
    #[error("MCP stdio server could not be started")]
    StdioSpawn,
    /// The stdio server closed stdout before the matching JSON-RPC result.
    #[error("MCP stdio server ended before the JSON-RPC result")]
    StdioTerminated,
    /// The stdio server sent too many messages before the matching JSON-RPC result.
    #[error("MCP stdio server exceeded the interleaved-message limit")]
    StdioMessageLimit,
    /// The stdio server did not complete one request by its configured deadline.
    #[error("MCP stdio server did not respond before the configured deadline")]
    StdioTimeout,
    /// The stdio server could not be reaped after a bounded forced termination.
    #[error("MCP stdio server did not terminate before the configured shutdown deadline")]
    StdioShutdownTimeout,
    /// A local JSON-RPC request exceeded its configured stdio byte limit.
    #[error("MCP stdio request exceeded the configured byte limit")]
    StdioRequestTooLarge,
    /// A JSON-RPC response or tool discovery record was invalid.
    #[error("MCP response violated the expected protocol contract")]
    MalformedResponse,
    /// The endpoint selected a different MCP protocol version.
    #[error("MCP endpoint did not select the supported protocol version")]
    ProtocolVersion,
    /// The endpoint returned a JSON-RPC error without exposing its remote detail.
    #[error("MCP endpoint rejected the JSON-RPC request")]
    RemoteError,
    /// A remote tool returned an error result without exposing its remote detail.
    #[error("MCP tool execution failed")]
    ToolExecutionFailed,
    /// A tool call or discovery was attempted before successful initialization.
    #[error("MCP client must be initialized before discovering or calling tools")]
    NotInitialized,
    /// Tool pagination or total discovery exceeded its configured bound.
    #[error("MCP tool discovery exceeded the configured limit")]
    ToolDiscoveryLimit,
    /// The server did not advertise the requested resources or prompts capability.
    #[error("MCP server did not advertise the requested context capability")]
    UnsupportedCapability,
    /// A local resource URI, prompt name, or prompt argument was invalid or exceeded its bound.
    #[error("MCP context request was invalid or exceeded the configured limit")]
    InvalidContextRequest,
    /// Context discovery, message count, or decoded content exceeded its configured bound.
    #[error("MCP context exceeded the configured item or content limit")]
    ContextLimit,
}

pub(crate) fn decode_rpc_result(value: &Value, id: u64) -> Result<Value, McpError> {
    if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || value.get("id").and_then(Value::as_u64) != Some(id)
    {
        return Err(McpError::MalformedResponse);
    }
    if value.get("error").is_some() {
        return Err(McpError::RemoteError);
    }
    value
        .get("result")
        .cloned()
        .ok_or(McpError::MalformedResponse)
}

pub(crate) fn decode_tool_result(value: &Value) -> Result<Value, McpError> {
    let content = value
        .get("content")
        .filter(|content| content.is_array())
        .cloned()
        .ok_or(McpError::MalformedResponse)?;
    if value.get("isError").and_then(Value::as_bool) == Some(true) {
        return Err(McpError::ToolExecutionFailed);
    }
    let mut result = serde_json::Map::new();
    result.insert("content".to_owned(), content);
    if let Some(structured_content) = value.get("structuredContent") {
        result.insert("structured_content".to_owned(), structured_content.clone());
    }
    Ok(json!({"mcp": Value::Object(result)}))
}

pub(crate) fn valid_cursor(value: &str) -> bool {
    !value.is_empty() && value.len() <= 1024 && !value.contains('\0')
}

pub(crate) fn paginated_params(cursor: Option<&str>) -> Value {
    let mut params = serde_json::Map::new();
    if let Some(cursor) = cursor {
        params.insert("cursor".to_owned(), Value::String(cursor.to_owned()));
    }
    Value::Object(params)
}

pub(crate) fn next_cursor(result: &Value) -> Result<Option<String>, McpError> {
    match result.get("nextCursor") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(cursor)) if valid_cursor(cursor) => Ok(Some(cursor.clone())),
        Some(_) => Err(McpError::MalformedResponse),
    }
}

pub(super) fn valid_printable_header_value(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value.bytes().all(|byte| matches!(byte, 0x21..=0x7e))
}

#[cfg(test)]
mod tests {
    use reqwest::header::HeaderValue;

    use super::{McpError, McpHeaderValue};

    #[test]
    fn retained_remote_header_values_are_bounded_and_safe_to_replay() {
        let value = McpHeaderValue::from_remote("session-a", 9).unwrap();
        assert_eq!(
            value.as_header_value(),
            HeaderValue::from_static("session-a")
        );

        assert!(matches!(
            McpHeaderValue::from_remote("", 9),
            Err(McpError::MalformedResponse)
        ));
        assert!(matches!(
            McpHeaderValue::from_remote("session-a", 8),
            Err(McpError::MalformedResponse)
        ));
        assert!(matches!(
            McpHeaderValue::from_remote("session\r\n", 16),
            Err(McpError::MalformedResponse)
        ));
    }
}
