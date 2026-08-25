//! Bounded Model Context Protocol (MCP) remote-tool integration for Rustee AI.
//!
//! This crate implements bounded MCP Streamable HTTP JSON/POST-response SSE and local stdio
//! paths. It explicitly initializes one trusted endpoint or application-trusted subprocess,
//! discovers a bounded set of tools, and turns each selected discovery record into a
//! [`rustee_ai::ToolExecutor`]. It also exposes capability-gated, read-only resources and
//! prompts as typed untrusted data; it never adds them to an AI request. A remote `tools/call`
//! is still impossible until Rustee's application-owned approval policy admits the
//! model-requested call.
//!
//! Opt-in SSE resumption uses only `GET` plus a received `Last-Event-ID`; it never replays the
//! originating POST or tool call. An explicitly opened standalone GET stream yields untrusted
//! server notifications only; server-initiated requests remain unsupported. Session-expiry
//! responses and a failed stdio connection clear local state but never replay a tool call.
//! MCP tool descriptions, schemas, annotations, arguments, and results are remote input:
//! applications must assign risk deliberately and validate, redact, and bound results before a
//! later provider request.

mod client;
mod context;
mod http_config;
mod http_context;
mod http_response;
mod http_transport;
mod protocol;
mod recovery;
mod remote_tool;
mod sse;
mod stdio;

pub use client::{MCP_PROTOCOL_VERSION, McpHttpClient};
pub use context::{
    McpPrompt, McpPromptArgument, McpPromptContent, McpPromptMessage, McpPromptResult,
    McpPromptRole, McpResource, McpResourceContents, McpResourceData, McpResourceLink,
    McpResourceTemplate,
};
pub use http_config::{
    MAX_HTTP_BEARER_TOKEN_BYTES, McpHttpConfig, McpHttpConfigError, is_valid_http_bearer_value,
};
pub use protocol::McpError;
pub use remote_tool::{McpRemoteTool, McpToolDefinition, McpToolDefinitionError};
pub use sse::{McpServerEventStream, McpServerNotification};
pub use stdio::{
    MAX_STDIO_ARGUMENT_BYTES, MAX_STDIO_ARGUMENT_COUNT, McpStdioClient, McpStdioConfig,
    McpStdioConfigError, McpStdioRemoteTool,
};

/// Runs the internal SSE frame parser against one bounded fuzz input.
///
/// This feature-gated harness entry point exists only for the workspace fuzz target. It does not
/// expose parsed remote data or form part of the default MCP integration API.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub fn fuzz_parse_sse_frame(frame: &[u8]) {
    let _ = sse::parse_sse_frame(frame);
}

/// Runs the bounded stdio JSON-RPC message decoder against one fuzz input.
///
/// This feature-gated harness entry point exists only for the workspace fuzz target. It does not
/// spawn a subprocess or form part of the default MCP integration API.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub fn fuzz_parse_stdio_message(line: &[u8]) {
    stdio::fuzz_decode_stdio_message(line);
}

use client::{MAX_SSE_EVENT_ID_BYTES, MAX_SSE_NOTIFICATION_METHOD_BYTES, McpSession};
#[cfg(test)]
use sse::{sse_payload, take_sse_frame};

#[cfg(test)]
mod tests;
