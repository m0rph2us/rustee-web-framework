use std::{collections::BTreeMap, time::Duration};

use serde_json::json;

use rustee_ai::{DenyAllToolApproval, ToolCall, ToolRegistry, ToolRisk, ToolRunError};
use tokio::{net::TcpListener, sync::oneshot, time::timeout};

use super::{
    MAX_HTTP_BEARER_TOKEN_BYTES, MCP_PROTOCOL_VERSION, McpError, McpHttpClient, McpHttpConfig,
    McpHttpConfigError, McpPromptContent, McpRemoteTool, McpResourceData, sse_payload,
    take_sse_frame,
};

mod configuration;
mod context;
mod session;
mod sse;
mod support;
mod tools;
mod transport;

use support::{
    Approve, assert_context_request_sequence, context_replies, initialize_result, json_reply,
    not_found_reply, read_request, server, sse_body_reply, sse_header_reply, sse_reply,
    status_reply, tool_context,
};
