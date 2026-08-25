//! Approval-gated MCP tool serving for Rustee applications.
//!
//! [`McpServer`] is a Tower service intended for `App::nest("/mcp", server)`. It implements the
//! JSON-response portion of MCP Streamable HTTP for `initialize`, `notifications/initialized`,
//! tool discovery/execution, and optional application-owned read-only resources and prompts. The
//! application owns tool exposure, authenticated execution context, and approval policy; remote
//! request IDs, tool metadata, and arguments never create authorization or idempotency by
//! themselves.

mod access;
mod context;
mod header;
mod rpc;
mod server;
mod server_config;

pub use access::{DenyAllMcpToolAccess, DenyAllMcpToolAccessError, McpToolAccessPolicy};
pub use context::{
    DenyAllMcpContextProvider, DenyAllMcpContextProviderError, McpContextCapabilities,
    McpContextProvider, McpContextValueError, McpServerPrompt, McpServerPromptArgument,
    McpServerPromptContent, McpServerPromptMessage, McpServerPromptResult, McpServerPromptRole,
    McpServerResource, McpServerResourceContents, McpServerResourceData, McpServerResourceTemplate,
};
pub use server::McpServer;
pub use server_config::{MAX_ALLOWED_ORIGINS, McpServerConfig, McpServerConfigError};

/// MCP protocol version supported by this server adapter.
pub const MCP_PROTOCOL_VERSION: &str = "2025-11-25";

/// Runs the bounded MCP server request parser against one fuzz input.
///
/// This feature-gated harness entry point exists only for the workspace fuzz target. It does not
/// dispatch an HTTP request, execute a tool, or form part of the default server API.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub fn fuzz_parse_request(input: &[u8]) {
    rpc::fuzz_parse_request(input);
}

#[cfg(test)]
mod tests;
