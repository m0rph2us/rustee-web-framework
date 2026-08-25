//! OAuth protected-resource metadata and bearer authorization for Rustee MCP servers.
//!
//! This optional adapter exposes the public metadata required for an MCP HTTP resource and
//! applies a verified bearer principal to a mounted `rustee_mcp::McpServer`. Token signature,
//! issuer, and audience validation remain with a configured [`rustee_auth::BearerAuthenticator`].
//! Applications
//! must configure that verifier with the exact `resource` URI from
//! [`McpOAuthResourceServerConfig`], then keep tool, context, tenant, and approval policy in their
//! existing Rustee boundaries.

mod config;
mod server;

pub use config::{McpOAuthResourceServerConfig, McpOAuthResourceServerConfigError};
pub use server::{
    McpOAuthProtectedResourceMetadata, McpOAuthResourceServer, McpOAuthResourceServerLayer,
};

#[cfg(test)]
mod tests;
