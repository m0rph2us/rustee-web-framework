//! Application-owned authorization boundary for MCP tool visibility and execution.

use std::{collections::BTreeSet, error::Error as StdError};

use rustee_ai::ToolExecutionContext;
use rustee_core::Request;

/// Application authorization boundary for MCP tool discovery and execution.
///
/// Authentication middleware should place verified identity in the request extensions before this
/// policy runs. The policy must return an application-created [`ToolExecutionContext`]; it must
/// not derive its idempotency key from the untrusted JSON-RPC request ID or tool arguments.
pub trait McpToolAccessPolicy: Clone + Send + Sync + 'static {
    /// Application policy failure.
    type Error: StdError + Send + Sync + 'static;

    /// Returns the registered tool names visible to this authenticated request.
    ///
    /// Returning a name not registered in the server is harmless: it is filtered before any MCP
    /// response or execution. Returning an empty set is a normal deny-all result.
    ///
    /// # Errors
    ///
    /// Returns the application policy failure when authenticated tool visibility cannot be
    /// resolved.
    fn permitted_tools(&self, request: &Request) -> Result<BTreeSet<String>, Self::Error>;

    /// Creates trusted execution metadata after a tool is visible and requested.
    ///
    /// The returned idempotency key is application-owned. It should be stable only for retries of
    /// the same authorized semantic action, never for arbitrary client-selected JSON-RPC IDs.
    ///
    /// # Errors
    ///
    /// Returns the application policy failure when trusted execution context cannot be created.
    fn execution_context(
        &self,
        request: &Request,
        tool_name: &str,
    ) -> Result<ToolExecutionContext, Self::Error>;
}

/// Fail-closed MCP access policy useful as an explicit development default.
#[derive(Clone, Copy, Debug, Default)]
pub struct DenyAllMcpToolAccess;

/// Unreachable execution-context request under [`DenyAllMcpToolAccess`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("MCP tool execution is not permitted")]
pub struct DenyAllMcpToolAccessError;

impl McpToolAccessPolicy for DenyAllMcpToolAccess {
    type Error = DenyAllMcpToolAccessError;

    fn permitted_tools(&self, _: &Request) -> Result<BTreeSet<String>, Self::Error> {
        Ok(BTreeSet::new())
    }

    fn execution_context(&self, _: &Request, _: &str) -> Result<ToolExecutionContext, Self::Error> {
        Err(DenyAllMcpToolAccessError)
    }
}
