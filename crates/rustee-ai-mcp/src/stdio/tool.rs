//! Approval-gated stdio MCP tool adaptation.

use std::fmt;

use futures_util::future::BoxFuture;
use rustee_ai::{ToolDefinition, ToolExecutionContext, ToolExecutionError, ToolExecutor, ToolRisk};
use serde_json::Value;

use super::McpStdioClient;
use crate::McpToolDefinition;

/// Selected stdio tool executor that stays behind Rustee approval policy.
#[derive(Clone)]
pub struct McpStdioRemoteTool {
    client: McpStdioClient,
    definition: ToolDefinition,
    risk: ToolRisk,
    forward_idempotency_key: bool,
}

impl McpStdioRemoteTool {
    /// Creates one approval-gated executor from an explicitly selected discovered stdio tool.
    ///
    /// The caller owns the risk classification and must not treat remote metadata as trusted.
    #[must_use]
    pub fn from_discovery(
        client: McpStdioClient,
        discovered: McpToolDefinition,
        risk: ToolRisk,
    ) -> Self {
        Self {
            client,
            definition: discovered.into_definition(),
            risk,
            forward_idempotency_key: false,
        }
    }

    /// Enables forwarding a stable Rustee idempotency key in MCP call metadata.
    ///
    /// Enable this only when the selected remote tool understands and safely honors that metadata.
    #[must_use]
    pub const fn with_rustee_idempotency_metadata(mut self) -> Self {
        self.forward_idempotency_key = true;
        self
    }
}

impl ToolExecutor for McpStdioRemoteTool {
    fn definition(&self) -> &ToolDefinition {
        &self.definition
    }

    fn risk(&self) -> ToolRisk {
        self.risk
    }

    fn execute(
        &self,
        context: ToolExecutionContext,
        arguments: Value,
    ) -> BoxFuture<'static, Result<Value, ToolExecutionError>> {
        let client = self.client.clone();
        let name = self.definition.name().to_owned();
        let key = self
            .forward_idempotency_key
            .then(|| context.idempotency_key().to_owned());
        Box::pin(async move {
            client
                .call_tool(name, arguments, key)
                .await
                .map_err(|_| ToolExecutionError::HandlerFailed)
        })
    }
}

impl fmt::Debug for McpStdioRemoteTool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpStdioRemoteTool")
            .field("name", &self.definition.name())
            .field("risk", &self.risk)
            .field("forward_idempotency_key", &self.forward_idempotency_key)
            .finish_non_exhaustive()
    }
}
