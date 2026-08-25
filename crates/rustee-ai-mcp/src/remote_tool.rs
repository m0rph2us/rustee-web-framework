//! Reviewed remote-tool discovery records and approval-gated `ToolExecutor` adapters.

use std::fmt;

use futures_util::future::BoxFuture;
use rustee_ai::{ToolDefinition, ToolExecutionContext, ToolExecutionError, ToolExecutor, ToolRisk};
use serde_json::Value;

use super::McpHttpClient;

const MAX_TOOL_NAME_BYTES: usize = 128;

/// A discovered MCP tool that has not yet been advertised or approved for execution.
#[derive(Clone, Eq, PartialEq)]
pub struct McpToolDefinition {
    definition: ToolDefinition,
    description: Option<String>,
}

impl McpToolDefinition {
    pub(super) fn from_wire(value: &Value) -> Result<Self, McpToolDefinitionError> {
        let name = value
            .get("name")
            .and_then(Value::as_str)
            .ok_or(McpToolDefinitionError::InvalidName)?;
        if name.len() > MAX_TOOL_NAME_BYTES {
            return Err(McpToolDefinitionError::NameTooLong);
        }
        let input_schema = value
            .get("inputSchema")
            .filter(|schema| schema.is_object())
            .ok_or(McpToolDefinitionError::InvalidInputSchema)?
            .clone();
        let definition = ToolDefinition::new(name, input_schema)
            .map_err(|_| McpToolDefinitionError::InvalidName)?;
        let description = match value.get("description") {
            None | Some(Value::Null) => None,
            Some(Value::String(description)) => Some(description.clone()),
            Some(_) => return Err(McpToolDefinitionError::InvalidDescription),
        };
        Ok(Self {
            definition,
            description,
        })
    }

    /// Returns the provider-visible tool declaration selected from remote discovery.
    #[must_use]
    pub const fn definition(&self) -> &ToolDefinition {
        &self.definition
    }

    pub(crate) fn into_definition(self) -> ToolDefinition {
        self.definition
    }

    /// Returns untrusted remote documentation for application review only.
    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Returns the stable MCP tool name.
    #[must_use]
    pub fn name(&self) -> &str {
        self.definition.name()
    }
}

impl fmt::Debug for McpToolDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpToolDefinition")
            .field("name", &self.name())
            .field("input_schema", &"[REMOTE UNTRUSTED]")
            .field(
                "description_length",
                &self.description.as_ref().map(String::len),
            )
            .finish_non_exhaustive()
    }
}

/// Invalid bounded MCP tool discovery data.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum McpToolDefinitionError {
    /// The MCP tool name did not meet the portable tool-name contract.
    #[error("MCP tool name was invalid")]
    InvalidName,
    /// The MCP tool name exceeded the protocol's bounded tool-name limit.
    #[error("MCP tool name exceeded the maximum length")]
    NameTooLong,
    /// The MCP tool did not publish an object JSON Schema for arguments.
    #[error("MCP tool input schema must be a JSON object")]
    InvalidInputSchema,
    /// The optional remote tool description was malformed.
    #[error("MCP tool description was invalid")]
    InvalidDescription,
}

/// A selected remote MCP tool that runs only through Rustee's approval boundary.
#[derive(Clone)]
pub struct McpRemoteTool {
    client: McpHttpClient,
    definition: ToolDefinition,
    risk: ToolRisk,
    forward_idempotency_key: bool,
}

impl McpRemoteTool {
    /// Turns one reviewed discovery result into an approval-gated Rustee tool.
    ///
    /// Discovery is intentionally one tool at a time: the application chooses a risk class for
    /// each remote capability instead of accepting remote annotations as authorization.
    #[must_use]
    pub fn from_discovery(
        client: McpHttpClient,
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

    /// Opts in to forwarding Rustee's application idempotency key in MCP `_meta`.
    ///
    /// MCP does not define universal idempotency semantics. Enable this only when the selected
    /// server documents the `io.rustee/idempotency-key` extension and protects that metadata.
    #[must_use]
    pub const fn with_rustee_idempotency_metadata(mut self) -> Self {
        self.forward_idempotency_key = true;
        self
    }
}

impl ToolExecutor for McpRemoteTool {
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
        let idempotency_key = self
            .forward_idempotency_key
            .then(|| context.idempotency_key().to_owned());
        Box::pin(async move {
            client
                .call_tool(name, arguments, idempotency_key)
                .await
                .map_err(|_| ToolExecutionError::HandlerFailed)
        })
    }
}

impl fmt::Debug for McpRemoteTool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpRemoteTool")
            .field("name", &self.definition.name())
            .field("risk", &self.risk)
            .field("forward_idempotency_key", &self.forward_idempotency_key)
            .finish_non_exhaustive()
    }
}
