//! MCP initialization and application-owned read-only context method handlers.

use rustee_ai::{ToolApprovalPolicy, ToolExecutionAuditSink};
use rustee_core::{Request, Response};
use serde_json::{Value, json};

use crate::{
    MCP_PROTOCOL_VERSION, McpContextProvider, McpToolAccessPolicy,
    context::ContextWireBudget,
    rpc::{parse_prompt_get, parse_resource_uri, unique_values, valid_list_params},
};

use super::McpServer;

impl<Access, Approval, Audit, Context> McpServer<Access, Approval, Audit, Context>
where
    Access: McpToolAccessPolicy,
    Approval: ToolApprovalPolicy,
    Audit: ToolExecutionAuditSink,
    Context: McpContextProvider,
{
    pub(super) fn initialize(&self, id: &Value, params: &Value) -> Response {
        if params.get("protocolVersion").and_then(Value::as_str) != Some(MCP_PROTOCOL_VERSION)
            || !params.is_object()
        {
            return Self::rpc_error(id, -32602, "unsupported protocol version");
        }
        let context_capabilities = self.context.capabilities();
        let mut capabilities = serde_json::Map::new();
        capabilities.insert("tools".to_owned(), json!({"listChanged":false}));
        if context_capabilities.resources() {
            capabilities.insert(
                "resources".to_owned(),
                json!({"subscribe":false,"listChanged":false}),
            );
        }
        if context_capabilities.prompts() {
            capabilities.insert("prompts".to_owned(), json!({"listChanged":false}));
        }
        self.rpc_result(
            id,
            &json!({
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": capabilities,
                "serverInfo": {
                    "name": self.config.server_name,
                    "version": self.config.server_version,
                },
            }),
        )
    }

    pub(super) fn list_resources(&self, id: &Value, request: &Request, params: &Value) -> Response {
        if !self.context.capabilities().resources() {
            return Self::rpc_error(id, -32601, "method not found");
        }
        if !valid_list_params(params) {
            return Self::rpc_error(id, -32602, "invalid resources/list parameters");
        }
        let Ok(resources) = self.context.list_resources(request) else {
            return Self::rpc_error(id, -32000, "context provider failed");
        };
        if resources.len() > self.config.max_context_items
            || !unique_values(resources.iter().map(|resource| resource.uri().to_string()))
        {
            return Self::rpc_error(id, -32000, "invalid context provider result");
        }
        let mut budget = ContextWireBudget::new(self.config.max_response_bytes);
        let Some(resources) = resources
            .iter()
            .map(|resource| resource.wire(&mut budget))
            .collect::<Option<Vec<_>>>()
        else {
            return Self::response_limit_error();
        };
        self.rpc_result(id, &json!({"resources":resources}))
    }

    pub(super) fn list_resource_templates(
        &self,
        id: &Value,
        request: &Request,
        params: &Value,
    ) -> Response {
        if !self.context.capabilities().resources() {
            return Self::rpc_error(id, -32601, "method not found");
        }
        if !valid_list_params(params) {
            return Self::rpc_error(id, -32602, "invalid resources/templates/list parameters");
        }
        let Ok(templates) = self.context.list_resource_templates(request) else {
            return Self::rpc_error(id, -32000, "context provider failed");
        };
        if templates.len() > self.config.max_context_items
            || !unique_values(templates.iter().map(|template| template.name().to_owned()))
        {
            return Self::rpc_error(id, -32000, "invalid context provider result");
        }
        let mut budget = ContextWireBudget::new(self.config.max_response_bytes);
        let Some(templates) = templates
            .iter()
            .map(|template| template.wire(&mut budget))
            .collect::<Option<Vec<_>>>()
        else {
            return Self::response_limit_error();
        };
        self.rpc_result(id, &json!({"resourceTemplates":templates}))
    }

    pub(super) fn read_resource(&self, id: &Value, request: &Request, params: &Value) -> Response {
        if !self.context.capabilities().resources() {
            return Self::rpc_error(id, -32601, "method not found");
        }
        let Some(uri) = parse_resource_uri(params) else {
            return Self::rpc_error(id, -32602, "invalid resources/read parameters");
        };
        let Ok(contents) = self.context.read_resource(request, &uri) else {
            return Self::rpc_error(id, -32000, "context provider failed");
        };
        if contents.is_empty()
            || contents.len() > self.config.max_context_items
            || contents.iter().any(|content| content.uri() != &uri)
        {
            return Self::rpc_error(id, -32000, "invalid context provider result");
        }
        let mut budget = ContextWireBudget::new(self.config.max_response_bytes);
        let Some(contents) = contents
            .iter()
            .map(|content| content.wire(&mut budget))
            .collect::<Option<Vec<_>>>()
        else {
            return Self::response_limit_error();
        };
        self.rpc_result(id, &json!({"contents":contents}))
    }

    pub(super) fn list_prompts(&self, id: &Value, request: &Request, params: &Value) -> Response {
        if !self.context.capabilities().prompts() {
            return Self::rpc_error(id, -32601, "method not found");
        }
        if !valid_list_params(params) {
            return Self::rpc_error(id, -32602, "invalid prompts/list parameters");
        }
        let Ok(prompts) = self.context.list_prompts(request) else {
            return Self::rpc_error(id, -32000, "context provider failed");
        };
        if prompts.len() > self.config.max_context_items
            || !unique_values(prompts.iter().map(|prompt| prompt.name().to_owned()))
        {
            return Self::rpc_error(id, -32000, "invalid context provider result");
        }
        let mut budget = ContextWireBudget::new(self.config.max_response_bytes);
        let Some(prompts) = prompts
            .iter()
            .map(|prompt| prompt.wire(&mut budget))
            .collect::<Option<Vec<_>>>()
        else {
            return Self::response_limit_error();
        };
        self.rpc_result(id, &json!({"prompts":prompts}))
    }

    pub(super) fn get_prompt(&self, id: &Value, request: &Request, params: &Value) -> Response {
        if !self.context.capabilities().prompts() {
            return Self::rpc_error(id, -32601, "method not found");
        }
        let Some((name, arguments)) = parse_prompt_get(params) else {
            return Self::rpc_error(id, -32602, "invalid prompts/get parameters");
        };
        let Ok(result) = self.context.get_prompt(request, name, &arguments) else {
            return Self::rpc_error(id, -32000, "context provider failed");
        };
        if result.message_count() > self.config.max_context_items {
            return Self::rpc_error(id, -32000, "invalid context provider result");
        }
        let mut budget = ContextWireBudget::new(self.config.max_response_bytes);
        let Some(result) = result.wire(&mut budget) else {
            return Self::response_limit_error();
        };
        self.rpc_result(id, &result)
    }
}
