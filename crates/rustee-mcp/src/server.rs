//! HTTP/MCP dispatch with approval-gated tool execution.

mod context;

use std::{
    convert::Infallible,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::Poll,
};

use futures_util::future::BoxFuture;
use http::{HeaderValue, Method, StatusCode, header::ALLOW};
use rustee_ai::{
    ToolApprovalPolicy, ToolCall, ToolExecutionAuditSink, ToolExecutionContext, ToolRegistry,
};
use rustee_core::{IntoResponse, Request, Response, response};
use serde_json::{Value, json};
use tower::Service;

use super::{
    DenyAllMcpContextProvider, McpContextProvider, McpServerConfig, McpToolAccessPolicy,
    rpc::{
        RequestBodyError, RpcRequest, collect_limited, is_json_request, parse_tool_call,
        response_limit_error as rpc_response_limit_error, rpc_error_response, rpc_result_response,
        tool_failure, tool_success, valid_protocol_header,
    },
};

enum PreparedToolCall {
    InvalidParameters,
    Failed,
    Ready(ToolExecutionContext, ToolCall),
}

/// Stateless MCP JSON-response server with mandatory tool approval and terminal execution audit.
#[derive(Clone)]
pub struct McpServer<Access, Approval, Audit, Context = DenyAllMcpContextProvider> {
    pub(super) config: McpServerConfig,
    registry: Arc<ToolRegistry>,
    access: Access,
    approval: Approval,
    audit: Audit,
    context: Context,
    next_call_id: Arc<AtomicU64>,
}

impl<Access, Approval, Audit> McpServer<Access, Approval, Audit>
where
    Access: McpToolAccessPolicy,
    Approval: ToolApprovalPolicy,
    Audit: ToolExecutionAuditSink,
{
    /// Creates a mountable server from application-owned tool visibility, approval, and audit.
    ///
    /// Every `tools/call` uses [`ToolRegistry::execute_with_execution_audit`]. An audit write is
    /// therefore mandatory before the handler starts and a failed terminal audit remains an
    /// explicit reconciliation condition instead of a claimed rollback.
    #[must_use]
    pub fn new(
        config: McpServerConfig,
        registry: ToolRegistry,
        access: Access,
        approval: Approval,
        audit: Audit,
    ) -> Self {
        Self {
            config,
            registry: Arc::new(registry),
            access,
            approval,
            audit,
            context: DenyAllMcpContextProvider,
            next_call_id: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl<Access, Approval, Audit, Context> McpServer<Access, Approval, Audit, Context>
where
    Access: McpToolAccessPolicy,
    Approval: ToolApprovalPolicy,
    Audit: ToolExecutionAuditSink,
    Context: McpContextProvider,
{
    /// Replaces the default fail-closed context provider with an application-owned provider.
    ///
    /// Context access is read-only but still authorization-sensitive. The replacement provider is
    /// passed each authenticated request and must enforce its own data visibility policy.
    #[must_use]
    pub fn with_context_provider<NewContext>(
        self,
        context: NewContext,
    ) -> McpServer<Access, Approval, Audit, NewContext>
    where
        NewContext: McpContextProvider,
    {
        McpServer {
            config: self.config,
            registry: self.registry,
            access: self.access,
            approval: self.approval,
            audit: self.audit,
            context,
            next_call_id: self.next_call_id,
        }
    }

    /// Handles one prefix-stripped HTTP request.
    ///
    /// Mount the service at an exact prefix such as <code>/mcp</code>. Only `POST /` is accepted;
    /// this first adapter deliberately does not offer Streamable HTTP SSE `GET` responses or
    /// session management.
    pub async fn handle(&self, mut request: Request) -> Response {
        if !self.config.allows_origin(&request) {
            return rustee_core::Error::new(
                StatusCode::FORBIDDEN,
                "forbidden",
                "MCP origin is not permitted",
            )
            .into_response();
        }
        if request.uri().path() != "/" {
            return rustee_core::Error::not_found("the requested MCP endpoint was not found")
                .into_response();
        }
        if request.method() != Method::POST {
            let mut response = response(StatusCode::METHOD_NOT_ALLOWED, rustee_core::empty_body());
            response
                .headers_mut()
                .insert(ALLOW, HeaderValue::from_static("POST"));
            return response;
        }
        if !is_json_request(&request) {
            return rustee_core::Error::unsupported_media_type(
                "expected an application/json content type",
            )
            .into_response();
        }
        let body = match collect_limited(&mut request, self.config.max_request_bytes).await {
            Ok(body) => body,
            Err(RequestBodyError::TooLarge) => {
                return rustee_core::Error::new(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "payload_too_large",
                    "request body exceeds the configured limit",
                )
                .into_response();
            }
            Err(RequestBodyError::Read) => {
                return rustee_core::Error::bad_request("request body could not be read")
                    .into_response();
            }
        };
        let Ok(value) = serde_json::from_slice::<Value>(&body) else {
            return Self::rpc_error(&Value::Null, -32700, "parse error");
        };
        let Ok(rpc) = RpcRequest::parse(&value) else {
            return Self::rpc_error(&Value::Null, -32600, "invalid request");
        };
        let Some(id) = rpc.id else {
            return Self::handle_notification(&rpc);
        };

        if rpc.method != "initialize" && !valid_protocol_header(&request) {
            return Self::rpc_error(&id, -32600, "missing or unsupported MCP protocol version");
        }
        match rpc.method.as_str() {
            "initialize" => self.initialize(&id, &rpc.params),
            "tools/list" => self.list_tools(&id, &request, &rpc.params),
            "resources/list" => self.list_resources(&id, &request, &rpc.params),
            "resources/templates/list" => self.list_resource_templates(&id, &request, &rpc.params),
            "resources/read" => self.read_resource(&id, &request, &rpc.params),
            "prompts/list" => self.list_prompts(&id, &request, &rpc.params),
            "prompts/get" => self.get_prompt(&id, &request, &rpc.params),
            "tools/call" => match self.prepare_tool_call(&request, &rpc.params) {
                PreparedToolCall::InvalidParameters => {
                    Self::rpc_error(&id, -32602, "invalid tools/call parameters")
                }
                PreparedToolCall::Failed => self.rpc_result(&id, &tool_failure()),
                PreparedToolCall::Ready(context, call) => {
                    self.execute_tool(&id, context, call).await
                }
            },
            _ => Self::rpc_error(&id, -32601, "method not found"),
        }
    }

    fn handle_notification(rpc: &RpcRequest) -> Response {
        if rpc.method == "notifications/initialized" {
            return response(StatusCode::ACCEPTED, rustee_core::empty_body());
        }
        response(StatusCode::ACCEPTED, rustee_core::empty_body())
    }

    fn list_tools(&self, id: &Value, request: &Request, params: &Value) -> Response {
        if !params.is_object() || params.get("cursor").is_some() {
            return Self::rpc_error(id, -32602, "invalid tools/list parameters");
        }
        let Ok(permitted) = self.access.permitted_tools(request) else {
            return Self::rpc_error(id, -32000, "tool access policy failed");
        };
        let definitions = self.registry.definitions();
        let mut tools = Vec::with_capacity(definitions.len().min(self.config.max_tool_items));
        for definition in definitions {
            if !permitted.contains(definition.name()) {
                continue;
            }
            if tools.len() == self.config.max_tool_items {
                return Self::rpc_error(id, -32000, "tool list exceeds configured limit");
            }
            tools.push(json!({
                "name": definition.name(),
                "inputSchema": definition.input_schema(),
            }));
        }
        self.rpc_result(id, &json!({"tools":tools}))
    }

    fn prepare_tool_call(&self, request: &Request, params: &Value) -> PreparedToolCall {
        let Some((name, arguments)) = parse_tool_call(params) else {
            return PreparedToolCall::InvalidParameters;
        };
        let call_id = format!(
            "mcp-server-{}",
            self.next_call_id.fetch_add(1, Ordering::Relaxed) + 1
        );
        let Ok(call) = ToolCall::new(call_id, name, arguments) else {
            return PreparedToolCall::InvalidParameters;
        };
        let Ok(permitted) = self.access.permitted_tools(request) else {
            return PreparedToolCall::Failed;
        };
        if !permitted.contains(call.name()) {
            return PreparedToolCall::Failed;
        }
        let Ok(context) = self.access.execution_context(request, call.name()) else {
            return PreparedToolCall::Failed;
        };
        PreparedToolCall::Ready(context, call)
    }

    async fn execute_tool(
        &self,
        id: &Value,
        context: ToolExecutionContext,
        call: ToolCall,
    ) -> Response {
        let result = self
            .registry
            .execute_with_execution_audit(context, call, &self.approval, &self.audit)
            .await;
        match result {
            Ok(result) => match tool_success(result.content(), self.config.max_response_bytes) {
                Some(result) => self.rpc_result(id, &result),
                None => Self::response_limit_error(),
            },
            Err(_) => self.rpc_result(id, &tool_failure()),
        }
    }

    fn rpc_result(&self, id: &Value, result: &Value) -> Response {
        rpc_result_response(id, result, self.config.max_response_bytes)
    }

    fn rpc_error(id: &Value, code: i64, message: &'static str) -> Response {
        rpc_error_response(id, code, message)
    }

    fn response_limit_error() -> Response {
        rpc_response_limit_error()
    }
}

impl<Access, Approval, Audit, Context> fmt::Debug for McpServer<Access, Approval, Audit, Context> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServer")
            .field("config", &self.config)
            .field("registry", &self.registry)
            .field("access", &"[APPLICATION POLICY]")
            .field("approval", &"[APPLICATION POLICY]")
            .field("audit", &"[APPLICATION AUDIT]")
            .field("context", &"[APPLICATION CONTEXT PROVIDER]")
            .finish_non_exhaustive()
    }
}

impl<Access, Approval, Audit, Context> Service<Request>
    for McpServer<Access, Approval, Audit, Context>
where
    Access: McpToolAccessPolicy,
    Approval: ToolApprovalPolicy,
    Audit: ToolExecutionAuditSink,
    Context: McpContextProvider,
{
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let server = self.clone();
        Box::pin(async move { Ok(server.handle(request).await) })
    }
}
