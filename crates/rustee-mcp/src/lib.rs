//! Approval-gated MCP tool serving for Rustee applications.
//!
//! [`McpServer`] is a Tower service intended for `App::nest("/mcp", server)`. It implements the
//! JSON-response portion of MCP Streamable HTTP for `initialize`, `notifications/initialized`,
//! tool discovery/execution, and optional application-owned read-only resources and prompts. The
//! application owns tool exposure, authenticated execution context, and approval policy; remote
//! request IDs, tool metadata, and arguments never create authorization or idempotency by
//! themselves.

mod context;

pub use context::{
    McpContextValueError, McpServerPrompt, McpServerPromptArgument, McpServerPromptContent,
    McpServerPromptMessage, McpServerPromptResult, McpServerPromptRole, McpServerResource,
    McpServerResourceContents, McpServerResourceData, McpServerResourceTemplate,
};

use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    error::Error as StdError,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::Poll,
};

use bytes::Bytes;
use futures_util::future::BoxFuture;
use http::{
    HeaderValue, Method, StatusCode,
    header::{ALLOW, CONTENT_TYPE, ORIGIN},
};
use http_body_util::BodyExt;
use rustee_ai::{
    ToolApprovalPolicy, ToolCall, ToolExecutionAuditSink, ToolExecutionContext, ToolRegistry,
};
use rustee_core::{IntoResponse, Request, Response, full_body, response};
use serde_json::{Value, json};
use tower::Service;
use url::Url;

/// MCP protocol version supported by this server adapter.
pub const MCP_PROTOCOL_VERSION: &str = "2025-11-25";

const DEFAULT_MAX_REQUEST_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_CONTEXT_ITEMS: usize = 64;
const MAX_CONTEXT_ARGUMENTS: usize = 64;
const MAX_CONTEXT_ARGUMENT_VALUE_BYTES: usize = 8192;

/// Optional MCP features owned by an application's read-only context provider.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct McpContextCapabilities {
    resources: bool,
    prompts: bool,
}

impl McpContextCapabilities {
    /// Enables the read-only MCP resource methods.
    #[must_use]
    pub const fn with_resources(mut self) -> Self {
        self.resources = true;
        self
    }

    /// Enables the read-only MCP prompt methods.
    #[must_use]
    pub const fn with_prompts(mut self) -> Self {
        self.prompts = true;
        self
    }

    const fn resources(self) -> bool {
        self.resources
    }

    const fn prompts(self) -> bool {
        self.prompts
    }
}

/// Application trust boundary for optional, read-only MCP resources and prompts.
///
/// This provider receives the authenticated request after origin and request-size admission. It
/// is intentionally separate from [`McpToolAccessPolicy`]: exposing context neither invokes a
/// tool nor grants a tool execution capability. Applications must perform tenant, user, and data
/// authorization here, and must return only data safe to disclose to the remote MCP client.
pub trait McpContextProvider: Clone + Send + Sync + 'static {
    /// Application provider failure.
    type Error: StdError + Send + Sync + 'static;

    /// Declares which optional method families this provider serves.
    fn capabilities(&self) -> McpContextCapabilities;

    /// Lists the visible concrete resources for this authenticated request.
    ///
    /// # Errors
    ///
    /// Returns the application provider failure when resource visibility cannot be resolved.
    fn list_resources(&self, request: &Request) -> Result<Vec<McpServerResource>, Self::Error>;

    /// Lists visible parameterized resource templates for this authenticated request.
    ///
    /// # Errors
    ///
    /// Returns the application provider failure when resource-template visibility cannot be
    /// resolved.
    fn list_resource_templates(
        &self,
        request: &Request,
    ) -> Result<Vec<McpServerResourceTemplate>, Self::Error>;

    /// Reads a resource explicitly selected by the remote client.
    ///
    /// # Errors
    ///
    /// Returns the application provider failure when the selected resource cannot be authorized
    /// or read.
    fn read_resource(
        &self,
        request: &Request,
        uri: &Url,
    ) -> Result<Vec<McpServerResourceContents>, Self::Error>;

    /// Lists visible prompt declarations for this authenticated request.
    ///
    /// # Errors
    ///
    /// Returns the application provider failure when prompt visibility cannot be resolved.
    fn list_prompts(&self, request: &Request) -> Result<Vec<McpServerPrompt>, Self::Error>;

    /// Returns an explicitly selected, application-authorized prompt result.
    ///
    /// # Errors
    ///
    /// Returns the application provider failure when the selected prompt cannot be authorized or
    /// resolved.
    fn get_prompt(
        &self,
        request: &Request,
        name: &str,
        arguments: &BTreeMap<String, String>,
    ) -> Result<McpServerPromptResult, Self::Error>;
}

/// Fail-closed default that keeps MCP resources and prompts undiscoverable.
#[derive(Clone, Copy, Debug, Default)]
pub struct DenyAllMcpContextProvider;

/// Unreachable context request under [`DenyAllMcpContextProvider`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("MCP context access is not permitted")]
pub struct DenyAllMcpContextProviderError;

impl McpContextProvider for DenyAllMcpContextProvider {
    type Error = DenyAllMcpContextProviderError;

    fn capabilities(&self) -> McpContextCapabilities {
        McpContextCapabilities::default()
    }

    fn list_resources(&self, _: &Request) -> Result<Vec<McpServerResource>, Self::Error> {
        Err(DenyAllMcpContextProviderError)
    }

    fn list_resource_templates(
        &self,
        _: &Request,
    ) -> Result<Vec<McpServerResourceTemplate>, Self::Error> {
        Err(DenyAllMcpContextProviderError)
    }

    fn read_resource(
        &self,
        _: &Request,
        _: &Url,
    ) -> Result<Vec<McpServerResourceContents>, Self::Error> {
        Err(DenyAllMcpContextProviderError)
    }

    fn list_prompts(&self, _: &Request) -> Result<Vec<McpServerPrompt>, Self::Error> {
        Err(DenyAllMcpContextProviderError)
    }

    fn get_prompt(
        &self,
        _: &Request,
        _: &str,
        _: &BTreeMap<String, String>,
    ) -> Result<McpServerPromptResult, Self::Error> {
        Err(DenyAllMcpContextProviderError)
    }
}

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

/// Public server identity, request bounds, and browser-origin admission.
#[derive(Clone, Eq, PartialEq)]
pub struct McpServerConfig {
    server_name: String,
    server_version: String,
    max_request_bytes: usize,
    max_response_bytes: usize,
    max_context_items: usize,
    allowed_origins: BTreeSet<String>,
}

impl McpServerConfig {
    /// Creates server metadata advertised in a successful MCP initialization response.
    ///
    /// # Errors
    ///
    /// Returns [`McpServerConfigError::InvalidServerInfo`] when either value is blank or contains
    /// a NUL byte.
    pub fn new(
        server_name: impl Into<String>,
        server_version: impl Into<String>,
    ) -> Result<Self, McpServerConfigError> {
        let server_name = server_name.into();
        let server_version = server_version.into();
        if invalid_server_info(&server_name) || invalid_server_info(&server_version) {
            return Err(McpServerConfigError::InvalidServerInfo);
        }
        Ok(Self {
            server_name,
            server_version,
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            max_context_items: DEFAULT_MAX_CONTEXT_ITEMS,
            allowed_origins: BTreeSet::new(),
        })
    }

    /// Sets the maximum JSON-RPC request body collected by this service.
    ///
    /// # Errors
    ///
    /// Returns [`McpServerConfigError::ZeroRequestLimit`] when `max_request_bytes` is zero.
    pub fn with_max_request_bytes(
        mut self,
        max_request_bytes: usize,
    ) -> Result<Self, McpServerConfigError> {
        if max_request_bytes == 0 {
            return Err(McpServerConfigError::ZeroRequestLimit);
        }
        self.max_request_bytes = max_request_bytes;
        Ok(self)
    }

    /// Sets the maximum successful JSON-RPC response body emitted by this service.
    ///
    /// # Errors
    ///
    /// Returns [`McpServerConfigError::ZeroResponseLimit`] when `max_response_bytes` is zero.
    pub fn with_max_response_bytes(
        mut self,
        max_response_bytes: usize,
    ) -> Result<Self, McpServerConfigError> {
        if max_response_bytes == 0 {
            return Err(McpServerConfigError::ZeroResponseLimit);
        }
        self.max_response_bytes = max_response_bytes;
        Ok(self)
    }

    /// Sets the maximum items returned by one context-provider operation.
    ///
    /// The byte size of the resulting response remains subject to `max_response_bytes`.
    ///
    /// # Errors
    ///
    /// Returns [`McpServerConfigError::ZeroContextItemLimit`] when `max_context_items` is zero.
    pub fn with_max_context_items(
        mut self,
        max_context_items: usize,
    ) -> Result<Self, McpServerConfigError> {
        if max_context_items == 0 {
            return Err(McpServerConfigError::ZeroContextItemLimit);
        }
        self.max_context_items = max_context_items;
        Ok(self)
    }

    /// Replaces the exact HTTP(S) origins accepted when an `Origin` header is present.
    ///
    /// The empty default intentionally rejects every request that carries `Origin`, while native
    /// MCP clients without that header continue through normal authentication. Origins are
    /// normalized to scheme/host/port and may not contain a path, query, fragment, or credentials.
    /// This is a DNS-rebinding defense, not an authentication or CORS policy.
    ///
    /// # Errors
    ///
    /// Returns [`McpServerConfigError::InvalidAllowedOrigin`] when any origin is not a valid
    /// absolute HTTP(S) origin.
    pub fn with_allowed_origins<Origins, Origin>(
        mut self,
        origins: Origins,
    ) -> Result<Self, McpServerConfigError>
    where
        Origins: IntoIterator<Item = Origin>,
        Origin: AsRef<str>,
    {
        let mut allowed_origins = BTreeSet::new();
        for origin in origins {
            let origin = canonical_origin(origin.as_ref())
                .ok_or(McpServerConfigError::InvalidAllowedOrigin)?;
            allowed_origins.insert(origin);
        }
        self.allowed_origins = allowed_origins;
        Ok(self)
    }

    fn allows_origin(&self, request: &Request) -> bool {
        let Some(origin) = request.headers().get(ORIGIN) else {
            return true;
        };
        origin
            .to_str()
            .ok()
            .and_then(canonical_origin)
            .is_some_and(|origin| self.allowed_origins.contains(&origin))
    }
}

impl fmt::Debug for McpServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerConfig")
            .field("server_name", &self.server_name)
            .field("server_version", &self.server_version)
            .field("max_request_bytes", &self.max_request_bytes)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("max_context_items", &self.max_context_items)
            .field("allowed_origins", &self.allowed_origins)
            .finish()
    }
}

/// Invalid public MCP server configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum McpServerConfigError {
    /// Public server name or version was blank or malformed.
    #[error("MCP server name and version must be non-blank and valid")]
    InvalidServerInfo,
    /// The request reader needs a positive byte limit.
    #[error("MCP request byte limit must be non-zero")]
    ZeroRequestLimit,
    /// The response writer needs a positive byte limit.
    #[error("MCP response byte limit must be non-zero")]
    ZeroResponseLimit,
    /// A context provider needs a positive per-operation item limit.
    #[error("MCP context item limit must be non-zero")]
    ZeroContextItemLimit,
    /// An explicit browser origin was not a canonical HTTP(S) origin.
    #[error("MCP allowed origin must be an absolute HTTP(S) origin without a path or credentials")]
    InvalidAllowedOrigin,
}

/// Stateless MCP JSON-response server with mandatory tool approval and terminal execution audit.
#[derive(Clone)]
pub struct McpServer<Access, Approval, Audit, Context = DenyAllMcpContextProvider> {
    config: McpServerConfig,
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

    fn initialize(&self, id: &Value, params: &Value) -> Response {
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

    fn list_tools(&self, id: &Value, request: &Request, params: &Value) -> Response {
        if !params.is_object() || params.get("cursor").is_some() {
            return Self::rpc_error(id, -32602, "invalid tools/list parameters");
        }
        let Ok(permitted) = self.access.permitted_tools(request) else {
            return Self::rpc_error(id, -32000, "tool access policy failed");
        };
        let tools = self
            .registry
            .definitions()
            .filter(|definition| permitted.contains(definition.name()))
            .map(|definition| {
                json!({
                    "name": definition.name(),
                    "inputSchema": definition.input_schema(),
                })
            })
            .collect::<Vec<_>>();
        self.rpc_result(id, &json!({"tools":tools}))
    }

    fn list_resources(&self, id: &Value, request: &Request, params: &Value) -> Response {
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
        self.rpc_result(
            id,
            &json!({"resources":resources.iter().map(McpServerResource::wire).collect::<Vec<_>>() }),
        )
    }

    fn list_resource_templates(&self, id: &Value, request: &Request, params: &Value) -> Response {
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
        self.rpc_result(
            id,
            &json!({"resourceTemplates":templates.iter().map(McpServerResourceTemplate::wire).collect::<Vec<_>>() }),
        )
    }

    fn read_resource(&self, id: &Value, request: &Request, params: &Value) -> Response {
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
        self.rpc_result(
            id,
            &json!({"contents":contents.iter().map(McpServerResourceContents::wire).collect::<Vec<_>>() }),
        )
    }

    fn list_prompts(&self, id: &Value, request: &Request, params: &Value) -> Response {
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
        self.rpc_result(
            id,
            &json!({"prompts":prompts.iter().map(McpServerPrompt::wire).collect::<Vec<_>>() }),
        )
    }

    fn get_prompt(&self, id: &Value, request: &Request, params: &Value) -> Response {
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
        self.rpc_result(id, &result.wire())
    }

    fn prepare_tool_call(&self, request: &Request, params: &Value) -> PreparedToolCall {
        let Some((name, arguments)) = parse_tool_call(params) else {
            return PreparedToolCall::InvalidParameters;
        };
        let Ok(permitted) = self.access.permitted_tools(request) else {
            return PreparedToolCall::Failed;
        };
        if !permitted.contains(&name) {
            return PreparedToolCall::Failed;
        }
        let Ok(context) = self.access.execution_context(request, &name) else {
            return PreparedToolCall::Failed;
        };
        let call_id = format!(
            "mcp-server-{}",
            self.next_call_id.fetch_add(1, Ordering::Relaxed) + 1
        );
        let Ok(call) = ToolCall::new(call_id, name, arguments) else {
            return PreparedToolCall::InvalidParameters;
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
            Ok(result) => self.rpc_result(id, &tool_success(result.content())),
            Err(_) => self.rpc_result(id, &tool_failure()),
        }
    }

    fn rpc_result(&self, id: &Value, result: &Value) -> Response {
        self.json_response(&json!({"jsonrpc":"2.0","id":id,"result":result}))
    }

    fn rpc_error(id: &Value, code: i64, message: &'static str) -> Response {
        Self::json_response_unbounded(&json!({
            "jsonrpc":"2.0",
            "id":id,
            "error":{"code":code,"message":message}
        }))
    }

    fn json_response(&self, value: &Value) -> Response {
        let encoded = serde_json::to_vec(value).expect("MCP JSON-RPC values are serializable");
        if encoded.len() > self.config.max_response_bytes {
            return Self::rpc_error(
                &Value::Null,
                -32000,
                "response body exceeds configured limit",
            );
        }
        json_bytes(encoded)
    }

    fn json_response_unbounded(value: &Value) -> Response {
        let encoded = serde_json::to_vec(value).expect("MCP JSON-RPC values are serializable");
        json_bytes(encoded)
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

struct RpcRequest {
    id: Option<Value>,
    method: String,
    params: Value,
}

enum PreparedToolCall {
    InvalidParameters,
    Failed,
    Ready(ToolExecutionContext, ToolCall),
}

impl RpcRequest {
    fn parse(value: &Value) -> Result<Self, ()> {
        if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            return Err(());
        }
        let method = value
            .get("method")
            .and_then(Value::as_str)
            .filter(|method| !method.is_empty())
            .ok_or(())?
            .to_owned();
        let id = match value.get("id") {
            None => None,
            Some(Value::String(_) | Value::Number(_)) => value.get("id").cloned(),
            Some(_) => return Err(()),
        };
        let params = value.get("params").cloned().unwrap_or_else(|| json!({}));
        Ok(Self { id, method, params })
    }
}

enum RequestBodyError {
    TooLarge,
    Read,
}

async fn collect_limited(request: &mut Request, limit: usize) -> Result<Bytes, RequestBodyError> {
    let mut body = Vec::new();
    while let Some(frame) = request.body_mut().frame().await {
        let frame = frame.map_err(|_| RequestBodyError::Read)?;
        if let Ok(data) = frame.into_data() {
            if body.len().saturating_add(data.len()) > limit {
                return Err(RequestBodyError::TooLarge);
            }
            body.extend_from_slice(&data);
        }
    }
    Ok(Bytes::from(body))
}

fn is_json_request(request: &Request) -> bool {
    request
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            let media_type = value.split(';').next().unwrap_or_default().trim();
            media_type.eq_ignore_ascii_case("application/json") || media_type.ends_with("+json")
        })
}

fn valid_protocol_header(request: &Request) -> bool {
    request
        .headers()
        .get("mcp-protocol-version")
        .and_then(|value| value.to_str().ok())
        == Some(MCP_PROTOCOL_VERSION)
}

fn parse_tool_call(params: &Value) -> Option<(String, Value)> {
    let name = params.get("name")?.as_str()?.to_owned();
    let arguments = match params.get("arguments") {
        None => Value::Object(serde_json::Map::default()),
        Some(arguments) if arguments.is_object() => arguments.clone(),
        Some(_) => return None,
    };
    Some((name, arguments))
}

fn valid_list_params(params: &Value) -> bool {
    params.is_object() && params.get("cursor").is_none()
}

fn parse_resource_uri(params: &Value) -> Option<Url> {
    let object = params.as_object()?;
    if object.len() != 1 {
        return None;
    }
    let uri = object.get("uri")?.as_str()?;
    if uri.is_empty() || uri.len() > 4096 || uri.chars().any(char::is_control) {
        return None;
    }
    Url::parse(uri).ok()
}

fn parse_prompt_get(params: &Value) -> Option<(&str, BTreeMap<String, String>)> {
    let object = params.as_object()?;
    let name = object.get("name")?.as_str()?;
    if name.is_empty() || name.len() > 128 || name.chars().any(char::is_control) {
        return None;
    }
    let arguments = match object.get("arguments") {
        None => BTreeMap::new(),
        Some(Value::Object(arguments)) if arguments.len() <= MAX_CONTEXT_ARGUMENTS => arguments
            .iter()
            .map(|(key, value)| {
                let value = value.as_str()?;
                (key.len() <= 128
                    && !key.is_empty()
                    && !key.chars().any(char::is_control)
                    && value.len() <= MAX_CONTEXT_ARGUMENT_VALUE_BYTES
                    && !value.contains('\0'))
                .then(|| (key.clone(), value.to_owned()))
            })
            .collect::<Option<BTreeMap<_, _>>>()?,
        Some(_) => return None,
    };
    (object.len() == 1 || (object.len() == 2 && object.contains_key("arguments")))
        .then_some((name, arguments))
}

fn unique_values<Key>(mut values: impl Iterator<Item = Key>) -> bool
where
    Key: Ord,
{
    let mut seen = BTreeSet::new();
    values.all(|value| seen.insert(value))
}

fn tool_success(content: &Value) -> Value {
    let text = serde_json::to_string(content).expect("tool result JSON is serializable");
    json!({
        "content":[{"type":"text","text":text}],
        "structuredContent":content,
    })
}

fn tool_failure() -> Value {
    json!({
        "content":[{"type":"text","text":"tool execution failed"}],
        "isError":true,
    })
}

fn json_bytes(encoded: Vec<u8>) -> Response {
    let mut response = response(StatusCode::OK, full_body(encoded));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    response
}

fn invalid_server_info(value: &str) -> bool {
    value.trim().is_empty() || value.contains('\0')
}

fn canonical_origin(value: &str) -> Option<String> {
    if value.trim() != value {
        return None;
    }
    let url = Url::parse(value).ok()?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    let origin = url.origin().ascii_serialization();
    (origin != "null").then_some(origin)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        convert::Infallible,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use http::{HeaderValue, Request as HttpRequest};
    use http_body_util::BodyExt;
    use rustee_ai::{
        AiExecutionContext, ToolApprovalAuditEvent, ToolApprovalDecision, ToolApprovalPolicy,
        ToolDefinition, ToolExecutionAuditEvent, ToolExecutionAuditSink, ToolRisk, TypedTool,
    };
    use rustee_core::full_body;
    use serde::{Deserialize, Serialize};
    use serde_json::{Value, json};
    use tower::Service;
    use url::Url;

    use super::{
        MCP_PROTOCOL_VERSION, McpContextCapabilities, McpContextProvider, McpServer,
        McpServerConfig, McpServerConfigError, McpServerPrompt, McpServerPromptArgument,
        McpServerPromptContent, McpServerPromptMessage, McpServerPromptResult, McpServerResource,
        McpServerResourceContents, McpServerResourceTemplate, McpToolAccessPolicy,
    };

    #[test]
    fn configuration_validates_public_metadata_and_bounds() {
        let config = McpServerConfig::new("rustee-mcp", "0.1.0").unwrap();
        assert_eq!(
            config.clone().with_max_request_bytes(0).unwrap_err(),
            McpServerConfigError::ZeroRequestLimit
        );
        assert_eq!(
            config.clone().with_max_context_items(0).unwrap_err(),
            McpServerConfigError::ZeroContextItemLimit
        );
        assert_eq!(
            McpServerConfig::new(" ", "0.1.0").unwrap_err(),
            McpServerConfigError::InvalidServerInfo
        );
        assert_eq!(
            config
                .clone()
                .with_allowed_origins(["https://console.example/paths-are-not-origins"])
                .unwrap_err(),
            McpServerConfigError::InvalidAllowedOrigin
        );
        assert!(
            config
                .with_allowed_origins(["https://CONSOLE.example:443", "http://localhost:3000"])
                .is_ok()
        );
    }

    #[tokio::test]
    async fn origin_header_is_fail_closed_and_requires_an_explicit_allowed_origin() {
        let (default_server, _, _) = server(["orders.lookup"]);
        let mut rejected = request(json!({
            "jsonrpc":"2.0",
            "id":1,
            "method":"initialize",
            "params":{"protocolVersion":MCP_PROTOCOL_VERSION}
        }));
        rejected.headers_mut().insert(
            http::header::ORIGIN,
            HeaderValue::from_static("https://untrusted.example"),
        );
        let rejected = default_server.handle(rejected).await;
        assert_eq!(rejected.status(), http::StatusCode::FORBIDDEN);

        let (mut allowed_server, _, _) = server(["orders.lookup"]);
        allowed_server.config = McpServerConfig::new("rustee-mcp-test", "0.1.0")
            .unwrap()
            .with_allowed_origins(["https://console.example"])
            .unwrap();
        let mut allowed = protocol_request(json!({
            "jsonrpc":"2.0","id":2,"method":"tools/list","params":{}
        }));
        allowed.headers_mut().insert(
            http::header::ORIGIN,
            HeaderValue::from_static("https://console.example"),
        );
        let allowed = allowed_server.handle(allowed).await;
        assert_eq!(allowed.status(), http::StatusCode::OK);

        let mut malformed = request(json!({
            "jsonrpc":"2.0",
            "id":3,
            "method":"initialize",
            "params":{"protocolVersion":MCP_PROTOCOL_VERSION}
        }));
        malformed.headers_mut().insert(
            http::header::ORIGIN,
            HeaderValue::from_static("not-an-origin"),
        );
        let malformed = allowed_server.handle(malformed).await;
        assert_eq!(malformed.status(), http::StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn initializes_lists_only_allowed_tools_and_accepts_notification() {
        let (server, _, _) = server(["orders.lookup"]);
        let initialize = server
            .handle(request(json!({
                "jsonrpc":"2.0",
                "id":1,
                "method":"initialize",
                "params":{"protocolVersion":MCP_PROTOCOL_VERSION}
            })))
            .await;
        let initialize = response_json(initialize).await;
        assert_eq!(
            initialize["result"]["protocolVersion"],
            MCP_PROTOCOL_VERSION
        );
        assert_eq!(
            initialize["result"]["capabilities"]["tools"]["listChanged"],
            false
        );

        let notification = server
            .handle(request(json!({
                "jsonrpc":"2.0",
                "method":"notifications/initialized"
            })))
            .await;
        assert_eq!(notification.status(), http::StatusCode::ACCEPTED);

        let listed = server
            .handle(protocol_request(json!({
                "jsonrpc":"2.0","id":2,"method":"tools/list","params":{}
            })))
            .await;
        let listed = response_json(listed).await;
        assert_eq!(listed["result"]["tools"].as_array().unwrap().len(), 1);
        assert_eq!(listed["result"]["tools"][0]["name"], "orders.lookup");
    }

    #[tokio::test]
    async fn serves_explicit_context_with_the_same_authenticated_request_boundary() {
        let (server, _, _) = server(["orders.lookup"]);
        let server = server.with_context_provider(AuthorizedContext);

        let initialize = response_json(
            server
                .handle(request(json!({
                    "jsonrpc":"2.0",
                    "id":20,
                    "method":"initialize",
                    "params":{"protocolVersion":MCP_PROTOCOL_VERSION}
                })))
                .await,
        )
        .await;
        assert_eq!(
            initialize["result"]["capabilities"]["resources"]["subscribe"],
            false
        );
        assert_eq!(
            initialize["result"]["capabilities"]["prompts"]["listChanged"],
            false
        );

        let resources = response_json(
            server
                .handle(protocol_request(json!({
                    "jsonrpc":"2.0","id":21,"method":"resources/list","params":{}
                })))
                .await,
        )
        .await;
        assert_eq!(
            resources["result"]["resources"][0]["uri"],
            "resource://tenant-a/customer/7"
        );
        assert_eq!(
            resources["result"]["resources"][0]["mimeType"],
            "application/json"
        );

        let templates = response_json(
            server
                .handle(protocol_request(json!({
                    "jsonrpc":"2.0","id":22,"method":"resources/templates/list","params":{}
                })))
                .await,
        )
        .await;
        assert_eq!(
            templates["result"]["resourceTemplates"][0]["uriTemplate"],
            "resource://tenant-a/customer/{customer_id}"
        );

        let resource = response_json(
            server
                .handle(protocol_request(json!({
                    "jsonrpc":"2.0","id":23,"method":"resources/read",
                    "params":{"uri":"resource://tenant-a/customer/7"}
                })))
                .await,
        )
        .await;
        assert_eq!(
            resource["result"]["contents"][0]["text"],
            "{\"customer_id\":\"7\"}"
        );

        let prompts = response_json(
            server
                .handle(protocol_request(json!({
                    "jsonrpc":"2.0","id":24,"method":"prompts/list","params":{}
                })))
                .await,
        )
        .await;
        assert_eq!(prompts["result"]["prompts"][0]["name"], "customer-summary");
        assert_eq!(
            prompts["result"]["prompts"][0]["arguments"][0]["required"],
            true
        );

        let prompt = response_json(
            server
                .handle(protocol_request(json!({
                    "jsonrpc":"2.0","id":25,"method":"prompts/get",
                    "params":{"name":"customer-summary","arguments":{"customer_id":"7"}}
                })))
                .await,
        )
        .await;
        assert_eq!(prompt["result"]["messages"][0]["role"], "user");
        assert_eq!(
            prompt["result"]["messages"][0]["content"]["text"],
            "Summarize customer 7."
        );
    }

    #[tokio::test]
    async fn fail_closed_context_is_not_advertised_or_invoked() {
        let (server, _, _) = server(["orders.lookup"]);
        let initialize = response_json(
            server
                .handle(request(json!({
                    "jsonrpc":"2.0","id":26,"method":"initialize",
                    "params":{"protocolVersion":MCP_PROTOCOL_VERSION}
                })))
                .await,
        )
        .await;
        assert!(
            initialize["result"]["capabilities"]
                .get("resources")
                .is_none()
        );
        assert!(
            initialize["result"]["capabilities"]
                .get("prompts")
                .is_none()
        );

        let response = response_json(
            server
                .handle(protocol_request(json!({
                    "jsonrpc":"2.0","id":27,"method":"resources/list","params":{}
                })))
                .await,
        )
        .await;
        assert_eq!(response["error"]["code"], -32601);
    }

    #[tokio::test]
    async fn rejects_unbounded_or_inconsistent_context_provider_results() {
        let (mut server, _, _) = server(["orders.lookup"]);
        server.config = McpServerConfig::new("rustee-mcp-test", "0.1.0")
            .unwrap()
            .with_max_context_items(1)
            .unwrap();
        let server = server.with_context_provider(InvalidContext);

        let list = response_json(
            server
                .handle(protocol_request(json!({
                    "jsonrpc":"2.0","id":28,"method":"resources/list","params":{}
                })))
                .await,
        )
        .await;
        assert_eq!(list["error"]["code"], -32000);

        let read = response_json(
            server
                .handle(protocol_request(json!({
                    "jsonrpc":"2.0","id":29,"method":"resources/read",
                    "params":{"uri":"resource://tenant-a/customer/7"}
                })))
                .await,
        )
        .await;
        assert_eq!(read["error"]["code"], -32000);
    }

    #[tokio::test]
    async fn approved_call_runs_through_terminal_audit_and_returns_structured_result() {
        let (server, calls, audit) = server(["orders.lookup"]);
        let response = server
            .handle(protocol_request(json!({
                "jsonrpc":"2.0",
                "id":"call-1",
                "method":"tools/call",
                "params":{"name":"orders.lookup","arguments":{"id":7}}
            })))
            .await;
        let response = response_json(response).await;
        assert_eq!(response["result"]["isError"], Value::Null);
        assert_eq!(
            response["result"]["structuredContent"],
            json!({"status":"found"})
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(audit.approvals.load(Ordering::SeqCst), 1);
        assert_eq!(audit.outcomes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn denied_or_hidden_tool_never_runs_handler_and_redacts_failure() {
        let (denied_server, calls, _) = server_with_approval(["orders.lookup"], false);
        let response = denied_server
            .handle(protocol_request(json!({
                "jsonrpc":"2.0",
                "id":3,
                "method":"tools/call",
                "params":{"name":"orders.lookup","arguments":{"id":7}}
            })))
            .await;
        let response = response_json(response).await;
        assert_eq!(response["result"]["isError"], true);
        assert!(!response.to_string().contains("handler secret"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let (hidden_server, hidden_calls, _) = server([]);
        let hidden = hidden_server
            .handle(protocol_request(json!({
                "jsonrpc":"2.0",
                "id":4,
                "method":"tools/call",
                "params":{"name":"orders.lookup","arguments":{"id":7}}
            })))
            .await;
        let hidden = response_json(hidden).await;
        assert_eq!(hidden["result"]["isError"], true);
        assert_eq!(hidden_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn handler_failure_returns_a_generic_mcp_tool_error_after_terminal_audit() {
        let (server, calls, audit) = server(["orders.lookup"]);
        let response = server
            .handle(protocol_request(json!({
                "jsonrpc":"2.0",
                "id":13,
                "method":"tools/call",
                "params":{"name":"orders.lookup","arguments":{"id":13}}
            })))
            .await;
        let response = response_json(response).await;
        assert_eq!(response["result"]["isError"], true);
        assert!(!response.to_string().contains("handler secret"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(audit.approvals.load(Ordering::SeqCst), 1);
        assert_eq!(audit.outcomes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn rejects_missing_protocol_header_and_oversized_body_without_executing() {
        let (server, calls, _) = server_configured(["orders.lookup"], 64);
        let missing_header = server
            .handle(request(json!({
                "jsonrpc":"2.0","id":5,"method":"tools/list","params":{}
            })))
            .await;
        let missing_header = response_json(missing_header).await;
        assert_eq!(missing_header["error"]["code"], -32600);

        let oversized = HttpRequest::builder()
            .method(http::Method::POST)
            .uri("/")
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(full_body("x".repeat(65)))
            .unwrap();
        let oversized = server.handle(oversized).await;
        assert_eq!(oversized.status(), http::StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn service_contract_mounts_below_a_prefix_and_rejects_child_paths() {
        let (server, _, _) = server(["orders.lookup"]);
        let app = rustee_router::App::new().nest("/mcp", server.clone());
        let mut mounted = protocol_request(json!({
            "jsonrpc":"2.0","id":6,"method":"tools/list","params":{}
        }));
        *mounted.uri_mut() = "/mcp".parse().unwrap();
        let response = app.call(mounted).await;
        assert_eq!(response.status(), http::StatusCode::OK);

        let mut server = server;
        let response = server
            .call(protocol_request(json!({
                "jsonrpc":"2.0","id":6,"method":"tools/list","params":{}
            })))
            .await
            .unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);

        let child = HttpRequest::builder()
            .method(http::Method::POST)
            .uri("/child")
            .header(http::header::CONTENT_TYPE, "application/json")
            .header("mcp-protocol-version", MCP_PROTOCOL_VERSION)
            .body(full_body("{}"))
            .unwrap();
        let response = server.call(child).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::NOT_FOUND);
    }

    #[derive(Clone, Deserialize)]
    struct LookupInput {
        id: u64,
    }

    #[derive(Serialize)]
    struct LookupOutput {
        status: &'static str,
    }

    #[derive(Debug, thiserror::Error)]
    #[error("handler secret")]
    struct HandlerFailure;

    #[derive(Clone)]
    struct AuthorizedContext;

    impl McpContextProvider for AuthorizedContext {
        type Error = Infallible;

        fn capabilities(&self) -> McpContextCapabilities {
            McpContextCapabilities::default()
                .with_resources()
                .with_prompts()
        }

        fn list_resources(
            &self,
            _: &rustee_core::Request,
        ) -> Result<Vec<McpServerResource>, Self::Error> {
            Ok(vec![
                McpServerResource::new(
                    Url::parse("resource://tenant-a/customer/7").unwrap(),
                    "customer-profile",
                )
                .unwrap()
                .with_mime_type("application/json")
                .unwrap(),
            ])
        }

        fn list_resource_templates(
            &self,
            _: &rustee_core::Request,
        ) -> Result<Vec<McpServerResourceTemplate>, Self::Error> {
            Ok(vec![
                McpServerResourceTemplate::new(
                    "resource://tenant-a/customer/{customer_id}",
                    "customer-profile",
                )
                .unwrap(),
            ])
        }

        fn read_resource(
            &self,
            _: &rustee_core::Request,
            uri: &Url,
        ) -> Result<Vec<McpServerResourceContents>, Self::Error> {
            assert_eq!(uri.as_str(), "resource://tenant-a/customer/7");
            Ok(vec![
                McpServerResourceContents::text(uri.clone(), "{\"customer_id\":\"7\"}")
                    .with_mime_type("application/json")
                    .unwrap(),
            ])
        }

        fn list_prompts(
            &self,
            _: &rustee_core::Request,
        ) -> Result<Vec<McpServerPrompt>, Self::Error> {
            Ok(vec![
                McpServerPrompt::new(
                    "customer-summary",
                    vec![McpServerPromptArgument::new("customer_id", true).unwrap()],
                )
                .unwrap(),
            ])
        }

        fn get_prompt(
            &self,
            _: &rustee_core::Request,
            name: &str,
            arguments: &BTreeMap<String, String>,
        ) -> Result<McpServerPromptResult, Self::Error> {
            assert_eq!(name, "customer-summary");
            assert_eq!(arguments.get("customer_id"), Some(&"7".to_owned()));
            Ok(McpServerPromptResult::new(vec![
                McpServerPromptMessage::user(McpServerPromptContent::Text(
                    "Summarize customer 7.".to_owned(),
                )),
            ]))
        }
    }

    #[derive(Clone)]
    struct InvalidContext;

    impl McpContextProvider for InvalidContext {
        type Error = Infallible;

        fn capabilities(&self) -> McpContextCapabilities {
            McpContextCapabilities::default().with_resources()
        }

        fn list_resources(
            &self,
            _: &rustee_core::Request,
        ) -> Result<Vec<McpServerResource>, Self::Error> {
            Ok(["one", "two"]
                .into_iter()
                .map(|name| {
                    McpServerResource::new(
                        Url::parse(&format!("resource://tenant-a/{name}")).unwrap(),
                        name,
                    )
                    .unwrap()
                })
                .collect())
        }

        fn list_resource_templates(
            &self,
            _: &rustee_core::Request,
        ) -> Result<Vec<McpServerResourceTemplate>, Self::Error> {
            Ok(Vec::new())
        }

        fn read_resource(
            &self,
            _: &rustee_core::Request,
            _: &Url,
        ) -> Result<Vec<McpServerResourceContents>, Self::Error> {
            Ok(vec![McpServerResourceContents::text(
                Url::parse("resource://tenant-a/another-customer").unwrap(),
                "unexpected",
            )])
        }

        fn list_prompts(
            &self,
            _: &rustee_core::Request,
        ) -> Result<Vec<McpServerPrompt>, Self::Error> {
            Ok(Vec::new())
        }

        fn get_prompt(
            &self,
            _: &rustee_core::Request,
            _: &str,
            _: &BTreeMap<String, String>,
        ) -> Result<McpServerPromptResult, Self::Error> {
            Ok(McpServerPromptResult::new(Vec::new()))
        }
    }

    #[derive(Clone)]
    struct Access {
        names: BTreeSet<String>,
    }

    impl McpToolAccessPolicy for Access {
        type Error = Infallible;

        fn permitted_tools(
            &self,
            _: &rustee_core::Request,
        ) -> Result<BTreeSet<String>, Self::Error> {
            Ok(self.names.clone())
        }

        fn execution_context(
            &self,
            _: &rustee_core::Request,
            _: &str,
        ) -> Result<rustee_ai::ToolExecutionContext, Self::Error> {
            Ok(rustee_ai::ToolExecutionContext::new(
                AiExecutionContext::new("tenant-a", "user-7").unwrap(),
                "mcp-semantic-action",
            )
            .unwrap())
        }
    }

    #[derive(Clone)]
    struct Approval {
        approved: bool,
    }

    impl ToolApprovalPolicy for Approval {
        type Error = Infallible;

        fn approve(
            &self,
            _: AiExecutionContext,
            _: rustee_ai::ToolCall,
            _: ToolRisk,
        ) -> futures_util::future::BoxFuture<'static, Result<ToolApprovalDecision, Self::Error>>
        {
            let decision = self.approved;
            Box::pin(futures_util::future::ready(Ok(if decision {
                ToolApprovalDecision::Approved
            } else {
                ToolApprovalDecision::Denied
            })))
        }
    }

    #[derive(Clone, Default)]
    struct Audit {
        approvals: Arc<AtomicUsize>,
        outcomes: Arc<AtomicUsize>,
    }

    impl ToolExecutionAuditSink for Audit {
        fn record_outcome(
            &self,
            _: ToolExecutionAuditEvent,
        ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
            let outcomes = Arc::clone(&self.outcomes);
            Box::pin(async move {
                outcomes.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    impl rustee_ai::ToolApprovalAuditSink for Audit {
        type Error = Infallible;

        fn record_approved(
            &self,
            _: ToolApprovalAuditEvent,
        ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
            let approvals = Arc::clone(&self.approvals);
            Box::pin(async move {
                approvals.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    fn server<const N: usize>(
        names: [&str; N],
    ) -> (McpServer<Access, Approval, Audit>, Arc<AtomicUsize>, Audit) {
        server_with_config(names, true, 1024 * 1024)
    }

    fn server_with_approval<const N: usize>(
        names: [&str; N],
        approved: bool,
    ) -> (McpServer<Access, Approval, Audit>, Arc<AtomicUsize>, Audit) {
        server_with_config(names, approved, 1024 * 1024)
    }

    fn server_configured<const N: usize>(
        names: [&str; N],
        max_request_bytes: usize,
    ) -> (McpServer<Access, Approval, Audit>, Arc<AtomicUsize>, Audit) {
        server_with_config(names, true, max_request_bytes)
    }

    fn server_with_config<const N: usize>(
        names: [&str; N],
        approved: bool,
        max_request_bytes: usize,
    ) -> (McpServer<Access, Approval, Audit>, Arc<AtomicUsize>, Audit) {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = Arc::clone(&calls);
        let mut registry = rustee_ai::ToolRegistry::new();
        registry
            .register(TypedTool::new(
                ToolDefinition::new("orders.lookup", json!({"type":"object"})).unwrap(),
                ToolRisk::ReadOnly,
                move |_context, input: LookupInput| {
                    if input.id == 13 {
                        return futures_util::future::ready(Err::<LookupOutput, HandlerFailure>(
                            HandlerFailure,
                        ));
                    }
                    assert_eq!(input.id, 7);
                    handler_calls.fetch_add(1, Ordering::SeqCst);
                    futures_util::future::ready(Ok::<LookupOutput, HandlerFailure>(LookupOutput {
                        status: "found",
                    }))
                },
            ))
            .unwrap();
        let access = Access {
            names: names.into_iter().map(str::to_owned).collect(),
        };
        let audit = Audit::default();
        let config = McpServerConfig::new("rustee-mcp-test", "0.1.0")
            .unwrap()
            .with_max_request_bytes(max_request_bytes)
            .unwrap();
        (
            McpServer::new(
                config,
                registry,
                access,
                Approval { approved },
                audit.clone(),
            ),
            calls,
            audit,
        )
    }

    #[allow(clippy::needless_pass_by_value)]
    fn request(value: Value) -> rustee_core::Request {
        HttpRequest::builder()
            .method(http::Method::POST)
            .uri("/")
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(full_body(value.to_string()))
            .unwrap()
    }

    fn protocol_request(value: Value) -> rustee_core::Request {
        let mut request = request(value);
        request.headers_mut().insert(
            "mcp-protocol-version",
            HeaderValue::from_static(MCP_PROTOCOL_VERSION),
        );
        request
    }

    async fn response_json(response: rustee_core::Response) -> Value {
        let (_, body) = response.into_parts();
        let body = body.collect().await.unwrap().to_bytes();
        serde_json::from_slice(&body).unwrap()
    }
}
