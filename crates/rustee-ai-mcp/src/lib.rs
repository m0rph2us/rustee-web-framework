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

mod context;
mod stdio;

pub use context::{
    McpPrompt, McpPromptArgument, McpPromptContent, McpPromptMessage, McpPromptResult,
    McpPromptRole, McpResource, McpResourceContents, McpResourceData, McpResourceLink,
    McpResourceTemplate,
};
pub use stdio::{McpStdioClient, McpStdioConfig, McpStdioConfigError, McpStdioRemoteTool};

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use futures_util::{StreamExt, future::BoxFuture};
use reqwest::{
    Client, Response, StatusCode,
    header::{ACCEPT, CONTENT_TYPE, HeaderValue},
};
use rustee_ai::{ToolDefinition, ToolExecutionContext, ToolExecutionError, ToolExecutor, ToolRisk};
use serde_json::{Value, json};
use tokio::{sync::Mutex, time::sleep};
use url::{Host, Url};

use context::{
    McpServerCapabilities, parse_prompt, parse_prompt_result, parse_resource,
    parse_resource_contents, parse_resource_template, parse_server_capabilities,
    valid_context_name, valid_context_request_string,
};

/// MCP protocol version implemented by this client.
pub const MCP_PROTOCOL_VERSION: &str = "2025-11-25";

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_LIST_PAGES: usize = 16;
const DEFAULT_MAX_TOOLS: usize = 128;
const DEFAULT_MAX_CONTEXT_ITEMS: usize = 64;
const DEFAULT_MAX_CONTEXT_BYTES: usize = 512 * 1024;
const MAX_TOOL_NAME_BYTES: usize = 128;
const MAX_SESSION_ID_BYTES: usize = 512;
const MAX_AUTOMATIC_SESSION_RECOVERY_ATTEMPTS: usize = 8;
const MAX_AUTOMATIC_SESSION_RECOVERY_BACKOFF: Duration = Duration::from_secs(30);
const MAX_AUTOMATIC_SSE_RESUMPTION_ATTEMPTS: usize = 8;
const MAX_AUTOMATIC_SSE_RESUMPTION_BACKOFF: Duration = Duration::from_secs(30);
const MAX_SSE_EVENT_ID_BYTES: usize = 512;
const MAX_SSE_NOTIFICATION_METHOD_BYTES: usize = 256;

#[derive(Clone, Copy)]
struct HttpAutomaticSessionRecovery {
    max_attempts: usize,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl HttpAutomaticSessionRecovery {
    fn delay_for(self, attempt: usize) -> Duration {
        let mut delay = self.initial_backoff;
        for _ in 0..attempt {
            delay = delay.saturating_mul(2).min(self.max_backoff);
        }
        delay
    }
}

#[derive(Clone, Copy)]
struct HttpAutomaticSseResumption {
    max_attempts: usize,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl HttpAutomaticSseResumption {
    fn delay_for(self, attempt: usize) -> Duration {
        let mut delay = self.initial_backoff;
        for _ in 0..attempt {
            delay = delay.saturating_mul(2).min(self.max_backoff);
        }
        delay
    }
}

/// Redacted configuration for one trusted MCP Streamable HTTP endpoint.
#[derive(Clone)]
pub struct McpHttpConfig {
    endpoint: Url,
    bearer_token: Option<String>,
    request_timeout: Duration,
    max_response_bytes: usize,
    max_list_pages: usize,
    max_tools: usize,
    max_context_items: usize,
    max_context_bytes: usize,
    automatic_session_recovery: Option<HttpAutomaticSessionRecovery>,
    automatic_sse_resumption: Option<HttpAutomaticSseResumption>,
}

impl McpHttpConfig {
    /// Creates configuration for one Streamable HTTP endpoint.
    ///
    /// Non-loopback endpoints must use HTTPS. Query strings, fragments, and embedded credentials
    /// are rejected so endpoint selection remains an explicit deployment decision.
    ///
    /// # Errors
    ///
    /// Returns [`McpHttpConfigError::InvalidEndpoint`] for an unsafe endpoint.
    pub fn new(endpoint: Url) -> Result<Self, McpHttpConfigError> {
        if !valid_endpoint(&endpoint) {
            return Err(McpHttpConfigError::InvalidEndpoint);
        }
        Ok(Self {
            endpoint,
            bearer_token: None,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            max_list_pages: DEFAULT_MAX_LIST_PAGES,
            max_tools: DEFAULT_MAX_TOOLS,
            max_context_items: DEFAULT_MAX_CONTEXT_ITEMS,
            max_context_bytes: DEFAULT_MAX_CONTEXT_BYTES,
            automatic_session_recovery: None,
            automatic_sse_resumption: None,
        })
    }

    /// Adds a bearer credential for the configured endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`McpHttpConfigError::BlankBearerToken`] when the credential is blank.
    pub fn with_bearer_token(
        mut self,
        bearer_token: impl Into<String>,
    ) -> Result<Self, McpHttpConfigError> {
        let bearer_token = bearer_token.into();
        if bearer_token.trim().is_empty() {
            return Err(McpHttpConfigError::BlankBearerToken);
        }
        self.bearer_token = Some(bearer_token);
        Ok(self)
    }

    /// Sets one finite request deadline.
    ///
    /// # Errors
    ///
    /// Returns [`McpHttpConfigError::ZeroTimeout`] when `request_timeout` is zero.
    pub fn with_request_timeout(
        mut self,
        request_timeout: Duration,
    ) -> Result<Self, McpHttpConfigError> {
        if request_timeout.is_zero() {
            return Err(McpHttpConfigError::ZeroTimeout);
        }
        self.request_timeout = request_timeout;
        Ok(self)
    }

    /// Sets the maximum decoded JSON or SSE response bytes accepted for one request.
    ///
    /// # Errors
    ///
    /// Returns [`McpHttpConfigError::ZeroResponseLimit`] when `max_response_bytes` is zero.
    pub fn with_max_response_bytes(
        mut self,
        max_response_bytes: usize,
    ) -> Result<Self, McpHttpConfigError> {
        if max_response_bytes == 0 {
            return Err(McpHttpConfigError::ZeroResponseLimit);
        }
        self.max_response_bytes = max_response_bytes;
        Ok(self)
    }

    /// Sets bounded pagination and total discovery limits for `tools/list`.
    ///
    /// # Errors
    ///
    /// Returns [`McpHttpConfigError::ZeroDiscoveryLimit`] when either limit is zero.
    pub fn with_tool_discovery_limits(
        mut self,
        max_list_pages: usize,
        max_tools: usize,
    ) -> Result<Self, McpHttpConfigError> {
        if max_list_pages == 0 || max_tools == 0 {
            return Err(McpHttpConfigError::ZeroDiscoveryLimit);
        }
        self.max_list_pages = max_list_pages;
        self.max_tools = max_tools;
        Ok(self)
    }

    /// Sets total item and decoded-content bounds for MCP resources and prompts.
    ///
    /// These bounds apply after the transport body bound and prevent one explicitly selected
    /// remote context result from becoming an unbounded application/provider input.
    ///
    /// # Errors
    ///
    /// Returns [`McpHttpConfigError::ZeroContextLimit`] when either limit is zero.
    pub fn with_context_limits(
        mut self,
        max_context_items: usize,
        max_context_bytes: usize,
    ) -> Result<Self, McpHttpConfigError> {
        if max_context_items == 0 || max_context_bytes == 0 {
            return Err(McpHttpConfigError::ZeroContextLimit);
        }
        self.max_context_items = max_context_items;
        self.max_context_bytes = max_context_bytes;
        Ok(self)
    }

    /// Enables bounded reinitialization after a session-bearing HTTP request receives 404.
    ///
    /// The expired request still returns [`McpError::SessionExpired`]. This option only attempts
    /// a new initialize/notification handshake for a later explicit request and never replays the
    /// request or tool call that encountered the expired session.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for zero or excessive attempts, zero delays, a maximum
    /// backoff below the initial backoff, or a backoff above the bounded limit.
    pub fn with_automatic_session_recovery(
        mut self,
        max_attempts: usize,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Result<Self, McpHttpConfigError> {
        if max_attempts == 0 {
            return Err(McpHttpConfigError::ZeroSessionRecoveryAttempts);
        }
        if max_attempts > MAX_AUTOMATIC_SESSION_RECOVERY_ATTEMPTS {
            return Err(McpHttpConfigError::SessionRecoveryAttemptLimit);
        }
        if initial_backoff.is_zero() || max_backoff.is_zero() {
            return Err(McpHttpConfigError::ZeroSessionRecoveryBackoff);
        }
        if max_backoff < initial_backoff {
            return Err(McpHttpConfigError::InvalidSessionRecoveryBackoff);
        }
        if max_backoff > MAX_AUTOMATIC_SESSION_RECOVERY_BACKOFF {
            return Err(McpHttpConfigError::SessionRecoveryBackoffLimit);
        }
        self.automatic_session_recovery = Some(HttpAutomaticSessionRecovery {
            max_attempts,
            initial_backoff,
            max_backoff,
        });
        Ok(self)
    }

    /// Enables bounded resumption of an interrupted response SSE stream.
    ///
    /// Resumption is attempted only after the stream supplied a valid event ID. Each attempt is
    /// an HTTP `GET` with that ID in `Last-Event-ID`; Rustee never resends the originating JSON-RPC
    /// `POST`, including a selected tool call. A server-provided SSE `retry` value is respected
    /// only when it fits this configuration's finite maximum backoff.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for zero or excessive attempts, zero delays, a maximum
    /// backoff below the initial backoff, or a backoff above the bounded limit.
    pub fn with_automatic_sse_resumption(
        mut self,
        max_attempts: usize,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Result<Self, McpHttpConfigError> {
        if max_attempts == 0 {
            return Err(McpHttpConfigError::ZeroSseResumptionAttempts);
        }
        if max_attempts > MAX_AUTOMATIC_SSE_RESUMPTION_ATTEMPTS {
            return Err(McpHttpConfigError::SseResumptionAttemptLimit);
        }
        if initial_backoff.is_zero() || max_backoff.is_zero() {
            return Err(McpHttpConfigError::ZeroSseResumptionBackoff);
        }
        if max_backoff < initial_backoff {
            return Err(McpHttpConfigError::InvalidSseResumptionBackoff);
        }
        if max_backoff > MAX_AUTOMATIC_SSE_RESUMPTION_BACKOFF {
            return Err(McpHttpConfigError::SseResumptionBackoffLimit);
        }
        self.automatic_sse_resumption = Some(HttpAutomaticSseResumption {
            max_attempts,
            initial_backoff,
            max_backoff,
        });
        Ok(self)
    }

    /// Returns the explicit remote transport endpoint.
    #[must_use]
    pub const fn endpoint(&self) -> &Url {
        &self.endpoint
    }
}

impl fmt::Debug for McpHttpConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpHttpConfig")
            .field("endpoint", &self.endpoint)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("request_timeout", &self.request_timeout)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("max_list_pages", &self.max_list_pages)
            .field("max_tools", &self.max_tools)
            .field("max_context_items", &self.max_context_items)
            .field("max_context_bytes", &self.max_context_bytes)
            .field(
                "automatic_session_recovery",
                &self.automatic_session_recovery.map(|recovery| {
                    (
                        recovery.max_attempts,
                        recovery.initial_backoff,
                        recovery.max_backoff,
                    )
                }),
            )
            .field(
                "automatic_sse_resumption",
                &self.automatic_sse_resumption.map(|resumption| {
                    (
                        resumption.max_attempts,
                        resumption.initial_backoff,
                        resumption.max_backoff,
                    )
                }),
            )
            .finish()
    }
}

/// Invalid MCP HTTP adapter configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum McpHttpConfigError {
    /// The endpoint was not a clean HTTPS URL or a loopback HTTP test endpoint.
    #[error(
        "MCP endpoint must use HTTPS unless it is loopback, without credentials, query, or fragment"
    )]
    InvalidEndpoint,
    /// A configured bearer credential was blank.
    #[error("MCP bearer token must not be blank")]
    BlankBearerToken,
    /// One request must have a finite deadline.
    #[error("MCP request timeout must be non-zero")]
    ZeroTimeout,
    /// JSON parsing must have a finite memory bound.
    #[error("MCP response byte limit must be non-zero")]
    ZeroResponseLimit,
    /// Tool discovery must have finite page and tool limits.
    #[error("MCP tool discovery limits must be non-zero")]
    ZeroDiscoveryLimit,
    /// Context discovery and decoding must have finite item and byte limits.
    #[error("MCP context item and byte limits must be non-zero")]
    ZeroContextLimit,
    /// Automatic session recovery must make at least one bounded attempt.
    #[error("MCP automatic session recovery attempts must be non-zero")]
    ZeroSessionRecoveryAttempts,
    /// Automatic session recovery attempts exceed the bounded limit.
    #[error("MCP automatic session recovery attempts exceed the bounded limit")]
    SessionRecoveryAttemptLimit,
    /// Automatic session recovery backoff must have non-zero values.
    #[error("MCP automatic session recovery backoff values must be non-zero")]
    ZeroSessionRecoveryBackoff,
    /// Automatic session recovery maximum backoff must not be below its initial value.
    #[error("MCP automatic session recovery maximum backoff must not be below its initial backoff")]
    InvalidSessionRecoveryBackoff,
    /// Automatic session recovery backoff must remain bounded.
    #[error("MCP automatic session recovery maximum backoff exceeds the bounded limit")]
    SessionRecoveryBackoffLimit,
    /// Automatic SSE resumption must make at least one bounded `GET` attempt.
    #[error("MCP automatic SSE resumption attempts must be non-zero")]
    ZeroSseResumptionAttempts,
    /// Automatic SSE resumption attempts exceed the bounded limit.
    #[error("MCP automatic SSE resumption attempts exceed the bounded limit")]
    SseResumptionAttemptLimit,
    /// Automatic SSE resumption backoff must have non-zero values.
    #[error("MCP automatic SSE resumption backoff values must be non-zero")]
    ZeroSseResumptionBackoff,
    /// Automatic SSE resumption maximum backoff must not be below its initial value.
    #[error("MCP automatic SSE resumption maximum backoff must not be below its initial backoff")]
    InvalidSseResumptionBackoff,
    /// Automatic SSE resumption backoff must remain bounded.
    #[error("MCP automatic SSE resumption maximum backoff exceeds the bounded limit")]
    SseResumptionBackoffLimit,
}

#[derive(Default)]
struct McpClientState {
    session: Mutex<Option<McpSession>>,
    initialize: Mutex<()>,
    next_request_id: AtomicU64,
}

#[derive(Clone, Eq, PartialEq)]
struct McpSession {
    id: Option<String>,
    protocol_version: String,
    capabilities: McpServerCapabilities,
}

/// Cloneable client for a single initialized MCP Streamable HTTP endpoint.
#[derive(Clone)]
pub struct McpHttpClient {
    client: Client,
    config: McpHttpConfig,
    state: Arc<McpClientState>,
}

impl McpHttpClient {
    /// Creates a client with the configured TLS and request-timeout boundary.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::Client`] when the HTTP client cannot be initialized.
    pub fn new(config: McpHttpConfig) -> Result<Self, McpError> {
        let client = Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|_| McpError::Client)?;
        Ok(Self::with_client(client, config))
    }

    /// Wraps an already-configured client for dependency injection and local protocol tests.
    #[must_use]
    pub fn with_client(client: Client, config: McpHttpConfig) -> Self {
        Self {
            client,
            config,
            state: Arc::new(McpClientState::default()),
        }
    }

    /// Performs MCP initialization and its required `notifications/initialized` notification.
    ///
    /// Calling this more than once is idempotent for this client instance. Application startup is
    /// the right place to make endpoint trust and initialization failure visible before tools are
    /// advertised to a provider.
    ///
    /// # Errors
    ///
    /// Returns a sanitized transport, protocol, or initialization failure.
    pub async fn initialize(&self) -> Result<(), McpError> {
        let _initializing = self.state.initialize.lock().await;
        self.initialize_unlocked().await
    }

    /// Opens one explicit Streamable HTTP `GET` SSE stream for server notifications.
    ///
    /// The client must already be initialized so the negotiated protocol and optional session
    /// header are carried on the `GET`. Returned notifications are remote input and are not
    /// forwarded to tools, resources, prompts, or an AI provider. Server-initiated JSON-RPC
    /// requests and responses are rejected because sampling and elicitation need their own
    /// application approval boundary.
    ///
    /// When `with_automatic_sse_resumption` is enabled, a disconnected stream that supplied an
    /// event ID is resumed with bounded `GET` plus `Last-Event-ID`; no JSON-RPC `POST` is sent.
    /// Dropping the returned stream closes only that HTTP stream and does not close the MCP
    /// session.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::NotInitialized`] before a successful initialization, or a sanitized
    /// transport, HTTP, content-type, or session failure.
    pub async fn open_server_event_stream(&self) -> Result<McpServerEventStream, McpError> {
        let session = self.session().await.ok_or(McpError::NotInitialized)?;
        let response = self
            .get_sse(
                Some(&session),
                &session.protocol_version,
                session.id.as_deref(),
                None,
            )
            .await?;
        Ok(McpServerEventStream {
            client: self.clone(),
            session,
            response: Some(response),
            buffer: Vec::new(),
            last_event_id: None,
            retry_delay: None,
            resume_attempts: 0,
            closed: false,
        })
    }

    async fn initialize_unlocked(&self) -> Result<(), McpError> {
        if self.session().await.is_some() {
            return Ok(());
        }

        let id = self.next_request_id();
        let reply = self.initialize_request(id).await?;
        let protocol_version = reply
            .result
            .get("protocolVersion")
            .and_then(Value::as_str)
            .filter(|value| *value == MCP_PROTOCOL_VERSION)
            .ok_or(McpError::ProtocolVersion)?
            .to_owned();
        let server_info = reply
            .result
            .get("serverInfo")
            .and_then(Value::as_object)
            .ok_or(McpError::MalformedResponse)?;
        let capabilities = parse_server_capabilities(
            reply
                .result
                .get("capabilities")
                .ok_or(McpError::MalformedResponse)?,
        )?;
        if server_info
            .get("name")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
            || server_info
                .get("version")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
        {
            return Err(McpError::MalformedResponse);
        }
        let session = McpSession {
            id: match reply.session_id {
                Some(session_id) if valid_session_id(&session_id) => Some(session_id),
                Some(_) => return Err(McpError::MalformedResponse),
                None => None,
            },
            protocol_version,
            capabilities,
        };
        self.notification(
            &session,
            "notifications/initialized",
            Value::Object(serde_json::Map::default()),
        )
        .await?;
        *self.state.session.lock().await = Some(session);
        Ok(())
    }

    /// Discovers every available tool up to the configured bounded pagination and total limits.
    ///
    /// The returned schema and description are untrusted remote input. This method never
    /// advertises a tool to a model or calls it; applications must select each discovery result,
    /// choose a [`ToolRisk`], and register a [`McpRemoteTool`] in a Rustee tool registry.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::NotInitialized`] until [`Self::initialize`] succeeds, or a sanitized
    /// protocol/discovery failure when the endpoint violates this bounded contract.
    pub async fn list_tools(&self) -> Result<Vec<McpToolDefinition>, McpError> {
        let session = self.session().await.ok_or(McpError::NotInitialized)?;
        let mut cursor: Option<String> = None;
        let mut cursors = BTreeSet::new();
        let mut names = BTreeSet::new();
        let mut tools = Vec::new();

        for _ in 0..self.config.max_list_pages {
            let mut params = serde_json::Map::new();
            if let Some(cursor) = &cursor {
                params.insert("cursor".to_owned(), Value::String(cursor.clone()));
            }
            let reply = self
                .request(
                    Some(&session),
                    self.next_request_id(),
                    "tools/list",
                    Value::Object(params),
                )
                .await?;
            let discovered = reply
                .result
                .get("tools")
                .and_then(Value::as_array)
                .ok_or(McpError::MalformedResponse)?;
            for value in discovered {
                let tool =
                    McpToolDefinition::from_wire(value).map_err(|_| McpError::MalformedResponse)?;
                if !names.insert(tool.name().to_owned()) {
                    return Err(McpError::MalformedResponse);
                }
                tools.push(tool);
                if tools.len() > self.config.max_tools {
                    return Err(McpError::ToolDiscoveryLimit);
                }
            }
            let next_cursor = match reply.result.get("nextCursor") {
                None | Some(Value::Null) => None,
                Some(Value::String(cursor)) if valid_cursor(cursor) => Some(cursor.clone()),
                Some(_) => return Err(McpError::MalformedResponse),
            };
            let Some(next_cursor) = next_cursor else {
                return Ok(tools);
            };
            if !cursors.insert(next_cursor.clone()) {
                return Err(McpError::MalformedResponse);
            }
            cursor = Some(next_cursor);
        }
        Err(McpError::ToolDiscoveryLimit)
    }

    /// Discovers application-selected MCP resources without fetching or forwarding their content.
    ///
    /// Resource metadata remains untrusted. A caller must still apply resource-specific access,
    /// consent, classification, and context-budget policy before calling [`Self::read_resource`]
    /// or using its result in an AI request.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::NotInitialized`], [`McpError::UnsupportedCapability`], or a sanitized
    /// transport/protocol/bounds failure.
    pub async fn list_resources(&self) -> Result<Vec<McpResource>, McpError> {
        let session = self.session().await.ok_or(McpError::NotInitialized)?;
        if !session.capabilities.resources {
            return Err(McpError::UnsupportedCapability);
        }
        let mut cursor = None;
        let mut cursors = BTreeSet::new();
        let mut uris = BTreeSet::new();
        let mut resources = Vec::new();
        for _ in 0..self.config.max_list_pages {
            let result = self
                .request(
                    Some(&session),
                    self.next_request_id(),
                    "resources/list",
                    paginated_params(cursor.as_deref()),
                )
                .await?
                .result;
            let discovered = result
                .get("resources")
                .and_then(Value::as_array)
                .ok_or(McpError::MalformedResponse)?;
            for value in discovered {
                let resource = parse_resource(value)?;
                if !uris.insert(resource.uri().as_str().to_owned()) {
                    return Err(McpError::MalformedResponse);
                }
                resources.push(resource);
                if resources.len() > self.config.max_context_items {
                    return Err(McpError::ContextLimit);
                }
            }
            let Some(next) = next_cursor(&result)? else {
                return Ok(resources);
            };
            if !cursors.insert(next.clone()) {
                return Err(McpError::MalformedResponse);
            }
            cursor = Some(next);
        }
        Err(McpError::ContextLimit)
    }

    /// Discovers parameterized MCP resource templates without expanding or fetching them.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::NotInitialized`], [`McpError::UnsupportedCapability`], or a sanitized
    /// transport/protocol/bounds failure.
    pub async fn list_resource_templates(&self) -> Result<Vec<McpResourceTemplate>, McpError> {
        let session = self.session().await.ok_or(McpError::NotInitialized)?;
        if !session.capabilities.resources {
            return Err(McpError::UnsupportedCapability);
        }
        let mut cursor = None;
        let mut cursors = BTreeSet::new();
        let mut names = BTreeSet::new();
        let mut templates = Vec::new();
        for _ in 0..self.config.max_list_pages {
            let result = self
                .request(
                    Some(&session),
                    self.next_request_id(),
                    "resources/templates/list",
                    paginated_params(cursor.as_deref()),
                )
                .await?
                .result;
            let discovered = result
                .get("resourceTemplates")
                .and_then(Value::as_array)
                .ok_or(McpError::MalformedResponse)?;
            for value in discovered {
                let template = parse_resource_template(value)?;
                if !names.insert(template.name().to_owned()) {
                    return Err(McpError::MalformedResponse);
                }
                templates.push(template);
                if templates.len() > self.config.max_context_items {
                    return Err(McpError::ContextLimit);
                }
            }
            let Some(next) = next_cursor(&result)? else {
                return Ok(templates);
            };
            if !cursors.insert(next.clone()) {
                return Err(McpError::MalformedResponse);
            }
            cursor = Some(next);
        }
        Err(McpError::ContextLimit)
    }

    /// Reads one explicitly selected MCP resource without adding it to an AI request.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::InvalidContextRequest`] for an invalid local URI,
    /// [`McpError::UnsupportedCapability`] when the server did not negotiate resources, or a
    /// sanitized remote failure.
    pub async fn read_resource(&self, uri: &Url) -> Result<Vec<McpResourceContents>, McpError> {
        if !valid_context_request_string(uri.as_str(), self.config.max_context_bytes) {
            return Err(McpError::InvalidContextRequest);
        }
        let session = self.session().await.ok_or(McpError::NotInitialized)?;
        if !session.capabilities.resources {
            return Err(McpError::UnsupportedCapability);
        }
        let result = self
            .request(
                Some(&session),
                self.next_request_id(),
                "resources/read",
                json!({"uri":uri.as_str()}),
            )
            .await?
            .result;
        let contents = result
            .get("contents")
            .and_then(Value::as_array)
            .filter(|contents| {
                !contents.is_empty() && contents.len() <= self.config.max_context_items
            })
            .ok_or(McpError::MalformedResponse)?;
        let mut total_bytes = 0_usize;
        let mut parsed = Vec::with_capacity(contents.len());
        for value in contents {
            let content = parse_resource_contents(
                value,
                self.config.max_context_bytes.saturating_sub(total_bytes),
            )?;
            if content.uri() != uri {
                return Err(McpError::MalformedResponse);
            }
            total_bytes = total_bytes.saturating_add(content.data().byte_len());
            if total_bytes > self.config.max_context_bytes {
                return Err(McpError::ContextLimit);
            }
            parsed.push(content);
        }
        Ok(parsed)
    }

    /// Discovers user-selectable MCP prompt declarations without requesting their messages.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::NotInitialized`], [`McpError::UnsupportedCapability`], or a sanitized
    /// transport/protocol/bounds failure.
    pub async fn list_prompts(&self) -> Result<Vec<McpPrompt>, McpError> {
        let session = self.session().await.ok_or(McpError::NotInitialized)?;
        if !session.capabilities.prompts {
            return Err(McpError::UnsupportedCapability);
        }
        let mut cursor = None;
        let mut cursors = BTreeSet::new();
        let mut names = BTreeSet::new();
        let mut prompts = Vec::new();
        for _ in 0..self.config.max_list_pages {
            let result = self
                .request(
                    Some(&session),
                    self.next_request_id(),
                    "prompts/list",
                    paginated_params(cursor.as_deref()),
                )
                .await?
                .result;
            let discovered = result
                .get("prompts")
                .and_then(Value::as_array)
                .ok_or(McpError::MalformedResponse)?;
            for value in discovered {
                let prompt = parse_prompt(value, self.config.max_context_items)?;
                if !names.insert(prompt.name().to_owned()) {
                    return Err(McpError::MalformedResponse);
                }
                prompts.push(prompt);
                if prompts.len() > self.config.max_context_items {
                    return Err(McpError::ContextLimit);
                }
            }
            let Some(next) = next_cursor(&result)? else {
                return Ok(prompts);
            };
            if !cursors.insert(next.clone()) {
                return Err(McpError::MalformedResponse);
            }
            cursor = Some(next);
        }
        Err(McpError::ContextLimit)
    }

    /// Gets one explicitly selected user-controlled MCP prompt without adding it to an AI request.
    ///
    /// The application owns consent, argument selection, content inspection, and any later model
    /// context rendering.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::InvalidContextRequest`] for unsafe local input,
    /// [`McpError::UnsupportedCapability`] when prompts were not negotiated, or a sanitized remote
    /// failure.
    pub async fn get_prompt(
        &self,
        name: &str,
        arguments: &BTreeMap<String, String>,
    ) -> Result<McpPromptResult, McpError> {
        if !valid_context_name(name) || arguments.len() > self.config.max_context_items {
            return Err(McpError::InvalidContextRequest);
        }
        let mut total_argument_bytes = 0_usize;
        for (key, value) in arguments {
            if !valid_context_name(key)
                || !valid_context_request_string(value, self.config.max_context_bytes)
            {
                return Err(McpError::InvalidContextRequest);
            }
            total_argument_bytes = total_argument_bytes.saturating_add(value.len());
            if total_argument_bytes > self.config.max_context_bytes {
                return Err(McpError::InvalidContextRequest);
            }
        }
        let session = self.session().await.ok_or(McpError::NotInitialized)?;
        if !session.capabilities.prompts {
            return Err(McpError::UnsupportedCapability);
        }
        let mut params = serde_json::Map::new();
        params.insert("name".to_owned(), Value::String(name.to_owned()));
        if !arguments.is_empty() {
            params.insert(
                "arguments".to_owned(),
                serde_json::to_value(arguments).map_err(|_| McpError::MalformedResponse)?,
            );
        }
        let result = self
            .request(
                Some(&session),
                self.next_request_id(),
                "prompts/get",
                Value::Object(params),
            )
            .await?
            .result;
        parse_prompt_result(
            &result,
            self.config.max_context_items,
            self.config.max_context_bytes,
        )
    }

    async fn call_tool(
        &self,
        name: String,
        arguments: Value,
        idempotency_key: Option<String>,
    ) -> Result<Value, McpError> {
        let session = self.session().await.ok_or(McpError::NotInitialized)?;
        let mut params = serde_json::Map::new();
        params.insert("name".to_owned(), Value::String(name));
        params.insert("arguments".to_owned(), arguments);
        if let Some(idempotency_key) = idempotency_key {
            params.insert(
                "_meta".to_owned(),
                json!({"io.rustee/idempotency-key": idempotency_key}),
            );
        }
        let reply = self
            .request(
                Some(&session),
                self.next_request_id(),
                "tools/call",
                Value::Object(params),
            )
            .await?;
        decode_tool_result(&reply.result)
    }

    async fn session(&self) -> Option<McpSession> {
        self.state.session.lock().await.clone()
    }

    async fn clear_expired_session(&self, expired: &McpSession) {
        self.clear_session_if_current(expired).await;
    }

    async fn clear_session_if_current(&self, current: &McpSession) {
        let mut session = self.state.session.lock().await;
        if session.as_ref() == Some(current) {
            *session = None;
        }
    }

    /// Releases the current MCP session from this client instance.
    ///
    /// Stateful endpoints receive the Streamable HTTP `DELETE` request. A `405 Method Not
    /// Allowed` is a valid stateless-server response and still clears local state. This method
    /// never replays a request or calls a tool.
    ///
    /// # Errors
    ///
    /// Returns a sanitized transport/status failure. A session-specific 404 clears local state
    /// and returns [`McpError::SessionExpired`].
    pub async fn close_session(&self) -> Result<(), McpError> {
        let Some(session) = self.session().await else {
            return Ok(());
        };
        let Some(session_id) = &session.id else {
            self.clear_session_if_current(&session).await;
            return Ok(());
        };
        let mut request = self
            .client
            .delete(self.config.endpoint.clone())
            .header(ACCEPT, "application/json, text/event-stream")
            .header("mcp-protocol-version", &session.protocol_version);
        if let Some(bearer_token) = &self.config.bearer_token {
            request = request.bearer_auth(bearer_token);
        }
        let session_id =
            HeaderValue::from_str(session_id).map_err(|_| McpError::MalformedResponse)?;
        let response = request
            .header("mcp-session-id", session_id)
            .send()
            .await
            .map_err(|_| McpError::Transport)?;
        if response.status() == StatusCode::NOT_FOUND {
            self.clear_expired_session(&session).await;
            return Err(McpError::SessionExpired);
        }
        if response.status().is_success() || response.status() == StatusCode::METHOD_NOT_ALLOWED {
            self.clear_session_if_current(&session).await;
            return Ok(());
        }
        Err(McpError::HttpStatus(response.status()))
    }

    fn next_request_id(&self) -> u64 {
        self.state.next_request_id.fetch_add(1, Ordering::Relaxed) + 1
    }

    async fn initialize_request(&self, id: u64) -> Result<McpReply, McpError> {
        let response = self
            .post(
                None,
                json!({
                    "jsonrpc":"2.0",
                    "id":id,
                    "method":"initialize",
                    "params":{
                        "protocolVersion": MCP_PROTOCOL_VERSION,
                        "capabilities": {},
                        "clientInfo": {
                            "name": "rustee-ai-mcp",
                            "version": env!("CARGO_PKG_VERSION"),
                        },
                    },
                }),
            )
            .await?;
        self.decode_response(response, id, None).await
    }

    async fn request(
        &self,
        session: Option<&McpSession>,
        id: u64,
        method: &str,
        params: Value,
    ) -> Result<McpReply, McpError> {
        let response = match self
            .post(
                session,
                json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}),
            )
            .await
        {
            Ok(response) => response,
            Err(McpError::SessionExpired) => {
                self.recover_expired_session().await;
                return Err(McpError::SessionExpired);
            }
            Err(error) => return Err(error),
        };
        match self.decode_response(response, id, session).await {
            Err(McpError::SessionExpired) => {
                self.recover_expired_session().await;
                Err(McpError::SessionExpired)
            }
            result => result,
        }
    }

    async fn decode_response(
        &self,
        response: Response,
        id: u64,
        session: Option<&McpSession>,
    ) -> Result<McpReply, McpError> {
        let session_id = response
            .headers()
            .get("mcp-session-id")
            .map(|value| value.to_str().map(str::to_owned))
            .transpose()
            .map_err(|_| McpError::MalformedResponse)?;
        if session_id
            .as_deref()
            .is_some_and(|session_id| !valid_session_id(session_id))
        {
            return Err(McpError::MalformedResponse);
        }
        let value = self
            .response_value(response, id, session, session_id.as_deref())
            .await?;
        let result = decode_rpc_result(&value, id)?;
        Ok(McpReply { result, session_id })
    }

    async fn notification(
        &self,
        session: &McpSession,
        method: &str,
        params: Value,
    ) -> Result<(), McpError> {
        let response = self
            .post(
                Some(session),
                json!({"jsonrpc":"2.0","method":method,"params":params}),
            )
            .await?;
        if response.status() == StatusCode::ACCEPTED || response.status().is_success() {
            return Ok(());
        }
        Err(McpError::HttpStatus(response.status()))
    }

    async fn post(&self, session: Option<&McpSession>, body: Value) -> Result<Response, McpError> {
        let mut request = self
            .client
            .post(self.config.endpoint.clone())
            .header(ACCEPT, "application/json, text/event-stream")
            .header(CONTENT_TYPE, "application/json")
            .json(&body);
        if let Some(bearer_token) = &self.config.bearer_token {
            request = request.bearer_auth(bearer_token);
        }
        if let Some(session) = session {
            request = request.header("mcp-protocol-version", &session.protocol_version);
            if let Some(session_id) = &session.id {
                let session_id =
                    HeaderValue::from_str(session_id).map_err(|_| McpError::MalformedResponse)?;
                request = request.header("mcp-session-id", session_id);
            }
        }
        let response = request.send().await.map_err(|_| McpError::Transport)?;
        if response.status() == StatusCode::NOT_FOUND
            && session.is_some_and(|session| session.id.is_some())
        {
            if let Some(session) = session {
                self.clear_expired_session(session).await;
            }
            return Err(McpError::SessionExpired);
        }
        if !response.status().is_success() {
            return Err(McpError::HttpStatus(response.status()));
        }
        Ok(response)
    }

    async fn recover_expired_session(&self) {
        let Some(recovery) = self.config.automatic_session_recovery else {
            return;
        };
        let _initializing = self.state.initialize.lock().await;
        if self.session().await.is_some() {
            return;
        }
        self.state.next_request_id.store(0, Ordering::Relaxed);
        for attempt in 0..recovery.max_attempts {
            sleep(recovery.delay_for(attempt)).await;
            if self.initialize_unlocked().await.is_ok() {
                return;
            }
        }
    }

    async fn response_value(
        &self,
        response: Response,
        id: u64,
        session: Option<&McpSession>,
        response_session_id: Option<&str>,
    ) -> Result<Value, McpError> {
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_ascii_lowercase)
            .ok_or(McpError::UnsupportedResponseContentType)?;
        if content_type.starts_with("application/json") {
            return self.json_response(response).await;
        }
        if content_type.starts_with("text/event-stream") {
            let protocol_version = session.map_or(MCP_PROTOCOL_VERSION, |session| {
                session.protocol_version.as_str()
            });
            let session_id = session
                .and_then(|session| session.id.as_deref())
                .or(response_session_id);
            return self
                .sse_response(response, id, session, protocol_version, session_id)
                .await;
        }
        Err(McpError::UnsupportedResponseContentType)
    }

    async fn json_response(&self, response: Response) -> Result<Value, McpError> {
        if response
            .content_length()
            .is_some_and(|length| length > self.config.max_response_bytes as u64)
        {
            return Err(McpError::ResponseTooLarge);
        }
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| McpError::Transport)?;
            if body.len().saturating_add(chunk.len()) > self.config.max_response_bytes {
                return Err(McpError::ResponseTooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&body).map_err(|_| McpError::MalformedResponse)
    }

    async fn sse_response(
        &self,
        response: Response,
        expected_id: u64,
        session: Option<&McpSession>,
        protocol_version: &str,
        session_id: Option<&str>,
    ) -> Result<Value, McpError> {
        let mut state = SseStreamState::default();
        let mut response = response;
        let mut attempts = 0_usize;
        loop {
            match self
                .consume_sse_response(response, expected_id, &mut state)
                .await?
            {
                SseReadOutcome::Result(value) => return Ok(value),
                SseReadOutcome::Disconnected => {}
            }
            let Some(resumption) = self.config.automatic_sse_resumption else {
                return Err(McpError::SseStreamTerminated);
            };
            let Some(last_event_id) = state.last_event_id.as_deref() else {
                return Err(McpError::SseStreamTerminated);
            };
            if attempts >= resumption.max_attempts {
                return Err(McpError::SseStreamTerminated);
            }
            let delay = state
                .retry_delay
                .unwrap_or_else(|| resumption.delay_for(attempts));
            if delay > resumption.max_backoff {
                return Err(McpError::SseRetryLimit);
            }
            sleep(delay).await;
            response = self
                .get_sse(session, protocol_version, session_id, Some(last_event_id))
                .await?;
            attempts += 1;
        }
    }

    async fn consume_sse_response(
        &self,
        response: Response,
        expected_id: u64,
        state: &mut SseStreamState,
    ) -> Result<SseReadOutcome, McpError> {
        let remaining = self
            .config
            .max_response_bytes
            .saturating_sub(state.total_bytes);
        if response
            .content_length()
            .is_some_and(|length| length > remaining as u64)
        {
            return Err(McpError::ResponseTooLarge);
        }
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let Ok(chunk) = chunk else {
                state.buffer.clear();
                return Ok(SseReadOutcome::Disconnected);
            };
            state.total_bytes = state.total_bytes.saturating_add(chunk.len());
            if state.total_bytes > self.config.max_response_bytes {
                return Err(McpError::ResponseTooLarge);
            }
            state.buffer.extend_from_slice(&chunk);
            while let Some(frame) = take_sse_frame(&mut state.buffer) {
                let frame = parse_sse_frame(&frame)?;
                if let Some(event_id) = frame.event_id {
                    state.last_event_id = Some(event_id);
                }
                if let Some(retry_delay) = frame.retry_delay {
                    state.retry_delay = Some(retry_delay);
                }
                let Some(payload) = frame.payload else {
                    continue;
                };
                let value = serde_json::from_str::<Value>(&payload)
                    .map_err(|_| McpError::MalformedResponse)?;
                if value.get("id").is_some() {
                    decode_rpc_result(&value, expected_id)?;
                    return Ok(SseReadOutcome::Result(value));
                }
                if !valid_sse_notification(&value) {
                    return Err(McpError::MalformedResponse);
                }
            }
            if state.buffer.len() > self.config.max_response_bytes {
                return Err(McpError::ResponseTooLarge);
            }
        }
        state.buffer.clear();
        Ok(SseReadOutcome::Disconnected)
    }

    async fn get_sse(
        &self,
        current_session: Option<&McpSession>,
        protocol_version: &str,
        session_id: Option<&str>,
        last_event_id: Option<&str>,
    ) -> Result<Response, McpError> {
        let mut request = self
            .client
            .get(self.config.endpoint.clone())
            .header(ACCEPT, "text/event-stream")
            .header("mcp-protocol-version", protocol_version);
        if let Some(last_event_id) = last_event_id {
            let last_event_id =
                HeaderValue::from_str(last_event_id).map_err(|_| McpError::MalformedResponse)?;
            request = request.header("last-event-id", last_event_id);
        }
        if let Some(bearer_token) = &self.config.bearer_token {
            request = request.bearer_auth(bearer_token);
        }
        if let Some(session_id) = session_id {
            let session_id =
                HeaderValue::from_str(session_id).map_err(|_| McpError::MalformedResponse)?;
            request = request.header("mcp-session-id", session_id);
        }
        let response = request.send().await.map_err(|_| McpError::Transport)?;
        if response.status() == StatusCode::NOT_FOUND && session_id.is_some() {
            if let Some(current_session) = current_session {
                self.clear_expired_session(current_session).await;
            }
            return Err(McpError::SessionExpired);
        }
        if response.status() == StatusCode::METHOD_NOT_ALLOWED {
            return Err(McpError::SseStreamTerminated);
        }
        if !response.status().is_success() {
            return Err(McpError::HttpStatus(response.status()));
        }
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_ascii_lowercase)
            .ok_or(McpError::UnsupportedResponseContentType)?;
        if !content_type.starts_with("text/event-stream") {
            return Err(McpError::UnsupportedResponseContentType);
        }
        Ok(response)
    }
}

/// One untrusted JSON-RPC notification received from an explicit MCP HTTP SSE `GET` stream.
///
/// Receiving this value does not invoke any tool, mutate local context, or add data to a model
/// request. The application chooses whether the declared method is meaningful and applies its
/// own authorization, validation, redaction, and bounded handling policy.
#[derive(Clone, Eq, PartialEq)]
pub struct McpServerNotification {
    method: String,
    params: Option<Value>,
}

impl McpServerNotification {
    fn from_wire(value: &Value) -> Result<Self, McpError> {
        if !valid_sse_notification(value) {
            return Err(McpError::MalformedResponse);
        }
        let method = value
            .get("method")
            .and_then(Value::as_str)
            .filter(|method| valid_sse_notification_method(method))
            .ok_or(McpError::MalformedResponse)?
            .to_owned();
        Ok(Self {
            method,
            params: value.get("params").cloned(),
        })
    }

    /// Returns the remote JSON-RPC notification method.
    #[must_use]
    pub fn method(&self) -> &str {
        &self.method
    }

    /// Returns untrusted remote notification parameters without interpreting them.
    #[must_use]
    pub const fn params(&self) -> Option<&Value> {
        self.params.as_ref()
    }
}

impl fmt::Debug for McpServerNotification {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerNotification")
            .field("method", &self.method)
            .field("has_params", &self.params.is_some())
            .finish()
    }
}

/// An explicit standalone MCP Streamable HTTP SSE `GET` listener.
///
/// Use [`Self::next_notification`] to read one remote notification at a time. Dropping this
/// value closes the HTTP stream without closing the MCP session. Server-initiated requests and
/// responses are rejected instead of being executed or answered automatically.
pub struct McpServerEventStream {
    client: McpHttpClient,
    session: McpSession,
    response: Option<Response>,
    buffer: Vec<u8>,
    last_event_id: Option<String>,
    retry_delay: Option<Duration>,
    resume_attempts: usize,
    closed: bool,
}

impl McpServerEventStream {
    /// Reads the next standalone server notification.
    ///
    /// A normal connection close returns [`McpError::SseStreamTerminated`] unless the stream
    /// supplied an event ID and the client explicitly enabled bounded SSE resumption. Resumption
    /// uses only `GET` plus `Last-Event-ID`; it never emits a JSON-RPC `POST`.
    ///
    /// # Errors
    ///
    /// Returns a sanitized protocol, transport, session, byte-limit, or termination failure.
    pub async fn next_notification(&mut self) -> Result<McpServerNotification, McpError> {
        if self.closed {
            return Err(McpError::SseStreamTerminated);
        }
        loop {
            if let Some(frame) = take_sse_frame(&mut self.buffer) {
                let frame = match parse_sse_frame(&frame) {
                    Ok(frame) => frame,
                    Err(error) => return Err(self.close_with(error)),
                };
                if let Some(event_id) = frame.event_id {
                    self.last_event_id = Some(event_id);
                }
                if let Some(retry_delay) = frame.retry_delay {
                    self.retry_delay = Some(retry_delay);
                }
                let Some(payload) = frame.payload else {
                    continue;
                };
                let Ok(value) = serde_json::from_str::<Value>(&payload) else {
                    return Err(self.close_with(McpError::MalformedResponse));
                };
                return McpServerNotification::from_wire(&value)
                    .map_err(|error| self.close_with(error));
            }

            if self.buffer.len() > self.client.config.max_response_bytes {
                return Err(self.close_with(McpError::ResponseTooLarge));
            }
            let next_chunk = match self.response.as_mut() {
                Some(response) => response.chunk().await,
                None => return Err(self.close_with(McpError::SseStreamTerminated)),
            };
            match next_chunk {
                Ok(Some(chunk)) => {
                    if self.buffer.len().saturating_add(chunk.len())
                        > self.client.config.max_response_bytes
                    {
                        return Err(self.close_with(McpError::ResponseTooLarge));
                    }
                    self.buffer.extend_from_slice(&chunk);
                }
                Ok(None) | Err(_) => self.resume_after_disconnect().await?,
            }
        }
    }

    /// Stops reading this SSE connection without closing the MCP session.
    pub fn close(mut self) {
        self.closed = true;
        self.response = None;
        self.buffer.clear();
    }

    fn close_with(&mut self, error: McpError) -> McpError {
        self.closed = true;
        self.response = None;
        self.buffer.clear();
        error
    }

    async fn resume_after_disconnect(&mut self) -> Result<(), McpError> {
        self.response = None;
        self.buffer.clear();
        let Some(resumption) = self.client.config.automatic_sse_resumption else {
            return Err(self.close_with(McpError::SseStreamTerminated));
        };
        let Some(last_event_id) = self.last_event_id.clone() else {
            return Err(self.close_with(McpError::SseStreamTerminated));
        };
        if self.resume_attempts >= resumption.max_attempts {
            return Err(self.close_with(McpError::SseStreamTerminated));
        }
        let delay = self
            .retry_delay
            .unwrap_or_else(|| resumption.delay_for(self.resume_attempts));
        if delay > resumption.max_backoff {
            return Err(self.close_with(McpError::SseRetryLimit));
        }
        sleep(delay).await;
        let client = self.client.clone();
        let session = self.session.clone();
        let response = client
            .get_sse(
                Some(&session),
                &session.protocol_version,
                session.id.as_deref(),
                Some(&last_event_id),
            )
            .await;
        match response {
            Ok(response) => {
                self.response = Some(response);
                self.resume_attempts += 1;
                Ok(())
            }
            Err(error) => Err(self.close_with(error)),
        }
    }
}

impl fmt::Debug for McpServerEventStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerEventStream")
            .field("has_event_id", &self.last_event_id.is_some())
            .field("resume_attempts", &self.resume_attempts)
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for McpHttpClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpHttpClient")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

struct McpReply {
    result: Value,
    session_id: Option<String>,
}

/// A discovered MCP tool that has not yet been advertised or approved for execution.
#[derive(Clone, Eq, PartialEq)]
pub struct McpToolDefinition {
    definition: ToolDefinition,
    description: Option<String>,
}

impl McpToolDefinition {
    fn from_wire(value: &Value) -> Result<Self, McpToolDefinitionError> {
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
            definition: discovered.definition,
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

/// Sanitized MCP adapter failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum McpError {
    /// The local HTTP client could not be initialized.
    #[error("MCP HTTP client could not be initialized")]
    Client,
    /// The remote endpoint did not respond within the bounded transport contract.
    #[error("MCP transport request failed")]
    Transport,
    /// The remote endpoint returned an unsuccessful HTTP status.
    #[error("MCP endpoint returned HTTP status {0}")]
    HttpStatus(StatusCode),
    /// A request response was neither JSON nor Streamable HTTP SSE.
    #[error("MCP endpoint returned an unsupported response content type")]
    UnsupportedResponseContentType,
    /// The remote body exceeded the configured in-memory response limit.
    #[error("MCP response exceeded the configured byte limit")]
    ResponseTooLarge,
    /// An SSE response ended before it returned the matching JSON-RPC result.
    #[error("MCP SSE response ended before the JSON-RPC result")]
    SseStreamTerminated,
    /// A remote SSE retry delay exceeded the caller's configured finite limit.
    #[error("MCP SSE retry delay exceeded the configured limit")]
    SseRetryLimit,
    /// A stateful endpoint returned 404 for the current MCP session.
    #[error("MCP session expired; initialize a new session before retrying")]
    SessionExpired,
    /// The configured stdio MCP server could not be started.
    #[error("MCP stdio server could not be started")]
    StdioSpawn,
    /// The stdio server closed stdout before the matching JSON-RPC result.
    #[error("MCP stdio server ended before the JSON-RPC result")]
    StdioTerminated,
    /// The stdio server sent too many messages before the matching JSON-RPC result.
    #[error("MCP stdio server exceeded the interleaved-message limit")]
    StdioMessageLimit,
    /// The stdio server did not complete one request by its configured deadline.
    #[error("MCP stdio server did not respond before the configured deadline")]
    StdioTimeout,
    /// The stdio server could not be reaped after a bounded forced termination.
    #[error("MCP stdio server did not terminate before the configured shutdown deadline")]
    StdioShutdownTimeout,
    /// A local JSON-RPC request exceeded its configured stdio byte limit.
    #[error("MCP stdio request exceeded the configured byte limit")]
    StdioRequestTooLarge,
    /// A JSON-RPC response or tool discovery record was invalid.
    #[error("MCP response violated the expected protocol contract")]
    MalformedResponse,
    /// The endpoint selected a different MCP protocol version.
    #[error("MCP endpoint did not select the supported protocol version")]
    ProtocolVersion,
    /// The endpoint returned a JSON-RPC error without exposing its remote detail.
    #[error("MCP endpoint rejected the JSON-RPC request")]
    RemoteError,
    /// A remote tool returned an error result without exposing its remote detail.
    #[error("MCP tool execution failed")]
    ToolExecutionFailed,
    /// A tool call or discovery was attempted before successful initialization.
    #[error("MCP client must be initialized before discovering or calling tools")]
    NotInitialized,
    /// Tool pagination or total discovery exceeded its configured bound.
    #[error("MCP tool discovery exceeded the configured limit")]
    ToolDiscoveryLimit,
    /// The server did not advertise the requested resources or prompts capability.
    #[error("MCP server did not advertise the requested context capability")]
    UnsupportedCapability,
    /// A local resource URI, prompt name, or prompt argument was invalid or exceeded its bound.
    #[error("MCP context request was invalid or exceeded the configured limit")]
    InvalidContextRequest,
    /// Context discovery, message count, or decoded content exceeded its configured bound.
    #[error("MCP context exceeded the configured item or content limit")]
    ContextLimit,
}

fn decode_rpc_result(value: &Value, id: u64) -> Result<Value, McpError> {
    if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || value.get("id").and_then(Value::as_u64) != Some(id)
    {
        return Err(McpError::MalformedResponse);
    }
    if value.get("error").is_some() {
        return Err(McpError::RemoteError);
    }
    value
        .get("result")
        .cloned()
        .ok_or(McpError::MalformedResponse)
}

fn decode_tool_result(value: &Value) -> Result<Value, McpError> {
    let content = value
        .get("content")
        .filter(|content| content.is_array())
        .cloned()
        .ok_or(McpError::MalformedResponse)?;
    if value.get("isError").and_then(Value::as_bool) == Some(true) {
        return Err(McpError::ToolExecutionFailed);
    }
    let mut result = serde_json::Map::new();
    result.insert("content".to_owned(), content);
    if let Some(structured_content) = value.get("structuredContent") {
        result.insert("structured_content".to_owned(), structured_content.clone());
    }
    Ok(json!({"mcp": Value::Object(result)}))
}

#[derive(Default)]
struct SseStreamState {
    total_bytes: usize,
    buffer: Vec<u8>,
    last_event_id: Option<String>,
    retry_delay: Option<Duration>,
}

enum SseReadOutcome {
    Result(Value),
    Disconnected,
}

struct SseFrame {
    event_id: Option<String>,
    retry_delay: Option<Duration>,
    payload: Option<String>,
}

fn take_sse_frame(buffer: &mut Vec<u8>) -> Option<Vec<u8>> {
    let delimiter = [(b"\r\n\r\n".as_slice(), 4), (b"\n\n", 2), (b"\r\r", 2)]
        .into_iter()
        .filter_map(|(needle, length)| {
            buffer
                .windows(needle.len())
                .position(|window| window == needle)
                .map(|index| (index, length))
        })
        .min_by_key(|(index, _)| *index)?;
    Some(buffer.drain(..delimiter.0 + delimiter.1).collect())
}

#[cfg(test)]
fn sse_payload(frame: &[u8]) -> Result<Option<String>, McpError> {
    Ok(parse_sse_frame(frame)?.payload)
}

fn parse_sse_frame(frame: &[u8]) -> Result<SseFrame, McpError> {
    let frame = std::str::from_utf8(frame)
        .map_err(|_| McpError::MalformedResponse)?
        .replace("\r\n", "\n")
        .replace('\r', "\n");
    let mut event_id = None;
    let mut retry_delay = None;
    let mut payload = Vec::new();
    for line in frame.lines() {
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "data" => payload.push(value),
            "id" => {
                if !valid_sse_event_id(value) {
                    return Err(McpError::MalformedResponse);
                }
                event_id = Some(value.to_owned());
            }
            "retry" if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) => {
                retry_delay = value.parse::<u64>().ok().map(Duration::from_millis);
            }
            _ => {}
        }
    }
    let payload = (!payload.is_empty())
        .then(|| payload.join("\n"))
        .filter(|payload| !payload.is_empty());
    Ok(SseFrame {
        event_id,
        retry_delay,
        payload,
    })
}

fn valid_sse_event_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SSE_EVENT_ID_BYTES
        && value.bytes().all(|byte| matches!(byte, 0x21..=0x7e))
}

fn valid_sse_notification(value: &Value) -> bool {
    value.get("jsonrpc").and_then(Value::as_str) == Some("2.0")
        && value.get("method").and_then(Value::as_str).is_some()
        && value.get("id").is_none()
        && value.get("result").is_none()
        && value.get("error").is_none()
}

fn valid_sse_notification_method(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SSE_NOTIFICATION_METHOD_BYTES
        && value.bytes().all(|byte| matches!(byte, 0x21..=0x7e))
}

fn valid_endpoint(value: &Url) -> bool {
    matches!(value.scheme(), "http" | "https")
        && value.host().is_some()
        && value.username().is_empty()
        && value.password().is_none()
        && value.query().is_none()
        && value.fragment().is_none()
        && (value.scheme() == "https" || is_loopback_host(value.host().as_ref()))
}

fn is_loopback_host(host: Option<&Host<&str>>) -> bool {
    match host {
        Some(Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(host)) => host.is_loopback(),
        Some(Host::Ipv6(host)) => host.is_loopback(),
        None => false,
    }
}

fn valid_cursor(value: &str) -> bool {
    !value.is_empty() && value.len() <= 1024 && !value.contains('\0')
}

fn paginated_params(cursor: Option<&str>) -> Value {
    let mut params = serde_json::Map::new();
    if let Some(cursor) = cursor {
        params.insert("cursor".to_owned(), Value::String(cursor.to_owned()));
    }
    Value::Object(params)
}

fn next_cursor(result: &Value) -> Result<Option<String>, McpError> {
    match result.get("nextCursor") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(cursor)) if valid_cursor(cursor) => Ok(Some(cursor.clone())),
        Some(_) => Err(McpError::MalformedResponse),
    }
}

fn valid_session_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SESSION_ID_BYTES
        && value.bytes().all(|byte| matches!(byte, 0x21..=0x7e))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap, convert::Infallible, fmt::Write as _, sync::Arc, time::Duration,
    };

    use serde_json::{Value, json};

    use rustee_ai::{
        AiExecutionContext, DenyAllToolApproval, ToolApprovalDecision, ToolApprovalPolicy,
        ToolCall, ToolExecutionContext, ToolRegistry, ToolRisk, ToolRunError,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::Mutex,
    };

    use super::{
        MCP_PROTOCOL_VERSION, McpError, McpHttpClient, McpHttpConfig, McpHttpConfigError,
        McpPromptContent, McpRemoteTool, McpResourceData, sse_payload, take_sse_frame,
    };

    #[test]
    fn configuration_requires_secure_endpoints_and_redacts_credentials() {
        let config = McpHttpConfig::new(url::Url::parse("https://mcp.example.test/tools").unwrap())
            .unwrap()
            .with_bearer_token("mcp-secret")
            .unwrap();
        assert!(!format!("{config:?}").contains("mcp-secret"));
        assert_eq!(
            McpHttpConfig::new(url::Url::parse("http://mcp.example.test/tools").unwrap())
                .unwrap_err(),
            McpHttpConfigError::InvalidEndpoint
        );
        assert_eq!(
            config.clone().with_max_response_bytes(0).unwrap_err(),
            McpHttpConfigError::ZeroResponseLimit
        );
        assert_eq!(
            config.clone().with_context_limits(0, 1).unwrap_err(),
            McpHttpConfigError::ZeroContextLimit
        );
        assert_eq!(
            config
                .clone()
                .with_automatic_session_recovery(
                    0,
                    Duration::from_millis(1),
                    Duration::from_millis(1),
                )
                .unwrap_err(),
            McpHttpConfigError::ZeroSessionRecoveryAttempts
        );
        assert_eq!(
            config
                .with_automatic_session_recovery(
                    1,
                    Duration::from_millis(2),
                    Duration::from_millis(1),
                )
                .unwrap_err(),
            McpHttpConfigError::InvalidSessionRecoveryBackoff
        );
        assert_eq!(
            McpHttpConfig::new(url::Url::parse("https://mcp.example.test/tools").unwrap())
                .unwrap()
                .with_automatic_sse_resumption(
                    0,
                    Duration::from_millis(1),
                    Duration::from_millis(1),
                )
                .unwrap_err(),
            McpHttpConfigError::ZeroSseResumptionAttempts
        );
    }

    #[tokio::test]
    async fn discovery_preserves_dotted_names_but_deny_policy_prevents_remote_call() {
        let (endpoint, server) = server(vec![
            json_reply(1, &initialize_result(), Some("session-a")),
            status_reply(202),
            json_reply(
                2,
                &json!({
                    "tools":[{
                        "name":"orders.lookup.v1",
                        "description":"remote description",
                        "inputSchema":{"type":"object"}
                    }]
                }),
                None,
            ),
        ])
        .await;
        let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
        client.initialize().await.unwrap();
        let discovered = client.list_tools().await.unwrap();
        assert_eq!(discovered[0].name(), "orders.lookup.v1");
        assert!(!format!("{:?}", discovered[0]).contains("remote description"));

        let mut registry = ToolRegistry::new();
        registry
            .register(McpRemoteTool::from_discovery(
                client,
                discovered.into_iter().next().unwrap(),
                ToolRisk::ReadOnly,
            ))
            .unwrap();
        let error = registry
            .execute(
                tool_context(),
                ToolCall::new("call-1", "orders.lookup.v1", json!({"id":7})).unwrap(),
                &DenyAllToolApproval,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, ToolRunError::Denied { .. }));

        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 3);
        assert!(
            requests
                .iter()
                .all(|request| !request.contains("tools/call"))
        );
        assert!(requests[1].contains("mcp-protocol-version: 2025-11-25"));
    }

    #[tokio::test]
    async fn approved_remote_tool_uses_session_and_returns_bounded_untrusted_result() {
        let (endpoint, server) = server(vec![
            json_reply(1, &initialize_result(), Some("session-b")),
            status_reply(202),
            json_reply(
                2,
                &json!({"tools":[{"name":"orders.lookup","inputSchema":{"type":"object"}}]}),
                None,
            ),
            sse_reply(
                3,
                &json!({
                    "content":[{"type":"text","text":"untrusted remote content"}],
                    "structuredContent":{"status":"found"}
                }),
                &[],
            ),
        ])
        .await;
        let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
        client.initialize().await.unwrap();
        let discovered = client.list_tools().await.unwrap().remove(0);
        let mut registry = ToolRegistry::new();
        registry
            .register(
                McpRemoteTool::from_discovery(client, discovered, ToolRisk::ReadOnly)
                    .with_rustee_idempotency_metadata(),
            )
            .unwrap();
        let result = registry
            .execute(
                tool_context(),
                ToolCall::new("call-2", "orders.lookup", json!({"id":7})).unwrap(),
                &Approve,
            )
            .await
            .unwrap();
        assert_eq!(
            result.content(),
            &json!({"mcp":{
                "content":[{"type":"text","text":"untrusted remote content"}],
                "structured_content":{"status":"found"}
            }})
        );

        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 4);
        assert!(requests[3].contains("\"method\":\"tools/call\""));
        assert!(requests[3].contains("\"io.rustee/idempotency-key\":\"external:order:7\""));
        assert!(requests[3].contains("mcp-session-id: session-b"));
    }

    #[tokio::test]
    async fn remote_error_details_are_not_exposed_through_tool_execution() {
        let (endpoint, server) = server(vec![
            json_reply(1, &initialize_result(), None),
            status_reply(202),
            json_reply(
                2,
                &json!({"tools":[{"name":"orders.fail","inputSchema":{"type":"object"}}]}),
                None,
            ),
            json_reply(
                3,
                &json!({"content":[{"type":"text","text":"private server detail"}],"isError":true}),
                None,
            ),
        ])
        .await;
        let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
        client.initialize().await.unwrap();
        let discovered = client.list_tools().await.unwrap().remove(0);
        let mut registry = ToolRegistry::new();
        registry
            .register(McpRemoteTool::from_discovery(
                client,
                discovered,
                ToolRisk::Privileged,
            ))
            .unwrap();
        let error = registry
            .execute(
                tool_context(),
                ToolCall::new("call-3", "orders.fail", json!({})).unwrap(),
                &Approve,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, ToolRunError::Execution(_)));
        assert!(!error.to_string().contains("private server detail"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn discovery_rejects_oversized_response_before_retaining_remote_data() {
        let (endpoint, server) = server(vec![
            json_reply(
                1,
                &initialize_result(),
                None,
            ),
            status_reply(202),
            json_reply(
                2,
                &json!({
                    "tools":[{
                        "name":"orders.large",
                        "description":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                        "inputSchema":{"type":"object"}
                    }]
                }),
                None,
            ),
        ])
        .await;
        let config = McpHttpConfig::new(endpoint)
            .unwrap()
            .with_max_response_bytes(256)
            .unwrap();
        let client = McpHttpClient::new(config).unwrap();
        client.initialize().await.unwrap();

        assert_eq!(
            client.list_tools().await.unwrap_err(),
            McpError::ResponseTooLarge
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn discovery_stops_at_the_configured_pagination_bound() {
        let (endpoint, server) = server(vec![
            json_reply(1, &initialize_result(), None),
            status_reply(202),
            json_reply(
                2,
                &json!({
                    "tools":[{"name":"orders.page","inputSchema":{"type":"object"}}],
                    "nextCursor":"next-page"
                }),
                None,
            ),
        ])
        .await;
        let config = McpHttpConfig::new(endpoint)
            .unwrap()
            .with_tool_discovery_limits(1, 8)
            .unwrap();
        let client = McpHttpClient::new(config).unwrap();
        client.initialize().await.unwrap();

        assert_eq!(
            client.list_tools().await.unwrap_err(),
            McpError::ToolDiscoveryLimit
        );
        server.await.unwrap();
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn context_discovery_and_reads_stay_explicit_and_bounded() {
        let (endpoint, server) = server(vec![
            json_reply(1, &context_initialize_result(), Some("context-session")),
            status_reply(202),
            json_reply(
                2,
                &json!({
                    "resources":[{
                        "uri":"resource://tenant-a/customer/7",
                        "name":"customer-record",
                        "mimeType":"text/plain",
                        "size":31
                    }],
                    "nextCursor":"resource-page-2"
                }),
                None,
            ),
            json_reply(3, &json!({"resources":[]}), None),
            json_reply(
                4,
                &json!({
                    "resourceTemplates":[{
                        "uriTemplate":"resource://tenant-a/customer/{id}",
                        "name":"customer-by-id",
                        "mimeType":"text/plain"
                    }]
                }),
                None,
            ),
            json_reply(
                5,
                &json!({
                    "contents":[{
                        "uri":"resource://tenant-a/customer/7",
                        "mimeType":"text/plain",
                        "text":"private customer context"
                    }]
                }),
                None,
            ),
            json_reply(
                6,
                &json!({
                    "prompts":[{
                        "name":"customer-summary",
                        "arguments":[{"name":"customer_id","required":true}]
                    }]
                }),
                None,
            ),
            json_reply(
                7,
                &json!({
                    "description":"Summarize one selected customer record.",
                    "messages":[
                        {"role":"user","content":{"type":"text","text":"Summarize the selected customer."}},
                        {"role":"assistant","content":{"type":"resource_link","uri":"resource://tenant-a/customer/7","name":"customer-record","mimeType":"text/plain"}}
                    ]
                }),
                None,
            ),
        ])
        .await;
        let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
        client.initialize().await.unwrap();

        let resources = client.list_resources().await.unwrap();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].name(), "customer-record");
        let templates = client.list_resource_templates().await.unwrap();
        assert_eq!(
            templates[0].uri_template(),
            "resource://tenant-a/customer/{id}"
        );

        let contents = client.read_resource(resources[0].uri()).await.unwrap();
        assert!(matches!(
            contents[0].data(),
            McpResourceData::Text(text) if text == "private customer context"
        ));
        assert!(!format!("{:?}", contents[0]).contains("private customer context"));

        let prompts = client.list_prompts().await.unwrap();
        assert!(prompts[0].arguments()[0].required());
        let arguments = BTreeMap::from([("customer_id".to_owned(), "7".to_owned())]);
        let prompt = client
            .get_prompt("customer-summary", &arguments)
            .await
            .unwrap();
        assert_eq!(prompt.messages().len(), 2);
        assert!(matches!(
            prompt.messages()[0].content(),
            McpPromptContent::Text(text) if text == "Summarize the selected customer."
        ));
        assert!(!format!("{prompt:?}").contains("Summarize the selected customer."));

        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 8);
        assert!(requests[2].contains("\"method\":\"resources/list\""));
        assert!(requests[3].contains("\"cursor\":\"resource-page-2\""));
        assert!(requests[4].contains("\"method\":\"resources/templates/list\""));
        assert!(requests[5].contains("\"method\":\"resources/read\""));
        assert!(requests[6].contains("\"method\":\"prompts/list\""));
        assert!(requests[7].contains("\"method\":\"prompts/get\""));
        assert!(requests[7].contains("\"customer_id\":\"7\""));
        assert!(
            requests[1..]
                .iter()
                .all(|request| request.contains("mcp-session-id: context-session"))
        );
        assert!(
            requests
                .iter()
                .all(|request| !request.contains("tools/call"))
        );
    }

    #[tokio::test]
    async fn context_capability_gate_does_not_send_an_unsupported_request() {
        let (endpoint, server) = server(vec![
            json_reply(1, &initialize_result(), None),
            status_reply(202),
        ])
        .await;
        let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
        client.initialize().await.unwrap();

        assert_eq!(
            client.list_resources().await.unwrap_err(),
            McpError::UnsupportedCapability
        );
        assert_eq!(
            client.list_prompts().await.unwrap_err(),
            McpError::UnsupportedCapability
        );

        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 2);
    }

    #[tokio::test]
    async fn initialization_rejects_missing_required_server_capabilities() {
        let (endpoint, server) = server(vec![json_reply(
            1,
            &json!({
                "protocolVersion":MCP_PROTOCOL_VERSION,
                "serverInfo":{"name":"fixture","version":"0.1.0"}
            }),
            None,
        )])
        .await;
        let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();

        assert_eq!(
            client.initialize().await.unwrap_err(),
            McpError::MalformedResponse
        );
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].contains("\"method\":\"initialize\""));
    }

    #[tokio::test]
    async fn session_expiry_requires_reinitialization_without_replaying_the_request() {
        let (endpoint, server) = server(vec![
            json_reply(1, &initialize_result(), Some("session-old")),
            status_reply(202),
            not_found_reply(),
            json_reply(3, &initialize_result(), Some("session-new")),
            status_reply(202),
            json_reply(
                4,
                &json!({"tools":[{"name":"orders.recovered","inputSchema":{"type":"object"}}]}),
                None,
            ),
        ])
        .await;
        let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
        client.initialize().await.unwrap();

        assert_eq!(
            client.list_tools().await.unwrap_err(),
            McpError::SessionExpired
        );
        client.initialize().await.unwrap();
        let tools = client.list_tools().await.unwrap();
        assert_eq!(tools[0].name(), "orders.recovered");

        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 6);
        assert!(!requests[3].contains("mcp-session-id"));
        assert!(requests[4].contains("mcp-session-id: session-new"));
        assert!(requests[5].contains("mcp-session-id: session-new"));
    }

    #[tokio::test]
    async fn automatic_session_recovery_prepares_a_new_session_without_replaying_the_request() {
        let (endpoint, server) = server(vec![
            json_reply(1, &initialize_result(), Some("session-old")),
            status_reply(202),
            not_found_reply(),
            json_reply(1, &initialize_result(), Some("session-new")),
            status_reply(202),
            json_reply(
                2,
                &json!({"tools":[{"name":"orders.recovered","inputSchema":{"type":"object"}}]}),
                None,
            ),
        ])
        .await;
        let config = McpHttpConfig::new(endpoint)
            .unwrap()
            .with_automatic_session_recovery(1, Duration::from_millis(1), Duration::from_millis(1))
            .unwrap();
        let client = McpHttpClient::new(config).unwrap();
        client.initialize().await.unwrap();

        assert_eq!(
            client.list_tools().await.unwrap_err(),
            McpError::SessionExpired
        );
        let recovered = client.list_tools().await.unwrap();
        assert_eq!(recovered[0].name(), "orders.recovered");

        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 6);
        assert!(requests[2].contains("\"method\":\"tools/list\""));
        assert!(requests[2].contains("mcp-session-id: session-old"));
        assert!(requests[3].contains("\"method\":\"initialize\""));
        assert!(!requests[3].contains("mcp-session-id"));
        assert!(requests[4].contains("\"method\":\"notifications/initialized\""));
        assert!(requests[4].contains("mcp-session-id: session-new"));
        assert!(requests[5].contains("\"method\":\"tools/list\""));
        assert!(requests[5].contains("mcp-session-id: session-new"));
    }

    #[tokio::test]
    async fn session_expiry_never_replays_an_approved_remote_tool_call() {
        let (endpoint, server) = server(vec![
            json_reply(1, &initialize_result(), Some("session-old")),
            status_reply(202),
            json_reply(
                2,
                &json!({"tools":[{"name":"orders.expired","inputSchema":{"type":"object"}}]}),
                None,
            ),
            not_found_reply(),
        ])
        .await;
        let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
        client.initialize().await.unwrap();
        let discovered = client.list_tools().await.unwrap().remove(0);
        let mut registry = ToolRegistry::new();
        registry
            .register(McpRemoteTool::from_discovery(
                client,
                discovered,
                ToolRisk::Privileged,
            ))
            .unwrap();

        let error = registry
            .execute(
                tool_context(),
                ToolCall::new("call-expired", "orders.expired", json!({"id":7})).unwrap(),
                &Approve,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, ToolRunError::Execution(_)));

        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 4);
        assert!(requests[3].contains("\"method\":\"tools/call\""));
    }

    #[tokio::test]
    async fn closing_a_session_handles_a_stateless_405_and_clears_local_state() {
        let (endpoint, server) = server(vec![
            json_reply(1, &initialize_result(), Some("session-close")),
            status_reply(202),
            status_reply(405),
        ])
        .await;
        let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
        client.initialize().await.unwrap();
        client.close_session().await.unwrap();
        assert_eq!(
            client.list_tools().await.unwrap_err(),
            McpError::NotInitialized
        );

        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests[2].starts_with("delete /mcp http/1.1\r\n"));
        assert!(requests[2].contains("mcp-protocol-version: 2025-11-25"));
        assert!(requests[2].contains("mcp-session-id: session-close"));
    }

    #[tokio::test]
    async fn discovery_accepts_a_bounded_sse_result_after_a_notification() {
        let notification = json!({
            "jsonrpc":"2.0",
            "method":"notifications/message",
            "params":{"level":"info"}
        });
        let (endpoint, server) = server(vec![
            json_reply(1, &initialize_result(), None),
            status_reply(202),
            sse_reply(
                2,
                &json!({"tools":[{"name":"orders.sse","inputSchema":{"type":"object"}}]}),
                &[notification],
            ),
        ])
        .await;
        let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
        client.initialize().await.unwrap();

        let tools = client.list_tools().await.unwrap();
        assert_eq!(tools[0].name(), "orders.sse");

        let requests = server.await.unwrap();
        assert!(requests[2].contains("accept: application/json, text/event-stream"));
    }

    #[tokio::test]
    async fn sse_resumption_uses_get_last_event_id_without_replaying_the_post() {
        let (endpoint, server) = server(vec![
            json_reply(1, &initialize_result(), Some("session-resume")),
            status_reply(202),
            sse_body_reply("id: stream-cursor-1\nretry: 1\ndata:\n\n"),
            sse_reply(
                2,
                &json!({"tools":[{"name":"orders.resumed","inputSchema":{"type":"object"}}]}),
                &[],
            ),
        ])
        .await;
        let config = McpHttpConfig::new(endpoint)
            .unwrap()
            .with_automatic_sse_resumption(1, Duration::from_millis(2), Duration::from_millis(5))
            .unwrap();
        let client = McpHttpClient::new(config).unwrap();
        client.initialize().await.unwrap();

        let tools = client.list_tools().await.unwrap();
        assert_eq!(tools[0].name(), "orders.resumed");

        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 4);
        assert!(requests[2].starts_with("post /mcp http/1.1\r\n"));
        assert!(requests[2].contains("\"method\":\"tools/list\""));
        assert!(requests[3].starts_with("get /mcp http/1.1\r\n"));
        assert!(requests[3].contains("accept: text/event-stream"));
        assert!(requests[3].contains("last-event-id: stream-cursor-1"));
        assert!(requests[3].contains("mcp-session-id: session-resume"));
        assert!(requests[3].contains("mcp-protocol-version: 2025-11-25"));
    }

    #[tokio::test]
    async fn sse_resumption_stops_after_its_bounded_get_attempt_without_replaying_the_post() {
        let (endpoint, server) = server(vec![
            json_reply(1, &initialize_result(), None),
            status_reply(202),
            sse_body_reply("id: stream-cursor-1\ndata:\n\n"),
            sse_body_reply("id: stream-cursor-2\ndata:\n\n"),
        ])
        .await;
        let config = McpHttpConfig::new(endpoint)
            .unwrap()
            .with_automatic_sse_resumption(1, Duration::from_millis(1), Duration::from_millis(1))
            .unwrap();
        let client = McpHttpClient::new(config).unwrap();
        client.initialize().await.unwrap();

        assert_eq!(
            client.list_tools().await.unwrap_err(),
            McpError::SseStreamTerminated
        );

        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 4);
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.contains("\"method\":\"tools/list\""))
                .count(),
            1
        );
        assert!(requests[3].starts_with("get /mcp http/1.1\r\n"));
        assert!(requests[3].contains("last-event-id: stream-cursor-1"));
    }

    #[tokio::test]
    async fn sse_resumption_session_expiry_clears_state_without_replaying_the_post() {
        let (endpoint, server) = server(vec![
            json_reply(1, &initialize_result(), Some("session-expired")),
            status_reply(202),
            sse_body_reply("id: stream-cursor-1\ndata:\n\n"),
            not_found_reply(),
        ])
        .await;
        let config = McpHttpConfig::new(endpoint)
            .unwrap()
            .with_automatic_sse_resumption(1, Duration::from_millis(1), Duration::from_millis(1))
            .unwrap();
        let client = McpHttpClient::new(config).unwrap();
        client.initialize().await.unwrap();

        assert_eq!(
            client.list_tools().await.unwrap_err(),
            McpError::SessionExpired
        );
        assert_eq!(
            client.list_tools().await.unwrap_err(),
            McpError::NotInitialized
        );

        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 4);
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.contains("\"method\":\"tools/list\""))
                .count(),
            1
        );
        assert!(requests[3].starts_with("get /mcp http/1.1\r\n"));
        assert!(requests[3].contains("mcp-session-id: session-expired"));
    }

    #[tokio::test]
    async fn standalone_sse_get_delivers_only_explicit_untrusted_notifications() {
        let notification = json!({
            "jsonrpc":"2.0",
            "method":"notifications/resources/list_changed",
            "params":{"private":"customer-7"}
        });
        let (endpoint, server) = server(vec![
            json_reply(1, &initialize_result(), Some("session-events")),
            status_reply(202),
            sse_body_reply(&format!("id: event-1\ndata: {notification}\n\n")),
        ])
        .await;
        let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
        client.initialize().await.unwrap();

        let mut stream = client.open_server_event_stream().await.unwrap();
        let received = stream.next_notification().await.unwrap();
        assert_eq!(received.method(), "notifications/resources/list_changed");
        assert_eq!(received.params(), notification.get("params"));
        assert!(!format!("{received:?}").contains("customer-7"));
        stream.close();

        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests[2].starts_with("get /mcp http/1.1\r\n"));
        assert!(requests[2].contains("accept: text/event-stream"));
        assert!(requests[2].contains("mcp-session-id: session-events"));
        assert!(requests[2].contains("mcp-protocol-version: 2025-11-25"));
    }

    #[tokio::test]
    async fn standalone_sse_get_resumes_with_last_event_id_without_a_post() {
        let first = json!({
            "jsonrpc":"2.0",
            "method":"notifications/progress",
            "params":{"progress":1}
        });
        let second = json!({
            "jsonrpc":"2.0",
            "method":"notifications/progress",
            "params":{"progress":2}
        });
        let (endpoint, server) = server(vec![
            json_reply(1, &initialize_result(), Some("session-events")),
            status_reply(202),
            sse_body_reply(&format!("id: event-1\nretry: 1\ndata: {first}\n\n")),
            sse_body_reply(&format!("id: event-2\ndata: {second}\n\n")),
        ])
        .await;
        let config = McpHttpConfig::new(endpoint)
            .unwrap()
            .with_automatic_sse_resumption(1, Duration::from_millis(2), Duration::from_millis(5))
            .unwrap();
        let client = McpHttpClient::new(config).unwrap();
        client.initialize().await.unwrap();

        let mut stream = client.open_server_event_stream().await.unwrap();
        assert_eq!(
            stream.next_notification().await.unwrap().params(),
            first.get("params")
        );
        assert_eq!(
            stream.next_notification().await.unwrap().params(),
            second.get("params")
        );

        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 4);
        assert!(requests[2].starts_with("get /mcp http/1.1\r\n"));
        assert!(!requests[2].contains("last-event-id:"));
        assert!(requests[3].starts_with("get /mcp http/1.1\r\n"));
        assert!(requests[3].contains("last-event-id: event-1"));
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.starts_with("post /mcp http/1.1\r\n"))
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn standalone_sse_get_rejects_server_requests_without_exposing_details() {
        let server_request = json!({
            "jsonrpc":"2.0",
            "id":"private-server-request",
            "method":"sampling/createMessage",
            "params":{"secret":"do not expose"}
        });
        let (endpoint, server) = server(vec![
            json_reply(1, &initialize_result(), None),
            status_reply(202),
            sse_body_reply(&format!("data: {server_request}\n\n")),
        ])
        .await;
        let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
        client.initialize().await.unwrap();

        let mut stream = client.open_server_event_stream().await.unwrap();
        let error = stream.next_notification().await.unwrap_err();
        assert_eq!(error, McpError::MalformedResponse);
        assert!(!error.to_string().contains("private-server-request"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn standalone_sse_get_session_expiry_clears_local_state() {
        let (endpoint, server) = server(vec![
            json_reply(1, &initialize_result(), Some("session-events")),
            status_reply(202),
            not_found_reply(),
        ])
        .await;
        let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
        client.initialize().await.unwrap();

        assert_eq!(
            client.open_server_event_stream().await.unwrap_err(),
            McpError::SessionExpired
        );
        assert_eq!(
            client.open_server_event_stream().await.unwrap_err(),
            McpError::NotInitialized
        );

        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests[2].starts_with("get /mcp http/1.1\r\n"));
        assert!(requests[2].contains("mcp-session-id: session-events"));
    }

    #[tokio::test]
    async fn standalone_sse_get_treats_a_server_405_as_a_closed_stream() {
        let (endpoint, server) = server(vec![
            json_reply(1, &initialize_result(), None),
            status_reply(202),
            "HTTP/1.1 405 Method Not Allowed\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                .to_owned(),
        ])
        .await;
        let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
        client.initialize().await.unwrap();

        assert_eq!(
            client.open_server_event_stream().await.unwrap_err(),
            McpError::SseStreamTerminated
        );
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests[2].starts_with("get /mcp http/1.1\r\n"));
    }

    #[tokio::test]
    async fn sse_response_bound_and_missing_terminal_result_are_sanitized() {
        let oversized = format!("data: {}", "x".repeat(512));
        let (endpoint, oversized_server) = server(vec![
            json_reply(1, &initialize_result(), None),
            status_reply(202),
            sse_body_reply(&oversized),
        ])
        .await;
        let config = McpHttpConfig::new(endpoint)
            .unwrap()
            .with_max_response_bytes(256)
            .unwrap();
        let client = McpHttpClient::new(config).unwrap();
        client.initialize().await.unwrap();
        assert_eq!(
            client.list_tools().await.unwrap_err(),
            McpError::ResponseTooLarge
        );
        oversized_server.await.unwrap();

        let (endpoint, server) = server(vec![
            json_reply(1, &initialize_result(), None),
            status_reply(202),
            sse_body_reply("event: message\n\n: keepalive\n\n"),
        ])
        .await;
        let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
        client.initialize().await.unwrap();
        assert_eq!(
            client.list_tools().await.unwrap_err(),
            McpError::SseStreamTerminated
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn sse_server_request_is_rejected_without_exposing_its_detail() {
        let server_request = json!({
            "jsonrpc":"2.0",
            "id":"private-server-request",
            "method":"sampling/createMessage",
            "params":{"secret":"do not expose"}
        });
        let (endpoint, server) = server(vec![
            json_reply(1, &initialize_result(), None),
            status_reply(202),
            sse_body_reply(&format!("data: {server_request}\n\n")),
        ])
        .await;
        let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
        client.initialize().await.unwrap();

        let error = client.list_tools().await.unwrap_err();
        assert_eq!(error, McpError::MalformedResponse);
        assert!(!error.to_string().contains("private-server-request"));
        server.await.unwrap();
    }

    #[test]
    fn sse_frame_parser_handles_crlf_multiline_data_and_ignores_non_data_events() {
        let mut frame_buffer = b"event: message\r\ndata: {\"jsonrpc\":\"2.0\",\r\ndata: \"method\":\"notifications/progress\"}\r\n\r\n".to_vec();
        let frame = take_sse_frame(&mut frame_buffer).unwrap();
        assert_eq!(
            sse_payload(&frame).unwrap(),
            Some("{\"jsonrpc\":\"2.0\",\n\"method\":\"notifications/progress\"}".to_owned())
        );
        assert!(
            sse_payload(b"event: message\nretry: 10\n\n")
                .unwrap()
                .is_none()
        );
        let mut cr_only_buffer = b"data: {\"jsonrpc\":\"2.0\"}\r\r".to_vec();
        let frame = take_sse_frame(&mut cr_only_buffer).unwrap();
        assert_eq!(
            sse_payload(&frame).unwrap(),
            Some("{\"jsonrpc\":\"2.0\"}".to_owned())
        );
    }

    #[test]
    fn malformed_mcp_errors_stay_sanitized() {
        assert_eq!(
            McpError::RemoteError.to_string(),
            "MCP endpoint rejected the JSON-RPC request"
        );
    }

    #[derive(Clone, Copy)]
    struct Approve;

    impl ToolApprovalPolicy for Approve {
        type Error = Infallible;

        fn approve(
            &self,
            _: AiExecutionContext,
            _: ToolCall,
            _: ToolRisk,
        ) -> futures_util::future::BoxFuture<'static, Result<ToolApprovalDecision, Self::Error>>
        {
            Box::pin(futures_util::future::ready(Ok(
                ToolApprovalDecision::Approved,
            )))
        }
    }

    fn tool_context() -> ToolExecutionContext {
        ToolExecutionContext::new(
            AiExecutionContext::new("tenant-a", "user-7").unwrap(),
            "external:order:7",
        )
        .unwrap()
    }

    async fn server(replies: Vec<String>) -> (url::Url, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint =
            url::Url::parse(&format!("http://{}/mcp", listener.local_addr().unwrap())).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let server = tokio::spawn(async move {
            for reply in replies {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_request(&mut stream).await;
                captured.lock().await.push(request);
                stream.write_all(reply.as_bytes()).await.unwrap();
            }
            captured.lock().await.clone()
        });
        (endpoint, server)
    }

    async fn read_request(stream: &mut tokio::net::TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            let read = stream.read(&mut buffer).await.unwrap();
            assert_ne!(read, 0);
            bytes.extend_from_slice(&buffer[..read]);
            let Some(headers_end) = bytes.windows(4).position(|value| value == b"\r\n\r\n") else {
                continue;
            };
            let headers = std::str::from_utf8(&bytes[..headers_end]).unwrap();
            let content_length = headers
                .lines()
                .find_map(|line| line.strip_prefix("content-length: "))
                .map_or(0, |value| value.parse::<usize>().unwrap());
            if bytes.len() >= headers_end + 4 + content_length {
                return String::from_utf8(bytes).unwrap().to_ascii_lowercase();
            }
        }
    }

    fn json_reply(id: u64, result: &Value, session_id: Option<&str>) -> String {
        let body = json!({"jsonrpc":"2.0","id":id,"result":result}).to_string();
        let session = session_id
            .map(|value| format!("mcp-session-id: {value}\r\n"))
            .unwrap_or_default();
        format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n{session}content-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    fn sse_reply(id: u64, result: &Value, notifications: &[Value]) -> String {
        let mut body = String::new();
        for (index, notification) in notifications.iter().enumerate() {
            write!(
                body,
                "id: notification-{index}\r\nevent: message\r\ndata: {notification}\r\n\r\n"
            )
            .expect("writing to String cannot fail");
        }
        write!(
            body,
            "id: result-{id}\r\nevent: message\r\ndata: {}\r\n\r\n",
            json!({"jsonrpc":"2.0","id":id,"result":result})
        )
        .expect("writing to String cannot fail");
        sse_body_reply(&body)
    }

    fn sse_body_reply(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream; charset=utf-8\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    fn status_reply(status: u16) -> String {
        format!("HTTP/1.1 {status} Accepted\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
    }

    fn not_found_reply() -> String {
        "HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_owned()
    }

    fn initialize_result() -> Value {
        json!({
            "protocolVersion":MCP_PROTOCOL_VERSION,
            "capabilities":{},
            "serverInfo":{"name":"fixture","version":"0.1.0"}
        })
    }

    fn context_initialize_result() -> Value {
        json!({
            "protocolVersion":MCP_PROTOCOL_VERSION,
            "capabilities":{"resources":{},"prompts":{}},
            "serverInfo":{"name":"fixture","version":"0.1.0"}
        })
    }
}
