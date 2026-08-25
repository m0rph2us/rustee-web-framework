//! Stateful Streamable HTTP MCP client initialization, discovery, and session lifecycle.

use std::{
    collections::BTreeSet,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use reqwest::{Client, StatusCode, header::ACCEPT};
use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::{
    McpError, McpHttpConfig,
    context::{McpServerCapabilities, parse_server_capabilities},
    protocol::{McpHeaderValue, decode_tool_result, next_cursor, paginated_params},
    sse::McpServerEventStream,
};

/// MCP protocol version implemented by this client.
pub const MCP_PROTOCOL_VERSION: &str = "2025-11-25";

pub(super) const MAX_SSE_EVENT_ID_BYTES: usize = 512;
pub(super) const MAX_SSE_NOTIFICATION_METHOD_BYTES: usize = 256;

#[derive(Default)]
pub(super) struct McpClientState {
    pub(super) session: Mutex<Option<McpSession>>,
    pub(super) initialize: Mutex<()>,
    pub(super) next_request_id: AtomicU64,
}

#[derive(Clone, Eq, PartialEq)]
pub(super) struct McpSession {
    pub(super) id: Option<McpHeaderValue>,
    pub(super) protocol_version: String,
    pub(super) capabilities: McpServerCapabilities,
}

/// Cloneable client for a single initialized MCP Streamable HTTP endpoint.
#[derive(Clone)]
pub struct McpHttpClient {
    pub(super) client: Client,
    pub(super) config: McpHttpConfig,
    pub(super) state: Arc<McpClientState>,
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
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| McpError::Client)?;
        Ok(Self::with_client(client, config))
    }

    /// Wraps an already-configured client for dependency injection and local protocol tests.
    ///
    /// Each MCP HTTP request still enforces the timeout in `config`. The injected client owns
    /// redirect policy; disable automatic redirects to preserve the configured endpoint boundary.
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
    /// The configured response-byte limit applies cumulatively across the stream and any resumed
    /// `GET` response.
    /// Dropping the returned stream closes only that HTTP stream and does not close the MCP
    /// session.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::NotInitialized`] before a successful initialization, or a sanitized
    /// transport, HTTP, content-type, byte-limit, or session failure.
    pub async fn open_server_event_stream(&self) -> Result<McpServerEventStream, McpError> {
        let session = self.session().await.ok_or(McpError::NotInitialized)?;
        let response = self
            .get_sse(
                Some(&session),
                &session.protocol_version,
                session.id.as_ref(),
                None,
            )
            .await?;
        McpServerEventStream::new(self.clone(), session, response)
    }

    pub(super) async fn initialize_unlocked(&self) -> Result<(), McpError> {
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
            id: reply.session_id,
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
    /// choose a `ToolRisk`, and register a [`crate::McpRemoteTool`] in a Rustee tool registry.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::NotInitialized`] until [`Self::initialize`] succeeds, or a sanitized
    /// protocol/discovery failure when the endpoint violates this bounded contract.
    pub async fn list_tools(&self) -> Result<Vec<crate::McpToolDefinition>, McpError> {
        let session = self.session().await.ok_or(McpError::NotInitialized)?;
        let mut cursor = None;
        let mut cursors = BTreeSet::new();
        let mut names = BTreeSet::new();
        let mut tools = Vec::new();

        for _ in 0..self.config.max_list_pages {
            let result = self
                .request(
                    Some(&session),
                    self.next_request_id(),
                    "tools/list",
                    paginated_params(cursor.as_deref()),
                )
                .await?
                .result;
            let discovered = result
                .get("tools")
                .and_then(Value::as_array)
                .ok_or(McpError::MalformedResponse)?;
            for value in discovered {
                let tool = crate::McpToolDefinition::from_wire(value)
                    .map_err(|_| McpError::MalformedResponse)?;
                if !names.insert(tool.name().to_owned()) {
                    return Err(McpError::MalformedResponse);
                }
                tools.push(tool);
                if tools.len() > self.config.max_tools {
                    return Err(McpError::ToolDiscoveryLimit);
                }
            }
            let Some(next) = next_cursor(&result)? else {
                return Ok(tools);
            };
            if !cursors.insert(next.clone()) {
                return Err(McpError::MalformedResponse);
            }
            cursor = Some(next);
        }
        Err(McpError::ToolDiscoveryLimit)
    }

    pub(crate) async fn call_tool(
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

    pub(super) async fn session(&self) -> Option<McpSession> {
        self.state.session.lock().await.clone()
    }

    pub(super) async fn clear_expired_session(&self, expired: &McpSession) {
        self.clear_session_if_current(expired).await;
    }

    pub(super) async fn clear_session_if_current(&self, current: &McpSession) {
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
        let response = request
            .header("mcp-session-id", session_id.as_header_value())
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

    pub(super) fn next_request_id(&self) -> u64 {
        self.state.next_request_id.fetch_add(1, Ordering::Relaxed) + 1
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
