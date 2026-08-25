use std::{fmt, time::Duration};

use rustee_core::is_valid_http_bearer_value as is_valid_core_http_bearer_value;
use url::{Host, Url};

use crate::recovery::{AutomaticRecovery, AutomaticRecoveryPolicyError};

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_MAX_REQUEST_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_LIST_PAGES: usize = 16;
const DEFAULT_MAX_TOOLS: usize = 128;
const DEFAULT_MAX_CONTEXT_ITEMS: usize = 64;
const DEFAULT_MAX_CONTEXT_BYTES: usize = 512 * 1024;

/// Maximum UTF-8 byte length of an HTTP-header-admissible bearer value for MCP requests.
pub const MAX_HTTP_BEARER_TOKEN_BYTES: usize = 16 * 1024;

/// Returns whether `value` is bounded and can be rendered in an HTTP Bearer header.
///
/// This validates the same header-value boundary used by the MCP HTTP transport. It does not
/// assign provider-specific semantics to an opaque credential.
#[must_use]
pub fn is_valid_http_bearer_value(value: &str) -> bool {
    is_valid_core_http_bearer_value(value, MAX_HTTP_BEARER_TOKEN_BYTES)
}

/// Redacted configuration for one trusted MCP Streamable HTTP endpoint.
#[derive(Clone)]
pub struct McpHttpConfig {
    pub(super) endpoint: Url,
    pub(super) bearer_token: Option<String>,
    pub(super) request_timeout: Duration,
    pub(super) max_request_bytes: usize,
    pub(super) max_response_bytes: usize,
    pub(super) max_list_pages: usize,
    pub(super) max_tools: usize,
    pub(super) max_context_items: usize,
    pub(super) max_context_bytes: usize,
    pub(super) automatic_session_recovery: Option<AutomaticRecovery>,
    pub(super) automatic_sse_resumption: Option<AutomaticRecovery>,
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
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
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
    /// Returns [`McpHttpConfigError::BlankBearerToken`] when the credential is blank or
    /// [`McpHttpConfigError::InvalidBearerToken`] when it cannot be encoded safely in an HTTP
    /// Bearer header.
    pub fn with_bearer_token(
        mut self,
        bearer_token: impl Into<String>,
    ) -> Result<Self, McpHttpConfigError> {
        let bearer_token = bearer_token.into();
        if bearer_token.trim().is_empty() {
            return Err(McpHttpConfigError::BlankBearerToken);
        }
        if !is_valid_http_bearer_value(&bearer_token) {
            return Err(McpHttpConfigError::InvalidBearerToken);
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

    /// Sets the maximum encoded JSON-RPC request bytes sent to the endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`McpHttpConfigError::ZeroRequestLimit`] when `max_request_bytes` is zero.
    pub fn with_max_request_bytes(
        mut self,
        max_request_bytes: usize,
    ) -> Result<Self, McpHttpConfigError> {
        if max_request_bytes == 0 {
            return Err(McpHttpConfigError::ZeroRequestLimit);
        }
        self.max_request_bytes = max_request_bytes;
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
    /// The expired request still returns [`crate::McpError::SessionExpired`]. This option only
    /// attempts a new initialize/notification handshake for a later explicit request and never
    /// replays the request or tool call that encountered the expired session.
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
        self.automatic_session_recovery = Some(
            AutomaticRecovery::new(max_attempts, initial_backoff, max_backoff)
                .map_err(session_recovery_config_error)?,
        );
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
        self.automatic_sse_resumption = Some(
            AutomaticRecovery::new(max_attempts, initial_backoff, max_backoff)
                .map_err(sse_resumption_config_error)?,
        );
        Ok(self)
    }

    /// Returns the explicit remote transport endpoint.
    #[must_use]
    pub const fn endpoint(&self) -> &Url {
        &self.endpoint
    }
}

fn session_recovery_config_error(error: AutomaticRecoveryPolicyError) -> McpHttpConfigError {
    match error {
        AutomaticRecoveryPolicyError::ZeroAttempts => {
            McpHttpConfigError::ZeroSessionRecoveryAttempts
        }
        AutomaticRecoveryPolicyError::AttemptLimit => {
            McpHttpConfigError::SessionRecoveryAttemptLimit
        }
        AutomaticRecoveryPolicyError::ZeroBackoff => McpHttpConfigError::ZeroSessionRecoveryBackoff,
        AutomaticRecoveryPolicyError::InvalidBackoff => {
            McpHttpConfigError::InvalidSessionRecoveryBackoff
        }
        AutomaticRecoveryPolicyError::BackoffLimit => {
            McpHttpConfigError::SessionRecoveryBackoffLimit
        }
    }
}

fn sse_resumption_config_error(error: AutomaticRecoveryPolicyError) -> McpHttpConfigError {
    match error {
        AutomaticRecoveryPolicyError::ZeroAttempts => McpHttpConfigError::ZeroSseResumptionAttempts,
        AutomaticRecoveryPolicyError::AttemptLimit => McpHttpConfigError::SseResumptionAttemptLimit,
        AutomaticRecoveryPolicyError::ZeroBackoff => McpHttpConfigError::ZeroSseResumptionBackoff,
        AutomaticRecoveryPolicyError::InvalidBackoff => {
            McpHttpConfigError::InvalidSseResumptionBackoff
        }
        AutomaticRecoveryPolicyError::BackoffLimit => McpHttpConfigError::SseResumptionBackoffLimit,
    }
}

impl fmt::Debug for McpHttpConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpHttpConfig")
            .field("endpoint", &"[REDACTED]")
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("request_timeout", &self.request_timeout)
            .field("max_request_bytes", &self.max_request_bytes)
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
    /// A configured bearer credential was not valid for an HTTP Bearer header.
    #[error(
        "MCP bearer token must be safe for an HTTP header and at most {MAX_HTTP_BEARER_TOKEN_BYTES} bytes"
    )]
    InvalidBearerToken,
    /// One request must have a finite deadline.
    #[error("MCP request timeout must be non-zero")]
    ZeroTimeout,
    /// JSON-RPC request encoding must have a finite byte bound.
    #[error("MCP request byte limit must be non-zero")]
    ZeroRequestLimit,
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
