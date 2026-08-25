//! Shared redacted access-token and error models for MCP OAuth adapters.

use std::{
    fmt,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use reqwest::StatusCode;
use rustee_ai_mcp::{
    MAX_HTTP_BEARER_TOKEN_BYTES, McpHttpConfig, McpHttpConfigError, is_valid_http_bearer_value,
};
use url::Url;

use crate::config::canonical_resource;

/// One redacted access token issued for the configured MCP resource.
#[derive(Clone, Eq, PartialEq)]
pub struct McpOAuthAccessToken {
    value: String,
    expires_at: Option<SystemTime>,
}

impl McpOAuthAccessToken {
    /// Creates a bounded opaque bearer token that can be rendered in an HTTP header.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthError::InvalidToken`] when the value is blank, oversized, or cannot be
    /// rendered safely in an HTTP Bearer header.
    pub fn new(
        value: impl Into<String>,
        expires_at: Option<SystemTime>,
    ) -> Result<Self, McpOAuthError> {
        let value = value.into();
        if !is_valid_http_bearer_value(&value) {
            return Err(McpOAuthError::InvalidToken);
        }
        Ok(Self { value, expires_at })
    }

    /// Returns the provider-declared expiry without exposing the bearer token.
    #[must_use]
    pub const fn expires_at(&self) -> Option<SystemTime> {
        self.expires_at
    }

    /// Reports whether the token is expired at the supplied application-controlled instant.
    #[must_use]
    pub fn is_expired_at(&self, now: SystemTime) -> bool {
        self.expires_at.is_some_and(|expires_at| expires_at <= now)
    }

    /// Applies this resource-bound token to a matching MCP HTTP configuration.
    ///
    /// This is explicit by design: a token never alters an existing client or causes a failed
    /// request to be replayed. Applications keep the resulting configuration and token storage
    /// lifecycle within their own authorization boundary.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthError::ResourceMismatch`] for a different MCP endpoint.
    pub fn apply_to_http_config(
        &self,
        config: McpHttpConfig,
        resource: &Url,
    ) -> Result<McpHttpConfig, McpOAuthError> {
        if canonical_resource(config.endpoint()) != canonical_resource(resource) {
            return Err(McpOAuthError::ResourceMismatch);
        }
        config
            .with_bearer_token(self.value.clone())
            .map_err(McpOAuthError::from)
    }

    pub(crate) fn into_secret_parts(self) -> (String, Option<SystemTime>) {
        (self.value, self.expires_at)
    }

    pub(crate) fn secret_value(&self) -> &str {
        &self.value
    }
}

impl fmt::Debug for McpOAuthAccessToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthAccessToken")
            .field("value", &"[REDACTED]")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// Sanitized MCP OAuth adapter failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum McpOAuthError {
    /// Token data was blank, oversized, or unsafe for an HTTP bearer header.
    #[error("MCP OAuth token was invalid")]
    InvalidToken,
    /// A token may only be applied to the exact resource it was issued for.
    #[error("MCP OAuth token resource did not match the MCP endpoint")]
    ResourceMismatch,
    /// The underlying MCP configuration rejected an otherwise redacted token handoff.
    #[error("MCP OAuth token could not be applied to the MCP client configuration")]
    HttpConfiguration,
    /// The bounded OAuth HTTP client could not be initialized.
    #[error("MCP OAuth HTTP client could not be initialized")]
    HttpClient,
    /// An authorization metadata request did not complete successfully.
    #[error("MCP OAuth authorization metadata request failed")]
    Transport,
    /// An authorization metadata endpoint returned an unexpected status.
    #[error("MCP OAuth authorization metadata endpoint returned HTTP status {0}")]
    HttpStatus(StatusCode),
    /// Authorization metadata was missing, oversized, malformed, or inconsistent with the resource.
    #[error("MCP OAuth authorization metadata was rejected")]
    InvalidMetadata,
    /// The fully-bound browser authorization URL exceeded the safe redirect budget.
    #[error("MCP OAuth authorization redirect exceeded the safe URL limit")]
    AuthorizationRedirectTooLong,
    /// The OAuth challenge did not contain a safe resource metadata URL.
    #[error("MCP OAuth authorization challenge was rejected")]
    InvalidChallenge,
    /// The application-owned atomic authorization transaction service failed.
    #[error("MCP OAuth authorization transaction service is unavailable")]
    TransactionStoreUnavailable,
    /// No matching, unconsumed state existed for the authorization callback.
    #[error("MCP OAuth authorization state was rejected")]
    StateRejected,
    /// The authorization callback arrived after its transaction expired.
    #[error("MCP OAuth authorization transaction expired")]
    TransactionExpired,
    /// Callback data was incomplete, unsafe, or otherwise malformed.
    #[error("MCP OAuth authorization callback was rejected")]
    CallbackRejected,
    /// The authorization server denied the consent or authorization request.
    #[error("MCP OAuth authorization was rejected by the provider")]
    ProviderRejected,
    /// The token endpoint could not be reached or its response could not be accepted.
    #[error("MCP OAuth token exchange was unavailable or rejected")]
    TokenExchangeUnavailable,
    /// The application-owned token store could not complete its operation.
    #[error("MCP OAuth token store is unavailable")]
    TokenStoreUnavailable,
    /// No usable token exists for the requested application-owned key.
    #[error("MCP OAuth token was not available")]
    TokenUnavailable,
    /// The stored access token has expired; the application must explicitly refresh or reauthorize.
    #[error("MCP OAuth access token expired")]
    TokenExpired,
    /// The authorization server did not issue a refresh token for this grant.
    #[error("MCP OAuth refresh token was not available")]
    RefreshTokenUnavailable,
    /// The selected authorization server did not publish an OAuth revocation endpoint.
    #[error("MCP OAuth token revocation was not supported by the authorization server")]
    RevocationUnsupported,
    /// Token revocation could not reach or be accepted by the authorization server.
    #[error("MCP OAuth token revocation was unavailable or rejected")]
    RevocationUnavailable,
}

/// Failure returned when a process-local MCP OAuth store cannot complete an operation safely.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InMemoryMcpOAuthStoreError {
    /// A poisoned lock prevents a local store operation from completing safely.
    #[error("MCP OAuth in-memory store state is unavailable")]
    StateUnavailable,
    /// An unconsumed authorization transaction exceeded the fixed local-store capacity.
    #[error("MCP OAuth in-memory transaction store capacity is exhausted")]
    TransactionCapacityExhausted,
    /// A live OAuth state was already present.
    #[error("MCP OAuth in-memory transaction store state already exists")]
    DuplicateTransactionState,
    /// Application-owned token slots exceeded the fixed local-store capacity.
    #[error("MCP OAuth in-memory token store capacity is exhausted")]
    TokenCapacityExhausted,
}

impl From<McpHttpConfigError> for McpOAuthError {
    fn from(_: McpHttpConfigError) -> Self {
        Self::HttpConfiguration
    }
}

pub(crate) fn is_valid_opaque_token(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_HTTP_BEARER_TOKEN_BYTES
        && value.bytes().all(|byte| !byte.is_ascii_control())
}

pub(crate) fn system_time_to_unix_seconds(value: Option<SystemTime>) -> Option<u64> {
    value.and_then(|time| {
        time.duration_since(UNIX_EPOCH)
            .ok()
            .map(|value| value.as_secs())
    })
}

pub(crate) fn unix_seconds_to_system_time(value: u64) -> Option<SystemTime> {
    UNIX_EPOCH.checked_add(Duration::from_secs(value))
}
