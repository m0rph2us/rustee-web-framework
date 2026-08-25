//! OAuth callback and token-exchange payload contracts.

use std::fmt;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use url::Url;

use crate::McpOAuthError;

pub(crate) const MAX_AUTHORIZATION_CODE_BYTES: usize = 16 * 1024;
pub(crate) const MAX_AUTHORIZATION_REDIRECT_BYTES: usize = 8 * 1024;
pub(crate) const MAX_PROVIDER_ERROR_BYTES: usize = 256;

/// An authorization URL the application may send to its user agent.
#[derive(Clone, Eq, PartialEq)]
pub struct McpOAuthAuthorizationRedirect {
    pub(crate) location: Url,
}

impl McpOAuthAuthorizationRedirect {
    pub(super) fn new(location: Url) -> Result<Self, McpOAuthError> {
        if location.as_str().len() > MAX_AUTHORIZATION_REDIRECT_BYTES {
            return Err(McpOAuthError::AuthorizationRedirectTooLong);
        }
        Ok(Self { location })
    }

    /// Returns the fully-bound authorization URL.
    #[must_use]
    pub const fn location(&self) -> &Url {
        &self.location
    }
}

impl fmt::Debug for McpOAuthAuthorizationRedirect {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthAuthorizationRedirect")
            .field("location", &"[REDACTED]")
            .finish()
    }
}

/// Query values returned by the configured OAuth callback route.
#[derive(Clone, Deserialize)]
pub struct McpOAuthAuthorizationCallback {
    /// Authorization code supplied after the user grants consent.
    #[serde(default)]
    pub code: Option<String>,
    /// State that selects and consumes exactly one stored transaction.
    #[serde(default)]
    pub state: Option<String>,
    /// Provider result code when authorization was denied.
    #[serde(default)]
    pub error: Option<String>,
    /// Provider diagnostic text. It is deliberately never copied into Rustee errors or logs.
    #[serde(default)]
    pub error_description: Option<String>,
}

impl fmt::Debug for McpOAuthAuthorizationCallback {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthAuthorizationCallback")
            .field("code", &self.code.as_ref().map(|_| "[REDACTED]"))
            .field("state", &self.state.as_ref().map(|_| "[REDACTED]"))
            .field("error", &self.error.as_ref().map(|_| "[REDACTED]"))
            .field(
                "error_description",
                &self.error_description.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

/// A verified authorization-code grant passed only to a trusted token exchanger.
#[derive(Clone)]
pub struct McpOAuthTokenExchangeRequest {
    pub(crate) client_id: String,
    pub(crate) code: String,
    pub(crate) redirect_uri: Url,
    pub(crate) code_verifier: String,
    pub(crate) resource: Url,
}

impl McpOAuthTokenExchangeRequest {
    /// Returns the pre-registered public client ID.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the exact registered callback URI.
    #[must_use]
    pub const fn redirect_uri(&self) -> &Url {
        &self.redirect_uri
    }

    /// Returns the exact MCP resource URI bound to this grant.
    #[must_use]
    pub const fn resource(&self) -> &Url {
        &self.resource
    }

    /// Exposes the authorization code only to a trusted exchanger implementation.
    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Exposes the one-time PKCE verifier only to a trusted exchanger implementation.
    #[must_use]
    pub fn code_verifier(&self) -> &str {
        &self.code_verifier
    }
}

impl fmt::Debug for McpOAuthTokenExchangeRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthTokenExchangeRequest")
            .field("client_id", &"[REDACTED]")
            .field("code", &"[REDACTED]")
            .field("redirect_uri", &"[REDACTED]")
            .field("code_verifier", &"[REDACTED]")
            .field("resource", &"[REDACTED]")
            .finish()
    }
}

pub(crate) fn pkce_challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}
