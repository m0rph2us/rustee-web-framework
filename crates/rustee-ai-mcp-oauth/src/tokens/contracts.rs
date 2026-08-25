//! OAuth token-grant requests and trusted exchanger or revoker contracts.

use std::fmt;

use futures_util::future::BoxFuture;
use url::Url;

use super::McpOAuthTokenSet;
use crate::McpOAuthTokenExchangeRequest;

/// One token refresh grant bound to a configured client and MCP resource.
#[derive(Clone)]
pub struct McpOAuthRefreshRequest {
    pub(super) client_id: String,
    pub(super) refresh_token: String,
    pub(super) resource: Url,
}

impl McpOAuthRefreshRequest {
    /// Returns the pre-registered public client ID.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the exact MCP resource URI that must receive the refreshed token.
    #[must_use]
    pub const fn resource(&self) -> &Url {
        &self.resource
    }

    /// Exposes the refresh token only to a trusted token exchanger implementation.
    #[must_use]
    pub fn refresh_token(&self) -> &str {
        &self.refresh_token
    }
}

impl fmt::Debug for McpOAuthRefreshRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthRefreshRequest")
            .field("client_id", &"[REDACTED]")
            .field("refresh_token", &"[REDACTED]")
            .field("resource", &"[REDACTED]")
            .finish()
    }
}

/// Standard OAuth token-type hint supplied to a trusted revocation endpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpOAuthRevocationTokenType {
    /// The request carries an access token because no refresh token was issued.
    AccessToken,
    /// The request carries the refresh token so future grants can be revoked at their root.
    RefreshToken,
}

impl McpOAuthRevocationTokenType {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::AccessToken => "access_token",
            Self::RefreshToken => "refresh_token",
        }
    }
}

/// An explicit token-revocation grant bound to one configured MCP resource.
#[derive(Clone)]
pub struct McpOAuthRevocationRequest {
    pub(super) client_id: String,
    pub(super) token: String,
    pub(super) token_type_hint: McpOAuthRevocationTokenType,
    pub(super) resource: Url,
}

impl McpOAuthRevocationRequest {
    /// Returns the pre-registered public client ID.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the local MCP resource binding used for application policy and audit routing.
    #[must_use]
    pub const fn resource(&self) -> &Url {
        &self.resource
    }

    /// Returns the standardized hint for the token passed to the revocation endpoint.
    #[must_use]
    pub const fn token_type_hint(&self) -> McpOAuthRevocationTokenType {
        self.token_type_hint
    }

    /// Exposes the token only to a trusted revocation adapter implementation.
    #[must_use]
    pub fn token(&self) -> &str {
        &self.token
    }
}

#[cfg(test)]
impl McpOAuthRevocationRequest {
    pub(crate) fn for_test(
        client_id: String,
        token: String,
        token_type_hint: McpOAuthRevocationTokenType,
        resource: Url,
    ) -> Self {
        Self {
            client_id,
            token,
            token_type_hint,
            resource,
        }
    }
}

impl fmt::Debug for McpOAuthRevocationRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthRevocationRequest")
            .field("client_id", &"[REDACTED]")
            .field("token", &"[REDACTED]")
            .field("token_type_hint", &self.token_type_hint)
            .field("resource", &"[REDACTED]")
            .finish()
    }
}

/// Trusted adapter for an explicit OAuth token revocation request.
pub trait McpOAuthTokenRevoker: Clone + Send + Sync + 'static {
    /// Revoker-specific failure type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Revokes the request token at a selected authorization-server endpoint.
    fn revoke(
        &self,
        endpoint: Url,
        request: McpOAuthRevocationRequest,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// Trusted adapter for authorization-code and refresh-token calls to a selected token endpoint.
pub trait McpOAuthTokenExchanger: Clone + Send + Sync + 'static {
    /// Exchanger-specific failure type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Performs one authorization-code + PKCE token request.
    fn exchange(
        &self,
        endpoint: Url,
        request: McpOAuthTokenExchangeRequest,
    ) -> BoxFuture<'static, Result<McpOAuthTokenSet, Self::Error>>;

    /// Performs one explicit refresh-token request. It never retries an MCP request.
    fn refresh(
        &self,
        endpoint: Url,
        request: McpOAuthRefreshRequest,
    ) -> BoxFuture<'static, Result<McpOAuthTokenSet, Self::Error>>;
}
