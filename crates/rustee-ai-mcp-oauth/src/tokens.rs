//! Token models, encrypted-store contracts, and OAuth token grant contracts.

use std::fmt;

use rustee_ai_mcp::{McpHttpConfig, is_valid_http_bearer_value};
use serde::{Deserialize, Serialize};
use url::Url;

use super::{McpOAuthAccessToken, McpOAuthError};
use crate::config::valid_resource_url;
use crate::model::{
    is_valid_opaque_token, system_time_to_unix_seconds, unix_seconds_to_system_time,
};

mod contracts;
mod http;
mod store;
#[cfg(test)]
mod tests;

pub use contracts::{
    McpOAuthRefreshRequest, McpOAuthRevocationRequest, McpOAuthRevocationTokenType,
    McpOAuthTokenExchanger, McpOAuthTokenRevoker,
};
pub use http::HttpMcpOAuthTokenExchanger;
#[cfg(test)]
pub(crate) use http::MAX_TOKEN_RESPONSE_BYTES;
pub use store::{InMemoryMcpOAuthTokenStore, McpOAuthTokenStore, McpOAuthTokenStoreKey};

/// A resource-bound access token and optional refresh token returned by an OAuth token endpoint.
///
/// The value is intentionally opaque. Pass it directly to [`McpOAuthTokenStore`] or use its
/// access token to configure a matching MCP client; only a dedicated encrypted store adapter
/// should call [`Self::into_secrets`].
#[derive(Clone, Eq, PartialEq)]
pub struct McpOAuthTokenSet {
    resource: Url,
    access_token: McpOAuthAccessToken,
    refresh_token: Option<String>,
}

impl McpOAuthTokenSet {
    /// Creates a resource-bound token set from a trusted token-endpoint response.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthError::InvalidToken`] for unsafe access or refresh-token values, and
    /// [`McpOAuthError::InvalidMetadata`] for an unsafe resource URL.
    pub fn new(
        resource: Url,
        access_token: McpOAuthAccessToken,
        refresh_token: Option<String>,
    ) -> Result<Self, McpOAuthError> {
        validate_token_set_fields(
            &resource,
            access_token.secret_value(),
            refresh_token.as_deref(),
        )?;
        Ok(Self {
            resource,
            access_token,
            refresh_token,
        })
    }

    /// Returns the exact MCP protected resource represented by this token set.
    #[must_use]
    pub const fn resource(&self) -> &Url {
        &self.resource
    }

    /// Returns the redacted access-token handle used to configure a matching MCP client.
    #[must_use]
    pub const fn access_token(&self) -> &McpOAuthAccessToken {
        &self.access_token
    }

    /// Applies this token set to a matching MCP HTTP configuration.
    ///
    /// The resource binding comes from the token set itself, so callers cannot accidentally
    /// substitute a different audience URI while configuring the client.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthError::ResourceMismatch`] when the configuration has another endpoint.
    pub fn apply_to_http_config(
        &self,
        config: McpHttpConfig,
    ) -> Result<McpHttpConfig, McpOAuthError> {
        self.access_token
            .apply_to_http_config(config, &self.resource)
    }

    /// Reports whether the provider supplied a refresh token.
    #[must_use]
    pub const fn has_refresh_token(&self) -> bool {
        self.refresh_token.is_some()
    }

    /// Converts the token set to a serializable secret record for a trusted encrypted token-store
    /// adapter. The returned record is not safe for logs, browser sessions, or ordinary caches.
    #[must_use]
    pub fn into_secrets(self) -> McpOAuthTokenSecrets {
        let (access_token, expires_at) = self.access_token.into_secret_parts();
        McpOAuthTokenSecrets {
            resource: self.resource,
            access_token,
            expires_at_unix_seconds: system_time_to_unix_seconds(expires_at),
            refresh_token: self.refresh_token,
        }
    }

    pub(super) fn refresh_request(
        &self,
        client_id: &str,
    ) -> Result<McpOAuthRefreshRequest, McpOAuthError> {
        let refresh_token = self
            .refresh_token
            .clone()
            .ok_or(McpOAuthError::RefreshTokenUnavailable)?;
        Ok(McpOAuthRefreshRequest {
            client_id: client_id.to_owned(),
            refresh_token,
            resource: self.resource.clone(),
        })
    }

    pub(super) fn revocation_request(&self, client_id: &str) -> McpOAuthRevocationRequest {
        let (token, token_type_hint) = self.refresh_token.as_ref().map_or_else(
            || {
                (
                    self.access_token.secret_value().to_owned(),
                    McpOAuthRevocationTokenType::AccessToken,
                )
            },
            |token| (token.clone(), McpOAuthRevocationTokenType::RefreshToken),
        );
        McpOAuthRevocationRequest {
            client_id: client_id.to_owned(),
            token,
            token_type_hint,
            resource: self.resource.clone(),
        }
    }
}

impl fmt::Debug for McpOAuthTokenSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthTokenSet")
            .field("resource", &"[REDACTED]")
            .field("access_token", &self.access_token)
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

/// Serializable secret payload for a dedicated application-owned encrypted token store.
///
/// This type exposes bearer values solely so a store adapter can encrypt and persist them. It
/// deliberately redacts every secret from `Debug`; it must never be included in ordinary Rustee
/// sessions, audit attributes, logs, URLs, or unencrypted storage.
#[derive(Clone, Serialize)]
pub struct McpOAuthTokenSecrets {
    resource: Url,
    access_token: String,
    expires_at_unix_seconds: Option<u64>,
    refresh_token: Option<String>,
}

impl<'de> Deserialize<'de> for McpOAuthTokenSecrets {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawMcpOAuthTokenSecrets {
            resource: Url,
            access_token: String,
            expires_at_unix_seconds: Option<u64>,
            refresh_token: Option<String>,
        }

        let raw = RawMcpOAuthTokenSecrets::deserialize(deserializer)?;
        let secrets = Self {
            resource: raw.resource,
            access_token: raw.access_token,
            expires_at_unix_seconds: raw.expires_at_unix_seconds,
            refresh_token: raw.refresh_token,
        };
        secrets.validate().map_err(serde::de::Error::custom)?;
        Ok(secrets)
    }
}

impl McpOAuthTokenSecrets {
    fn validate(&self) -> Result<(), McpOAuthError> {
        validate_token_set_fields(
            &self.resource,
            &self.access_token,
            self.refresh_token.as_deref(),
        )?;
        let _ = self.expires_at()?;
        Ok(())
    }

    fn expires_at(&self) -> Result<Option<std::time::SystemTime>, McpOAuthError> {
        self.expires_at_unix_seconds
            .map(|seconds| unix_seconds_to_system_time(seconds).ok_or(McpOAuthError::InvalidToken))
            .transpose()
    }

    /// Returns the resource to which these secrets are bound.
    #[must_use]
    pub const fn resource(&self) -> &Url {
        &self.resource
    }

    /// Returns the bearer value for encryption by a trusted store adapter only.
    #[must_use]
    pub fn access_token_for_encryption(&self) -> &str {
        &self.access_token
    }

    /// Returns the refresh value for encryption by a trusted store adapter only.
    #[must_use]
    pub fn refresh_token_for_encryption(&self) -> Option<&str> {
        self.refresh_token.as_deref()
    }

    /// Returns the provider-declared expiry in Unix seconds.
    #[must_use]
    pub const fn expires_at_unix_seconds(&self) -> Option<u64> {
        self.expires_at_unix_seconds
    }

    /// Restores a validated opaque token set after the application decrypts this record.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthError::InvalidToken`] or [`McpOAuthError::InvalidMetadata`] for a
    /// malformed record.
    pub fn into_token_set(self) -> Result<McpOAuthTokenSet, McpOAuthError> {
        self.validate()?;
        let expires_at = self.expires_at()?;
        let access_token = McpOAuthAccessToken::new(self.access_token, expires_at)?;
        McpOAuthTokenSet::new(self.resource, access_token, self.refresh_token)
    }
}

fn validate_token_set_fields(
    resource: &Url,
    access_token: &str,
    refresh_token: Option<&str>,
) -> Result<(), McpOAuthError> {
    if !valid_resource_url(resource) {
        return Err(McpOAuthError::InvalidMetadata);
    }
    if !is_valid_http_bearer_value(access_token)
        || refresh_token.is_some_and(|token| !is_valid_opaque_token(token))
    {
        return Err(McpOAuthError::InvalidToken);
    }
    Ok(())
}

impl fmt::Debug for McpOAuthTokenSecrets {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthTokenSecrets")
            .field("resource", &"[REDACTED]")
            .field("access_token", &"[REDACTED]")
            .field("expires_at_unix_seconds", &self.expires_at_unix_seconds)
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}
