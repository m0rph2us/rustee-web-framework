//! Protected-resource and authorization-server metadata models plus wire validation.

use std::fmt;

use serde::Deserialize;
use url::Url;

use crate::{
    McpOAuthError,
    config::{canonical_resource, valid_resource_url},
};
use rustee_core::is_valid_oauth_scope_token;

const MAX_DISCOVERY_URLS: usize = 3;

/// Protected-resource metadata discovered for one exact MCP endpoint.
#[derive(Clone, Eq, PartialEq)]
pub struct McpOAuthResourceMetadata {
    resource: Url,
    authorization_servers: Vec<Url>,
    scopes_supported: Vec<String>,
}

impl McpOAuthResourceMetadata {
    /// Returns the resource URI declared by the metadata document.
    #[must_use]
    pub const fn resource(&self) -> &Url {
        &self.resource
    }

    /// Returns explicit authorization-server choices in server-declared order.
    #[must_use]
    pub fn authorization_servers(&self) -> impl ExactSizeIterator<Item = &Url> {
        self.authorization_servers.iter()
    }

    /// Returns declared supported scopes without selecting or requesting them automatically.
    pub fn scopes_supported(&self) -> impl ExactSizeIterator<Item = &str> {
        self.scopes_supported.iter().map(String::as_str)
    }
}

impl fmt::Debug for McpOAuthResourceMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthResourceMetadata")
            .field("resource", &"[REDACTED]")
            .field(
                "authorization_server_count",
                &self.authorization_servers.len(),
            )
            .field("scope_count", &self.scopes_supported.len())
            .finish()
    }
}

/// OAuth or `OpenID` Connect authorization-server metadata accepted for MCP PKCE authorization.
#[derive(Clone, Eq, PartialEq)]
pub struct McpOAuthAuthorizationServerMetadata {
    issuer: Url,
    authorization_endpoint: Url,
    token_endpoint: Url,
    revocation_endpoint: Option<Url>,
}

impl McpOAuthAuthorizationServerMetadata {
    /// Creates authorization-server metadata supplied through a separately trusted deployment
    /// configuration. Discovery callers should prefer [`crate::HttpMcpOAuthDiscovery`], which also
    /// verifies the server's advertised PKCE `S256` capability.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthError::InvalidMetadata`] for unsafe issuer or endpoint URLs.
    pub fn new(
        issuer: Url,
        authorization_endpoint: Url,
        token_endpoint: Url,
    ) -> Result<Self, McpOAuthError> {
        if !valid_resource_url(&issuer)
            || !valid_resource_url(&authorization_endpoint)
            || !valid_resource_url(&token_endpoint)
        {
            return Err(McpOAuthError::InvalidMetadata);
        }
        Ok(Self {
            issuer,
            authorization_endpoint,
            token_endpoint,
            revocation_endpoint: None,
        })
    }

    /// Adds the selected server's trusted OAuth revocation endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthError::InvalidMetadata`] for an unsafe endpoint URL.
    pub fn with_revocation_endpoint(
        mut self,
        revocation_endpoint: Url,
    ) -> Result<Self, McpOAuthError> {
        if !valid_resource_url(&revocation_endpoint) {
            return Err(McpOAuthError::InvalidMetadata);
        }
        self.revocation_endpoint = Some(revocation_endpoint);
        Ok(self)
    }

    /// Returns the selected authorization-server issuer.
    #[must_use]
    pub const fn issuer(&self) -> &Url {
        &self.issuer
    }

    /// Returns the trusted authorization endpoint.
    #[must_use]
    pub const fn authorization_endpoint(&self) -> &Url {
        &self.authorization_endpoint
    }

    /// Returns the trusted token endpoint.
    #[must_use]
    pub const fn token_endpoint(&self) -> &Url {
        &self.token_endpoint
    }

    /// Returns the optional trusted OAuth revocation endpoint.
    #[must_use]
    pub const fn revocation_endpoint(&self) -> Option<&Url> {
        self.revocation_endpoint.as_ref()
    }
}

impl fmt::Debug for McpOAuthAuthorizationServerMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthAuthorizationServerMetadata")
            .field("issuer", &"[REDACTED]")
            .field("authorization_endpoint", &"[REDACTED]")
            .field("token_endpoint", &"[REDACTED]")
            .field(
                "has_revocation_endpoint",
                &self.revocation_endpoint.is_some(),
            )
            .finish()
    }
}

#[derive(Deserialize)]
pub(super) struct ResourceMetadataWire {
    resource: Url,
    authorization_servers: Vec<Url>,
    #[serde(default)]
    scopes_supported: Vec<String>,
}

impl ResourceMetadataWire {
    pub(super) fn into_public(
        self,
        expected_resource: &Url,
    ) -> Result<McpOAuthResourceMetadata, McpOAuthError> {
        if canonical_resource(&self.resource) != canonical_resource(expected_resource)
            || !valid_resource_url(&self.resource)
            || self.authorization_servers.is_empty()
            || self.authorization_servers.len() > MAX_DISCOVERY_URLS
            || self
                .authorization_servers
                .iter()
                .any(|url| !valid_resource_url(url))
            || self
                .scopes_supported
                .iter()
                .any(|scope| !is_valid_oauth_scope_token(scope, crate::config::MAX_SCOPE_BYTES))
        {
            return Err(McpOAuthError::InvalidMetadata);
        }
        Ok(McpOAuthResourceMetadata {
            resource: self.resource,
            authorization_servers: self.authorization_servers,
            scopes_supported: self.scopes_supported,
        })
    }
}

#[derive(Deserialize)]
pub(crate) struct AuthorizationServerMetadataWire {
    issuer: Url,
    authorization_endpoint: Url,
    token_endpoint: Url,
    #[serde(default)]
    revocation_endpoint: Option<Url>,
    #[serde(default)]
    code_challenge_methods_supported: Vec<String>,
}

impl AuthorizationServerMetadataWire {
    pub(crate) fn into_public(
        self,
        expected_issuer: &Url,
    ) -> Result<McpOAuthAuthorizationServerMetadata, McpOAuthError> {
        if canonical_resource(&self.issuer) != canonical_resource(expected_issuer)
            || !valid_resource_url(&self.issuer)
            || !valid_resource_url(&self.authorization_endpoint)
            || !valid_resource_url(&self.token_endpoint)
            || self
                .revocation_endpoint
                .as_ref()
                .is_some_and(|endpoint| !valid_resource_url(endpoint))
            || !self
                .code_challenge_methods_supported
                .iter()
                .any(|method| method == "S256")
        {
            return Err(McpOAuthError::InvalidMetadata);
        }
        Ok(McpOAuthAuthorizationServerMetadata {
            issuer: self.issuer,
            authorization_endpoint: self.authorization_endpoint,
            token_endpoint: self.token_endpoint,
            revocation_endpoint: self.revocation_endpoint,
        })
    }
}
