//! Bounded HTTP discovery orchestration for MCP OAuth metadata.

use std::fmt;

use reqwest::{StatusCode, header::HeaderMap};
use url::Url;

use super::{McpOAuthClientConfig, McpOAuthError};

mod challenge;
mod metadata;
mod transport;
mod urls;

pub use metadata::{McpOAuthAuthorizationServerMetadata, McpOAuthResourceMetadata};

use metadata::ResourceMetadataWire;
use transport::DiscoveryTransport;

pub(super) use metadata::AuthorizationServerMetadataWire;
pub(super) use urls::{authorization_server_metadata_urls, resource_metadata_urls};

pub(crate) const MAX_DISCOVERY_RESPONSE_BYTES: usize = 512 * 1024;

#[cfg(test)]
pub(super) const MAX_WWW_AUTHENTICATE_BYTES: usize = challenge::MAX_WWW_AUTHENTICATE_BYTES;

pub(super) fn www_authenticate_value(headers: &HeaderMap) -> Result<Option<String>, McpOAuthError> {
    challenge::www_authenticate_value(headers)
}

/// Bounded HTTP discovery adapter for MCP protected-resource and authorization-server metadata.
#[derive(Clone)]
pub struct HttpMcpOAuthDiscovery {
    transport: DiscoveryTransport,
    resource: Url,
}

impl HttpMcpOAuthDiscovery {
    /// Creates discovery for one configured MCP protected resource.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthError::HttpClient`] if the finite HTTP client cannot be created.
    pub fn new(config: &McpOAuthClientConfig) -> Result<Self, McpOAuthError> {
        Ok(Self {
            transport: DiscoveryTransport::new(config.http_timeout())?,
            resource: config.resource().clone(),
        })
    }

    /// Discovers protected-resource metadata from every `WWW-Authenticate` field in an HTTP
    /// response.
    ///
    /// Header fields are combined in received order under the same 8 KiB challenge limit used by
    /// [`Self::discover_resource_metadata`]. Invalid header bytes or an oversized aggregate fail
    /// closed rather than selecting an arbitrary field.
    ///
    /// # Errors
    ///
    /// Returns a sanitized challenge, transport, HTTP-status, response-bound, or metadata error.
    pub async fn discover_resource_metadata_from_headers(
        &self,
        headers: &HeaderMap,
    ) -> Result<McpOAuthResourceMetadata, McpOAuthError> {
        let challenge = www_authenticate_value(headers)?;
        self.discover_resource_metadata(challenge.as_deref()).await
    }

    /// Discovers protected-resource metadata from an optional 401 challenge or well-known URIs.
    ///
    /// A provided `WWW-Authenticate` value is used only for its bounded Bearer
    /// `resource_metadata` parameter. No scopes are automatically accepted or requested.
    ///
    /// # Errors
    ///
    /// Returns a sanitized challenge, transport, HTTP-status, response-bound, or metadata error.
    pub async fn discover_resource_metadata(
        &self,
        www_authenticate: Option<&str>,
    ) -> Result<McpOAuthResourceMetadata, McpOAuthError> {
        let candidates = resource_metadata_urls(&self.resource, www_authenticate)?;
        for url in candidates {
            match self
                .transport
                .fetch_json::<ResourceMetadataWire>(&url)
                .await
            {
                Ok(metadata) => return metadata.into_public(&self.resource),
                Err(McpOAuthError::HttpStatus(StatusCode::NOT_FOUND)) => {}
                Err(error) => return Err(error),
            }
        }
        Err(McpOAuthError::InvalidMetadata)
    }

    /// Discovers OAuth authorization-server metadata, then `OpenID` Connect metadata as fallback.
    ///
    /// The result is accepted only when it declares PKCE `S256` support and HTTPS (or loopback
    /// test) authorization and token endpoints.
    ///
    /// # Errors
    ///
    /// Returns a sanitized transport, HTTP-status, response-bound, or metadata error.
    pub async fn discover_authorization_server(
        &self,
        issuer: &Url,
    ) -> Result<McpOAuthAuthorizationServerMetadata, McpOAuthError> {
        for url in authorization_server_metadata_urls(issuer)? {
            match self
                .transport
                .fetch_json::<AuthorizationServerMetadataWire>(&url)
                .await
            {
                Ok(metadata) => return metadata.into_public(issuer),
                Err(McpOAuthError::HttpStatus(StatusCode::NOT_FOUND)) => {}
                Err(error) => return Err(error),
            }
        }
        Err(McpOAuthError::InvalidMetadata)
    }
}

impl fmt::Debug for HttpMcpOAuthDiscovery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpMcpOAuthDiscovery")
            .field("resource", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}
