//! Bounded OAuth 2.1 authorization support for one Rustee MCP Streamable HTTP resource.
//!
//! This optional adapter keeps user-consent, authorization-code, access-token, and refresh-token
//! lifecycle outside the core MCP transport. It discovers only explicitly selected protected
//! resources, binds authorization and token operations to the canonical resource URI, and never
//! retries the original MCP action after authorization changes.

use reqwest::header::HeaderMap;
#[cfg(feature = "fuzzing")]
use reqwest::header::{HeaderValue, WWW_AUTHENTICATE};
#[cfg(feature = "fuzzing")]
use url::Url;

mod authorization;
mod config;
mod discovery;
mod model;
mod tokens;

pub use authorization::{
    InMemoryMcpOAuthTransactionStore, McpOAuthAuthorizationCallback, McpOAuthAuthorizationFlow,
    McpOAuthAuthorizationRedirect, McpOAuthPendingAuthorization, McpOAuthTokenExchangeRequest,
    McpOAuthTransactionStore, McpOAuthValueGenerator, UuidMcpOAuthValueGenerator,
};
pub use config::{McpOAuthClientConfig, McpOAuthConfigError};
pub use discovery::{
    HttpMcpOAuthDiscovery, McpOAuthAuthorizationServerMetadata, McpOAuthResourceMetadata,
};
pub use model::{InMemoryMcpOAuthStoreError, McpOAuthAccessToken, McpOAuthError};
pub use tokens::{
    HttpMcpOAuthTokenExchanger, InMemoryMcpOAuthTokenStore, McpOAuthRefreshRequest,
    McpOAuthRevocationRequest, McpOAuthRevocationTokenType, McpOAuthTokenExchanger,
    McpOAuthTokenRevoker, McpOAuthTokenSecrets, McpOAuthTokenSet, McpOAuthTokenStore,
    McpOAuthTokenStoreKey,
};

/// Runs MCP OAuth `WWW-Authenticate` challenge admission against one bounded fuzz input.
///
/// NUL bytes separate simulated repeated header fields. This feature-gated harness entry point
/// exists only for the workspace fuzz target and does not expose parsed remote metadata or form
/// part of the default OAuth integration API.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub fn fuzz_www_authenticate_challenges(bytes: &[u8]) {
    let mut headers = HeaderMap::new();
    for field in bytes.split(|byte| *byte == b'\0') {
        if field.is_empty() {
            continue;
        }
        let Ok(value) = HeaderValue::from_bytes(field) else {
            return;
        };
        headers.append(WWW_AUTHENTICATE, value);
    }
    let Ok(challenge) = discovery::www_authenticate_value(&headers) else {
        return;
    };
    let Ok(resource) = Url::parse("https://resource.example.test/mcp") else {
        return;
    };
    let _ = discovery::resource_metadata_urls(&resource, challenge.as_deref());
}

#[cfg(test)]
use discovery::{
    AuthorizationServerMetadataWire, MAX_DISCOVERY_RESPONSE_BYTES, MAX_WWW_AUTHENTICATE_BYTES,
    authorization_server_metadata_urls, resource_metadata_urls, www_authenticate_value,
};
#[cfg(test)]
use tokens::MAX_TOKEN_RESPONSE_BYTES;

fn is_json_content_type(headers: &HeaderMap) -> bool {
    let mut values = headers.get_all(reqwest::header::CONTENT_TYPE).iter();
    let Some(value) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }
    value
        .to_str()
        .ok()
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
}

#[cfg(test)]
mod tests;
