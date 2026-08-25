//! Validated public configuration for one MCP OAuth protected resource.

use std::{collections::BTreeSet, fmt};

use rustee_core::is_valid_oauth_scope_token;
use url::{Host, Url};

pub(super) const MAX_AUTHORIZATION_SERVERS: usize = 8;
pub(super) const MAX_REQUIRED_SCOPES: usize = 32;
pub(super) const MAX_SCOPE_BYTES: usize = 256;
pub(super) const MAX_URL_BYTES: usize = 2048;
const MAX_WWW_AUTHENTICATE_BYTES: usize = 8_192;
const MAX_CHALLENGE_ERROR_BYTES: usize = "insufficient_scope".len();

/// Public configuration for one MCP OAuth protected resource.
///
/// `resource` is the canonical, externally visible MCP endpoint URL. The application must
/// configure its JWT or introspection verifier to require it as the access-token audience; this
/// type deliberately does not parse, retain, or inspect raw access tokens itself.
#[derive(Clone, Eq, PartialEq)]
pub struct McpOAuthResourceServerConfig {
    resource: Url,
    protected_resource_metadata: Url,
    authorization_servers: BTreeSet<String>,
    required_scopes: BTreeSet<String>,
}

impl McpOAuthResourceServerConfig {
    /// Creates public metadata for one protected MCP HTTP endpoint.
    ///
    /// `protected_resource_metadata` is the exact public metadata URL advertised in a Bearer
    /// challenge. Every URL must use HTTPS, except loopback HTTP used for local development and
    /// contract tests. At least one explicitly trusted authorization-server issuer is required.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthResourceServerConfigError`] when a URL is unsafe, no authorization
    /// server is configured, or the configured server limit is exceeded.
    pub fn new<I>(
        resource: Url,
        protected_resource_metadata: Url,
        authorization_servers: I,
    ) -> Result<Self, McpOAuthResourceServerConfigError>
    where
        I: IntoIterator<Item = Url>,
    {
        if !valid_public_url(&resource) {
            return Err(McpOAuthResourceServerConfigError::InvalidResourceUrl);
        }
        if !valid_public_url(&protected_resource_metadata) {
            return Err(McpOAuthResourceServerConfigError::InvalidProtectedResourceMetadataUrl);
        }

        let mut servers = BTreeSet::new();
        for server in authorization_servers {
            if !valid_public_url(&server) {
                return Err(McpOAuthResourceServerConfigError::InvalidAuthorizationServerUrl);
            }
            let server = server.to_string();
            if !servers.contains(&server) && servers.len() == MAX_AUTHORIZATION_SERVERS {
                return Err(McpOAuthResourceServerConfigError::TooManyAuthorizationServers);
            }
            servers.insert(server);
        }
        if servers.is_empty() {
            return Err(McpOAuthResourceServerConfigError::EmptyAuthorizationServers);
        }

        Ok(Self {
            resource,
            protected_resource_metadata,
            authorization_servers: servers,
            required_scopes: BTreeSet::new(),
        })
    }

    /// Requires every supplied scope after the bearer token is cryptographically verified.
    ///
    /// A resource without scoped authorization can omit this call, but a server that advertises
    /// scopes should normally require the same scopes here rather than relying on token presence.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthResourceServerConfigError`] for an empty requirement, an invalid RFC
    /// 6749 scope token, or more than 32 scopes.
    pub fn with_required_scopes<I, S>(
        mut self,
        scopes: I,
    ) -> Result<Self, McpOAuthResourceServerConfigError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut required_scopes = BTreeSet::new();
        for scope in scopes {
            if required_scopes.insert(scope.into()) && required_scopes.len() > MAX_REQUIRED_SCOPES {
                return Err(McpOAuthResourceServerConfigError::TooManyRequiredScopes);
            }
        }
        if required_scopes.is_empty() {
            return Err(McpOAuthResourceServerConfigError::EmptyScopeRequirement);
        }
        if required_scopes
            .iter()
            .any(|scope| !is_valid_oauth_scope_token(scope, MAX_SCOPE_BYTES))
        {
            return Err(McpOAuthResourceServerConfigError::InvalidScope);
        }
        let scope_parameter_len =
            required_scopes.iter().map(String::len).sum::<usize>() + required_scopes.len() - 1;
        if challenge_header_len(&self.protected_resource_metadata, scope_parameter_len)
            > MAX_WWW_AUTHENTICATE_BYTES
        {
            return Err(McpOAuthResourceServerConfigError::ScopeParameterTooLong);
        }
        self.required_scopes = required_scopes;
        Ok(self)
    }

    /// Returns the exact externally visible MCP resource URL.
    #[must_use]
    pub fn resource(&self) -> &Url {
        &self.resource
    }

    /// Returns the public protected-resource metadata URL advertised in challenges.
    #[must_use]
    pub fn protected_resource_metadata(&self) -> &Url {
        &self.protected_resource_metadata
    }

    /// Returns configured authorization-server issuers in deterministic order.
    pub fn authorization_servers(&self) -> impl ExactSizeIterator<Item = &str> {
        self.authorization_servers.iter().map(String::as_str)
    }

    /// Returns required verified OAuth scopes in deterministic order.
    pub fn required_scopes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.required_scopes.iter().map(String::as_str)
    }

    pub(super) fn scope_parameter(&self) -> Option<String> {
        (!self.required_scopes.is_empty()).then(|| {
            self.required_scopes
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(" ")
        })
    }

    pub(super) fn accepts_issuer(&self, issuer: Option<&str>) -> bool {
        issuer
            .and_then(|issuer| Url::parse(issuer).ok())
            .is_some_and(|issuer| self.authorization_servers.contains(issuer.as_str()))
    }
}

impl fmt::Debug for McpOAuthResourceServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthResourceServerConfig")
            .field("resource", &self.resource)
            .field(
                "protected_resource_metadata",
                &self.protected_resource_metadata,
            )
            .field("authorization_servers", &self.authorization_servers)
            .field("required_scopes", &self.required_scopes)
            .finish()
    }
}

/// Invalid public protected-resource configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum McpOAuthResourceServerConfigError {
    /// The canonical MCP resource URL was not a safe public HTTP(S) URL.
    #[error(
        "MCP OAuth resource URL must be an absolute HTTPS URL or loopback HTTP URL without credentials, query, or fragment"
    )]
    InvalidResourceUrl,
    /// The advertised protected-resource metadata URL was not a safe public HTTP(S) URL.
    #[error(
        "MCP OAuth protected-resource metadata URL must be an absolute HTTPS URL or loopback HTTP URL without credentials, query, or fragment"
    )]
    InvalidProtectedResourceMetadataUrl,
    /// An authorization-server issuer URL was unsafe or malformed.
    #[error(
        "MCP OAuth authorization-server URL must be an absolute HTTPS URL or loopback HTTP URL without credentials, query, or fragment"
    )]
    InvalidAuthorizationServerUrl,
    /// Public metadata needs at least one explicitly configured authorization server.
    #[error("MCP OAuth resource server needs at least one authorization server")]
    EmptyAuthorizationServers,
    /// Public metadata intentionally has a small issuer allowlist.
    #[error("MCP OAuth resource server supports at most eight authorization servers")]
    TooManyAuthorizationServers,
    /// A caller passed no scopes to the explicit scope-requirement builder.
    #[error("MCP OAuth scope requirement must not be empty")]
    EmptyScopeRequirement,
    /// A configured scope was not a bounded RFC 6749 scope token.
    #[error("MCP OAuth scope must be a bounded RFC 6749 scope token")]
    InvalidScope,
    /// Public metadata intentionally has a bounded scope list.
    #[error("MCP OAuth resource server supports at most 32 required scopes")]
    TooManyRequiredScopes,
    /// The combined advertised scopes would exceed the supported Bearer challenge header budget.
    #[error("MCP OAuth required scopes exceed the supported WWW-Authenticate header size")]
    ScopeParameterTooLong,
}

fn challenge_header_len(metadata: &Url, scope_parameter_len: usize) -> usize {
    "Bearer resource_metadata=\"".len()
        + metadata.as_str().len()
        + "\", scope=\"".len()
        + scope_parameter_len
        + "\", error=\"".len()
        + MAX_CHALLENGE_ERROR_BYTES
        + "\"".len()
}

fn valid_public_url(url: &Url) -> bool {
    if url.as_str().len() > MAX_URL_BYTES
        || url.username() != ""
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return false;
    }
    match (url.scheme(), url.host()) {
        ("https", Some(_)) => true,
        ("http", Some(Host::Ipv4(address))) => address.is_loopback(),
        ("http", Some(Host::Ipv6(address))) => address.is_loopback(),
        ("http", Some(Host::Domain(domain))) => domain.eq_ignore_ascii_case("localhost"),
        _ => false,
    }
}
