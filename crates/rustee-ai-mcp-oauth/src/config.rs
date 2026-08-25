//! Validated public-client configuration and MCP resource trust rules.

use std::{collections::BTreeSet, fmt, time::Duration};

use rustee_core::is_valid_oauth_scope_token;
use url::{Host, Url};

const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_TRANSACTION_TTL: Duration = Duration::from_mins(10);
pub(crate) const MAX_CLIENT_ID_BYTES: usize = 1024;
pub(crate) const MAX_TRANSACTION_TTL: Duration = Duration::from_hours(1);
pub(crate) const MAX_URL_BYTES: usize = 2 * 1024;
pub(crate) const MAX_SCOPE_BYTES: usize = 256;
pub(crate) const MAX_SCOPES: usize = 32;

/// Explicit public-client settings for one MCP protected resource.
#[derive(Clone, Eq, PartialEq)]
pub struct McpOAuthClientConfig {
    resource: Url,
    client_id: String,
    redirect_uri: Url,
    scopes: BTreeSet<String>,
    http_timeout: Duration,
    transaction_ttl: Duration,
}

impl McpOAuthClientConfig {
    /// Creates one OAuth configuration bound to the exact MCP HTTP endpoint.
    ///
    /// The initial adapter deliberately supports pre-registered public clients only. The
    /// application owns user consent, callback routing, and token persistence.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthConfigError`] for unsafe or oversized resource/redirect URLs, a blank
    /// client ID, or an invalid timeout.
    pub fn new(
        resource: Url,
        client_id: impl Into<String>,
        redirect_uri: Url,
    ) -> Result<Self, McpOAuthConfigError> {
        let client_id = client_id.into();
        if !valid_resource_url(&resource) {
            return Err(McpOAuthConfigError::InvalidResourceUrl);
        }
        if !valid_client_id(&client_id) {
            return Err(McpOAuthConfigError::InvalidClientId);
        }
        if !valid_redirect_uri(&redirect_uri) {
            return Err(McpOAuthConfigError::InvalidRedirectUri);
        }
        Ok(Self {
            resource,
            client_id,
            redirect_uri,
            scopes: BTreeSet::new(),
            http_timeout: DEFAULT_HTTP_TIMEOUT,
            transaction_ttl: DEFAULT_TRANSACTION_TTL,
        })
    }

    /// Adds one explicitly selected OAuth scope.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthConfigError::InvalidScope`] for blank, oversized, or non-RFC 6749
    /// scope-token values, and [`McpOAuthConfigError::TooManyScopes`] when the configuration
    /// already has the maximum number of distinct scopes.
    pub fn with_scope(mut self, scope: impl Into<String>) -> Result<Self, McpOAuthConfigError> {
        let scope = scope.into();
        if !is_valid_oauth_scope_token(&scope, MAX_SCOPE_BYTES) {
            return Err(McpOAuthConfigError::InvalidScope);
        }
        if !self.scopes.contains(&scope) && self.scopes.len() == MAX_SCOPES {
            return Err(McpOAuthConfigError::TooManyScopes);
        }
        self.scopes.insert(scope);
        Ok(self)
    }

    /// Sets the finite deadline for metadata and token HTTP operations.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthConfigError::ZeroHttpTimeout`] for a zero duration.
    pub fn with_http_timeout(
        mut self,
        http_timeout: Duration,
    ) -> Result<Self, McpOAuthConfigError> {
        if http_timeout.is_zero() {
            return Err(McpOAuthConfigError::ZeroHttpTimeout);
        }
        self.http_timeout = http_timeout;
        Ok(self)
    }

    /// Sets the maximum lifetime of a one-time OAuth state and PKCE verifier transaction.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthConfigError::ZeroTransactionTtl`] when the value is less than one
    /// second, [`McpOAuthConfigError::FractionalTransactionTtl`] when it cannot be stored as an
    /// exact Unix-second expiry, or [`McpOAuthConfigError::TransactionTtlTooLong`] when it would
    /// retain a one-time capability for more than one hour. Production transaction stores should
    /// enforce the returned TTL as well.
    pub fn with_transaction_ttl(
        mut self,
        transaction_ttl: Duration,
    ) -> Result<Self, McpOAuthConfigError> {
        if transaction_ttl.as_secs() == 0 {
            return Err(McpOAuthConfigError::ZeroTransactionTtl);
        }
        if transaction_ttl.subsec_nanos() != 0 {
            return Err(McpOAuthConfigError::FractionalTransactionTtl);
        }
        if transaction_ttl > MAX_TRANSACTION_TTL {
            return Err(McpOAuthConfigError::TransactionTtlTooLong);
        }
        self.transaction_ttl = transaction_ttl;
        Ok(self)
    }

    /// Returns the exact MCP resource URI that tokens must be audience-bound to.
    #[must_use]
    pub const fn resource(&self) -> &Url {
        &self.resource
    }

    /// Returns the pre-registered OAuth public-client identifier.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the exact callback URI registered with the authorization server.
    #[must_use]
    pub const fn redirect_uri(&self) -> &Url {
        &self.redirect_uri
    }

    /// Returns selected scopes in deterministic order.
    pub fn scopes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.scopes.iter().map(String::as_str)
    }

    /// Returns the finite HTTP timeout.
    #[must_use]
    pub const fn http_timeout(&self) -> Duration {
        self.http_timeout
    }

    /// Returns the maximum state/PKCE transaction lifetime.
    #[must_use]
    pub const fn transaction_ttl(&self) -> Duration {
        self.transaction_ttl
    }
}

impl fmt::Debug for McpOAuthClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthClientConfig")
            .field("resource", &"[REDACTED]")
            .field("client_id", &"[REDACTED]")
            .field("redirect_uri", &"[REDACTED]")
            .field("scope_count", &self.scopes.len())
            .field("http_timeout", &self.http_timeout)
            .field("transaction_ttl", &self.transaction_ttl)
            .finish()
    }
}

/// Invalid public MCP OAuth client configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum McpOAuthConfigError {
    /// The protected MCP resource was unsafe, oversized, or not a loopback HTTP test endpoint.
    #[error(
        "MCP OAuth resource must be a bounded HTTPS URL unless loopback, without credentials, query, or fragment"
    )]
    InvalidResourceUrl,
    /// The client identifier was blank or unsafe for an HTTP form request.
    #[error("MCP OAuth client ID must be non-blank, bounded, and free of control characters")]
    InvalidClientId,
    /// The registered callback URI was unsafe, oversized, or not HTTPS or loopback HTTP.
    #[error(
        "MCP OAuth redirect URI must be bounded HTTPS or loopback HTTP without credentials or a fragment"
    )]
    InvalidRedirectUri,
    /// A requested OAuth scope was not a bounded RFC 6749 scope token.
    #[error("MCP OAuth scope must be a bounded RFC 6749 scope token")]
    InvalidScope,
    /// The authorization request would carry too many distinct scopes.
    #[error("MCP OAuth supports at most {MAX_SCOPES} distinct scopes")]
    TooManyScopes,
    /// HTTP metadata and token operations require a finite timeout.
    #[error("MCP OAuth HTTP timeout must be non-zero")]
    ZeroHttpTimeout,
    /// State and PKCE verifier transactions must have a finite lifetime.
    #[error("MCP OAuth authorization transaction TTL must be at least one second")]
    ZeroTransactionTtl,
    /// State and PKCE verifier transactions are stored with exact Unix-second expiry.
    #[error("MCP OAuth authorization transaction TTL must be a whole number of seconds")]
    FractionalTransactionTtl,
    /// A one-time authorization capability must not remain valid for more than one hour.
    #[error("MCP OAuth authorization transaction TTL must not exceed one hour")]
    TransactionTtlTooLong,
    /// The application token-slot key was blank, oversized, or unsafe.
    #[error("MCP OAuth token-store key must be bounded and free of control characters")]
    InvalidTokenStoreKey,
}

pub(crate) fn canonical_resource(value: &Url) -> String {
    let mut value = value.clone();
    value.set_fragment(None);
    value.set_query(None);
    value.to_string()
}

pub(crate) fn valid_resource_url(value: &Url) -> bool {
    value.as_str().len() <= MAX_URL_BYTES
        && matches!(value.scheme(), "http" | "https")
        && value.host().is_some()
        && value.username().is_empty()
        && value.password().is_none()
        && value.query().is_none()
        && value.fragment().is_none()
        && (value.scheme() == "https" || is_loopback_host(value.host().as_ref()))
}

fn valid_redirect_uri(value: &Url) -> bool {
    value.as_str().len() <= MAX_URL_BYTES
        && matches!(value.scheme(), "http" | "https")
        && value.host().is_some()
        && value.username().is_empty()
        && value.password().is_none()
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

fn valid_client_id(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_CLIENT_ID_BYTES
        && value.bytes().all(|byte| !byte.is_ascii_control())
}
