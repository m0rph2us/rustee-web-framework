//! Trusted opaque-token introspection settings and secret-redacting diagnostics.

use std::{fmt, time::Duration};

use url::Url;

use crate::{
    OidcClientAuthentication,
    client_auth::{basic_authorization_is_within_limit, valid_client_id},
    trust::{
        MAX_AUDIENCE_BYTES, MAX_ISSUER_BYTES, MAX_LEEWAY_SECONDS,
        valid_https_url as valid_trusted_https_url, valid_text,
    },
};

const DEFAULT_CACHE_TTL: Duration = Duration::from_mins(1);
const DEFAULT_MAX_CACHE_ENTRIES: usize = 1_024;

/// Trusted settings for one OAuth 2.0 opaque access-token introspection endpoint.
#[derive(Clone, Eq, PartialEq)]
pub struct OpaqueIntrospectionConfig {
    pub(super) issuer: String,
    pub(super) audience: String,
    pub(super) endpoint: Url,
    pub(super) client_id: String,
    pub(super) authentication: OidcClientAuthentication,
    pub(super) leeway_seconds: u64,
    pub(super) cache_ttl: Duration,
    pub(super) max_cache_entries: usize,
}

impl OpaqueIntrospectionConfig {
    /// Creates an opaque-token configuration bound to one HTTPS issuer and endpoint.
    ///
    /// The client authentication setting is sent only to this configured endpoint. `None` is
    /// available for providers that explicitly support public or network-authenticated resource
    /// servers; confidential client authentication is normally the production choice.
    ///
    /// # Errors
    ///
    /// Returns [`OpaqueIntrospectionConfigError`] when a trusted identifier is invalid, HTTP
    /// Basic credentials are too large, or the endpoint is not an absolute HTTPS URL without
    /// credentials or a fragment.
    pub fn new(
        issuer: impl Into<String>,
        audience: impl Into<String>,
        endpoint: Url,
        client_id: impl Into<String>,
        authentication: OidcClientAuthentication,
    ) -> Result<Self, OpaqueIntrospectionConfigError> {
        let issuer = issuer.into();
        let audience = audience.into();
        let client_id = client_id.into();
        if issuer.trim().is_empty() || audience.trim().is_empty() || client_id.trim().is_empty() {
            return Err(OpaqueIntrospectionConfigError::BlankField);
        }
        if !valid_text(&issuer, MAX_ISSUER_BYTES)
            || !valid_text(&audience, MAX_AUDIENCE_BYTES)
            || !valid_client_id(&client_id)
        {
            return Err(OpaqueIntrospectionConfigError::InvalidField);
        }
        if !basic_authorization_is_within_limit(&client_id, &authentication) {
            return Err(OpaqueIntrospectionConfigError::ClientAuthenticationTooLarge);
        }
        if !is_valid_introspection_url(&endpoint) {
            return Err(OpaqueIntrospectionConfigError::InvalidEndpoint);
        }
        Ok(Self {
            issuer,
            audience,
            endpoint,
            client_id,
            authentication,
            leeway_seconds: 0,
            cache_ttl: DEFAULT_CACHE_TTL,
            max_cache_entries: DEFAULT_MAX_CACHE_ENTRIES,
        })
    }

    /// Allows bounded clock skew for supplied `exp` and `nbf` values.
    ///
    /// # Errors
    ///
    /// Returns [`OpaqueIntrospectionConfigError::LeewayTooLarge`] when the requested leeway
    /// exceeds five minutes.
    pub fn with_leeway_seconds(
        mut self,
        leeway_seconds: u64,
    ) -> Result<Self, OpaqueIntrospectionConfigError> {
        if leeway_seconds > MAX_LEEWAY_SECONDS {
            return Err(OpaqueIntrospectionConfigError::LeewayTooLarge);
        }
        self.leeway_seconds = leeway_seconds;
        Ok(self)
    }

    /// Sets the maximum successful-response cache lifetime.
    ///
    /// A zero duration disables caching and bypasses local cache state. A cached entry is always
    /// additionally capped at its introspection response's `exp` value, so it cannot outlive the
    /// token itself.
    #[must_use]
    pub const fn with_cache_ttl(mut self, cache_ttl: Duration) -> Self {
        self.cache_ttl = cache_ttl;
        self
    }

    /// Limits the number of cached token fingerprints retained in memory.
    ///
    /// A zero limit disables caching and bypasses local cache state. When a full cache contains
    /// only live entries, new tokens are verified remotely instead of evicting a still-valid
    /// entry unexpectedly.
    #[must_use]
    pub const fn with_max_cache_entries(mut self, max_cache_entries: usize) -> Self {
        self.max_cache_entries = max_cache_entries;
        self
    }

    /// Returns the required introspection response issuer.
    #[must_use]
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Returns the audience required in an active response.
    #[must_use]
    pub fn audience(&self) -> &str {
        &self.audience
    }

    /// Returns the configured HTTPS introspection endpoint.
    #[must_use]
    pub const fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    /// Returns the OAuth resource-server client ID.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the configured introspection client authentication method.
    #[must_use]
    pub const fn authentication(&self) -> &OidcClientAuthentication {
        &self.authentication
    }

    /// Returns the maximum duration a successful response may be cached.
    #[must_use]
    pub const fn cache_ttl(&self) -> Duration {
        self.cache_ttl
    }

    /// Returns the maximum number of token fingerprints cached in memory.
    #[must_use]
    pub const fn max_cache_entries(&self) -> usize {
        self.max_cache_entries
    }
}

impl fmt::Debug for OpaqueIntrospectionConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpaqueIntrospectionConfig")
            .field("issuer", &"[REDACTED]")
            .field("audience", &"[REDACTED]")
            .field("endpoint", &"[REDACTED]")
            .field("client_id", &"[REDACTED]")
            .field("authentication", &self.authentication)
            .field("leeway_seconds", &self.leeway_seconds)
            .field("cache_ttl", &self.cache_ttl)
            .field("max_cache_entries", &self.max_cache_entries)
            .finish()
    }
}

/// Invalid opaque-token introspection settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OpaqueIntrospectionConfigError {
    /// An issuer, audience, or client ID was blank.
    #[error("opaque introspection issuer, audience, and client ID must not be blank")]
    BlankField,
    /// An issuer, audience, or client ID was oversized or contained control characters.
    #[error(
        "opaque introspection issuer, audience, and client ID must be bounded and free of control characters"
    )]
    InvalidField,
    /// Basic client credentials would exceed the supported HTTP header size.
    #[error("opaque introspection HTTP Basic client credentials exceed the supported header size")]
    ClientAuthenticationTooLarge,
    /// The configured clock skew exceeded the fixed validation bound.
    #[error("opaque introspection clock-skew leeway must not exceed five minutes")]
    LeewayTooLarge,
    /// The introspection endpoint was not a valid HTTPS endpoint.
    #[error(
        "opaque introspection endpoint must be an absolute HTTPS URL without credentials or a fragment"
    )]
    InvalidEndpoint,
    /// The HTTP client timeout was zero.
    #[error("opaque introspection HTTP timeout must be greater than zero")]
    ZeroHttpTimeout,
    /// The HTTP client could not be initialized.
    #[error("opaque introspection HTTP client could not be initialized")]
    HttpClientInitialization,
}

fn is_valid_introspection_url(url: &Url) -> bool {
    valid_trusted_https_url(url)
}
