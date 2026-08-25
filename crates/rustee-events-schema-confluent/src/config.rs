//! Trusted, redacted Confluent Schema Registry endpoint configuration.

use std::{fmt, time::Duration};

use url::{Host, Url};

const DEFAULT_MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// Authentication applied to a Confluent Schema Registry request.
#[derive(Clone, Eq, PartialEq)]
pub enum ConfluentSchemaRegistryAuth {
    /// A Confluent API key and secret sent using HTTP Basic authentication.
    Basic {
        /// Registry API key.
        api_key: String,
        /// Registry API secret.
        api_secret: String,
    },
    /// An application-managed OAuth or service bearer token.
    Bearer(String),
    /// No HTTP authorization header; intended for a loopback test registry or an externally
    /// authenticated TLS client injected with [`crate::ConfluentSchemaRegistry::with_client`].
    None,
}

impl ConfluentSchemaRegistryAuth {
    pub(super) fn validate(&self) -> bool {
        match self {
            Self::Basic {
                api_key,
                api_secret,
            } => !api_key.trim().is_empty() && !api_secret.is_empty(),
            Self::Bearer(token) => !token.trim().is_empty(),
            Self::None => true,
        }
    }
}

impl fmt::Debug for ConfluentSchemaRegistryAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Basic { .. } => "basic",
            Self::Bearer(_) => "bearer",
            Self::None => "none",
        };
        formatter
            .debug_struct("ConfluentSchemaRegistryAuth")
            .field("kind", &kind)
            .finish()
    }
}

/// Redacted configuration for a Confluent Schema Registry deployment endpoint.
#[derive(Clone, Eq, PartialEq)]
pub struct ConfluentSchemaRegistryConfig {
    pub(crate) base_url: Url,
    pub(crate) auth: ConfluentSchemaRegistryAuth,
    pub(crate) request_timeout: Duration,
    pub(crate) max_response_bytes: usize,
}

impl ConfluentSchemaRegistryConfig {
    /// Creates configuration for one registry endpoint.
    ///
    /// Non-loopback registry endpoints must use HTTPS. The URL must not contain credentials,
    /// query parameters, or fragments; use [`ConfluentSchemaRegistryAuth`] or an injected TLS
    /// client for credentials instead.
    ///
    /// # Errors
    ///
    /// Returns [`ConfluentSchemaRegistryConfigError`] for an unsafe endpoint or empty credential.
    pub fn new(
        mut base_url: Url,
        auth: ConfluentSchemaRegistryAuth,
    ) -> Result<Self, ConfluentSchemaRegistryConfigError> {
        if !valid_base_url(&base_url) {
            return Err(ConfluentSchemaRegistryConfigError::InvalidBaseUrl);
        }
        if !auth.validate() {
            return Err(ConfluentSchemaRegistryConfigError::InvalidAuthentication);
        }
        if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }
        Ok(Self {
            base_url,
            auth,
            request_timeout: Duration::from_secs(5),
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
        })
    }

    /// Sets the bounded timeout for one registry HTTP request.
    ///
    /// # Errors
    ///
    /// Returns [`ConfluentSchemaRegistryConfigError::ZeroTimeout`] when `request_timeout` is zero.
    pub fn with_request_timeout(
        mut self,
        request_timeout: Duration,
    ) -> Result<Self, ConfluentSchemaRegistryConfigError> {
        if request_timeout.is_zero() {
            return Err(ConfluentSchemaRegistryConfigError::ZeroTimeout);
        }
        self.request_timeout = request_timeout;
        Ok(self)
    }

    /// Sets the maximum decoded JSON response size accepted from the registry.
    ///
    /// # Errors
    ///
    /// Returns [`ConfluentSchemaRegistryConfigError::ZeroResponseLimit`] when the limit is zero.
    pub fn with_max_response_bytes(
        mut self,
        max_response_bytes: usize,
    ) -> Result<Self, ConfluentSchemaRegistryConfigError> {
        if max_response_bytes == 0 {
            return Err(ConfluentSchemaRegistryConfigError::ZeroResponseLimit);
        }
        self.max_response_bytes = max_response_bytes;
        Ok(self)
    }
}

impl fmt::Debug for ConfluentSchemaRegistryConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfluentSchemaRegistryConfig")
            .field("base_url", &"[REDACTED]")
            .field("base_url_length", &self.base_url.as_str().len())
            .field("auth", &self.auth)
            .field("request_timeout", &self.request_timeout)
            .field("max_response_bytes", &self.max_response_bytes)
            .finish()
    }
}

/// Invalid Confluent Schema Registry adapter configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConfluentSchemaRegistryConfigError {
    /// The endpoint was not a clean HTTPS URL or a loopback HTTP test URL.
    #[error(
        "Confluent Schema Registry URL must use HTTPS unless it is loopback, without credentials, query, or fragment"
    )]
    InvalidBaseUrl,
    /// Configured Basic or bearer authentication was blank.
    #[error("Confluent Schema Registry authentication must not be blank")]
    InvalidAuthentication,
    /// The adapter must bound every registry request.
    #[error("Confluent Schema Registry request timeout must be non-zero")]
    ZeroTimeout,
    /// Successful JSON responses must have a non-zero memory bound.
    #[error("Confluent Schema Registry response limit must be non-zero")]
    ZeroResponseLimit,
}

fn valid_base_url(value: &Url) -> bool {
    matches!(value.scheme(), "http" | "https")
        && value.host().is_some()
        && value.username().is_empty()
        && value.password().is_none()
        && value.query().is_none()
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
