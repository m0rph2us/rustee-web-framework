//! Trusted, redacted `RabbitMQ` Management API endpoint configuration.

use std::{fmt, time::Duration};

use url::{Host, Url};

const DEFAULT_MAX_RESPONSE_BYTES: usize = 1024 * 1024;

/// Redacted settings for a read-only `RabbitMQ` Management HTTP API client.
///
/// Its `Debug` output keeps the endpoint, credentials, and virtual host redacted.
#[derive(Clone, Eq, PartialEq)]
pub struct RabbitMqManagementConfig {
    pub(crate) base_url: Url,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) vhost: String,
    pub(crate) request_timeout: Duration,
    pub(crate) max_response_bytes: usize,
}

impl RabbitMqManagementConfig {
    /// Creates settings for one management API virtual host.
    ///
    /// HTTP is permitted only for loopback test endpoints. Every non-loopback management endpoint
    /// must use HTTPS and a monitor-only account.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqManagementConfigError`] for an unsafe URL or invalid credentials/vhost.
    pub fn new(
        mut base_url: Url,
        username: impl Into<String>,
        password: impl Into<String>,
        vhost: impl Into<String>,
    ) -> Result<Self, RabbitMqManagementConfigError> {
        let username = username.into();
        let password = password.into();
        let vhost = vhost.into();
        if !valid_base_url(&base_url) {
            return Err(RabbitMqManagementConfigError::InvalidBaseUrl);
        }
        if username.trim().is_empty()
            || password.is_empty()
            || vhost.contains('\0')
            || !is_safe_url_path_segment(&vhost)
        {
            return Err(RabbitMqManagementConfigError::InvalidCredentialOrVhost);
        }
        if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }
        Ok(Self {
            base_url,
            username,
            password,
            vhost,
            request_timeout: Duration::from_secs(5),
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
        })
    }

    /// Sets a non-zero bounded management API request timeout.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqManagementConfigError::ZeroTimeout`] for a zero duration.
    pub fn with_request_timeout(
        mut self,
        request_timeout: Duration,
    ) -> Result<Self, RabbitMqManagementConfigError> {
        if request_timeout.is_zero() {
            return Err(RabbitMqManagementConfigError::ZeroTimeout);
        }
        self.request_timeout = request_timeout;
        Ok(self)
    }

    /// Sets the maximum decoded queue snapshot size accepted from the Management API.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqManagementConfigError::ZeroResponseLimit`] for a zero limit.
    pub fn with_max_response_bytes(
        mut self,
        max_response_bytes: usize,
    ) -> Result<Self, RabbitMqManagementConfigError> {
        if max_response_bytes == 0 {
            return Err(RabbitMqManagementConfigError::ZeroResponseLimit);
        }
        self.max_response_bytes = max_response_bytes;
        Ok(self)
    }
}

impl fmt::Debug for RabbitMqManagementConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RabbitMqManagementConfig")
            .field("base_url", &"[REDACTED]")
            .field("base_url_length", &self.base_url.as_str().len())
            .field("vhost", &"[REDACTED]")
            .field("vhost_length", &self.vhost.len())
            .field("request_timeout", &self.request_timeout)
            .field("max_response_bytes", &self.max_response_bytes)
            .finish_non_exhaustive()
    }
}

/// Invalid management audit client configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RabbitMqManagementConfigError {
    /// The base URL was unsafe or was not a clean HTTP(S) URL without embedded credentials.
    #[error(
        "RabbitMQ management base URL must use HTTPS unless it is loopback, without credentials, query, or fragment"
    )]
    InvalidBaseUrl,
    /// The read-only account or virtual host was malformed.
    #[error("RabbitMQ management username, password, and virtual host must be non-empty and valid")]
    InvalidCredentialOrVhost,
    /// Requests must use a finite timeout.
    #[error("RabbitMQ management request timeout must be non-zero")]
    ZeroTimeout,
    /// Queue snapshots must use a non-zero response limit.
    #[error("RabbitMQ management response limit must be non-zero")]
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

pub(crate) fn is_safe_url_path_segment(value: &str) -> bool {
    !matches!(value, "." | "..")
}
