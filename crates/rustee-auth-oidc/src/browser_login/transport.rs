//! Browser callback and redirect responses plus shared OIDC HTTP-client policy.

use std::{fmt, time::Duration};

use http::{HeaderValue, StatusCode, header::LOCATION};
use reqwest::Client;
use rustee_core::{IntoResponse, Response, empty_body, response};
use serde::Deserialize;
use url::Url;

use super::{MAX_AUTHORIZATION_REDIRECT_BYTES, OidcBrowserConfigError, OidcLoginError};

mod discovery;
mod token;

pub use discovery::{HttpOidcDiscovery, OidcDiscovery, OidcProviderMetadata};
pub use token::{
    HttpOidcTokenExchanger, OidcTokenExchangeRequest, OidcTokenExchanger, OidcTokenResponse,
};

/// A redirect target that a browser login handler returns with HTTP 302 or 303.
#[derive(Clone, Eq, PartialEq)]
pub struct AuthorizationRedirect {
    pub(super) location: Url,
    location_header: HeaderValue,
}

impl fmt::Debug for AuthorizationRedirect {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationRedirect")
            .field("location", &"[REDACTED]")
            .finish()
    }
}

impl AuthorizationRedirect {
    pub(super) fn new(location: Url) -> Result<Self, OidcLoginError> {
        let value = location.as_str();
        if value.len() > MAX_AUTHORIZATION_REDIRECT_BYTES {
            return Err(OidcLoginError::InvalidProviderMetadata);
        }
        let location_header =
            HeaderValue::from_str(value).map_err(|_| OidcLoginError::InvalidProviderMetadata)?;
        Ok(Self {
            location,
            location_header,
        })
    }

    /// Returns the fully-bound provider authorization URL.
    #[must_use]
    pub const fn location(&self) -> &Url {
        &self.location
    }
}

impl IntoResponse for AuthorizationRedirect {
    fn into_response(self) -> Response {
        let mut response = response(StatusCode::FOUND, empty_body());
        response
            .headers_mut()
            .insert(LOCATION, self.location_header);
        response
    }
}

/// Query values returned by an OIDC authorization callback.
#[derive(Clone, Deserialize)]
pub struct AuthorizationCallback {
    /// The authorization code when the provider accepted login.
    #[serde(default)]
    pub code: Option<String>,
    /// The state returned by the provider and bound to one server-side transaction.
    #[serde(default)]
    pub state: Option<String>,
    /// A provider error code, if authorization was rejected.
    #[serde(default)]
    pub error: Option<String>,
    /// Provider diagnostic text that Rustee intentionally does not expose in responses.
    #[serde(default)]
    pub error_description: Option<String>,
}

impl fmt::Debug for AuthorizationCallback {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationCallback")
            .field("code", &self.code.as_ref().map(|_| "[REDACTED]"))
            .field("state", &self.state.as_ref().map(|_| "[REDACTED]"))
            .field("error", &self.error.as_ref().map(|_| "[REDACTED]"))
            .field(
                "error_description",
                &self.error_description.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

fn http_client(timeout: Duration) -> Result<Client, OidcBrowserConfigError> {
    if timeout.is_zero() {
        return Err(OidcBrowserConfigError::ZeroHttpTimeout);
    }
    Client::builder()
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| OidcBrowserConfigError::HttpClientInitialization)
}
