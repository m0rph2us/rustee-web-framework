//! OIDC discovery metadata admission and bounded HTTPS retrieval.

use std::{fmt, time::Duration};

use futures_util::future::BoxFuture;
use reqwest::Client;
use serde::Deserialize;
use url::Url;

use crate::{OidcHttpError, http::decode_json_response};

use super::super::{
    OidcBrowserConfig, OidcBrowserConfigError, OidcLoginError,
    config::{is_valid_https_url, is_valid_issuer_url},
};
use super::http_client;

/// The OIDC discovery metadata needed for browser authorization-code login.
#[derive(Clone, Deserialize, Eq, PartialEq)]
pub struct OidcProviderMetadata {
    pub(in crate::browser_login) issuer: String,
    pub(in crate::browser_login) authorization_endpoint: Url,
    pub(in crate::browser_login) token_endpoint: Url,
    #[serde(rename = "jwks_uri")]
    pub(in crate::browser_login) jwks_url: Url,
}

impl fmt::Debug for OidcProviderMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OidcProviderMetadata")
            .field("issuer", &"[REDACTED]")
            .field("authorization_endpoint", &"[REDACTED]")
            .field("token_endpoint", &"[REDACTED]")
            .field("jwks_url", &"[REDACTED]")
            .finish()
    }
}

impl OidcProviderMetadata {
    /// Returns the provider-declared issuer identifier.
    #[must_use]
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Returns the provider authorization endpoint.
    #[must_use]
    pub const fn authorization_endpoint(&self) -> &Url {
        &self.authorization_endpoint
    }

    /// Returns the provider token endpoint.
    #[must_use]
    pub const fn token_endpoint(&self) -> &Url {
        &self.token_endpoint
    }

    /// Returns the provider JWKS endpoint.
    #[must_use]
    pub const fn jwks_url(&self) -> &Url {
        &self.jwks_url
    }

    pub(in crate::browser_login) fn validate(
        &self,
        config: &OidcBrowserConfig,
    ) -> Result<(), OidcLoginError> {
        let Ok(issuer) = Url::parse(&self.issuer) else {
            return Err(OidcLoginError::InvalidProviderMetadata);
        };
        if issuer != *config.issuer()
            || !is_valid_issuer_url(&issuer)
            || !is_valid_https_url(&self.authorization_endpoint)
            || !is_valid_https_url(&self.token_endpoint)
            || !is_valid_https_url(&self.jwks_url)
            || self.jwks_url != *config.jwks_url()
        {
            return Err(OidcLoginError::InvalidProviderMetadata);
        }
        Ok(())
    }
}

/// Fetches an OIDC discovery document for one configured issuer.
pub trait OidcDiscovery: Clone + Send + Sync + 'static {
    /// Fetcher-specific failure type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Returns provider metadata for the supplied trusted issuer URL.
    fn discover(
        &self,
        issuer: Url,
    ) -> BoxFuture<'static, Result<OidcProviderMetadata, Self::Error>>;
}

/// HTTPS OIDC discovery-document fetcher with a bounded JSON response.
#[derive(Clone)]
pub struct HttpOidcDiscovery {
    client: Client,
}

impl HttpOidcDiscovery {
    /// Creates a discovery fetcher with a finite HTTP timeout.
    ///
    /// # Errors
    ///
    /// Returns [`OidcBrowserConfigError`] when the timeout is zero or the client cannot be built.
    pub fn new(timeout: Duration) -> Result<Self, OidcBrowserConfigError> {
        Ok(Self {
            client: http_client(timeout)?,
        })
    }
}

impl fmt::Debug for HttpOidcDiscovery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpOidcDiscovery")
            .finish_non_exhaustive()
    }
}

impl OidcDiscovery for HttpOidcDiscovery {
    type Error = OidcHttpError;

    fn discover(
        &self,
        issuer: Url,
    ) -> BoxFuture<'static, Result<OidcProviderMetadata, Self::Error>> {
        let client = self.client.clone();
        Box::pin(async move {
            let response = client
                .get(discovery_document_url(issuer))
                .send()
                .await
                .map_err(|_| OidcHttpError::Request)?
                .error_for_status()
                .map_err(|_| OidcHttpError::Request)?;
            decode_json_response(response).await
        })
    }
}

fn discovery_document_url(mut issuer: Url) -> Url {
    let path = issuer.path().trim_end_matches('/');
    issuer.set_path(&format!("{path}/.well-known/openid-configuration"));
    issuer.set_query(None);
    issuer
}
