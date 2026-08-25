//! Bounded HTTPS JWKS retrieval and its injectable fetcher contract.

use std::{fmt, time::Duration};

use futures_util::future::BoxFuture;
use jsonwebtoken::jwk::JwkSet;
use reqwest::Client;
use url::Url;

use super::super::{
    OidcHttpError,
    http::decode_json_response,
    resource_server_config::{OidcConfigError, is_valid_jwks_url},
};

/// Fetches a JSON Web Key Set for one trusted configuration endpoint.
pub trait JwksFetcher: Clone + Send + Sync + 'static {
    /// Fetcher-specific failure type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Retrieves the current key set.
    fn fetch(&self) -> BoxFuture<'static, Result<JwkSet, Self::Error>>;
}

/// HTTPS-capable JWKS fetcher with a bounded JSON response for production OIDC deployments.
#[derive(Clone)]
pub struct HttpJwksFetcher {
    client: Client,
    url: Url,
}

impl HttpJwksFetcher {
    /// Creates a JWKS fetcher with a finite timeout and an HTTPS endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`OidcConfigError`] if the endpoint is not valid for a remote JWKS or the client
    /// timeout is zero.
    pub fn new(url: Url, timeout: Duration) -> Result<Self, OidcConfigError> {
        if !is_valid_jwks_url(&url) {
            return Err(OidcConfigError::InvalidJwksUrl);
        }
        if timeout.is_zero() {
            return Err(OidcConfigError::ZeroFetchTimeout);
        }
        let client = Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| OidcConfigError::HttpClientInitialization)?;
        Ok(Self { client, url })
    }
}

impl fmt::Debug for HttpJwksFetcher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpJwksFetcher")
            .field("url", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

impl JwksFetcher for HttpJwksFetcher {
    type Error = OidcHttpError;

    fn fetch(&self) -> BoxFuture<'static, Result<JwkSet, Self::Error>> {
        let client = self.client.clone();
        let url = self.url.clone();
        Box::pin(async move {
            let response = client
                .get(url)
                .send()
                .await
                .map_err(|_| OidcHttpError::Request)?
                .error_for_status()
                .map_err(|_| OidcHttpError::Request)?;
            decode_json_response(response).await
        })
    }
}
