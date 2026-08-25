//! Authorization-code token-exchange contracts and bounded HTTPS transport.

use std::{fmt, time::Duration};

use futures_util::future::BoxFuture;
use reqwest::Client;
use serde::Deserialize;
use url::Url;

use crate::{
    OidcClientAuthentication, OidcHttpError, client_auth::basic_authorization_header,
    http::decode_json_response,
};

use super::super::OidcBrowserConfigError;
use super::http_client;

/// An authorization-code request passed to a trusted token-endpoint adapter.
#[derive(Clone)]
pub struct OidcTokenExchangeRequest {
    pub(in crate::browser_login) client_id: String,
    pub(in crate::browser_login) authentication: OidcClientAuthentication,
    pub(in crate::browser_login) code: String,
    pub(in crate::browser_login) redirect_uri: Url,
    pub(in crate::browser_login) code_verifier: String,
}

impl OidcTokenExchangeRequest {
    /// Returns the configured OAuth client ID.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the configured confidential-client authentication setting.
    #[must_use]
    pub const fn authentication(&self) -> &OidcClientAuthentication {
        &self.authentication
    }

    /// Exposes the provider-issued authorization code only to a trusted exchanger.
    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Returns the exact redirect URI bound to this authorization code.
    #[must_use]
    pub const fn redirect_uri(&self) -> &Url {
        &self.redirect_uri
    }

    /// Exposes the one-time PKCE verifier only to a trusted exchanger.
    #[must_use]
    pub fn code_verifier(&self) -> &str {
        &self.code_verifier
    }
}

impl fmt::Debug for OidcTokenExchangeRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OidcTokenExchangeRequest")
            .field("client_id", &"[REDACTED]")
            .field("authentication", &self.authentication)
            .field("code", &"[REDACTED]")
            .field("redirect_uri", &"[REDACTED]")
            .field("code_verifier", &"[REDACTED]")
            .finish()
    }
}

/// Token-endpoint result limited to the ID token needed to establish a Rustee browser session.
#[derive(Clone)]
pub struct OidcTokenResponse {
    id_token: Option<String>,
}

impl OidcTokenResponse {
    /// Creates a token response from a provider-supplied optional ID token.
    ///
    /// [`crate::OidcBrowserLogin`] rejects blank or oversized ID tokens before invoking its
    /// verifier.
    #[must_use]
    pub fn new(id_token: Option<String>) -> Self {
        Self { id_token }
    }

    pub(in crate::browser_login) fn into_id_token(self) -> Option<String> {
        self.id_token.filter(|token| !token.trim().is_empty())
    }
}

impl fmt::Debug for OidcTokenResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OidcTokenResponse")
            .field("id_token", &self.id_token.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

/// Exchanges a verified callback's authorization code at the transaction-bound token endpoint.
pub trait OidcTokenExchanger: Clone + Send + Sync + 'static {
    /// Exchanger-specific failure type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Performs an Authorization Code + PKCE token request.
    fn exchange(
        &self,
        endpoint: Url,
        request: OidcTokenExchangeRequest,
    ) -> BoxFuture<'static, Result<OidcTokenResponse, Self::Error>>;
}

/// HTTPS token-endpoint exchanger with a bounded JSON response for production browser login.
#[derive(Clone)]
pub struct HttpOidcTokenExchanger {
    client: Client,
}

impl HttpOidcTokenExchanger {
    /// Creates a token exchanger with a finite HTTP timeout.
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

impl fmt::Debug for HttpOidcTokenExchanger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpOidcTokenExchanger")
            .finish_non_exhaustive()
    }
}

impl OidcTokenExchanger for HttpOidcTokenExchanger {
    type Error = OidcHttpError;

    fn exchange(
        &self,
        endpoint: Url,
        request: OidcTokenExchangeRequest,
    ) -> BoxFuture<'static, Result<OidcTokenResponse, Self::Error>> {
        let client = self.client.clone();
        Box::pin(async move {
            let mut form = vec![
                ("grant_type", "authorization_code".to_owned()),
                ("code", request.code),
                ("redirect_uri", request.redirect_uri.into()),
                ("code_verifier", request.code_verifier),
            ];
            let request = match request.authentication {
                OidcClientAuthentication::None => {
                    form.push(("client_id", request.client_id));
                    client.post(endpoint)
                }
                OidcClientAuthentication::ClientSecretPost(secret) => {
                    form.push(("client_id", request.client_id));
                    form.push(("client_secret", secret.expose().to_owned()));
                    client.post(endpoint)
                }
                OidcClientAuthentication::ClientSecretBasic(secret) => {
                    client.post(endpoint).header(
                        "authorization",
                        basic_authorization_header(&request.client_id, &secret),
                    )
                }
            };
            let response = request
                .header("accept", "application/json")
                .form(&form)
                .send()
                .await
                .map_err(|_| OidcHttpError::Request)?
                .error_for_status()
                .map_err(|_| OidcHttpError::Request)?;
            let response: OidcTokenResponseWire = decode_json_response(response).await?;
            Ok(OidcTokenResponse::new(response.id_token))
        })
    }
}

#[derive(Deserialize)]
struct OidcTokenResponseWire {
    #[serde(default)]
    id_token: Option<String>,
}
