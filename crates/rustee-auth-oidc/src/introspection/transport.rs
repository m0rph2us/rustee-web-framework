//! Opaque-token introspection adapter contracts and bounded HTTPS transport.

use std::{fmt, time::Duration};

use futures_util::future::BoxFuture;
use reqwest::Client;
use url::Url;

use super::{OpaqueTokenIntrospection, config::OpaqueIntrospectionConfigError};
use crate::{
    OidcClientAuthentication, OidcHttpError, client_auth::basic_authorization_header,
    http::decode_json_response,
};

/// A raw opaque credential and the trusted client identity used to introspect it.
///
/// This request deliberately has a redacted [`Debug`] implementation. Custom introspectors must
/// use the token only for the outgoing provider request and must never log or persist it.
#[derive(Clone)]
pub struct OpaqueTokenIntrospectionRequest {
    token: String,
    client_id: String,
    authentication: OidcClientAuthentication,
}

impl OpaqueTokenIntrospectionRequest {
    pub(super) fn new(
        token: String,
        client_id: String,
        authentication: OidcClientAuthentication,
    ) -> Self {
        Self {
            token,
            client_id,
            authentication,
        }
    }

    /// Returns the raw credential for a trusted custom introspection adapter.
    ///
    /// The value is a bearer credential and must not be logged, serialized, or returned in an
    /// application response.
    #[must_use]
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Returns the resource-server client ID.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns client authentication to use exclusively at the trusted endpoint.
    #[must_use]
    pub const fn authentication(&self) -> &OidcClientAuthentication {
        &self.authentication
    }
}

impl fmt::Debug for OpaqueTokenIntrospectionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpaqueTokenIntrospectionRequest")
            .field("token", &"[REDACTED]")
            .field("client_id", &"[REDACTED]")
            .field("authentication", &self.authentication)
            .finish()
    }
}

/// An adapter that retrieves a provider's opaque-token introspection response.
pub trait OpaqueTokenIntrospector: Clone + Send + Sync + 'static {
    /// Adapter-specific failure type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Retrieves one response from the configured trusted endpoint.
    fn introspect(
        &self,
        endpoint: Url,
        request: OpaqueTokenIntrospectionRequest,
    ) -> BoxFuture<'static, Result<OpaqueTokenIntrospection, Self::Error>>;
}

/// HTTPS opaque-token introspection adapter with a bounded JSON response for production use.
#[derive(Clone)]
pub struct HttpOpaqueTokenIntrospector {
    client: Client,
}

impl HttpOpaqueTokenIntrospector {
    /// Creates an introspection adapter with a finite request timeout.
    ///
    /// # Errors
    ///
    /// Returns [`OpaqueIntrospectionConfigError`] when the timeout is zero or the client cannot
    /// be built.
    pub fn new(timeout: Duration) -> Result<Self, OpaqueIntrospectionConfigError> {
        if timeout.is_zero() {
            return Err(OpaqueIntrospectionConfigError::ZeroHttpTimeout);
        }
        let client = Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| OpaqueIntrospectionConfigError::HttpClientInitialization)?;
        Ok(Self { client })
    }
}

impl fmt::Debug for HttpOpaqueTokenIntrospector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpOpaqueTokenIntrospector")
            .finish_non_exhaustive()
    }
}

impl OpaqueTokenIntrospector for HttpOpaqueTokenIntrospector {
    type Error = OidcHttpError;

    fn introspect(
        &self,
        endpoint: Url,
        request: OpaqueTokenIntrospectionRequest,
    ) -> BoxFuture<'static, Result<OpaqueTokenIntrospection, Self::Error>> {
        let client = self.client.clone();
        Box::pin(async move {
            let mut form = vec![
                ("token", request.token),
                ("token_type_hint", "access_token".to_owned()),
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
            decode_json_response(response).await
        })
    }
}
