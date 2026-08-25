//! Bounded HTTP OAuth token-exchange and revocation adapter.

use std::{
    fmt,
    time::{Duration, SystemTime},
};

use futures_util::{StreamExt, future::BoxFuture};
use reqwest::{Client, header::ACCEPT};
use serde::Deserialize;
use url::Url;

use super::{
    McpOAuthRefreshRequest, McpOAuthRevocationRequest, McpOAuthTokenExchanger,
    McpOAuthTokenRevoker, McpOAuthTokenSet,
};
use crate::{
    McpOAuthAccessToken, McpOAuthClientConfig, McpOAuthError, McpOAuthTokenExchangeRequest,
    config::valid_resource_url, is_json_content_type,
};

pub(crate) const MAX_TOKEN_RESPONSE_BYTES: usize = 512 * 1024;

/// Bounded HTTP exchanger for a pre-registered public OAuth client.
#[derive(Clone)]
pub struct HttpMcpOAuthTokenExchanger {
    client: Client,
}

impl HttpMcpOAuthTokenExchanger {
    /// Creates a token exchanger with the configured finite HTTP timeout.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthError::HttpClient`] if the HTTP client cannot be initialized.
    pub fn new(config: &McpOAuthClientConfig) -> Result<Self, McpOAuthError> {
        let client = Client::builder()
            .timeout(config.http_timeout())
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| McpOAuthError::HttpClient)?;
        Ok(Self { client })
    }
}

impl fmt::Debug for HttpMcpOAuthTokenExchanger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpMcpOAuthTokenExchanger")
            .finish_non_exhaustive()
    }
}

impl McpOAuthTokenExchanger for HttpMcpOAuthTokenExchanger {
    type Error = McpOAuthError;

    fn exchange(
        &self,
        endpoint: Url,
        request: McpOAuthTokenExchangeRequest,
    ) -> BoxFuture<'static, Result<McpOAuthTokenSet, Self::Error>> {
        let client = self.client.clone();
        Box::pin(async move {
            if !valid_resource_url(&endpoint) {
                return Err(McpOAuthError::InvalidMetadata);
            }
            let resource = request.resource.clone();
            let form = vec![
                ("grant_type", "authorization_code".to_owned()),
                ("client_id", request.client_id),
                ("code", request.code),
                ("redirect_uri", request.redirect_uri.into()),
                ("code_verifier", request.code_verifier),
                ("resource", resource.to_string()),
            ];
            fetch_token_set(client, endpoint, form, resource, None).await
        })
    }

    fn refresh(
        &self,
        endpoint: Url,
        request: McpOAuthRefreshRequest,
    ) -> BoxFuture<'static, Result<McpOAuthTokenSet, Self::Error>> {
        let client = self.client.clone();
        Box::pin(async move {
            if !valid_resource_url(&endpoint) {
                return Err(McpOAuthError::InvalidMetadata);
            }
            let resource = request.resource.clone();
            let fallback_refresh_token = request.refresh_token.clone();
            let form = vec![
                ("grant_type", "refresh_token".to_owned()),
                ("client_id", request.client_id),
                ("refresh_token", request.refresh_token),
                ("resource", resource.to_string()),
            ];
            fetch_token_set(
                client,
                endpoint,
                form,
                resource,
                Some(fallback_refresh_token),
            )
            .await
        })
    }
}

impl McpOAuthTokenRevoker for HttpMcpOAuthTokenExchanger {
    type Error = McpOAuthError;

    fn revoke(
        &self,
        endpoint: Url,
        request: McpOAuthRevocationRequest,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let client = self.client.clone();
        Box::pin(async move {
            if !valid_resource_url(&endpoint) {
                return Err(McpOAuthError::InvalidMetadata);
            }
            let form = vec![
                ("client_id", request.client_id),
                ("token", request.token),
                (
                    "token_type_hint",
                    request.token_type_hint.as_str().to_owned(),
                ),
            ];
            let response = client
                .post(endpoint)
                .header(ACCEPT, "application/json")
                .form(&form)
                .send()
                .await
                .map_err(|_| McpOAuthError::RevocationUnavailable)?;
            response
                .status()
                .is_success()
                .then_some(())
                .ok_or(McpOAuthError::RevocationUnavailable)
        })
    }
}

#[derive(Deserialize)]
struct TokenResponseWire {
    access_token: String,
    token_type: String,
    #[serde(default)]
    expires_in: Option<u64>,
    #[serde(default)]
    refresh_token: Option<String>,
}

async fn fetch_token_set(
    client: Client,
    endpoint: Url,
    form: Vec<(&'static str, String)>,
    resource: Url,
    fallback_refresh_token: Option<String>,
) -> Result<McpOAuthTokenSet, McpOAuthError> {
    let response = client
        .post(endpoint)
        .header(ACCEPT, "application/json")
        .form(&form)
        .send()
        .await
        .map_err(|_| McpOAuthError::TokenExchangeUnavailable)?;
    if !response.status().is_success()
        || response
            .content_length()
            .is_some_and(|bytes| bytes > MAX_TOKEN_RESPONSE_BYTES as u64)
        || !is_json_content_type(response.headers())
    {
        return Err(McpOAuthError::TokenExchangeUnavailable);
    }
    let body = collect_token_response_body(response).await?;
    let response: TokenResponseWire =
        serde_json::from_slice(&body).map_err(|_| McpOAuthError::TokenExchangeUnavailable)?;
    if !response.token_type.eq_ignore_ascii_case("bearer") {
        return Err(McpOAuthError::TokenExchangeUnavailable);
    }
    let expires_at = response
        .expires_in
        .and_then(|seconds| SystemTime::now().checked_add(Duration::from_secs(seconds)));
    let access_token = McpOAuthAccessToken::new(response.access_token, expires_at)
        .map_err(|_| McpOAuthError::TokenExchangeUnavailable)?;
    McpOAuthTokenSet::new(
        resource,
        access_token,
        response.refresh_token.or(fallback_refresh_token),
    )
    .map_err(|_| McpOAuthError::TokenExchangeUnavailable)
}

async fn collect_token_response_body(
    response: reqwest::Response,
) -> Result<Vec<u8>, McpOAuthError> {
    let mut body = Vec::new();
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|_| McpOAuthError::TokenExchangeUnavailable)?;
        if chunk.len() > MAX_TOKEN_RESPONSE_BYTES.saturating_sub(body.len()) {
            return Err(McpOAuthError::TokenExchangeUnavailable);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}
