//! Bounded HTTPS retrieval for OAuth discovery documents.

use std::time::Duration;

use futures_util::StreamExt;
use reqwest::{Client, header::ACCEPT};
use serde::de::DeserializeOwned;
use url::Url;

use crate::{McpOAuthError, is_json_content_type};

use super::MAX_DISCOVERY_RESPONSE_BYTES;

#[derive(Clone)]
pub(super) struct DiscoveryTransport {
    client: Client,
}

impl DiscoveryTransport {
    pub(super) fn new(timeout: Duration) -> Result<Self, McpOAuthError> {
        let client = Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| McpOAuthError::HttpClient)?;
        Ok(Self { client })
    }

    pub(super) async fn fetch_json<T: DeserializeOwned>(
        &self,
        url: &Url,
    ) -> Result<T, McpOAuthError> {
        let response = self
            .client
            .get(url.clone())
            .header(ACCEPT, "application/json")
            .send()
            .await
            .map_err(|_| McpOAuthError::Transport)?;
        if !response.status().is_success() {
            return Err(McpOAuthError::HttpStatus(response.status()));
        }
        if !is_json_content_type(response.headers())
            || response
                .content_length()
                .is_some_and(|bytes| bytes > MAX_DISCOVERY_RESPONSE_BYTES as u64)
        {
            return Err(McpOAuthError::InvalidMetadata);
        }
        let body = collect_response_body(response).await?;
        serde_json::from_slice(&body).map_err(|_| McpOAuthError::InvalidMetadata)
    }
}

async fn collect_response_body(response: reqwest::Response) -> Result<Vec<u8>, McpOAuthError> {
    let mut body = Vec::new();
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|_| McpOAuthError::Transport)?;
        if chunk.len() > MAX_DISCOVERY_RESPONSE_BYTES.saturating_sub(body.len()) {
            return Err(McpOAuthError::InvalidMetadata);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}
