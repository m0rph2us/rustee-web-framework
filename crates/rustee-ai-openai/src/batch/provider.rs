//! Batch lifecycle provider adapter.

use std::fmt;

use reqwest::{Client, header::CONTENT_TYPE};
use rustee_ai_batch::{AiBatchProvider, AiBatchReceipt, AiBatchReference};
use serde_json::json;

use super::{
    OpenAiBatchRequest, OpenAiBatchSnapshot, decode_batch, valid_provider_path_identifier,
};
use crate::{
    OpenAiConfig, OpenAiError,
    response::{decode_json_response, encode_json_request},
};

/// `OpenAI` Batch API adapter sharing the existing credential, base URL, and deadline.
#[derive(Clone)]
pub struct OpenAiBatchProvider {
    pub(super) client: Client,
    pub(super) config: OpenAiConfig,
}

impl OpenAiBatchProvider {
    /// Builds a batch adapter with the configured TLS client and deadline.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiError::Client`] when the HTTP client cannot be constructed.
    pub fn new(config: OpenAiConfig) -> Result<Self, OpenAiError> {
        let client = Client::builder()
            .timeout(config.request_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| OpenAiError::Client)?;
        Ok(Self { client, config })
    }

    /// Wraps an application-provided HTTP client for dependency injection and contract tests.
    ///
    /// Each adapter request still enforces the timeout in `config`. The injected client owns
    /// redirect policy; disable automatic redirects to preserve the configured endpoint boundary.
    #[must_use]
    pub fn with_client(client: Client, config: OpenAiConfig) -> Self {
        Self { client, config }
    }

    /// Retrieves one safe batch lifecycle snapshot without downloading result file contents.
    ///
    /// # Errors
    ///
    /// Returns an [`OpenAiError`] when the receipt cannot form a valid endpoint, the request
    /// fails, or the provider response is not a valid batch snapshot.
    pub async fn retrieve(
        &self,
        receipt: &AiBatchReceipt,
    ) -> Result<OpenAiBatchSnapshot, OpenAiError> {
        if !valid_provider_path_identifier(receipt.provider_batch_id()) {
            return Err(OpenAiError::MalformedBatch);
        }
        let endpoint = self
            .config
            .base_url
            .join(&format!("batches/{}", receipt.provider_batch_id()))
            .map_err(|_| OpenAiError::InvalidEndpoint)?;
        let response = self
            .client
            .get(endpoint)
            .timeout(self.config.request_timeout)
            .bearer_auth(&self.config.api_key)
            .send()
            .await
            .map_err(|_| OpenAiError::Transport)?;
        if !response.status().is_success() {
            return Err(OpenAiError::HttpStatus(response.status()));
        }
        let value = decode_json_response(
            response,
            self.config.max_response_bytes,
            OpenAiError::MalformedBatch,
        )
        .await?;
        decode_batch(&value)
    }

    /// Requests cancellation and returns the provider's current safe batch snapshot.
    ///
    /// # Errors
    ///
    /// Returns an [`OpenAiError`] when the receipt cannot form a valid endpoint, the request
    /// fails, or the provider response is not a valid batch snapshot.
    pub async fn cancel(
        &self,
        receipt: &AiBatchReceipt,
    ) -> Result<OpenAiBatchSnapshot, OpenAiError> {
        if !valid_provider_path_identifier(receipt.provider_batch_id()) {
            return Err(OpenAiError::MalformedBatch);
        }
        let endpoint = self
            .config
            .base_url
            .join(&format!("batches/{}/cancel", receipt.provider_batch_id()))
            .map_err(|_| OpenAiError::InvalidEndpoint)?;
        let response = self
            .client
            .post(endpoint)
            .timeout(self.config.request_timeout)
            .bearer_auth(&self.config.api_key)
            .send()
            .await
            .map_err(|_| OpenAiError::Transport)?;
        if !response.status().is_success() {
            return Err(OpenAiError::HttpStatus(response.status()));
        }
        let value = decode_json_response(
            response,
            self.config.max_response_bytes,
            OpenAiError::MalformedBatch,
        )
        .await?;
        decode_batch(&value)
    }
}

impl fmt::Debug for OpenAiBatchProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiBatchProvider")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl AiBatchProvider<OpenAiBatchRequest> for OpenAiBatchProvider {
    type Error = OpenAiError;

    fn submit(
        &self,
        reference: AiBatchReference,
        request: OpenAiBatchRequest,
    ) -> futures_util::future::BoxFuture<'static, Result<AiBatchReceipt, Self::Error>> {
        let provider = self.clone();
        Box::pin(async move {
            let endpoint = provider
                .config
                .base_url
                .join("batches")
                .map_err(|_| OpenAiError::InvalidEndpoint)?;
            let mut body = json!({
                "input_file_id": request.input_file_id(),
                "endpoint": request.endpoint().path(),
                "completion_window": "24h",
                "metadata": {"rustee_run_key": reference.run_key()},
            });
            if let Some(expiration) = request.output_expiration() {
                body["output_expires_after"] = json!({
                    "anchor": "created_at",
                    "seconds": expiration.seconds(),
                });
            }
            let body = encode_json_request(&body, provider.config.max_request_bytes)?;
            let response = provider
                .client
                .post(endpoint)
                .timeout(provider.config.request_timeout)
                .bearer_auth(&provider.config.api_key)
                .header(CONTENT_TYPE, "application/json")
                .body(body)
                .send()
                .await
                .map_err(|_| OpenAiError::Transport)?;
            if !response.status().is_success() {
                return Err(OpenAiError::HttpStatus(response.status()));
            }
            let value = decode_json_response(
                response,
                provider.config.max_response_bytes,
                OpenAiError::MalformedBatch,
            )
            .await?;
            Ok(decode_batch(&value)?.into_receipt())
        })
    }
}
