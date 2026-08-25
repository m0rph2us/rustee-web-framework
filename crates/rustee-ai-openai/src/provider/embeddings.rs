//! Embeddings API execution, input admission, and response-order reconstruction.

use std::fmt;

use reqwest::{Client, header::CONTENT_TYPE};
use rustee_ai_rag::{Embedding, EmbeddingBatchLimits, EmbeddingInput, EmbeddingProvider};
use serde_json::{Value, json};

use crate::{
    OpenAiConfig, OpenAiError,
    response::{decode_json_response, encode_json_request},
};

/// `OpenAI` embeddings provider for [`EmbeddingProvider`] batches.
#[derive(Clone)]
pub struct OpenAiEmbeddingsProvider {
    client: Client,
    config: OpenAiConfig,
}

impl OpenAiEmbeddingsProvider {
    /// Builds an embeddings provider with a TLS-enabled HTTP client and configured timeout.
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

    /// Wraps an already-configured HTTP client for dependency injection and contract tests.
    ///
    /// Each adapter request still enforces the timeout in `config`. The injected client owns
    /// redirect policy; disable automatic redirects to preserve the configured endpoint boundary.
    #[must_use]
    pub fn with_client(client: Client, config: OpenAiConfig) -> Self {
        Self { client, config }
    }
}

impl fmt::Debug for OpenAiEmbeddingsProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiEmbeddingsProvider")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl EmbeddingProvider for OpenAiEmbeddingsProvider {
    type Error = OpenAiError;

    fn batch_limits(&self) -> EmbeddingBatchLimits {
        self.config.embedding_batch_limits
    }

    fn embed(
        &self,
        model: String,
        inputs: Vec<EmbeddingInput>,
    ) -> futures_util::future::BoxFuture<'static, Result<Vec<Embedding>, Self::Error>> {
        let provider = self.clone();
        Box::pin(async move {
            validate_embedding_batch(&inputs, provider.batch_limits())?;
            let expected_embeddings = inputs.len();
            let endpoint = provider
                .config
                .base_url
                .join("embeddings")
                .map_err(|_| OpenAiError::InvalidEndpoint)?;
            let body = embedding_request_body(&model, &inputs);
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
                OpenAiError::MalformedEmbeddingResponse,
            )
            .await?;
            decode_embeddings(&value, expected_embeddings)
        })
    }
}

/// Validates one direct embeddings API request before any provider dispatch.
pub(crate) fn validate_embedding_batch(
    inputs: &[EmbeddingInput],
    limits: EmbeddingBatchLimits,
) -> Result<(), OpenAiError> {
    if inputs.is_empty() {
        return Err(OpenAiError::EmptyEmbeddingBatch);
    }
    if inputs.len() > limits.max_inputs() {
        return Err(OpenAiError::EmbeddingBatchInputLimit);
    }
    let content_bytes = inputs.iter().try_fold(0_usize, |total, input| {
        total
            .checked_add(input.content().len())
            .ok_or(OpenAiError::EmbeddingBatchContentLimit)
    })?;
    if content_bytes > limits.max_content_bytes() {
        return Err(OpenAiError::EmbeddingBatchContentLimit);
    }
    Ok(())
}

/// Renders a provider request body while preserving application input order.
#[must_use]
pub(crate) fn embedding_request_body(model: &str, inputs: &[EmbeddingInput]) -> Value {
    json!({
        "model": model,
        "input": inputs.iter().map(EmbeddingInput::content).collect::<Vec<_>>(),
    })
}

/// Decodes indexed Embeddings API data back into the original input order.
pub(crate) fn decode_embeddings(
    value: &Value,
    expected_embeddings: usize,
) -> Result<Vec<Embedding>, OpenAiError> {
    let data = value
        .get("data")
        .and_then(Value::as_array)
        .ok_or(OpenAiError::MalformedEmbeddingResponse)?;
    if data.len() != expected_embeddings {
        return Err(OpenAiError::MalformedEmbeddingResponse);
    }
    let mut ordered = std::iter::repeat_with(|| None)
        .take(expected_embeddings)
        .collect::<Vec<Option<Embedding>>>();
    for item in data {
        let index = item
            .get("index")
            .and_then(Value::as_u64)
            .and_then(|index| usize::try_from(index).ok())
            .filter(|index| *index < expected_embeddings)
            .ok_or(OpenAiError::MalformedEmbeddingResponse)?;
        let values = item
            .get("embedding")
            .and_then(Value::as_array)
            .ok_or(OpenAiError::MalformedEmbeddingResponse)?
            .iter()
            .map(|value| {
                value
                    .as_f64()
                    .and_then(|_| value.to_string().parse::<f32>().ok())
            })
            .collect::<Option<Vec<_>>>()
            .ok_or(OpenAiError::MalformedEmbeddingResponse)?;
        let embedding =
            Embedding::new(values).map_err(|_| OpenAiError::MalformedEmbeddingResponse)?;
        if ordered[index].replace(embedding).is_some() {
            return Err(OpenAiError::MalformedEmbeddingResponse);
        }
    }
    ordered
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or(OpenAiError::MalformedEmbeddingResponse)
}
