//! Validated Batch endpoint and request-row models.

use std::fmt;

use rustee_ai::ChatRequest;
use rustee_ai_rag::EmbeddingInput;
use serde_json::Value;

use crate::provider::{embedding_request_body, request_body};

use super::{
    OPENAI_BATCH_FILE_MAX_BYTES, OpenAiBatchEmbeddingsRowError, OpenAiBatchInputError,
    OpenAiBatchResponsesRowError,
};

/// Batch endpoint admitted by the current `OpenAI` Batch API adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenAiBatchEndpoint {
    /// Runs JSONL entries against the `OpenAI` Responses endpoint.
    Responses,
    /// Runs JSONL entries against the `OpenAI` Embeddings endpoint.
    Embeddings,
}

impl OpenAiBatchEndpoint {
    pub(crate) const fn path(self) -> &'static str {
        match self {
            Self::Responses => "/v1/responses",
            Self::Embeddings => "/v1/embeddings",
        }
    }
}

/// One application-owned request body placed in an `OpenAI` Batch input JSONL row.
pub struct OpenAiBatchInputRow {
    pub(super) custom_id: String,
    pub(super) endpoint: OpenAiBatchEndpoint,
    pub(super) body: Value,
}

impl OpenAiBatchInputRow {
    /// Validates a bounded correlation ID and object-shaped provider request body.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiBatchInputError`] when `custom_id` is unsafe or `body` is not a JSON object.
    pub fn new(
        custom_id: impl Into<String>,
        endpoint: OpenAiBatchEndpoint,
        body: Value,
    ) -> Result<Self, OpenAiBatchInputError> {
        let custom_id = custom_id.into();
        if !valid_provider_identifier(&custom_id) {
            return Err(OpenAiBatchInputError::UnsafeCustomId);
        }
        if !body.is_object() {
            return Err(OpenAiBatchInputError::BodyMustBeObject);
        }
        Ok(Self {
            custom_id,
            endpoint,
            body,
        })
    }

    /// Builds a typed Responses Batch row from an already validated Rustee chat request.
    ///
    /// This shares the adapter's text/function/tool-result mapping with ordinary Responses calls.
    /// Provider-only request options that are not represented by [`ChatRequest`] remain explicit
    /// application-owned JSON bodies passed through [`Self::new`].
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiBatchResponsesRowError`] when the correlation ID is unsafe or the request
    /// cannot be represented by the Responses Batch endpoint.
    pub fn from_chat_request(
        custom_id: impl Into<String>,
        request: &ChatRequest,
    ) -> Result<Self, OpenAiBatchResponsesRowError> {
        let body = request_body(request, OPENAI_BATCH_FILE_MAX_BYTES)
            .map_err(|source| OpenAiBatchResponsesRowError::Request { source })?;
        Self::new(custom_id, OpenAiBatchEndpoint::Responses, body)
            .map_err(|source| OpenAiBatchResponsesRowError::Input { source })
    }

    /// Builds a typed Embeddings Batch row from ordered Rustee embedding inputs.
    ///
    /// The input order is preserved for the provider's per-row embedding response validation.
    /// Provider-only embedding fields remain explicit application-owned JSON bodies passed through
    /// [`Self::new`].
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiBatchEmbeddingsRowError`] when `model` is unsafe, inputs are empty, or the
    /// generic Batch row envelope is invalid.
    pub fn from_embedding_inputs(
        custom_id: impl Into<String>,
        model: &str,
        inputs: &[EmbeddingInput],
    ) -> Result<Self, OpenAiBatchEmbeddingsRowError> {
        if !valid_provider_identifier(model) {
            return Err(OpenAiBatchEmbeddingsRowError::UnsafeModel);
        }
        if inputs.is_empty() {
            return Err(OpenAiBatchEmbeddingsRowError::EmptyInputs);
        }
        Self::new(
            custom_id,
            OpenAiBatchEndpoint::Embeddings,
            embedding_request_body(model, inputs),
        )
        .map_err(|source| OpenAiBatchEmbeddingsRowError::Input { source })
    }
}

impl fmt::Debug for OpenAiBatchInputRow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiBatchInputRow")
            .field("custom_id", &"[REDACTED]")
            .field("endpoint", &self.endpoint)
            .field("body", &"[REDACTED]")
            .finish()
    }
}
pub(crate) fn valid_provider_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}
