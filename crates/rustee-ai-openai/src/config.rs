//! Trusted `OpenAI` endpoint configuration and bounded transport settings.

use std::{fmt, time::Duration};

use rustee_ai_rag::EmbeddingBatchLimits;
use rustee_core::is_valid_http_bearer_value;
use url::Url;

use super::batch::OPENAI_BATCH_FILE_MAX_BYTES;

const DEFAULT_MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;

/// Maximum UTF-8 byte length of an HTTP-header-admissible `OpenAI` API key.
pub const MAX_OPENAI_API_KEY_BYTES: usize = 16 * 1024;

/// `OpenAI` Responses API configuration with redacted credentials.
#[derive(Clone)]
pub struct OpenAiConfig {
    pub(crate) api_key: String,
    pub(crate) base_url: Url,
    pub(crate) request_timeout: Duration,
    pub(crate) max_request_bytes: usize,
    pub(crate) max_response_bytes: usize,
    pub(crate) max_sse_event_bytes: usize,
    pub(crate) max_batch_file_bytes: usize,
    pub(crate) embedding_batch_limits: EmbeddingBatchLimits,
}

impl OpenAiConfig {
    /// Creates configuration for `OpenAI`'s public API endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiConfigError::BlankApiKey`] when `api_key` is blank or
    /// [`OpenAiConfigError::InvalidApiKey`] when it cannot be encoded safely in an HTTP Bearer
    /// header.
    pub fn new(api_key: impl Into<String>) -> Result<Self, OpenAiConfigError> {
        let api_key = api_key.into();
        if api_key.trim().is_empty() {
            return Err(OpenAiConfigError::BlankApiKey);
        }
        if !is_valid_api_key(&api_key) {
            return Err(OpenAiConfigError::InvalidApiKey);
        }
        Ok(Self {
            api_key,
            base_url: Url::parse("https://api.openai.com/v1/")
                .map_err(|_| OpenAiConfigError::InvalidBaseUrl)?,
            request_timeout: Duration::from_mins(1),
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            max_sse_event_bytes: 1024 * 1024,
            max_batch_file_bytes: OPENAI_BATCH_FILE_MAX_BYTES,
            embedding_batch_limits: EmbeddingBatchLimits::default(),
        })
    }

    /// Replaces the API base URL, primarily for a compatible gateway or contract test server.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiConfigError::InvalidBaseUrl`] for a non-HTTP(S) absolute URL, one with
    /// embedded credentials, or one with a query or fragment component.
    pub fn with_base_url(mut self, mut base_url: Url) -> Result<Self, OpenAiConfigError> {
        if !valid_base_url(&base_url) {
            return Err(OpenAiConfigError::InvalidBaseUrl);
        }
        if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }
        self.base_url = base_url;
        Ok(self)
    }

    /// Replaces the finite per-request timeout.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiConfigError::ZeroTimeout`] when `timeout` is zero.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Result<Self, OpenAiConfigError> {
        if timeout.is_zero() {
            return Err(OpenAiConfigError::ZeroTimeout);
        }
        self.request_timeout = timeout;
        Ok(self)
    }

    /// Sets the maximum JSON request body retained for one API call.
    ///
    /// This bound applies to Responses, Embeddings, and Batch lifecycle requests after Rustee
    /// renders their provider-specific JSON payloads.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiConfigError::ZeroRequestLimit`] when `max_request_bytes` is zero.
    pub fn with_max_request_bytes(
        mut self,
        max_request_bytes: usize,
    ) -> Result<Self, OpenAiConfigError> {
        if max_request_bytes == 0 {
            return Err(OpenAiConfigError::ZeroRequestLimit);
        }
        self.max_request_bytes = max_request_bytes;
        Ok(self)
    }

    /// Sets the maximum decoded JSON response buffered for one non-streaming API request.
    ///
    /// This bound applies to both Responses and Embeddings API results after HTTP content decoding.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiConfigError::ZeroResponseLimit`] when `max_response_bytes` is zero.
    pub fn with_max_response_bytes(
        mut self,
        max_response_bytes: usize,
    ) -> Result<Self, OpenAiConfigError> {
        if max_response_bytes == 0 {
            return Err(OpenAiConfigError::ZeroResponseLimit);
        }
        self.max_response_bytes = max_response_bytes;
        Ok(self)
    }

    /// Sets the maximum complete SSE frame buffered before the stream is rejected.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiConfigError::ZeroSseEventLimit`] when `max_sse_event_bytes` is zero.
    pub fn with_max_sse_event_bytes(
        mut self,
        max_sse_event_bytes: usize,
    ) -> Result<Self, OpenAiConfigError> {
        if max_sse_event_bytes == 0 {
            return Err(OpenAiConfigError::ZeroSseEventLimit);
        }
        self.max_sse_event_bytes = max_sse_event_bytes;
        Ok(self)
    }

    /// Sets the maximum input or output batch file retained in memory for one explicit operation.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiConfigError::InvalidBatchFileLimit`] when the limit is zero or exceeds the
    /// provider's documented Batch file limit.
    pub fn with_max_batch_file_bytes(
        mut self,
        max_batch_file_bytes: usize,
    ) -> Result<Self, OpenAiConfigError> {
        if max_batch_file_bytes == 0 || max_batch_file_bytes > OPENAI_BATCH_FILE_MAX_BYTES {
            return Err(OpenAiConfigError::InvalidBatchFileLimit);
        }
        self.max_batch_file_bytes = max_batch_file_bytes;
        Ok(self)
    }

    /// Sets the input-count and combined-content limit for one embeddings API request.
    ///
    /// The same limits are reported through [`rustee_ai_rag::EmbeddingProvider`] so a
    /// [`rustee_ai_rag::RagIngestor`] splits a document before the adapter is called. Direct
    /// adapter calls enforce this policy as well.
    #[must_use]
    pub fn with_embedding_batch_limits(mut self, limits: EmbeddingBatchLimits) -> Self {
        self.embedding_batch_limits = limits;
        self
    }
}

impl fmt::Debug for OpenAiConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiConfig")
            .field("api_key", &"[REDACTED]")
            .field("base_url", &"[REDACTED]")
            .field("request_timeout", &self.request_timeout)
            .field("max_request_bytes", &self.max_request_bytes)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("max_sse_event_bytes", &self.max_sse_event_bytes)
            .field("max_batch_file_bytes", &self.max_batch_file_bytes)
            .field("embedding_batch_limits", &self.embedding_batch_limits)
            .finish()
    }
}

/// Invalid `OpenAI` adapter configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OpenAiConfigError {
    /// An API credential is required for every provider request.
    #[error("OpenAI API key must not be blank")]
    BlankApiKey,
    /// An API credential could not be encoded safely in an HTTP Bearer header.
    #[error(
        "OpenAI API key must be safe for an HTTP header and at most {MAX_OPENAI_API_KEY_BYTES} bytes"
    )]
    InvalidApiKey,
    /// The adapter only supports a credential-free absolute HTTP(S) API base URL.
    #[error(
        "OpenAI API base URL must be absolute HTTP(S), credential-free, and have no query or fragment"
    )]
    InvalidBaseUrl,
    /// The adapter must bound a provider request.
    #[error("OpenAI request timeout must be non-zero")]
    ZeroTimeout,
    /// Outgoing JSON encoding needs a finite memory bound.
    #[error("OpenAI request byte limit must be non-zero")]
    ZeroRequestLimit,
    /// Non-streaming response collection needs a finite memory bound.
    #[error("OpenAI response byte limit must be non-zero")]
    ZeroResponseLimit,
    /// The stream parser must bound its buffered SSE event size.
    #[error("OpenAI SSE event limit must be non-zero")]
    ZeroSseEventLimit,
    /// Batch file operations must use a bounded limit within the provider's allowed range.
    #[error("OpenAI Batch file limit must be non-zero and within the provider maximum")]
    InvalidBatchFileLimit,
}

fn valid_base_url(url: &Url) -> bool {
    matches!(url.scheme(), "http" | "https")
        && url.host().is_some()
        && url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none()
        && url.fragment().is_none()
}

fn is_valid_api_key(value: &str) -> bool {
    is_valid_http_bearer_value(value, MAX_OPENAI_API_KEY_BYTES)
}
