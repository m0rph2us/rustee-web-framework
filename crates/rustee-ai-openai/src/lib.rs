//! `OpenAI` Responses API adapter for Rustee AI contracts.
//!
//! This crate sends only application-approved prompts, tool declarations, and tool results. It
//! keeps credentials and raw provider error bodies out of Rustee's public error surface.

use std::{collections::HashSet, fmt, time::Duration};

use futures_util::StreamExt;
use reqwest::{
    Client, StatusCode,
    multipart::{Form, Part},
};
use rustee_ai::{
    AiEventStream, AiEventStreamFuture, AiProvider, AiStreamEvent, ChatRequest, ChatResponse,
    MessageRole, ToolCall, Usage,
};
use rustee_ai_batch::{AiBatchProvider, AiBatchReceipt, AiBatchReference};
use rustee_ai_rag::{Embedding, EmbeddingInput, EmbeddingProvider};
use serde_json::{Value, json};
use url::Url;

/// `OpenAI` Responses API configuration with redacted credentials.
#[derive(Clone)]
pub struct OpenAiConfig {
    api_key: String,
    base_url: Url,
    request_timeout: Duration,
    max_sse_event_bytes: usize,
    max_batch_file_bytes: usize,
}

impl OpenAiConfig {
    /// Creates configuration for `OpenAI`'s public API endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiConfigError::BlankApiKey`] when `api_key` is blank.
    pub fn new(api_key: impl Into<String>) -> Result<Self, OpenAiConfigError> {
        let api_key = api_key.into();
        if api_key.trim().is_empty() {
            return Err(OpenAiConfigError::BlankApiKey);
        }
        Ok(Self {
            api_key,
            base_url: Url::parse("https://api.openai.com/v1/")
                .map_err(|_| OpenAiConfigError::InvalidBaseUrl)?,
            request_timeout: Duration::from_mins(1),
            max_sse_event_bytes: 1024 * 1024,
            max_batch_file_bytes: OPENAI_BATCH_FILE_MAX_BYTES,
        })
    }

    /// Replaces the API base URL, primarily for a compatible gateway or contract test server.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiConfigError::InvalidBaseUrl`] for a non-HTTP(S) URL or one with a query or
    /// fragment component.
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
}

impl fmt::Debug for OpenAiConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiConfig")
            .field("api_key", &"[REDACTED]")
            .field("base_url", &self.base_url)
            .field("request_timeout", &self.request_timeout)
            .field("max_sse_event_bytes", &self.max_sse_event_bytes)
            .field("max_batch_file_bytes", &self.max_batch_file_bytes)
            .finish()
    }
}

/// Invalid `OpenAI` adapter configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OpenAiConfigError {
    /// An API credential is required for every provider request.
    #[error("OpenAI API key must not be blank")]
    BlankApiKey,
    /// The adapter only supports a clean HTTP(S) API base URL.
    #[error("OpenAI API base URL must use HTTP(S) and have no query or fragment")]
    InvalidBaseUrl,
    /// The adapter must bound a provider request.
    #[error("OpenAI request timeout must be non-zero")]
    ZeroTimeout,
    /// The stream parser must bound its buffered SSE event size.
    #[error("OpenAI SSE event limit must be non-zero")]
    ZeroSseEventLimit,
    /// Batch file operations must use a bounded limit within the provider's allowed range.
    #[error("OpenAI Batch file limit must be non-zero and within the provider maximum")]
    InvalidBatchFileLimit,
}

/// `OpenAI` Responses API provider.
#[derive(Clone)]
pub struct OpenAiResponsesProvider {
    client: Client,
    config: OpenAiConfig,
}

impl OpenAiResponsesProvider {
    /// Builds a provider with a TLS-enabled HTTP client and the configured request timeout.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiError::Client`] when the HTTP client cannot be constructed.
    pub fn new(config: OpenAiConfig) -> Result<Self, OpenAiError> {
        let client = Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|_| OpenAiError::Client)?;
        Ok(Self { client, config })
    }

    /// Wraps an already-configured HTTP client for dependency injection and contract tests.
    #[must_use]
    pub fn with_client(client: Client, config: OpenAiConfig) -> Self {
        Self { client, config }
    }

    async fn send(
        &self,
        request: ChatRequest,
        stream: bool,
    ) -> Result<reqwest::Response, OpenAiError> {
        let mut body = request_body(&request)?;
        if stream {
            body["stream"] = Value::Bool(true);
        }
        let endpoint = self
            .config
            .base_url
            .join("responses")
            .map_err(|_| OpenAiError::InvalidEndpoint)?;
        let response = self
            .client
            .post(endpoint)
            .bearer_auth(&self.config.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|_| OpenAiError::Transport)?;
        if !response.status().is_success() {
            return Err(OpenAiError::HttpStatus(response.status()));
        }
        Ok(response)
    }
}

/// `OpenAI` embeddings provider for [`EmbeddingProvider`] batches.
#[derive(Clone)]
pub struct OpenAiEmbeddingsProvider {
    client: Client,
    config: OpenAiConfig,
}

/// Batch endpoint admitted by the current `OpenAI` Batch API adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenAiBatchEndpoint {
    /// Runs JSONL entries against the `OpenAI` Responses endpoint.
    Responses,
    /// Runs JSONL entries against the `OpenAI` Embeddings endpoint.
    Embeddings,
}

impl OpenAiBatchEndpoint {
    const fn path(self) -> &'static str {
        match self {
            Self::Responses => "/v1/responses",
            Self::Embeddings => "/v1/embeddings",
        }
    }
}

/// Maximum Batch input or result file size accepted by the current `OpenAI` API contract.
pub const OPENAI_BATCH_FILE_MAX_BYTES: usize = 200 * 1024 * 1024;

/// Maximum request/output rows accepted by the current `OpenAI` Batch API contract.
pub const OPENAI_BATCH_MAX_REQUESTS: usize = 50_000;

const OPENAI_BATCH_OUTPUT_EXPIRATION_MIN_SECONDS: u64 = 60 * 60;
const OPENAI_BATCH_OUTPUT_EXPIRATION_MAX_SECONDS: u64 = 30 * 24 * 60 * 60;

/// One application-owned request body placed in an `OpenAI` Batch input JSONL row.
pub struct OpenAiBatchInputRow {
    custom_id: String,
    endpoint: OpenAiBatchEndpoint,
    body: Value,
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
        let body = request_body(request)
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
            .field("custom_id", &self.custom_id)
            .field("endpoint", &self.endpoint)
            .field("body", &"[REDACTED]")
            .finish()
    }
}

/// Builder for one endpoint-homogeneous, content-redacted `OpenAI` Batch input JSONL file.
pub struct OpenAiBatchJsonlBuilder {
    endpoint: OpenAiBatchEndpoint,
    custom_ids: HashSet<String>,
    rows: Vec<OpenAiBatchInputRow>,
}

impl OpenAiBatchJsonlBuilder {
    /// Starts an input file whose rows must all target `endpoint`.
    #[must_use]
    pub fn new(endpoint: OpenAiBatchEndpoint) -> Self {
        Self {
            endpoint,
            custom_ids: HashSet::new(),
            rows: Vec::new(),
        }
    }

    /// Adds one provider request row after correlation, endpoint, and count validation.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiBatchInputError`] for mismatched endpoint, duplicate correlation ID, or
    /// provider row-count limit exhaustion.
    pub fn push(&mut self, row: OpenAiBatchInputRow) -> Result<(), OpenAiBatchInputError> {
        if row.endpoint != self.endpoint {
            return Err(OpenAiBatchInputError::EndpointMismatch);
        }
        if self.rows.len() >= OPENAI_BATCH_MAX_REQUESTS {
            return Err(OpenAiBatchInputError::TooManyRows);
        }
        if !self.custom_ids.insert(row.custom_id.clone()) {
            return Err(OpenAiBatchInputError::DuplicateCustomId);
        }
        self.rows.push(row);
        Ok(())
    }

    /// Serializes the validated generic Batch envelope into a short-lived upload value.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiBatchInputError`] when there are no rows, serialization fails, or the
    /// generated file exceeds the provider size limit.
    pub fn build(self) -> Result<OpenAiBatchInputJsonl, OpenAiBatchInputError> {
        if self.rows.is_empty() {
            return Err(OpenAiBatchInputError::Empty);
        }
        let mut bytes = Vec::new();
        for row in self.rows {
            serde_json::to_writer(
                &mut bytes,
                &json!({
                    "custom_id": row.custom_id,
                    "method": "POST",
                    "url": row.endpoint.path(),
                    "body": row.body,
                }),
            )
            .map_err(|_| OpenAiBatchInputError::Serialization)?;
            bytes.push(b'\n');
            if bytes.len() > OPENAI_BATCH_FILE_MAX_BYTES {
                return Err(OpenAiBatchInputError::TooLarge);
            }
        }
        OpenAiBatchInputJsonl::new(bytes)
    }
}

impl fmt::Debug for OpenAiBatchJsonlBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiBatchJsonlBuilder")
            .field("endpoint", &self.endpoint)
            .field("rows", &self.rows.len())
            .finish_non_exhaustive()
    }
}

/// Short-lived application-created JSONL content for one explicit Batch input-file upload.
///
/// This value is neither serializable nor cloneable. It must not be placed in a job payload,
/// cache, trace, or debug record.
pub struct OpenAiBatchInputJsonl {
    bytes: Vec<u8>,
}

impl OpenAiBatchInputJsonl {
    /// Validates bounded non-empty JSONL bytes before an explicit upload request.
    ///
    /// JSON line shape, authorization, and retention are application-owned. The `OpenAI` API
    /// remains the authority for provider-specific batch-row validation.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiBatchInputError`] when `bytes` is empty or exceeds the documented provider
    /// file limit.
    pub fn new(bytes: Vec<u8>) -> Result<Self, OpenAiBatchInputError> {
        if bytes.is_empty() {
            return Err(OpenAiBatchInputError::Empty);
        }
        if bytes.len() > OPENAI_BATCH_FILE_MAX_BYTES {
            return Err(OpenAiBatchInputError::TooLarge);
        }
        Ok(Self { bytes })
    }

    /// Returns the application-provided byte length without exposing its content.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns whether the application attempted to construct an empty input.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl fmt::Debug for OpenAiBatchInputJsonl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiBatchInputJsonl")
            .field("bytes", &"[REDACTED]")
            .field("length", &self.bytes.len())
            .finish()
    }
}

/// Invalid local `OpenAI` Batch input-file content.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OpenAiBatchInputError {
    /// The provider cannot receive an empty input file.
    #[error("OpenAI Batch input JSONL must not be empty")]
    Empty,
    /// The local input exceeds the provider's documented maximum Batch file size.
    #[error("OpenAI Batch input JSONL exceeds the provider file limit")]
    TooLarge,
    /// The application correlation ID was not a bounded safe identifier.
    #[error("OpenAI Batch custom ID was unsafe")]
    UnsafeCustomId,
    /// The generic Batch envelope requires an object-shaped provider request body.
    #[error("OpenAI Batch request body must be a JSON object")]
    BodyMustBeObject,
    /// All rows in one provider Batch must target the same endpoint.
    #[error("OpenAI Batch row endpoint did not match the builder endpoint")]
    EndpointMismatch,
    /// A provider Batch cannot contain the same application correlation ID twice.
    #[error("OpenAI Batch custom ID was duplicated")]
    DuplicateCustomId,
    /// The builder reached the provider's documented per-file request limit.
    #[error("OpenAI Batch input exceeded the provider row limit")]
    TooManyRows,
    /// The generic Batch envelope could not be serialized.
    #[error("OpenAI Batch input could not be serialized")]
    Serialization,
}

/// Failure while mapping a typed Rustee request into an `OpenAI` Responses Batch row.
#[derive(Debug, thiserror::Error)]
pub enum OpenAiBatchResponsesRowError {
    /// The generic Batch row validation did not accept its content-free envelope metadata.
    #[error("OpenAI Responses Batch row was invalid")]
    Input {
        /// Generic Batch row validation failure.
        #[source]
        source: OpenAiBatchInputError,
    },
    /// Rustee's typed chat request could not map to the Responses request shape.
    #[error("Rustee chat request could not map to an OpenAI Responses Batch row")]
    Request {
        /// Safe adapter mapping failure.
        #[source]
        source: OpenAiError,
    },
}

/// Failure while mapping typed Rustee embedding inputs into an `OpenAI` Embeddings Batch row.
#[derive(Debug, thiserror::Error)]
pub enum OpenAiBatchEmbeddingsRowError {
    /// An embedding Batch row requires a bounded provider model alias.
    #[error("OpenAI Batch embedding model was unsafe")]
    UnsafeModel,
    /// An embedding Batch row needs at least one input in its preserved order.
    #[error("OpenAI Batch embedding row must contain at least one input")]
    EmptyInputs,
    /// The generic Batch row validation did not accept its content-free envelope metadata.
    #[error("OpenAI Embeddings Batch row was invalid")]
    Input {
        /// Generic Batch row validation failure.
        #[source]
        source: OpenAiBatchInputError,
    },
}

/// Safe provider file reference returned after an explicit Batch input upload.
#[derive(Clone, Eq, PartialEq)]
pub struct OpenAiBatchInputFile {
    id: String,
}

impl OpenAiBatchInputFile {
    /// Returns the provider-assigned input-file ID.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }
}

impl fmt::Debug for OpenAiBatchInputFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiBatchInputFile")
            .field("id", &self.id)
            .finish()
    }
}

/// Safe acknowledgement of one explicitly deleted provider Batch artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenAiBatchFileDeletion {
    id: String,
}

impl OpenAiBatchFileDeletion {
    /// Returns the provider file ID confirmed as deleted.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }
}

/// Explicitly downloaded unparsed Batch file content.
///
/// The caller owns row decoding, authorization, partial-result handling, retention, and erase.
pub struct OpenAiBatchFileContent {
    bytes: Vec<u8>,
}

impl OpenAiBatchFileContent {
    /// Returns the downloaded byte length without exposing the content.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns whether the provider file was empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Consumes the explicitly selected file content for application-owned processing.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Creates a bounded, fail-closed iterator over provider Batch output rows.
    ///
    /// The iterator parses only row structure and returns raw response bodies through an explicit
    /// consuming API. It never invokes a model, cache, evaluator, ledger, or durable job.
    #[must_use]
    pub fn output_rows(&self) -> OpenAiBatchOutputRows<'_> {
        OpenAiBatchOutputRows::new(&self.bytes, OPENAI_BATCH_MAX_REQUESTS)
    }

    /// Creates a Batch output iterator with an application-selected lower row bound.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiBatchOutputParseError::InvalidRowLimit`] when `max_rows` is zero or exceeds
    /// the provider's documented maximum.
    pub fn output_rows_with_limit(
        &self,
        max_rows: usize,
    ) -> Result<OpenAiBatchOutputRows<'_>, OpenAiBatchOutputParseError> {
        if max_rows == 0 || max_rows > OPENAI_BATCH_MAX_REQUESTS {
            return Err(OpenAiBatchOutputParseError::InvalidRowLimit);
        }
        Ok(OpenAiBatchOutputRows::new(&self.bytes, max_rows))
    }
}

impl fmt::Debug for OpenAiBatchFileContent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiBatchFileContent")
            .field("bytes", &"[REDACTED]")
            .field("length", &self.bytes.len())
            .finish()
    }
}

/// Fail-closed iterator over one explicitly downloaded `OpenAI` Batch JSONL output file.
pub struct OpenAiBatchOutputRows<'a> {
    bytes: &'a [u8],
    offset: usize,
    rows_seen: usize,
    max_rows: usize,
    failed: bool,
}

impl<'a> OpenAiBatchOutputRows<'a> {
    const fn new(bytes: &'a [u8], max_rows: usize) -> Self {
        Self {
            bytes,
            offset: 0,
            rows_seen: 0,
            max_rows,
            failed: false,
        }
    }
}

impl Iterator for OpenAiBatchOutputRows<'_> {
    type Item = Result<OpenAiBatchOutputRow, OpenAiBatchOutputParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed || self.offset >= self.bytes.len() {
            return None;
        }
        let remaining = &self.bytes[self.offset..];
        let (line, next_offset) = match remaining.iter().position(|byte| *byte == b'\n') {
            Some(index) => (&remaining[..index], self.offset + index + 1),
            None => (remaining, self.bytes.len()),
        };
        self.offset = next_offset;
        self.rows_seen += 1;
        let line_number = self.rows_seen;
        if self.rows_seen > self.max_rows {
            self.failed = true;
            return Some(Err(OpenAiBatchOutputParseError::TooManyRows));
        }
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            self.failed = true;
            return Some(Err(OpenAiBatchOutputParseError::MalformedRow {
                line_number,
            }));
        }
        let Ok(value) = serde_json::from_slice::<Value>(line) else {
            self.failed = true;
            return Some(Err(OpenAiBatchOutputParseError::MalformedRow {
                line_number,
            }));
        };
        if let Ok(row) = decode_batch_output_row(&value) {
            Some(Ok(row))
        } else {
            self.failed = true;
            Some(Err(OpenAiBatchOutputParseError::MalformedRow {
                line_number,
            }))
        }
    }
}

impl fmt::Debug for OpenAiBatchOutputRows<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiBatchOutputRows")
            .field("rows_seen", &self.rows_seen)
            .field("max_rows", &self.max_rows)
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

/// Safe structural parse failure for an `OpenAI` Batch output file.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OpenAiBatchOutputParseError {
    /// The caller supplied a row bound outside the provider's supported range.
    #[error("OpenAI Batch output row limit is outside the supported range")]
    InvalidRowLimit,
    /// A JSONL line was malformed or did not meet the safe output-row contract.
    #[error("OpenAI Batch output row {line_number} was malformed")]
    MalformedRow {
        /// One-based position in the explicitly selected output file.
        line_number: usize,
    },
    /// The downloaded file exceeded the caller-selected row bound.
    #[error("OpenAI Batch output exceeded the configured row limit")]
    TooManyRows,
}

/// One validated provider Batch output row with an application correlation ID.
pub struct OpenAiBatchOutputRow {
    custom_id: String,
    outcome: OpenAiBatchRowOutcome,
}

impl OpenAiBatchOutputRow {
    /// Returns the bounded application correlation ID chosen in the input JSONL row.
    #[must_use]
    pub fn custom_id(&self) -> &str {
        &self.custom_id
    }

    /// Consumes the row so the caller explicitly selects response or failure handling.
    #[must_use]
    pub fn into_outcome(self) -> OpenAiBatchRowOutcome {
        self.outcome
    }
}

impl fmt::Debug for OpenAiBatchOutputRow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let outcome = match &self.outcome {
            OpenAiBatchRowOutcome::Response(response) => format!(
                "response(status={}, request_id={})",
                response.status_code, response.request_id
            ),
            OpenAiBatchRowOutcome::Error(error) => format!("error(code={})", error.code),
        };
        formatter
            .debug_struct("OpenAiBatchOutputRow")
            .field("custom_id", &self.custom_id)
            .field("outcome", &outcome)
            .finish()
    }
}

/// Mutually exclusive outcome for one `OpenAI` Batch output row.
pub enum OpenAiBatchRowOutcome {
    /// The provider returned an HTTP response body for this row.
    Response(OpenAiBatchResponse),
    /// The provider could not produce an HTTP response for this row.
    Error(OpenAiBatchRowError),
}

impl fmt::Debug for OpenAiBatchRowOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Response(response) => formatter.debug_tuple("Response").field(response).finish(),
            Self::Error(error) => formatter.debug_tuple("Error").field(error).finish(),
        }
    }
}

/// Safe response metadata plus an explicitly consumable provider response body.
pub struct OpenAiBatchResponse {
    status_code: u16,
    request_id: String,
    body: OpenAiBatchResponseBody,
}

impl OpenAiBatchResponse {
    /// Returns the provider HTTP status code.
    #[must_use]
    pub const fn status_code(&self) -> u16 {
        self.status_code
    }

    /// Returns the bounded provider request ID for support/reconciliation.
    #[must_use]
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    /// Consumes response metadata and body for application-owned response handling.
    #[must_use]
    pub fn into_body(self) -> OpenAiBatchResponseBody {
        self.body
    }
}

impl fmt::Debug for OpenAiBatchResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiBatchResponse")
            .field("status_code", &self.status_code)
            .field("request_id", &self.request_id)
            .field("body", &"[REDACTED]")
            .finish()
    }
}

/// Unparsed provider response body retained only after an explicit output-row parse.
pub struct OpenAiBatchResponseBody {
    body: Value,
}

impl OpenAiBatchResponseBody {
    /// Consumes and validates one Responses API body as a Rustee chat response.
    ///
    /// This is an explicit model-specific conversion for rows sent to `/v1/responses`; callers
    /// still own tenant authorization, structured/domain validation, partial-result policy, and
    /// every side effect. Bodies from another Batch endpoint fail with [`OpenAiError`].
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiError::MalformedResponse`] when the body is not a completed Responses API
    /// response accepted by Rustee's ordinary Responses adapter.
    pub fn into_chat_response(self) -> Result<ChatResponse, OpenAiError> {
        decode_response(&self.body)
    }

    /// Consumes and validates one Embeddings API body in the input row's declared order.
    ///
    /// This is an explicit model-specific conversion for rows sent to `/v1/embeddings`. The
    /// caller supplies the input count from its authorized catalog so duplicate, missing, or
    /// out-of-range provider indexes fail closed. Tenant authorization, chunk-to-domain mapping,
    /// partial-result policy, and every vector-store side effect remain application-owned.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiError::MalformedEmbeddingResponse`] when the body does not contain exactly
    /// `expected_embeddings` finite vectors with each provider index represented once.
    pub fn into_embeddings(
        self,
        expected_embeddings: usize,
    ) -> Result<Vec<Embedding>, OpenAiError> {
        decode_embeddings(&self.body, expected_embeddings)
    }

    /// Consumes the untrusted provider body for application-selected decoding and validation.
    #[must_use]
    pub fn into_json(self) -> Value {
        self.body
    }
}

impl fmt::Debug for OpenAiBatchResponseBody {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiBatchResponseBody")
            .field("body", &"[REDACTED]")
            .finish()
    }
}

/// Safe provider failure code for one `OpenAI` Batch output row.
pub struct OpenAiBatchRowError {
    code: String,
}

impl OpenAiBatchRowError {
    /// Returns the bounded provider failure code; provider error message text is not retained.
    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }
}

impl fmt::Debug for OpenAiBatchRowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiBatchRowError")
            .field("code", &self.code)
            .finish()
    }
}

/// Caller-selected expiration policy for one `OpenAI` Batch output/error artifact pair.
///
/// The provider anchors expiration at batch creation. This policy never deletes an input file,
/// settles billing, or replaces the application retention decision for copied domain results.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OpenAiBatchOutputExpiration {
    seconds: u64,
}

impl OpenAiBatchOutputExpiration {
    /// Creates an `OpenAI`-supported output/error-file expiration of one hour through 30 days.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiBatchOutputExpirationError::InvalidDuration`] when `duration` is not a
    /// whole number of seconds in the provider-supported range.
    pub fn new(duration: Duration) -> Result<Self, OpenAiBatchOutputExpirationError> {
        let seconds = duration.as_secs();
        if duration.subsec_nanos() != 0
            || !(OPENAI_BATCH_OUTPUT_EXPIRATION_MIN_SECONDS
                ..=OPENAI_BATCH_OUTPUT_EXPIRATION_MAX_SECONDS)
                .contains(&seconds)
        {
            return Err(OpenAiBatchOutputExpirationError::InvalidDuration);
        }
        Ok(Self { seconds })
    }

    /// Returns the approved output/error artifact lifetime as whole seconds.
    #[must_use]
    pub const fn seconds(self) -> u64 {
        self.seconds
    }
}

/// Invalid `OpenAI` Batch output/error artifact expiration configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OpenAiBatchOutputExpirationError {
    /// The selected duration was outside the provider-supported whole-second range.
    #[error(
        "OpenAI Batch output expiration must be a whole duration from one hour through 30 days"
    )]
    InvalidDuration,
}

/// Application-uploaded `OpenAI` JSONL input file selected for one batch submission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenAiBatchRequest {
    input_file_id: String,
    endpoint: OpenAiBatchEndpoint,
    output_expiration: Option<OpenAiBatchOutputExpiration>,
}

impl OpenAiBatchRequest {
    /// Creates a request from an already uploaded `OpenAI` file with purpose `batch`.
    ///
    /// The catalog/provider-work adapter owns JSONL creation and file upload. This type never
    /// accepts prompt text or request bodies.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiError::MalformedBatch`] when the provider file ID is not a bounded safe
    /// identifier.
    pub fn new(
        input_file_id: impl Into<String>,
        endpoint: OpenAiBatchEndpoint,
    ) -> Result<Self, OpenAiError> {
        let input_file_id = input_file_id.into();
        if !valid_provider_identifier(&input_file_id) {
            return Err(OpenAiError::MalformedBatch);
        }
        Ok(Self {
            input_file_id,
            endpoint,
            output_expiration: None,
        })
    }

    /// Creates a submission request from a file uploaded through [`OpenAiBatchProvider`].
    #[must_use]
    pub fn from_uploaded_input(
        input_file: &OpenAiBatchInputFile,
        endpoint: OpenAiBatchEndpoint,
    ) -> Self {
        Self {
            input_file_id: input_file.id.clone(),
            endpoint,
            output_expiration: None,
        }
    }

    /// Returns the selected uploaded input file ID.
    #[must_use]
    pub fn input_file_id(&self) -> &str {
        &self.input_file_id
    }

    /// Returns the single endpoint encoded by every JSONL line.
    #[must_use]
    pub const fn endpoint(&self) -> OpenAiBatchEndpoint {
        self.endpoint
    }

    /// Requests an explicit provider expiration for generated output and error files.
    ///
    /// This does not delete the input file or any application-owned result data. The application
    /// still records the retention decision and must reconcile a cancelled batch before deleting
    /// any provider file.
    #[must_use]
    pub const fn with_output_expiration(mut self, expiration: OpenAiBatchOutputExpiration) -> Self {
        self.output_expiration = Some(expiration);
        self
    }

    /// Returns the caller-selected output/error-file expiration policy, if any.
    #[must_use]
    pub const fn output_expiration(&self) -> Option<OpenAiBatchOutputExpiration> {
        self.output_expiration
    }
}

/// Safe progress snapshot for an `OpenAI` batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenAiBatchSnapshot {
    receipt: AiBatchReceipt,
    status: OpenAiBatchStatus,
    completed_requests: u64,
    failed_requests: u64,
    total_requests: u64,
    output_file_id: Option<String>,
    error_file_id: Option<String>,
}

impl OpenAiBatchSnapshot {
    /// Returns the safe batch receipt.
    #[must_use]
    pub const fn receipt(&self) -> &AiBatchReceipt {
        &self.receipt
    }

    /// Returns the normalized provider lifecycle status.
    #[must_use]
    pub const fn status(&self) -> OpenAiBatchStatus {
        self.status
    }

    /// Returns completed requests reported by the provider.
    #[must_use]
    pub const fn completed_requests(&self) -> u64 {
        self.completed_requests
    }

    /// Returns failed requests reported by the provider.
    #[must_use]
    pub const fn failed_requests(&self) -> u64 {
        self.failed_requests
    }

    /// Returns total requests reported by the provider.
    #[must_use]
    pub const fn total_requests(&self) -> u64 {
        self.total_requests
    }

    /// Returns the optional successful-output file ID; download remains explicit.
    #[must_use]
    pub fn output_file_id(&self) -> Option<&str> {
        self.output_file_id.as_deref()
    }

    /// Returns the optional error-output file ID; download remains explicit.
    #[must_use]
    pub fn error_file_id(&self) -> Option<&str> {
        self.error_file_id.as_deref()
    }
}

/// `OpenAI` batch lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenAiBatchStatus {
    /// The provider is validating the uploaded input file.
    Validating,
    /// The provider is running batch requests.
    InProgress,
    /// The provider is finalizing output files.
    Finalizing,
    /// The batch completed and may have output/error files.
    Completed,
    /// The batch failed before normal completion.
    Failed,
    /// The provider completion window elapsed.
    Expired,
    /// The provider is processing a cancellation request.
    Cancelling,
    /// The batch was cancelled and may have partial output.
    Cancelled,
}

/// `OpenAI` Batch API adapter sharing the existing credential, base URL, and deadline.
#[derive(Clone)]
pub struct OpenAiBatchProvider {
    client: Client,
    config: OpenAiConfig,
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
            .build()
            .map_err(|_| OpenAiError::Client)?;
        Ok(Self { client, config })
    }

    /// Wraps an application-provided HTTP client for dependency injection and contract tests.
    #[must_use]
    pub fn with_client(client: Client, config: OpenAiConfig) -> Self {
        Self { client, config }
    }

    /// Uploads one application-created JSONL file with `batch` purpose for later submission.
    ///
    /// This method is explicit and short-lived: it does not create a durable job, batch request,
    /// cache entry, or application result record.
    ///
    /// # Errors
    ///
    /// Returns an [`OpenAiError`] when the configured bound is exceeded, the upload cannot be
    /// sent, or the provider does not return a valid Batch-purpose file ID.
    pub async fn upload_batch_input(
        &self,
        input: OpenAiBatchInputJsonl,
    ) -> Result<OpenAiBatchInputFile, OpenAiError> {
        if input.bytes.len() > self.config.max_batch_file_bytes {
            return Err(OpenAiError::BatchFileTooLarge);
        }
        let endpoint = self
            .config
            .base_url
            .join("files")
            .map_err(|_| OpenAiError::InvalidEndpoint)?;
        let form = Form::new().text("purpose", "batch").part(
            "file",
            Part::bytes(input.bytes).file_name("rustee-batch-input.jsonl"),
        );
        let response = self
            .client
            .post(endpoint)
            .bearer_auth(&self.config.api_key)
            .multipart(form)
            .send()
            .await
            .map_err(|_| OpenAiError::Transport)?;
        if !response.status().is_success() {
            return Err(OpenAiError::HttpStatus(response.status()));
        }
        decode_batch_input_file(
            &response
                .json::<Value>()
                .await
                .map_err(|_| OpenAiError::MalformedBatchFile)?,
        )
    }

    /// Explicitly downloads one Batch input, output, or error file without parsing its rows.
    ///
    /// The caller must select an authorized file ID and process the returned bytes. No batch
    /// status transition, cache insertion, evaluation, or retry is triggered by this method.
    ///
    /// # Errors
    ///
    /// Returns an [`OpenAiError`] when `file_id` is unsafe, the configured bound is exceeded, or
    /// the download cannot be completed.
    pub async fn download_batch_file(
        &self,
        file_id: &str,
    ) -> Result<OpenAiBatchFileContent, OpenAiError> {
        if !valid_provider_identifier(file_id) {
            return Err(OpenAiError::MalformedBatchFile);
        }
        let endpoint = self
            .config
            .base_url
            .join(&format!("files/{file_id}/content"))
            .map_err(|_| OpenAiError::InvalidEndpoint)?;
        let response = self
            .client
            .get(endpoint)
            .bearer_auth(&self.config.api_key)
            .send()
            .await
            .map_err(|_| OpenAiError::Transport)?;
        if !response.status().is_success() {
            return Err(OpenAiError::HttpStatus(response.status()));
        }
        Ok(OpenAiBatchFileContent {
            bytes: read_bounded_batch_file(response, self.config.max_batch_file_bytes).await?,
        })
    }

    /// Explicitly deletes one authorized Batch input, output, or error file.
    ///
    /// No retention schedule, retry, batch cancellation, or local ledger mutation is implied.
    ///
    /// # Errors
    ///
    /// Returns an [`OpenAiError`] when `file_id` is unsafe, the request fails, or the provider
    /// does not confirm deletion of that exact file ID.
    pub async fn delete_batch_file(
        &self,
        file_id: &str,
    ) -> Result<OpenAiBatchFileDeletion, OpenAiError> {
        if !valid_provider_identifier(file_id) {
            return Err(OpenAiError::MalformedBatchFile);
        }
        let endpoint = self
            .config
            .base_url
            .join(&format!("files/{file_id}"))
            .map_err(|_| OpenAiError::InvalidEndpoint)?;
        let response = self
            .client
            .delete(endpoint)
            .bearer_auth(&self.config.api_key)
            .send()
            .await
            .map_err(|_| OpenAiError::Transport)?;
        if !response.status().is_success() {
            return Err(OpenAiError::HttpStatus(response.status()));
        }
        decode_batch_file_deletion(
            &response
                .json::<Value>()
                .await
                .map_err(|_| OpenAiError::MalformedBatchFile)?,
            file_id,
        )
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
        let endpoint = self
            .config
            .base_url
            .join(&format!("batches/{}", receipt.provider_batch_id()))
            .map_err(|_| OpenAiError::InvalidEndpoint)?;
        let response = self
            .client
            .get(endpoint)
            .bearer_auth(&self.config.api_key)
            .send()
            .await
            .map_err(|_| OpenAiError::Transport)?;
        if !response.status().is_success() {
            return Err(OpenAiError::HttpStatus(response.status()));
        }
        decode_batch(
            &response
                .json::<Value>()
                .await
                .map_err(|_| OpenAiError::MalformedBatch)?,
        )
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
        let endpoint = self
            .config
            .base_url
            .join(&format!("batches/{}/cancel", receipt.provider_batch_id()))
            .map_err(|_| OpenAiError::InvalidEndpoint)?;
        let response = self
            .client
            .post(endpoint)
            .bearer_auth(&self.config.api_key)
            .send()
            .await
            .map_err(|_| OpenAiError::Transport)?;
        if !response.status().is_success() {
            return Err(OpenAiError::HttpStatus(response.status()));
        }
        decode_batch(
            &response
                .json::<Value>()
                .await
                .map_err(|_| OpenAiError::MalformedBatch)?,
        )
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
            let response = provider
                .client
                .post(endpoint)
                .bearer_auth(&provider.config.api_key)
                .json(&body)
                .send()
                .await
                .map_err(|_| OpenAiError::Transport)?;
            if !response.status().is_success() {
                return Err(OpenAiError::HttpStatus(response.status()));
            }
            Ok(decode_batch(
                &response
                    .json::<Value>()
                    .await
                    .map_err(|_| OpenAiError::MalformedBatch)?,
            )?
            .receipt)
        })
    }
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
            .build()
            .map_err(|_| OpenAiError::Client)?;
        Ok(Self { client, config })
    }

    /// Wraps an already-configured HTTP client for dependency injection and contract tests.
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

    fn embed(
        &self,
        model: String,
        inputs: Vec<EmbeddingInput>,
    ) -> futures_util::future::BoxFuture<'static, Result<Vec<Embedding>, Self::Error>> {
        let provider = self.clone();
        Box::pin(async move {
            let expected_embeddings = inputs.len();
            let endpoint = provider
                .config
                .base_url
                .join("embeddings")
                .map_err(|_| OpenAiError::InvalidEndpoint)?;
            let body = embedding_request_body(&model, &inputs);
            let response = provider
                .client
                .post(endpoint)
                .bearer_auth(&provider.config.api_key)
                .json(&body)
                .send()
                .await
                .map_err(|_| OpenAiError::Transport)?;
            if !response.status().is_success() {
                return Err(OpenAiError::HttpStatus(response.status()));
            }
            let value = response
                .json::<Value>()
                .await
                .map_err(|_| OpenAiError::MalformedEmbeddingResponse)?;
            decode_embeddings(&value, expected_embeddings)
        })
    }
}

impl fmt::Debug for OpenAiResponsesProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiResponsesProvider")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl AiProvider for OpenAiResponsesProvider {
    type Error = OpenAiError;

    fn complete(
        &self,
        request: ChatRequest,
    ) -> futures_util::future::BoxFuture<'static, Result<ChatResponse, Self::Error>> {
        let provider = self.clone();
        Box::pin(async move {
            let response = provider.send(request, false).await?;
            let value = response
                .json::<Value>()
                .await
                .map_err(|_| OpenAiError::MalformedResponse)?;
            decode_response(&value)
        })
    }

    fn stream(&self, request: ChatRequest) -> AiEventStreamFuture<Self::Error> {
        let provider = self.clone();
        Box::pin(async move {
            let response = provider.send(request, true).await?;
            let max_sse_event_bytes = provider.config.max_sse_event_bytes;
            let stream = async_stream::try_stream! {
                let mut byte_stream = response.bytes_stream();
                let mut buffer = Vec::new();
                let mut completed = false;
                let mut done = false;

                while !done {
                    let Some(chunk) = byte_stream.next().await else {
                        break;
                    };
                    let chunk = chunk.map_err(|_| OpenAiError::Transport)?;
                    buffer.extend_from_slice(&chunk);
                    while let Some(frame) = take_sse_frame(&mut buffer) {
                        let payload = sse_payload(&frame)?;
                        if payload == "[DONE]" {
                            done = true;
                            break;
                        }
                        if let Some(event) = decode_stream_event(&payload)? {
                            if matches!(event, AiStreamEvent::Completed(_)) {
                                completed = true;
                            }
                            yield event;
                        }
                    }
                    if buffer.len() > max_sse_event_bytes {
                        Err::<(), OpenAiError>(OpenAiError::StreamEventTooLarge)?;
                    }
                }
                if !completed {
                    Err::<(), OpenAiError>(OpenAiError::StreamTerminated)?;
                }
            };
            Ok(Box::pin(stream) as AiEventStream<OpenAiError>)
        })
    }
}

/// Safe, provider-normalized failure from the `OpenAI` adapter.
#[derive(Debug, thiserror::Error)]
pub enum OpenAiError {
    /// The underlying HTTP client could not be initialized.
    #[error("OpenAI HTTP client could not be initialized")]
    Client,
    /// The configured API endpoint could not be resolved.
    #[error("OpenAI API endpoint is invalid")]
    InvalidEndpoint,
    /// The HTTP request could not be sent or read.
    #[error("OpenAI request failed")]
    Transport,
    /// The provider rejected a request; raw error detail is deliberately withheld.
    #[error("OpenAI returned HTTP status {0}")]
    HttpStatus(StatusCode),
    /// A generic message role cannot encode the required function-call output metadata.
    #[error("use ChatRequest::with_tool_results for provider function-call outputs")]
    UnsupportedToolMessage,
    /// Provider JSON did not satisfy the Responses API fields required by Rustee.
    #[error("OpenAI response did not match the expected Responses API shape")]
    MalformedResponse,
    /// Provider JSON did not satisfy the embeddings API fields or input-order contract.
    #[error("OpenAI embeddings response did not match the expected API shape")]
    MalformedEmbeddingResponse,
    /// Provider JSON did not satisfy the Batch API lifecycle fields required by Rustee.
    #[error("OpenAI batch response did not match the expected API shape")]
    MalformedBatch,
    /// Provider JSON did not satisfy the Batch-purpose file fields required by Rustee.
    #[error("OpenAI Batch file response did not match the expected API shape")]
    MalformedBatchFile,
    /// An explicit Batch input or output file exceeded the configured memory bound.
    #[error("OpenAI Batch file exceeded the configured size limit")]
    BatchFileTooLarge,
    /// The provider returned an incomplete, cancelled, or failed response.
    #[error("OpenAI response did not complete")]
    IncompleteResponse,
    /// A malformed or overlarge SSE event was received.
    #[error("OpenAI streaming event was invalid")]
    MalformedStreamEvent,
    /// An SSE event exceeded the configured memory bound.
    #[error("OpenAI streaming event exceeded the configured size limit")]
    StreamEventTooLarge,
    /// The stream ended before a terminal completion event arrived.
    #[error("OpenAI stream ended before completion")]
    StreamTerminated,
}

fn request_body(request: &ChatRequest) -> Result<Value, OpenAiError> {
    let mut input = Vec::with_capacity(request.messages().len() + request.tool_results().len());
    for message in request.messages() {
        let role = match message.role() {
            MessageRole::System => "system",
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
            MessageRole::Tool => return Err(OpenAiError::UnsupportedToolMessage),
        };
        input.push(json!({
            "type": "message",
            "role": role,
            "content": [{"type": "input_text", "text": message.content()}],
        }));
    }
    for result in request.tool_results() {
        let output =
            serde_json::to_string(result.content()).map_err(|_| OpenAiError::MalformedResponse)?;
        input.push(json!({
            "type": "function_call_output",
            "call_id": result.call_id(),
            "output": output,
        }));
    }
    let tools = request
        .tools()
        .iter()
        .map(|tool| {
            json!({
                "type": "function",
                "name": tool.name(),
                "parameters": tool.input_schema(),
            })
        })
        .collect::<Vec<_>>();
    let mut body = json!({
        "model": request.model(),
        "input": input,
    });
    if !tools.is_empty() {
        body["tools"] = Value::Array(tools);
    }
    Ok(body)
}

fn embedding_request_body(model: &str, inputs: &[EmbeddingInput]) -> Value {
    json!({
        "model": model,
        "input": inputs.iter().map(EmbeddingInput::content).collect::<Vec<_>>(),
    })
}

fn decode_embeddings(
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

fn decode_response(value: &Value) -> Result<ChatResponse, OpenAiError> {
    if value
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| status != "completed")
    {
        return Err(OpenAiError::IncompleteResponse);
    }
    let id = value
        .get("id")
        .and_then(Value::as_str)
        .ok_or(OpenAiError::MalformedResponse)?;
    let model = value
        .get("model")
        .and_then(Value::as_str)
        .ok_or(OpenAiError::MalformedResponse)?;
    let output = value
        .get("output")
        .and_then(Value::as_array)
        .ok_or(OpenAiError::MalformedResponse)?;
    let mut content = String::new();
    let mut tool_calls = Vec::new();
    for item in output {
        match item.get("type").and_then(Value::as_str) {
            Some("message") => {
                let items = item
                    .get("content")
                    .and_then(Value::as_array)
                    .ok_or(OpenAiError::MalformedResponse)?;
                for item in items {
                    if item.get("type").and_then(Value::as_str) == Some("output_text") {
                        let text = item
                            .get("text")
                            .and_then(Value::as_str)
                            .ok_or(OpenAiError::MalformedResponse)?;
                        content.push_str(text);
                    }
                }
            }
            Some("function_call") => {
                let call_id = item
                    .get("call_id")
                    .and_then(Value::as_str)
                    .ok_or(OpenAiError::MalformedResponse)?;
                let name = item
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or(OpenAiError::MalformedResponse)?;
                let arguments = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .ok_or(OpenAiError::MalformedResponse)?;
                let arguments =
                    serde_json::from_str(arguments).map_err(|_| OpenAiError::MalformedResponse)?;
                tool_calls.push(
                    ToolCall::new(call_id, name, arguments)
                        .map_err(|_| OpenAiError::MalformedResponse)?,
                );
            }
            _ => {}
        }
    }
    let usage = usage(value.get("usage"))?;
    ChatResponse::new(id, model, content, tool_calls, usage)
        .map_err(|_| OpenAiError::MalformedResponse)
}

fn decode_batch(value: &Value) -> Result<OpenAiBatchSnapshot, OpenAiError> {
    let receipt = AiBatchReceipt::new(
        value
            .get("id")
            .and_then(Value::as_str)
            .ok_or(OpenAiError::MalformedBatch)?,
    )
    .map_err(|_| OpenAiError::MalformedBatch)?;
    let status = match value
        .get("status")
        .and_then(Value::as_str)
        .ok_or(OpenAiError::MalformedBatch)?
    {
        "validating" => OpenAiBatchStatus::Validating,
        "in_progress" => OpenAiBatchStatus::InProgress,
        "finalizing" => OpenAiBatchStatus::Finalizing,
        "completed" => OpenAiBatchStatus::Completed,
        "failed" => OpenAiBatchStatus::Failed,
        "expired" => OpenAiBatchStatus::Expired,
        "cancelling" => OpenAiBatchStatus::Cancelling,
        "cancelled" => OpenAiBatchStatus::Cancelled,
        _ => return Err(OpenAiError::MalformedBatch),
    };
    let request_counts = value
        .get("request_counts")
        .and_then(Value::as_object)
        .ok_or(OpenAiError::MalformedBatch)?;
    let count = |name| {
        request_counts
            .get(name)
            .and_then(Value::as_u64)
            .ok_or(OpenAiError::MalformedBatch)
    };
    let completed_requests = count("completed")?;
    let failed_requests = count("failed")?;
    let total_requests = count("total")?;
    if completed_requests.saturating_add(failed_requests) > total_requests {
        return Err(OpenAiError::MalformedBatch);
    }
    Ok(OpenAiBatchSnapshot {
        receipt,
        status,
        completed_requests,
        failed_requests,
        total_requests,
        output_file_id: optional_provider_identifier(value.get("output_file_id"))?,
        error_file_id: optional_provider_identifier(value.get("error_file_id"))?,
    })
}

fn decode_batch_output_row(value: &Value) -> Result<OpenAiBatchOutputRow, ()> {
    let custom_id = value
        .get("custom_id")
        .and_then(Value::as_str)
        .filter(|custom_id| valid_provider_identifier(custom_id))
        .ok_or(())?
        .to_owned();
    let response = value.get("response").filter(|response| !response.is_null());
    let error = value.get("error").filter(|error| !error.is_null());
    let outcome = match (response, error) {
        (Some(response), None) => {
            let status_code = response
                .get("status_code")
                .and_then(Value::as_u64)
                .and_then(|status_code| u16::try_from(status_code).ok())
                .filter(|status_code| (100..=599).contains(status_code))
                .ok_or(())?;
            let request_id = response
                .get("request_id")
                .and_then(Value::as_str)
                .filter(|request_id| valid_provider_identifier(request_id))
                .ok_or(())?
                .to_owned();
            let body = response
                .get("body")
                .filter(|body| body.is_object())
                .ok_or(())?
                .clone();
            OpenAiBatchRowOutcome::Response(OpenAiBatchResponse {
                status_code,
                request_id,
                body: OpenAiBatchResponseBody { body },
            })
        }
        (None, Some(error)) => {
            let code = error
                .get("code")
                .and_then(Value::as_str)
                .filter(|code| valid_provider_identifier(code))
                .ok_or(())?
                .to_owned();
            OpenAiBatchRowOutcome::Error(OpenAiBatchRowError { code })
        }
        _ => return Err(()),
    };
    Ok(OpenAiBatchOutputRow { custom_id, outcome })
}

fn decode_batch_input_file(value: &Value) -> Result<OpenAiBatchInputFile, OpenAiError> {
    let id = value
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| valid_provider_identifier(id))
        .ok_or(OpenAiError::MalformedBatchFile)?;
    if value.get("purpose").and_then(Value::as_str) != Some("batch") {
        return Err(OpenAiError::MalformedBatchFile);
    }
    Ok(OpenAiBatchInputFile { id: id.to_owned() })
}

fn decode_batch_file_deletion(
    value: &Value,
    expected_file_id: &str,
) -> Result<OpenAiBatchFileDeletion, OpenAiError> {
    let id = value
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| valid_provider_identifier(id) && *id == expected_file_id)
        .ok_or(OpenAiError::MalformedBatchFile)?;
    if value.get("deleted").and_then(Value::as_bool) != Some(true) {
        return Err(OpenAiError::MalformedBatchFile);
    }
    Ok(OpenAiBatchFileDeletion { id: id.to_owned() })
}

async fn read_bounded_batch_file(
    response: reqwest::Response,
    max_batch_file_bytes: usize,
) -> Result<Vec<u8>, OpenAiError> {
    let max_batch_file_bytes_u64 =
        u64::try_from(max_batch_file_bytes).map_err(|_| OpenAiError::BatchFileTooLarge)?;
    if response
        .content_length()
        .is_some_and(|length| length > max_batch_file_bytes_u64)
    {
        return Err(OpenAiError::BatchFileTooLarge);
    }
    let mut content = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| OpenAiError::Transport)?;
        if content.len().saturating_add(chunk.len()) > max_batch_file_bytes {
            return Err(OpenAiError::BatchFileTooLarge);
        }
        content.extend_from_slice(&chunk);
    }
    Ok(content)
}

fn decode_stream_event(payload: &str) -> Result<Option<AiStreamEvent>, OpenAiError> {
    let value: Value =
        serde_json::from_str(payload).map_err(|_| OpenAiError::MalformedStreamEvent)?;
    match value.get("type").and_then(Value::as_str) {
        Some("response.output_text.delta") => value
            .get("delta")
            .and_then(Value::as_str)
            .map(|delta| Some(AiStreamEvent::TextDelta(delta.to_owned())))
            .ok_or(OpenAiError::MalformedStreamEvent),
        Some("response.function_call_arguments.done") => {
            let call_id = value
                .get("call_id")
                .and_then(Value::as_str)
                .ok_or(OpenAiError::MalformedStreamEvent)?;
            let name = value
                .get("name")
                .and_then(Value::as_str)
                .ok_or(OpenAiError::MalformedStreamEvent)?;
            let arguments = value
                .get("arguments")
                .and_then(Value::as_str)
                .ok_or(OpenAiError::MalformedStreamEvent)?;
            let arguments =
                serde_json::from_str(arguments).map_err(|_| OpenAiError::MalformedStreamEvent)?;
            let call = ToolCall::new(call_id, name, arguments)
                .map_err(|_| OpenAiError::MalformedStreamEvent)?;
            Ok(Some(AiStreamEvent::ToolCall(call)))
        }
        Some("response.completed") => Ok(Some(AiStreamEvent::Completed(usage(
            value
                .get("response")
                .and_then(|response| response.get("usage")),
        )?))),
        Some("response.failed" | "error") => Err(OpenAiError::MalformedStreamEvent),
        _ => Ok(None),
    }
}

fn usage(value: Option<&Value>) -> Result<Usage, OpenAiError> {
    let Some(value) = value else {
        return Ok(Usage::default());
    };
    let input_tokens = value
        .get("input_tokens")
        .and_then(Value::as_u64)
        .ok_or(OpenAiError::MalformedResponse)?;
    let output_tokens = value
        .get("output_tokens")
        .and_then(Value::as_u64)
        .ok_or(OpenAiError::MalformedResponse)?;
    Ok(Usage {
        input_tokens,
        output_tokens,
    })
}

fn take_sse_frame(buffer: &mut Vec<u8>) -> Option<Vec<u8>> {
    let delimiter = buffer
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|index| (index, 2))
        .or_else(|| {
            buffer
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|index| (index, 4))
        })?;
    Some(buffer.drain(..delimiter.0 + delimiter.1).collect())
}

fn sse_payload(frame: &[u8]) -> Result<String, OpenAiError> {
    let frame = std::str::from_utf8(frame).map_err(|_| OpenAiError::MalformedStreamEvent)?;
    let payload = frame
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(|line| line.strip_prefix(' ').unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n");
    (!payload.is_empty())
        .then_some(payload)
        .ok_or(OpenAiError::MalformedStreamEvent)
}

fn valid_base_url(url: &Url) -> bool {
    matches!(url.scheme(), "http" | "https") && url.query().is_none() && url.fragment().is_none()
}

fn valid_provider_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn optional_provider_identifier(value: Option<&Value>) -> Result<Option<String>, OpenAiError> {
    match value {
        Some(Value::Null) | None => Ok(None),
        Some(Value::String(value)) if valid_provider_identifier(value) => Ok(Some(value.clone())),
        _ => Err(OpenAiError::MalformedBatch),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_util::TryStreamExt;
    use rustee_ai::{
        AiProvider, AiStreamEvent, ChatMessage, ChatRequest, MessageRole, ToolDefinition,
    };
    use rustee_ai_batch::{AiBatchProvider, AiBatchReceipt, AiBatchReference};
    use rustee_ai_rag::{EmbeddingInput, EmbeddingProvider};
    use serde_json::{Value, json};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::oneshot,
    };
    use url::Url;

    use super::{
        OPENAI_BATCH_FILE_MAX_BYTES, OpenAiBatchEmbeddingsRowError, OpenAiBatchEndpoint,
        OpenAiBatchFileContent, OpenAiBatchInputError, OpenAiBatchInputJsonl, OpenAiBatchInputRow,
        OpenAiBatchJsonlBuilder, OpenAiBatchOutputExpiration, OpenAiBatchOutputExpirationError,
        OpenAiBatchProvider, OpenAiBatchRequest, OpenAiBatchResponseBody,
        OpenAiBatchResponsesRowError, OpenAiBatchRowOutcome, OpenAiBatchStatus, OpenAiConfig,
        OpenAiConfigError, OpenAiEmbeddingsProvider, OpenAiError, OpenAiResponsesProvider,
        decode_batch, decode_batch_input_file, decode_embeddings, decode_response,
        decode_stream_event, embedding_request_body, request_body, sse_payload, take_sse_frame,
    };

    fn request() -> ChatRequest {
        ChatRequest::new(
            "gpt-test",
            [ChatMessage::new(MessageRole::User, "what is the status?").unwrap()],
        )
        .unwrap()
        .with_tools([ToolDefinition::new("lookup_order", json!({"type":"object"})).unwrap()])
    }

    #[test]
    fn request_mapping_uses_responses_input_and_function_tool_shape() {
        assert_eq!(
            request_body(&request()).unwrap(),
            json!({
                "model":"gpt-test",
                "input":[{
                    "type":"message",
                    "role":"user",
                    "content":[{"type":"input_text", "text":"what is the status?"}],
                }],
                "tools":[{
                    "type":"function",
                    "name":"lookup_order",
                    "parameters":{"type":"object"},
                }],
            })
        );
    }

    #[test]
    fn embedding_request_mapping_keeps_batch_input_order() {
        let inputs = vec![
            EmbeddingInput::new("chunk-1", "first text").unwrap(),
            EmbeddingInput::new("chunk-2", "second text").unwrap(),
        ];

        assert_eq!(
            embedding_request_body("text-embedding-test", &inputs),
            json!({
                "model":"text-embedding-test",
                "input":["first text", "second text"],
            })
        );
        assert!(!format!("{:?}", inputs[0]).contains("first text"));
    }

    #[test]
    fn response_mapping_collects_text_tool_calls_and_usage() {
        let response = decode_response(&json!({
            "id":"resp_1",
            "model":"gpt-test-2026",
            "status":"completed",
            "output":[
                {"type":"message","content":[{"type":"output_text","text":"Checking. "}]},
                {"type":"function_call","call_id":"call_1","name":"lookup_order","arguments":r#"{"id":7}"#},
                {"type":"message","content":[{"type":"output_text","text":"Done."}]}
            ],
            "usage":{"input_tokens":10,"output_tokens":4}
        }))
        .unwrap();

        assert_eq!(response.content(), "Checking. Done.");
        assert_eq!(response.tool_calls()[0].id(), "call_1");
        assert_eq!(response.usage().total_tokens(), 14);
    }

    #[test]
    fn stream_frames_support_crlf_and_function_call_events() {
        let mut buffer = b"event: response.function_call_arguments.done\r\ndata: {\"type\":\"response.function_call_arguments.done\",\"call_id\":\"call_1\",\"name\":\"lookup_order\",\"arguments\":\"{\\\"id\\\":7}\"}\r\n\r\n".to_vec();
        let frame = take_sse_frame(&mut buffer).unwrap();
        let event = decode_stream_event(&sse_payload(&frame).unwrap())
            .unwrap()
            .unwrap();

        assert!(matches!(event, AiStreamEvent::ToolCall(_)));
        assert!(buffer.is_empty());
    }

    #[test]
    fn configuration_redacts_credentials_and_validates_bounds() {
        let config = OpenAiConfig::new("sk-secret").unwrap();
        assert!(!format!("{config:?}").contains("sk-secret"));
        assert_eq!(
            OpenAiConfig::new(" ").unwrap_err(),
            OpenAiConfigError::BlankApiKey
        );
        assert_eq!(
            config
                .clone()
                .with_base_url(Url::parse("ftp://example.test/v1/").unwrap())
                .unwrap_err(),
            OpenAiConfigError::InvalidBaseUrl
        );
        assert_eq!(
            config.clone().with_max_batch_file_bytes(0).unwrap_err(),
            OpenAiConfigError::InvalidBatchFileLimit
        );
        assert_eq!(
            config
                .with_max_batch_file_bytes(OPENAI_BATCH_FILE_MAX_BYTES + 1)
                .unwrap_err(),
            OpenAiConfigError::InvalidBatchFileLimit
        );
    }

    #[test]
    fn request_mapping_sends_approved_tool_results_as_function_outputs() {
        let tool_result = serde_json::from_value(json!({
            "call_id":"call_1",
            "name":"lookup_order",
            "content":{"status":"found"}
        }))
        .unwrap();
        let body = request_body(&request().with_tool_results([tool_result])).unwrap();

        assert_eq!(
            body["input"][1],
            json!({
                "type":"function_call_output",
                "call_id":"call_1",
                "output":"{\"status\":\"found\"}"
            })
        );
    }

    #[test]
    fn incomplete_responses_are_not_returned_as_successes() {
        assert!(matches!(
            decode_response(&json!({"status":"incomplete"})),
            Err(OpenAiError::IncompleteResponse)
        ));
    }

    #[test]
    fn batch_decoder_preserves_only_safe_status_counts_and_file_identifiers() {
        let snapshot = decode_batch(&json!({
            "id":"batch_1",
            "status":"completed",
            "request_counts":{"total":10,"completed":8,"failed":2},
            "output_file_id":"file_output_1",
            "error_file_id":"file_error_1"
        }))
        .unwrap();
        assert_eq!(snapshot.status(), OpenAiBatchStatus::Completed);
        assert_eq!(snapshot.completed_requests(), 8);
        assert_eq!(snapshot.output_file_id(), Some("file_output_1"));
        assert!(
            decode_batch(&json!({
                "id":"batch_1",
                "status":"unknown-state",
                "request_counts":{"total":1,"completed":0,"failed":0}
            }))
            .is_err()
        );
        assert!(
            decode_batch(&json!({
                "id":"batch_1",
                "status":"completed",
                "request_counts":{"total":1,"completed":1,"failed":1}
            }))
            .is_err()
        );
    }

    #[test]
    fn batch_input_constructor_and_file_decoder_are_bounded_and_redacted() {
        let input = OpenAiBatchInputJsonl::new(b"private prompt\n".to_vec()).unwrap();
        assert_eq!(input.len(), 15);
        assert!(!input.is_empty());
        assert!(!format!("{input:?}").contains("private prompt"));
        assert_eq!(
            OpenAiBatchInputJsonl::new(Vec::new()).unwrap_err(),
            OpenAiBatchInputError::Empty
        );
        assert!(
            decode_batch_input_file(&json!({
                "id":"file_input_1",
                "purpose":"assistants"
            }))
            .is_err()
        );
    }

    #[test]
    fn batch_output_expiration_is_whole_second_and_provider_bounded() {
        assert_eq!(
            OpenAiBatchOutputExpiration::new(Duration::from_secs(59 * 60)).unwrap_err(),
            OpenAiBatchOutputExpirationError::InvalidDuration
        );
        assert_eq!(
            OpenAiBatchOutputExpiration::new(Duration::from_secs(31 * 24 * 60 * 60)).unwrap_err(),
            OpenAiBatchOutputExpirationError::InvalidDuration
        );
        assert_eq!(
            OpenAiBatchOutputExpiration::new(
                Duration::from_secs(60 * 60) + Duration::from_nanos(1)
            )
            .unwrap_err(),
            OpenAiBatchOutputExpirationError::InvalidDuration
        );
        let expiration =
            OpenAiBatchOutputExpiration::new(Duration::from_secs(24 * 60 * 60)).unwrap();
        let request = OpenAiBatchRequest::new("file_input_1", OpenAiBatchEndpoint::Responses)
            .unwrap()
            .with_output_expiration(expiration);
        assert_eq!(request.output_expiration(), Some(expiration));
    }

    #[test]
    fn batch_input_builder_serializes_only_a_validated_generic_envelope() {
        let row = OpenAiBatchInputRow::new(
            "row_1",
            OpenAiBatchEndpoint::Responses,
            json!({"model":"gpt-test","input":"private prompt"}),
        )
        .unwrap();
        assert!(!format!("{row:?}").contains("private prompt"));
        let mut builder = OpenAiBatchJsonlBuilder::new(OpenAiBatchEndpoint::Responses);
        builder.push(row).unwrap();
        assert_eq!(
            builder
                .push(
                    OpenAiBatchInputRow::new(
                        "row_1",
                        OpenAiBatchEndpoint::Responses,
                        json!({"model":"gpt-test"}),
                    )
                    .unwrap(),
                )
                .unwrap_err(),
            OpenAiBatchInputError::DuplicateCustomId
        );
        assert!(
            builder
                .push(
                    OpenAiBatchInputRow::new(
                        "row_2",
                        OpenAiBatchEndpoint::Embeddings,
                        json!({"model":"embedding-test"}),
                    )
                    .unwrap(),
                )
                .is_err()
        );
        let input = builder.build().unwrap();
        assert!(!format!("{input:?}").contains("private prompt"));
        let envelope: Value = serde_json::from_slice(&input.bytes).unwrap();
        assert_eq!(envelope["custom_id"], "row_1");
        assert_eq!(envelope["method"], "POST");
        assert_eq!(envelope["url"], "/v1/responses");
        assert_eq!(envelope["body"]["input"], "private prompt");
        assert!(matches!(
            OpenAiBatchInputRow::new("bad id", OpenAiBatchEndpoint::Responses, json!({})),
            Err(OpenAiBatchInputError::UnsafeCustomId)
        ));
        assert!(matches!(
            OpenAiBatchInputRow::new("row_3", OpenAiBatchEndpoint::Responses, json!(null)),
            Err(OpenAiBatchInputError::BodyMustBeObject)
        ));
    }

    #[test]
    fn responses_batch_row_reuses_the_typed_chat_request_mapping() {
        let row = OpenAiBatchInputRow::from_chat_request("row_typed_1", &request()).unwrap();
        assert!(!format!("{row:?}").contains("what is the status"));
        let mut builder = OpenAiBatchJsonlBuilder::new(OpenAiBatchEndpoint::Responses);
        builder.push(row).unwrap();
        let input = builder.build().unwrap();
        let envelope: Value = serde_json::from_slice(&input.bytes).unwrap();

        assert_eq!(envelope["url"], "/v1/responses");
        assert_eq!(envelope["body"]["model"], "gpt-test");
        assert_eq!(envelope["body"]["input"][0]["type"], "message");
        assert_eq!(envelope["body"]["tools"][0]["name"], "lookup_order");

        let unsupported = ChatRequest::new(
            "gpt-test",
            [ChatMessage::new(MessageRole::Tool, "private tool message").unwrap()],
        )
        .unwrap();
        assert!(matches!(
            OpenAiBatchInputRow::from_chat_request("row_typed_2", &unsupported),
            Err(OpenAiBatchResponsesRowError::Request {
                source: OpenAiError::UnsupportedToolMessage,
            })
        ));
    }

    #[test]
    fn embeddings_batch_row_preserves_typed_input_order() {
        let row = OpenAiBatchInputRow::from_embedding_inputs(
            "row_embedding_1",
            "text-embedding-3-small",
            &[
                EmbeddingInput::new("chunk-1", "first private text").unwrap(),
                EmbeddingInput::new("chunk-2", "second private text").unwrap(),
            ],
        )
        .unwrap();
        assert!(!format!("{row:?}").contains("first private text"));
        let mut builder = OpenAiBatchJsonlBuilder::new(OpenAiBatchEndpoint::Embeddings);
        builder.push(row).unwrap();
        let input = builder.build().unwrap();
        let envelope: Value = serde_json::from_slice(&input.bytes).unwrap();

        assert_eq!(envelope["url"], "/v1/embeddings");
        assert_eq!(envelope["body"]["model"], "text-embedding-3-small");
        assert_eq!(
            envelope["body"]["input"],
            json!(["first private text", "second private text"])
        );
        assert!(matches!(
            OpenAiBatchInputRow::from_embedding_inputs(
                "row_embedding_2",
                "bad model",
                &[EmbeddingInput::new("chunk-1", "text").unwrap()],
            ),
            Err(OpenAiBatchEmbeddingsRowError::UnsafeModel)
        ));
        assert!(matches!(
            OpenAiBatchInputRow::from_embedding_inputs(
                "row_embedding_3",
                "text-embedding-3-small",
                &[],
            ),
            Err(OpenAiBatchEmbeddingsRowError::EmptyInputs)
        ));
    }

    #[tokio::test]
    async fn batch_file_upload_and_download_stay_explicit_and_content_redacted() {
        let input = OpenAiBatchInputJsonl::new(
            b"{\"custom_id\":\"row-1\",\"body\":\"private prompt\"}\n".to_vec(),
        )
        .unwrap();
        assert!(!format!("{input:?}").contains("private prompt"));
        let (upload_url, captured_upload, upload_server) = response_server(
            "application/json",
            json!({"id":"file_input_2","purpose":"batch"}).to_string(),
        )
        .await;
        let uploader = OpenAiBatchProvider::new(
            OpenAiConfig::new("sk-secret")
                .unwrap()
                .with_base_url(upload_url)
                .unwrap(),
        )
        .unwrap();
        let input_file = uploader.upload_batch_input(input).await.unwrap();
        let sent = captured_upload.await.unwrap();
        upload_server.await.unwrap();

        assert_eq!(input_file.id(), "file_input_2");
        assert!(sent.starts_with("POST /v1/files HTTP/1.1\r\n"));
        assert!(sent.contains("name=\"purpose\""));
        assert!(sent.contains("\r\n\r\nbatch\r\n"));
        assert!(sent.contains("filename=\"rustee-batch-input.jsonl\""));
        assert!(sent.contains("private prompt"));
        let request =
            OpenAiBatchRequest::from_uploaded_input(&input_file, OpenAiBatchEndpoint::Responses);
        assert_eq!(request.input_file_id(), "file_input_2");

        let output = "{\"custom_id\":\"row-1\",\"response\":{\"status_code\":200}}\n";
        let (download_url, captured_download, download_server) =
            response_server("application/jsonl", output.to_owned()).await;
        let downloader = OpenAiBatchProvider::new(
            OpenAiConfig::new("sk-secret")
                .unwrap()
                .with_base_url(download_url)
                .unwrap(),
        )
        .unwrap();
        let content = downloader
            .download_batch_file("file_output_2")
            .await
            .unwrap();
        let sent = captured_download.await.unwrap();
        download_server.await.unwrap();

        assert!(sent.starts_with("GET /v1/files/file_output_2/content HTTP/1.1\r\n"));
        assert_eq!(content.len(), output.len());
        assert!(!format!("{content:?}").contains("status_code"));
        assert_eq!(content.into_bytes(), output.as_bytes());
    }

    #[tokio::test]
    async fn batch_file_download_rejects_a_content_length_above_the_configured_bound() {
        let (url, captured_request, server) =
            response_server("application/jsonl", "large".into()).await;
        let provider = OpenAiBatchProvider::new(
            OpenAiConfig::new("sk-secret")
                .unwrap()
                .with_base_url(url)
                .unwrap()
                .with_max_batch_file_bytes(4)
                .unwrap(),
        )
        .unwrap();

        assert!(matches!(
            provider.download_batch_file("file_output_3").await,
            Err(OpenAiError::BatchFileTooLarge)
        ));
        let sent = captured_request.await.unwrap();
        server.await.unwrap();
        assert!(sent.starts_with("GET /v1/files/file_output_3/content HTTP/1.1\r\n"));
    }

    #[tokio::test]
    async fn batch_file_deletion_requires_an_exact_provider_acknowledgement() {
        let (url, captured_request, server) = response_server(
            "application/json",
            json!({"id":"file_output_4","object":"file","deleted":true}).to_string(),
        )
        .await;
        let provider = OpenAiBatchProvider::new(
            OpenAiConfig::new("sk-secret")
                .unwrap()
                .with_base_url(url)
                .unwrap(),
        )
        .unwrap();

        let deletion = provider.delete_batch_file("file_output_4").await.unwrap();
        let sent = captured_request.await.unwrap();
        server.await.unwrap();
        assert_eq!(deletion.id(), "file_output_4");
        assert!(sent.starts_with("DELETE /v1/files/file_output_4 HTTP/1.1\r\n"));
    }

    #[test]
    fn batch_output_rows_keep_model_content_explicit_and_fail_closed() {
        let content = OpenAiBatchFileContent {
            bytes: concat!(
                "{\"custom_id\":\"row_1\",\"response\":{\"status_code\":200,\"request_id\":\"req_1\",\"body\":{\"output\":\"private completion\"}},\"error\":null}\n",
                "{\"custom_id\":\"row_2\",\"response\":null,\"error\":{\"code\":\"batch_cancelled\",\"message\":\"private provider message\"}}\n"
            )
            .as_bytes()
            .to_vec(),
        };
        let mut rows = content.output_rows_with_limit(2).unwrap();
        let success = rows.next().unwrap().unwrap();
        assert_eq!(success.custom_id(), "row_1");
        let response = match success.into_outcome() {
            OpenAiBatchRowOutcome::Response(response) => response,
            OpenAiBatchRowOutcome::Error(_) => panic!("expected successful provider row"),
        };
        assert_eq!(response.status_code(), 200);
        assert_eq!(response.request_id(), "req_1");
        let body = response.into_body();
        assert!(!format!("{body:?}").contains("private completion"));
        assert_eq!(body.into_json()["output"], "private completion");

        let failure = rows.next().unwrap().unwrap();
        let error = match failure.into_outcome() {
            OpenAiBatchRowOutcome::Response(_) => panic!("expected failed provider row"),
            OpenAiBatchRowOutcome::Error(error) => error,
        };
        assert_eq!(error.code(), "batch_cancelled");
        assert!(!format!("{error:?}").contains("private provider message"));
        assert!(rows.next().is_none());

        let malformed = OpenAiBatchFileContent {
            bytes: b"{not json}\n{\"custom_id\":\"row_3\"}\n".to_vec(),
        };
        let mut rows = malformed.output_rows();
        assert!(rows.next().unwrap().is_err());
        assert!(rows.next().is_none());
    }

    #[test]
    fn responses_batch_body_decoding_is_explicit_and_reuses_response_validation() {
        let body = OpenAiBatchResponseBody {
            body: json!({
                "id":"resp_batch_1",
                "model":"gpt-batch",
                "output":[{"type":"message","content":[{"type":"output_text","text":"private completion"}]}],
                "usage":{"input_tokens":2,"output_tokens":1}
            }),
        };
        assert!(!format!("{body:?}").contains("private completion"));
        let response = body.into_chat_response().unwrap();
        assert_eq!(response.content(), "private completion");
        assert_eq!(response.usage().total_tokens(), 3);

        let malformed = OpenAiBatchResponseBody {
            body: json!({"output": []}),
        };
        assert!(matches!(
            malformed.into_chat_response(),
            Err(OpenAiError::MalformedResponse)
        ));
    }

    #[test]
    fn embeddings_batch_body_decoding_is_explicit_and_preserves_input_order() {
        let body = OpenAiBatchResponseBody {
            body: json!({
                "data":[
                    {"index":1,"embedding":[2.0,-1.0]},
                    {"index":0,"embedding":[0.25,0.5]}
                ]
            }),
        };
        assert!(!format!("{body:?}").contains("0.25"));
        let embeddings = body.into_embeddings(2).unwrap();
        assert_eq!(embeddings[0].values(), &[0.25, 0.5]);
        assert_eq!(embeddings[1].values(), &[2.0, -1.0]);

        let malformed = OpenAiBatchResponseBody {
            body: json!({"data":[{"index":0,"embedding":[0.25]}]}),
        };
        assert!(matches!(
            malformed.into_embeddings(2),
            Err(OpenAiError::MalformedEmbeddingResponse)
        ));
    }

    #[test]
    fn batch_output_rows_enforce_an_application_row_bound() {
        let content = OpenAiBatchFileContent {
            bytes: concat!(
                "{\"custom_id\":\"row_1\",\"response\":null,\"error\":{\"code\":\"batch_expired\"}}\n",
                "{\"custom_id\":\"row_2\",\"response\":null,\"error\":{\"code\":\"batch_expired\"}}\n"
            )
            .as_bytes()
            .to_vec(),
        };
        assert!(content.output_rows_with_limit(0).is_err());
        let mut rows = content.output_rows_with_limit(1).unwrap();
        assert!(rows.next().unwrap().is_ok());
        assert!(rows.next().unwrap().is_err());
        assert!(rows.next().is_none());
    }

    #[tokio::test]
    async fn batch_provider_submits_uploaded_file_with_the_stable_run_key() {
        let (url, captured_request, server) = response_server(
            "application/json",
            json!({
                "id":"batch_1",
                "status":"validating",
                "request_counts":{"total":0,"completed":0,"failed":0},
                "output_file_id":null,
                "error_file_id":null
            })
            .to_string(),
        )
        .await;
        let config = OpenAiConfig::new("sk-secret")
            .unwrap()
            .with_base_url(url)
            .unwrap();
        let provider = OpenAiBatchProvider::new(config).unwrap();

        let receipt = provider
            .submit(
                AiBatchReference::new("tenant-a.v1", "catalog-1", "run-1").unwrap(),
                OpenAiBatchRequest::new("file_input_1", OpenAiBatchEndpoint::Responses)
                    .unwrap()
                    .with_output_expiration(
                        OpenAiBatchOutputExpiration::new(Duration::from_secs(60 * 60)).unwrap(),
                    ),
            )
            .await
            .unwrap();
        let sent = captured_request.await.unwrap();
        server.await.unwrap();

        assert_eq!(receipt.provider_batch_id(), "batch_1");
        assert!(sent.starts_with("POST /v1/batches HTTP/1.1\r\n"));
        assert!(sent.contains("\"input_file_id\":\"file_input_1\""));
        assert!(sent.contains("\"endpoint\":\"/v1/responses\""));
        assert!(sent.contains("\"rustee_run_key\":\"run-1\""));
        assert!(
            sent.contains("\"output_expires_after\":{\"anchor\":\"created_at\",\"seconds\":3600}")
        );
        assert!(!sent.contains("private prompt"));
    }

    #[tokio::test]
    async fn batch_provider_retrieves_and_cancels_without_downloading_result_contents() {
        let (retrieve_url, captured_retrieve, retrieve_server) = response_server(
            "application/json",
            json!({
                "id":"batch_2",
                "status":"in_progress",
                "request_counts":{"total":5,"completed":2,"failed":0},
                "output_file_id":null,
                "error_file_id":null
            })
            .to_string(),
        )
        .await;
        let config = OpenAiConfig::new("sk-secret")
            .unwrap()
            .with_base_url(retrieve_url)
            .unwrap();
        let provider = OpenAiBatchProvider::new(config).unwrap();
        let receipt = AiBatchReceipt::new("batch_2").unwrap();
        let snapshot = provider.retrieve(&receipt).await.unwrap();
        let sent = captured_retrieve.await.unwrap();
        retrieve_server.await.unwrap();

        assert_eq!(snapshot.status(), OpenAiBatchStatus::InProgress);
        assert!(sent.starts_with("GET /v1/batches/batch_2 HTTP/1.1\r\n"));

        let (cancel_url, captured_cancel, cancel_server) = response_server(
            "application/json",
            json!({
                "id":"batch_2",
                "status":"cancelling",
                "request_counts":{"total":5,"completed":2,"failed":0},
                "output_file_id":null,
                "error_file_id":null
            })
            .to_string(),
        )
        .await;
        let cancel_provider = OpenAiBatchProvider::new(
            OpenAiConfig::new("sk-secret")
                .unwrap()
                .with_base_url(cancel_url)
                .unwrap(),
        )
        .unwrap();
        let cancelled = cancel_provider.cancel(&receipt).await.unwrap();
        let sent = captured_cancel.await.unwrap();
        cancel_server.await.unwrap();

        assert_eq!(cancelled.status(), OpenAiBatchStatus::Cancelling);
        assert!(sent.starts_with("POST /v1/batches/batch_2/cancel HTTP/1.1\r\n"));
    }

    #[tokio::test]
    async fn provider_sends_a_responses_request_and_decodes_the_response() {
        let (url, captured_request, server) = response_server(
            "application/json",
            json!({
                "id":"resp_network",
                "model":"gpt-network",
                "output":[{"type":"message","content":[{"type":"output_text","text":"network ok"}]}],
                "usage":{"input_tokens":3,"output_tokens":2}
            })
            .to_string(),
        )
        .await;
        let provider = OpenAiResponsesProvider::new(
            OpenAiConfig::new("sk-contract")
                .unwrap()
                .with_base_url(url)
                .unwrap(),
        )
        .unwrap();

        let response = provider.complete(request()).await.unwrap();
        let sent = captured_request.await.unwrap();
        server.await.unwrap();

        assert!(sent.starts_with("POST /v1/responses HTTP/1.1\r\n"));
        assert!(sent.contains("authorization: Bearer sk-contract\r\n"));
        assert!(sent.contains("\"model\":\"gpt-test\""));
        assert_eq!(response.content(), "network ok");
    }

    #[tokio::test]
    async fn embeddings_provider_reorders_indexed_response_and_redacts_input_debug() {
        let (url, captured_request, server) = response_server(
            "application/json",
            json!({
                "data":[
                    {"index":1,"embedding":[2.0,-1.0]},
                    {"index":0,"embedding":[0.25,0.5]}
                ]
            })
            .to_string(),
        )
        .await;
        let provider = OpenAiEmbeddingsProvider::new(
            OpenAiConfig::new("sk-contract")
                .unwrap()
                .with_base_url(url)
                .unwrap(),
        )
        .unwrap();

        let embeddings = provider
            .embed(
                "text-embedding-test".to_owned(),
                vec![
                    EmbeddingInput::new("chunk-1", "first text").unwrap(),
                    EmbeddingInput::new("chunk-2", "second text").unwrap(),
                ],
            )
            .await
            .unwrap();
        let sent = captured_request.await.unwrap();
        server.await.unwrap();

        assert!(sent.starts_with("POST /v1/embeddings HTTP/1.1\r\n"));
        assert!(sent.contains("authorization: Bearer sk-contract\r\n"));
        assert!(sent.contains("\"model\":\"text-embedding-test\""));
        assert!(sent.contains("\"input\":[\"first text\",\"second text\"]"));
        assert_eq!(embeddings[0].values(), &[0.25, 0.5]);
        assert_eq!(embeddings[1].values(), &[2.0, -1.0]);
    }

    #[test]
    fn embedding_response_rejects_duplicate_or_missing_indexes() {
        assert!(matches!(
            decode_embeddings(
                &json!({
                    "data":[
                        {"index":0,"embedding":[0.25]},
                        {"index":0,"embedding":[0.5]}
                    ]
                }),
                2,
            ),
            Err(OpenAiError::MalformedEmbeddingResponse)
        ));
    }

    #[tokio::test]
    async fn provider_normalizes_sse_text_and_completion_events() {
        let stream_body = concat!(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":2,\"output_tokens\":1}}}\n\n",
            "data: [DONE]\n\n",
        );
        let (url, _request, server) =
            response_server("text/event-stream", stream_body.to_owned()).await;
        let provider = OpenAiResponsesProvider::new(
            OpenAiConfig::new("sk-contract")
                .unwrap()
                .with_base_url(url)
                .unwrap(),
        )
        .unwrap();

        let events = provider
            .stream(request())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        server.await.unwrap();

        assert_eq!(
            events,
            vec![
                AiStreamEvent::TextDelta("hello".to_owned()),
                AiStreamEvent::Completed(rustee_ai::Usage {
                    input_tokens: 2,
                    output_tokens: 1,
                }),
            ]
        );
    }

    #[tokio::test]
    #[ignore = "requires RUSTEE_OPENAI_BATCH_LIVE=1, OPENAI_API_KEY, RUSTEE_OPENAI_BATCH_MODEL, and RUSTEE_OPENAI_BATCH_RUN_KEY; creates a billable provider Batch then requests cancellation"]
    async fn live_batch_lifecycle_is_explicitly_opt_in() {
        assert_eq!(
            std::env::var("RUSTEE_OPENAI_BATCH_LIVE").as_deref(),
            Ok("1"),
            "set RUSTEE_OPENAI_BATCH_LIVE=1 only after approving provider spend and artifact retention"
        );
        let api_key = std::env::var("OPENAI_API_KEY").expect("OPENAI_API_KEY is required");
        let model = std::env::var("RUSTEE_OPENAI_BATCH_MODEL")
            .expect("RUSTEE_OPENAI_BATCH_MODEL is required");
        let run_key = std::env::var("RUSTEE_OPENAI_BATCH_RUN_KEY")
            .expect("RUSTEE_OPENAI_BATCH_RUN_KEY is required");
        let provider = OpenAiBatchProvider::new(OpenAiConfig::new(api_key).unwrap()).unwrap();
        let mut builder = OpenAiBatchJsonlBuilder::new(OpenAiBatchEndpoint::Responses);
        builder
            .push(
                OpenAiBatchInputRow::new(
                    "rustee-live-batch-qualification-1",
                    OpenAiBatchEndpoint::Responses,
                    json!({
                        "model": model,
                        "input": "Reply with exactly: ok",
                        "max_output_tokens": 16,
                    }),
                )
                .unwrap(),
            )
            .unwrap();
        let input_file = provider
            .upload_batch_input(builder.build().unwrap())
            .await
            .unwrap();
        let reference =
            AiBatchReference::new("rustee-live", "openai-batch-qualification", run_key).unwrap();
        let receipt = provider
            .submit(
                reference,
                OpenAiBatchRequest::from_uploaded_input(
                    &input_file,
                    OpenAiBatchEndpoint::Responses,
                ),
            )
            .await
            .unwrap();
        let retrieved = provider.retrieve(&receipt).await.unwrap();
        assert_eq!(retrieved.receipt(), &receipt);
        let cancelled = provider.cancel(&receipt).await.unwrap();
        assert_eq!(cancelled.receipt(), &receipt);
    }

    async fn response_server(
        content_type: &'static str,
        body: String,
    ) -> (Url, oneshot::Receiver<String>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}/v1/", listener.local_addr().unwrap())).unwrap();
        let (request_sender, request) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut socket).await;
            request_sender.send(request).unwrap();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        (url, request, server)
    }

    async fn read_http_request(socket: &mut tokio::net::TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 1024];
        loop {
            let read = socket.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0);
            bytes.extend_from_slice(&chunk[..read]);
            let Some(headers_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n")
            else {
                continue;
            };
            let headers = std::str::from_utf8(&bytes[..headers_end]).unwrap();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.split_once(':')
                        .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                        .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            if bytes.len() >= headers_end + 4 + content_length {
                return String::from_utf8(bytes).unwrap();
            }
        }
    }
}
