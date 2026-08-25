//! `OpenAI` Responses API adapter for Rustee AI contracts.
//!
//! This crate sends only application-approved prompts, tool declarations, and tool results. It
//! keeps credentials and raw provider error bodies out of Rustee's public error surface.

use reqwest::StatusCode;

mod batch;
mod config;
mod provider;
mod response;

pub use batch::{
    OPENAI_BATCH_FILE_MAX_BYTES, OPENAI_BATCH_MAX_REQUESTS, OpenAiBatchEmbeddingsRowError,
    OpenAiBatchEndpoint, OpenAiBatchFileContent, OpenAiBatchFileDeletion, OpenAiBatchInputError,
    OpenAiBatchInputFile, OpenAiBatchInputJsonl, OpenAiBatchInputRow, OpenAiBatchJsonlBuilder,
    OpenAiBatchOutputExpiration, OpenAiBatchOutputExpirationError, OpenAiBatchOutputParseError,
    OpenAiBatchOutputRow, OpenAiBatchOutputRows, OpenAiBatchProvider, OpenAiBatchRequest,
    OpenAiBatchResponse, OpenAiBatchResponseBody, OpenAiBatchResponsesRowError,
    OpenAiBatchRowError, OpenAiBatchRowOutcome, OpenAiBatchSnapshot, OpenAiBatchStatus,
};
pub use config::{MAX_OPENAI_API_KEY_BYTES, OpenAiConfig, OpenAiConfigError};
pub use provider::{OpenAiEmbeddingsProvider, OpenAiResponsesProvider};

#[cfg(test)]
use batch::{decode_batch, decode_batch_input_file, valid_provider_path_identifier};

#[cfg(test)]
use provider::{decode_embeddings, embedding_request_body, request_body, validate_embedding_batch};

#[cfg(test)]
use response::{
    SseFrameBuffer, append_sse_chunk, decode_response, decode_stream_event, sse_payload,
    take_sse_frame,
};

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
    /// An outgoing JSON request exceeded the configured memory bound.
    #[error("OpenAI request exceeded the configured size limit")]
    RequestTooLarge,
    /// An outgoing JSON request could not be encoded.
    #[error("OpenAI request could not be encoded")]
    RequestEncoding,
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
    /// An embeddings request must contain at least one provider-bound input.
    #[error("OpenAI embedding batch must not be empty")]
    EmptyEmbeddingBatch,
    /// An embeddings request exceeded its configured input-count limit.
    #[error("OpenAI embedding batch exceeded the configured input limit")]
    EmbeddingBatchInputLimit,
    /// An embeddings request exceeded its configured combined content-byte limit.
    #[error("OpenAI embedding batch exceeded the configured content byte limit")]
    EmbeddingBatchContentLimit,
    /// Provider JSON did not satisfy the Batch API lifecycle fields required by Rustee.
    #[error("OpenAI batch response did not match the expected API shape")]
    MalformedBatch,
    /// Provider JSON did not satisfy the Batch-purpose file fields required by Rustee.
    #[error("OpenAI Batch file response did not match the expected API shape")]
    MalformedBatchFile,
    /// A non-streaming provider response exceeded the configured memory bound.
    #[error("OpenAI response exceeded the configured size limit")]
    ResponseTooLarge,
    /// A successful provider response did not declare the media type required by its API path.
    #[error("OpenAI response content type was unexpected")]
    UnexpectedContentType,
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

#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub fn fuzz_parse_sse_input(input: &[u8]) {
    let mut buffer = response::SseFrameBuffer::default();
    if response::append_sse_chunk(&mut buffer, input, 1024 * 1024).is_err() {
        return;
    }

    while let Some(frame) = response::take_sse_frame(&mut buffer) {
        let Ok(payload) = response::sse_payload(&frame) else {
            continue;
        };
        if payload != "[DONE]" {
            let _ = response::decode_stream_event(&payload);
        }
    }
}

#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub fn fuzz_parse_batch_output_jsonl(input: &[u8]) {
    let content = batch::OpenAiBatchFileContent::from_download(input.to_vec());
    for _ in content.output_rows() {}
}

#[cfg(test)]
mod tests;
