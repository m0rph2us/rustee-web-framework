use std::time::Duration;

use futures_util::TryStreamExt;
use rustee_ai::{AiProvider, AiStreamEvent, ChatMessage, ChatRequest, MessageRole};
use rustee_ai_batch::{AiBatchProvider, AiBatchReceipt, AiBatchReference};
use rustee_ai_rag::{EmbeddingBatchLimits, EmbeddingInput, EmbeddingProvider};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::oneshot};
use url::Url;

use super::{
    OPENAI_BATCH_FILE_MAX_BYTES, OPENAI_BATCH_MAX_REQUESTS, OpenAiBatchEmbeddingsRowError,
    OpenAiBatchEndpoint, OpenAiBatchFileContent, OpenAiBatchInputError, OpenAiBatchInputJsonl,
    OpenAiBatchInputRow, OpenAiBatchJsonlBuilder, OpenAiBatchOutputExpiration,
    OpenAiBatchOutputExpirationError, OpenAiBatchProvider, OpenAiBatchRequest,
    OpenAiBatchResponseBody, OpenAiBatchResponsesRowError, OpenAiBatchRowOutcome,
    OpenAiBatchStatus, OpenAiConfig, OpenAiConfigError, OpenAiEmbeddingsProvider, OpenAiError,
    OpenAiResponsesProvider, SseFrameBuffer, append_sse_chunk, decode_batch,
    decode_batch_input_file, decode_embeddings, decode_response, decode_stream_event,
    embedding_request_body, request_body, sse_payload, take_sse_frame,
    valid_provider_path_identifier, validate_embedding_batch,
};

mod batch;
mod configuration;
mod embeddings;
mod responses;
mod sse;
mod support;

use support::{declared_length_response_server, read_http_request, request, response_server};
