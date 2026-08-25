//! Responses API execution, request assembly, and stream lifecycle.

use std::fmt;

use futures_util::StreamExt;
use reqwest::{Client, header::CONTENT_TYPE};
use rustee_ai::{
    AiEventStream, AiEventStreamFuture, AiProvider, AiStreamEvent, ChatRequest, ChatResponse,
    MessageRole,
};
use serde_json::{Value, json};

use crate::{
    OpenAiConfig, OpenAiError,
    response::{
        SseFrameBuffer, append_sse_chunk, decode_json_response, decode_response,
        decode_stream_event, encode_json_request, has_event_stream_content_type, sse_payload,
        take_sse_frame,
    },
};

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

    async fn send(
        &self,
        request: ChatRequest,
        stream: bool,
    ) -> Result<reqwest::Response, OpenAiError> {
        let mut body = request_body(&request, self.config.max_request_bytes)?;
        if stream {
            body["stream"] = Value::Bool(true);
        }
        let body = encode_json_request(&body, self.config.max_request_bytes)?;
        let endpoint = self
            .config
            .base_url
            .join("responses")
            .map_err(|_| OpenAiError::InvalidEndpoint)?;
        let response = self
            .client
            .post(endpoint)
            .timeout(self.config.request_timeout)
            .bearer_auth(&self.config.api_key)
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|_| OpenAiError::Transport)?;
        if !response.status().is_success() {
            return Err(OpenAiError::HttpStatus(response.status()));
        }
        Ok(response)
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
            let value = decode_json_response(
                response,
                provider.config.max_response_bytes,
                OpenAiError::MalformedResponse,
            )
            .await?;
            decode_response(&value)
        })
    }

    fn stream(&self, request: ChatRequest) -> AiEventStreamFuture<Self::Error> {
        let provider = self.clone();
        Box::pin(async move {
            let response = provider.send(request, true).await?;
            if !has_event_stream_content_type(response.headers()) {
                return Err(OpenAiError::UnexpectedContentType);
            }
            let max_sse_event_bytes = provider.config.max_sse_event_bytes;
            let stream = async_stream::try_stream! {
                let mut byte_stream = response.bytes_stream();
                let mut buffer = SseFrameBuffer::default();
                let mut completed = false;
                let mut done = false;

                while !done {
                    let Some(chunk) = byte_stream.next().await else {
                        break;
                    };
                    let chunk = chunk.map_err(|_| OpenAiError::Transport)?;
                    append_sse_chunk(&mut buffer, &chunk, max_sse_event_bytes)?;
                    while let Some(frame) = take_sse_frame(&mut buffer) {
                        let payload = sse_payload(&frame)?;
                        if payload == "[DONE]" {
                            done = true;
                            break;
                        }
                        if let Some(event) = decode_stream_event(&payload)? {
                            let terminal = matches!(event, AiStreamEvent::Completed(_));
                            completed |= terminal;
                            yield event;
                            if terminal {
                                done = true;
                                break;
                            }
                        }
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

/// Renders the typed Rustee request as a Responses API payload.
///
/// Function-call outputs are JSON text for the provider and are materialized within
/// `max_request_bytes` before they are inserted into the outer payload.
pub(crate) fn request_body(
    request: &ChatRequest,
    max_request_bytes: usize,
) -> Result<Value, OpenAiError> {
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
        let output = String::from_utf8(encode_json_request(result.content(), max_request_bytes)?)
            .map_err(|_| OpenAiError::RequestEncoding)?;
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
