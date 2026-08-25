//! Bounded HTTP-body handling and completed `OpenAI` Responses API decoding.

use futures_util::StreamExt;
use rustee_ai::{ChatResponse, ToolCall, Usage};
use rustee_core::is_standard_json_media_type;
use rustee_json::{BoundedJsonError, to_vec_bounded};
use serde::Serialize;
use serde_json::Value;

use crate::OpenAiError;

mod sse;

pub(super) use sse::{
    SseFrameBuffer, append_sse_chunk, decode_stream_event, sse_payload, take_sse_frame,
};

pub(super) fn encode_json_request<T>(
    value: &T,
    max_request_bytes: usize,
) -> Result<Vec<u8>, OpenAiError>
where
    T: Serialize + ?Sized,
{
    match to_vec_bounded(value, max_request_bytes) {
        Ok(body) => Ok(body),
        Err(BoundedJsonError::TooLarge) => Err(OpenAiError::RequestTooLarge),
        Err(BoundedJsonError::Serialize(_)) => Err(OpenAiError::RequestEncoding),
    }
}

pub(super) async fn collect_response_body(
    response: reqwest::Response,
    max_response_bytes: usize,
) -> Result<Vec<u8>, OpenAiError> {
    if response
        .content_length()
        .is_some_and(|length| length > max_response_bytes as u64)
    {
        return Err(OpenAiError::ResponseTooLarge);
    }

    let mut body = Vec::new();
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|_| OpenAiError::Transport)?;
        if chunk.len() > max_response_bytes.saturating_sub(body.len()) {
            return Err(OpenAiError::ResponseTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

pub(super) async fn decode_json_response(
    response: reqwest::Response,
    max_response_bytes: usize,
    malformed_response: OpenAiError,
) -> Result<Value, OpenAiError> {
    if !has_json_content_type(response.headers()) {
        return Err(OpenAiError::UnexpectedContentType);
    }
    let body = collect_response_body(response, max_response_bytes).await?;
    serde_json::from_slice(&body).map_err(|_| malformed_response)
}

pub(super) fn has_event_stream_content_type(headers: &reqwest::header::HeaderMap) -> bool {
    let Some((kind, subtype)) = content_type_parts(headers) else {
        return false;
    };
    kind.eq_ignore_ascii_case("text") && subtype.eq_ignore_ascii_case("event-stream")
}

fn has_json_content_type(headers: &reqwest::header::HeaderMap) -> bool {
    single_content_type(headers).is_some_and(is_standard_json_media_type)
}

fn content_type_parts(headers: &reqwest::header::HeaderMap) -> Option<(&str, &str)> {
    let value = single_content_type(headers)?;
    let value = value.split(';').next()?.trim();
    value.split_once('/')
}

fn single_content_type(headers: &reqwest::header::HeaderMap) -> Option<&str> {
    let mut values = headers.get_all(reqwest::header::CONTENT_TYPE).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    value.to_str().ok()
}

pub(super) fn decode_response(value: &Value) -> Result<ChatResponse, OpenAiError> {
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

#[cfg(test)]
mod tests {
    use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};

    use super::{has_event_stream_content_type, has_json_content_type};

    #[test]
    fn response_content_types_require_one_expected_media_type() {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/problem+json; charset=utf-8"),
        );
        assert!(has_json_content_type(&headers));
        assert!(!has_event_stream_content_type(&headers));

        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream; charset=utf-8"),
        );
        assert!(!has_json_content_type(&headers));
        assert!(has_event_stream_content_type(&headers));

        headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        assert!(!has_json_content_type(&headers));
        assert!(!has_event_stream_content_type(&headers));

        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/+json"));
        assert!(!has_json_content_type(&headers));

        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.append(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        assert!(!has_json_content_type(&headers));
        assert!(!has_event_stream_content_type(&headers));
    }
}
