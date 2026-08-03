//! HTTP streaming responses for Rustee AI events.
//!
//! The adapter turns a provider-neutral [`rustee_ai::AiEventStream`] into SSE or NDJSON while
//! keeping upstream error details out of the browser response. Dropping the response body drops
//! the upstream stream and therefore participates in normal transport cancellation.

use std::{convert::Infallible, error::Error as StdError};

use bytes::Bytes;
use futures_util::StreamExt;
use http::{
    HeaderValue, StatusCode,
    header::{CACHE_CONTROL, CONTENT_TYPE},
};
use rustee_ai::{AiEventStream, AiStreamEvent};
use rustee_core::{Response, response, stream_body};
use serde_json::{Value, json};

/// Creates a `text/event-stream` response from a provider-neutral AI stream.
///
/// Every event is encoded as one JSON SSE data frame. An upstream provider error becomes one
/// generic `ai_stream_failed` event; provider error text is never sent to the client.
#[must_use]
pub fn sse<E>(stream: AiEventStream<E>) -> Response
where
    E: StdError + Send + Sync + 'static,
{
    stream_response(stream, StreamFormat::Sse)
}

/// Creates an `application/x-ndjson` response from a provider-neutral AI stream.
///
/// Every event is encoded as one newline-delimited JSON object. Upstream provider error details
/// are normalized to a generic error object.
#[must_use]
pub fn ndjson<E>(stream: AiEventStream<E>) -> Response
where
    E: StdError + Send + Sync + 'static,
{
    stream_response(stream, StreamFormat::Ndjson)
}

#[derive(Clone, Copy)]
enum StreamFormat {
    Sse,
    Ndjson,
}

fn stream_response<E>(stream: AiEventStream<E>, format: StreamFormat) -> Response
where
    E: StdError + Send + Sync + 'static,
{
    let stream = stream.map(move |event| {
        let value = match event {
            Ok(event) => event_value(event),
            Err(_) => json!({"type":"error", "code":"ai_stream_failed"}),
        };
        Ok::<_, Infallible>(Bytes::from(encode(format, &value)))
    });
    let mut response = response(StatusCode::OK, stream_body(stream));
    let headers = response.headers_mut();
    match format {
        StreamFormat::Sse => {
            headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("text/event-stream; charset=utf-8"),
            );
            headers.insert(
                CACHE_CONTROL,
                HeaderValue::from_static("no-cache, no-transform"),
            );
            headers.insert("x-accel-buffering", HeaderValue::from_static("no"));
        }
        StreamFormat::Ndjson => {
            headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("application/x-ndjson; charset=utf-8"),
            );
            headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        }
    }
    response
}

fn event_value(event: AiStreamEvent) -> Value {
    match event {
        AiStreamEvent::TextDelta(delta) => json!({"type":"text_delta", "delta":delta}),
        AiStreamEvent::ToolCall(call) => json!({
            "type":"tool_call",
            "id":call.id(),
            "name":call.name(),
            "arguments":call.arguments(),
        }),
        AiStreamEvent::ToolResult(result) => json!({
            "type":"tool_result",
            "call_id":result.call_id(),
            "name":result.name(),
            "content":result.content(),
        }),
        AiStreamEvent::Completed(usage) => json!({
            "type":"completed",
            "usage":{
                "input_tokens":usage.input_tokens,
                "output_tokens":usage.output_tokens,
            },
        }),
    }
}

fn encode(format: StreamFormat, value: &Value) -> String {
    let value = serde_json::to_string(value).expect("AI web events are valid JSON values");
    match format {
        StreamFormat::Sse => format!("data: {value}\n\n"),
        StreamFormat::Ndjson => format!("{value}\n"),
    }
}

#[cfg(test)]
mod tests {
    use futures_util::stream;
    use http::{
        StatusCode,
        header::{CACHE_CONTROL, CONTENT_TYPE},
    };
    use http_body_util::BodyExt;
    use rustee_ai::{AiEventStream, AiStreamEvent, Usage};

    use super::{ndjson, sse};

    #[tokio::test]
    async fn sse_encodes_events_with_streaming_headers() {
        let stream: AiEventStream<TestError> = Box::pin(stream::iter(vec![
            Ok(AiStreamEvent::TextDelta("hello\nworld".to_owned())),
            Ok(AiStreamEvent::Completed(Usage {
                input_tokens: 3,
                output_tokens: 2,
            })),
        ]));

        let response = sse(stream);
        let (parts, body) = response.into_parts();
        let body = body.collect().await.unwrap().to_bytes();

        assert_eq!(parts.status, StatusCode::OK);
        assert_eq!(
            parts.headers[CONTENT_TYPE],
            "text/event-stream; charset=utf-8"
        );
        assert_eq!(parts.headers[CACHE_CONTROL], "no-cache, no-transform");
        let frames = std::str::from_utf8(&body)
            .unwrap()
            .split("\n\n")
            .filter_map(|frame| frame.strip_prefix("data: "))
            .map(serde_json::from_str::<serde_json::Value>)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            frames,
            vec![
                serde_json::json!({"type":"text_delta", "delta":"hello\nworld"}),
                serde_json::json!({
                    "type":"completed",
                    "usage":{"input_tokens":3,"output_tokens":2}
                }),
            ]
        );
    }

    #[tokio::test]
    async fn ndjson_redacts_upstream_errors() {
        let stream: AiEventStream<TestError> = Box::pin(stream::iter(vec![Err(TestError)]));

        let response = ndjson(stream);
        let (parts, body) = response.into_parts();
        let body = body.collect().await.unwrap().to_bytes();

        assert_eq!(
            parts.headers[CONTENT_TYPE],
            "application/x-ndjson; charset=utf-8"
        );
        assert_eq!(parts.headers[CACHE_CONTROL], "no-store");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({"type":"error", "code":"ai_stream_failed"})
        );
    }

    #[derive(Debug, thiserror::Error)]
    #[error("provider secret diagnostic")]
    struct TestError;
}
