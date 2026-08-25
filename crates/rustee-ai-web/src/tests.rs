use futures_util::stream;
use http::{
    StatusCode,
    header::{CACHE_CONTROL, CONTENT_TYPE},
};
use http_body_util::BodyExt;
use rustee_ai::{AiEventStream, AiStreamEvent, ToolCall, ToolResult, Usage};
use serde_json::json;
use tokio::sync::oneshot;

use super::{AiStreamResponseConfig, AiStreamResponseConfigError, ndjson, sse, sse_with_config};

struct DropNotifier(Option<oneshot::Sender<()>>);

impl Drop for DropNotifier {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

fn sse_frames(body: &[u8]) -> Vec<serde_json::Value> {
    std::str::from_utf8(body)
        .unwrap()
        .split("\n\n")
        .filter_map(|frame| frame.strip_prefix("data: "))
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()
        .unwrap()
}

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
    let frames = sse_frames(&body);
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
async fn sse_encodes_tool_calls_and_results() {
    let result = serde_json::from_value::<ToolResult>(json!({
        "call_id": "call-7",
        "name": "orders.lookup",
        "content": {"status": "ready"}
    }))
    .unwrap();
    let stream: AiEventStream<TestError> = Box::pin(stream::iter(vec![
        Ok(AiStreamEvent::ToolCall(
            ToolCall::new("call-7", "orders.lookup", json!({"order_id": 42})).unwrap(),
        )),
        Ok(AiStreamEvent::ToolResult(result)),
    ]));

    let (_, body) = sse(stream).into_parts();
    let body = body.collect().await.unwrap().to_bytes();

    assert_eq!(
        sse_frames(&body),
        vec![
            json!({
                "type": "tool_call",
                "id": "call-7",
                "name": "orders.lookup",
                "arguments": {"order_id": 42}
            }),
            json!({
                "type": "tool_result",
                "call_id": "call-7",
                "name": "orders.lookup",
                "content": {"status": "ready"}
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

#[tokio::test]
async fn ndjson_encodes_successful_events_without_sse_framing() {
    let stream: AiEventStream<TestError> = Box::pin(stream::iter(vec![Ok(
        AiStreamEvent::TextDelta("ready".to_owned()),
    )]));

    let (_, body) = ndjson(stream).into_parts();
    let body = body.collect().await.unwrap().to_bytes();

    assert_eq!(
        &body[..],
        b"{\"type\":\"text_delta\",\"delta\":\"ready\"}\n"
    );
}

#[test]
fn stream_response_config_reserves_room_for_terminal_errors() {
    assert_eq!(
        AiStreamResponseConfig::default()
            .with_max_frame_bytes(127)
            .unwrap_err(),
        AiStreamResponseConfigError::FrameLimitTooSmall
    );
    assert_eq!(
        AiStreamResponseConfig::default()
            .with_max_frame_bytes(128)
            .unwrap()
            .max_frame_bytes(),
        128
    );
}

#[tokio::test]
async fn oversized_sse_event_emits_one_terminal_error_and_stops_the_stream() {
    let (dropped_sender, dropped) = oneshot::channel();
    let stream: AiEventStream<TestError> = Box::pin(async_stream::stream! {
        let _notifier = DropNotifier(Some(dropped_sender));
        yield Ok(AiStreamEvent::TextDelta("x".repeat(128)));
        std::future::pending::<()>().await;
    });
    let config = AiStreamResponseConfig::default()
        .with_max_frame_bytes(128)
        .unwrap();

    let response = sse_with_config(stream, config);
    let (_, body) = response.into_parts();
    let body = body.collect().await.unwrap().to_bytes();

    assert_eq!(
        &body[..],
        b"data: {\"type\":\"error\",\"code\":\"ai_stream_event_too_large\"}\n\n"
    );
    dropped
        .await
        .expect("the terminal response frame must drop the upstream stream");
}

#[tokio::test]
async fn completed_sse_event_stops_the_upstream_stream() {
    let (dropped_sender, dropped) = oneshot::channel();
    let stream: AiEventStream<TestError> = Box::pin(async_stream::stream! {
        let _notifier = DropNotifier(Some(dropped_sender));
        yield Ok(AiStreamEvent::Completed(Usage {
            input_tokens: 3,
            output_tokens: 2,
        }));
        std::future::pending::<()>().await;
    });

    let (_, body) = sse(stream).into_parts();
    let body = body.collect().await.unwrap().to_bytes();

    assert_eq!(
        sse_frames(&body),
        vec![json!({
            "type":"completed",
            "usage":{"input_tokens":3,"output_tokens":2}
        })]
    );
    dropped
        .await
        .expect("the completed response frame must drop the upstream stream");
}

#[derive(Debug, thiserror::Error)]
#[error("provider secret diagnostic")]
struct TestError;
