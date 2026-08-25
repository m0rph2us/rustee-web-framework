//! `OpenAI` Responses SSE framing and stream-lifecycle regression coverage.

use super::*;

#[test]
fn stream_frames_support_crlf_and_function_call_events() {
    let mut buffer = SseFrameBuffer::default();
    append_sse_chunk(
        &mut buffer,
        b"event: response.function_call_arguments.done\r\ndata: {\"type\":\"response.function_call_arguments.done\",\"call_id\":\"call_1\",\"name\":\"lookup_order\",\"arguments\":\"{\\\"id\\\":7}\"}\r\n\r\n",
        1024,
    )
    .unwrap();
    let frame = take_sse_frame(&mut buffer).unwrap();
    let event = decode_stream_event(&sse_payload(&frame).unwrap())
        .unwrap()
        .unwrap();

    assert!(matches!(event, AiStreamEvent::ToolCall(_)));
    assert!(buffer.is_empty());
}

#[test]
fn sse_chunk_limit_is_checked_before_the_buffer_grows() {
    let mut buffer = SseFrameBuffer::default();
    append_sse_chunk(&mut buffer, b"data: ", 8).unwrap();

    assert!(matches!(
        append_sse_chunk(&mut buffer, b"oversized", 8),
        Err(OpenAiError::StreamEventTooLarge)
    ));
    assert!(!buffer.is_empty());
}

#[test]
fn sse_buffer_drains_many_frames_without_losing_boundaries() {
    let frame = b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n";
    let mut chunk = Vec::with_capacity(frame.len() * 512);
    for _ in 0..512 {
        chunk.extend_from_slice(frame);
    }

    let mut buffer = SseFrameBuffer::default();
    append_sse_chunk(&mut buffer, &chunk, chunk.len()).unwrap();

    let mut frames = 0;
    while let Some(frame) = take_sse_frame(&mut buffer) {
        assert!(decode_stream_event(&sse_payload(&frame).unwrap()).is_ok());
        frames += 1;
    }

    assert_eq!(frames, 512);
    assert!(buffer.is_empty());
}

#[tokio::test]
async fn provider_normalizes_sse_text_and_completion_events() {
    let stream_body = concat!(
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":2,\"output_tokens\":1}}}\n\n",
        "data: malformed tail that must not be parsed\n\n",
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
async fn provider_normalizes_cr_terminated_sse_events() {
    let stream_body = concat!(
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\r\r",
        "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":2,\"output_tokens\":1}}}\r\r",
        "data: [DONE]\r\r",
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
async fn responses_provider_rejects_an_unexpected_stream_content_type() {
    let (url, _request, server) = response_server("application/json", "{}".to_owned()).await;
    let provider = OpenAiResponsesProvider::new(
        OpenAiConfig::new("sk-contract")
            .unwrap()
            .with_base_url(url)
            .unwrap(),
    )
    .unwrap();

    assert!(matches!(
        provider.stream(request()).await,
        Err(OpenAiError::UnexpectedContentType)
    ));
    server.await.unwrap();
}
