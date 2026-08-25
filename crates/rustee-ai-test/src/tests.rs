use futures_util::StreamExt;
use rustee_ai::{
    AiProvider, AiStreamEvent, ChatMessage, ChatRequest, ChatResponse, MessageRole, Usage,
};

use crate::{RecordedAiError, RecordedAiOperation, RecordedAiProvider};

fn request() -> ChatRequest {
    ChatRequest::new(
        "support.default",
        [ChatMessage::new(MessageRole::User, "private customer question").unwrap()],
    )
    .unwrap()
}

fn response() -> ChatResponse {
    ChatResponse::new(
        "response-1",
        "fake-model",
        "private completion",
        [],
        Usage::default(),
    )
    .unwrap()
}

#[tokio::test]
async fn completion_uses_a_fifo_script_and_records_only_safe_metadata() {
    let provider = RecordedAiProvider::new();
    provider.queue_completion(response());

    assert_eq!(
        provider.complete(request()).await.unwrap().id(),
        "response-1"
    );
    assert_eq!(
        provider.complete(request()).await.unwrap_err(),
        RecordedAiError::NoQueuedCompletion
    );

    let records = provider.recorded_requests();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].operation(), RecordedAiOperation::Complete);
    assert_eq!(records[0].model(), "support.default");
    assert_eq!(records[0].message_count(), 1);
    assert!(!format!("{records:?}").contains("private customer question"));
}

#[tokio::test]
async fn stream_replays_queued_events_and_errors_in_order() {
    let provider = RecordedAiProvider::new();
    provider.queue_stream([
        Ok(AiStreamEvent::TextDelta("first".to_owned())),
        Err(RecordedAiError::Unavailable),
    ]);

    let events = provider
        .stream(request())
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;

    assert_eq!(events.len(), 2);
    assert_eq!(
        events[0].as_ref().unwrap(),
        &AiStreamEvent::TextDelta("first".to_owned())
    );
    assert_eq!(
        events[1].as_ref().unwrap_err(),
        &RecordedAiError::Unavailable
    );
    assert_eq!(
        provider.recorded_requests()[0].operation(),
        RecordedAiOperation::Stream
    );
}

#[tokio::test]
async fn stream_open_failures_are_explicit_and_deterministic() {
    let provider = RecordedAiProvider::new();
    provider.queue_stream_failure(RecordedAiError::Unavailable);

    let Err(error) = provider.stream(request()).await else {
        panic!("queued stream failure must not open a stream");
    };
    assert_eq!(error, RecordedAiError::Unavailable);
}
