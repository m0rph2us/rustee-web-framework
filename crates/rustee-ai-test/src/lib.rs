//! Deterministic provider fakes for application-level AI tests.
//!
//! [`RecordedAiProvider`] returns only responses and stream events explicitly queued by a test. It
//! records sanitized invocation metadata, never prompt, completion, tool arguments, or tool result
//! content. Real provider protocol behavior remains the responsibility of provider adapter tests.

use std::{
    collections::VecDeque,
    fmt,
    sync::{Arc, Mutex, MutexGuard},
};

use futures_util::{future, stream};
use rustee_ai::{
    AiEventStream, AiEventStreamFuture, AiProvider, AiStreamEvent, ChatRequest, ChatResponse,
};

type RecordedStream = Vec<Result<AiStreamEvent, RecordedAiError>>;
type QueuedStream = Result<RecordedStream, RecordedAiError>;

/// Operation for one sanitized provider invocation record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordedAiOperation {
    /// A non-streaming completion call.
    Complete,
    /// A streaming provider call.
    Stream,
}

/// Content-free metadata recorded for one fake-provider invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordedAiRequest {
    operation: RecordedAiOperation,
    model: String,
    message_count: usize,
    tool_count: usize,
    tool_result_count: usize,
}

impl RecordedAiRequest {
    fn from_request(operation: RecordedAiOperation, request: &ChatRequest) -> Self {
        Self {
            operation,
            model: request.model().to_owned(),
            message_count: request.messages().len(),
            tool_count: request.tools().len(),
            tool_result_count: request.tool_results().len(),
        }
    }

    /// Returns the requested provider operation.
    #[must_use]
    pub const fn operation(&self) -> RecordedAiOperation {
        self.operation
    }

    /// Returns the deployment-owned model alias.
    #[must_use]
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Returns the number of request messages without retaining their content.
    #[must_use]
    pub const fn message_count(&self) -> usize {
        self.message_count
    }

    /// Returns the number of tool declarations without retaining schemas.
    #[must_use]
    pub const fn tool_count(&self) -> usize {
        self.tool_count
    }

    /// Returns the number of approved tool results without retaining their content.
    #[must_use]
    pub const fn tool_result_count(&self) -> usize {
        self.tool_result_count
    }
}

/// Deterministic fake-provider failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RecordedAiError {
    /// A test invoked completion without queuing a response or failure.
    #[error("no completion result was queued for the recorded AI provider")]
    NoQueuedCompletion,
    /// A test invoked streaming without queuing events or an open failure.
    #[error("no stream result was queued for the recorded AI provider")]
    NoQueuedStream,
    /// A test deliberately injected a normalized provider failure.
    #[error("recorded AI provider is unavailable")]
    Unavailable,
}

#[derive(Default)]
struct RecordedAiState {
    completions: VecDeque<Result<ChatResponse, RecordedAiError>>,
    streams: VecDeque<QueuedStream>,
    requests: Vec<RecordedAiRequest>,
}

/// Cloneable provider fake with FIFO completion and stream scripts.
#[derive(Clone, Default)]
pub struct RecordedAiProvider {
    state: Arc<Mutex<RecordedAiState>>,
}

impl RecordedAiProvider {
    /// Creates a fake provider without any queued behavior.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Queues the next completed response.
    pub fn queue_completion(&self, response: ChatResponse) {
        self.state().completions.push_back(Ok(response));
    }

    /// Queues the next completion failure.
    pub fn queue_completion_failure(&self, error: RecordedAiError) {
        self.state().completions.push_back(Err(error));
    }

    /// Queues the complete event sequence for the next opened stream.
    pub fn queue_stream(
        &self,
        events: impl IntoIterator<Item = Result<AiStreamEvent, RecordedAiError>>,
    ) {
        self.state()
            .streams
            .push_back(Ok(events.into_iter().collect()));
    }

    /// Queues an error returned while opening the next stream.
    pub fn queue_stream_failure(&self, error: RecordedAiError) {
        self.state().streams.push_back(Err(error));
    }

    /// Returns invocation metadata in call order without prompt or result content.
    #[must_use]
    pub fn recorded_requests(&self) -> Vec<RecordedAiRequest> {
        self.state().requests.clone()
    }

    fn state(&self) -> MutexGuard<'_, RecordedAiState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl fmt::Debug for RecordedAiProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state();
        formatter
            .debug_struct("RecordedAiProvider")
            .field("queued_completions", &state.completions.len())
            .field("queued_streams", &state.streams.len())
            .field("recorded_requests", &state.requests.len())
            .finish()
    }
}

impl AiProvider for RecordedAiProvider {
    type Error = RecordedAiError;

    fn complete(
        &self,
        request: ChatRequest,
    ) -> futures_util::future::BoxFuture<'static, Result<ChatResponse, Self::Error>> {
        let result = {
            let mut state = self.state();
            state.requests.push(RecordedAiRequest::from_request(
                RecordedAiOperation::Complete,
                &request,
            ));
            state
                .completions
                .pop_front()
                .unwrap_or(Err(RecordedAiError::NoQueuedCompletion))
        };
        Box::pin(future::ready(result))
    }

    fn stream(&self, request: ChatRequest) -> AiEventStreamFuture<Self::Error> {
        let result = {
            let mut state = self.state();
            state.requests.push(RecordedAiRequest::from_request(
                RecordedAiOperation::Stream,
                &request,
            ));
            state
                .streams
                .pop_front()
                .unwrap_or(Err(RecordedAiError::NoQueuedStream))
        };
        match result {
            Ok(events) => {
                let stream: AiEventStream<Self::Error> = Box::pin(stream::iter(events));
                Box::pin(future::ready(Ok(stream)))
            }
            Err(error) => Box::pin(future::ready(Err(error))),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;
    use rustee_ai::{
        AiProvider, AiStreamEvent, ChatMessage, ChatRequest, ChatResponse, MessageRole, Usage,
    };

    use super::{RecordedAiError, RecordedAiOperation, RecordedAiProvider};

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
}
