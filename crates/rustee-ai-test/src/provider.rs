//! FIFO-scripted provider behavior for application-level AI tests.

use std::{
    collections::VecDeque,
    fmt,
    sync::{Arc, Mutex, MutexGuard},
};

use futures_util::{future, stream};
use rustee_ai::{
    AiEventStream, AiEventStreamFuture, AiProvider, AiStreamEvent, ChatRequest, ChatResponse,
};

use super::model::{RecordedAiError, RecordedAiOperation, RecordedAiRequest};

type RecordedStream = Vec<Result<AiStreamEvent, RecordedAiError>>;
type QueuedStream = Result<RecordedStream, RecordedAiError>;

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
