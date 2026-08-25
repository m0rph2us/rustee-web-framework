//! Provider and advisor contracts for normalized AI completions and streams.

use std::{error::Error as StdError, fmt};

use futures_util::{future::BoxFuture, stream::BoxStream};

use crate::{ChatRequest, ChatResponse, ToolCall, ToolResult, Usage};

/// Provider-neutral streaming events.
#[derive(Clone, Eq, PartialEq)]
pub enum AiStreamEvent {
    /// Text fragment.
    TextDelta(String),
    /// A requested but unexecuted tool call.
    ToolCall(ToolCall),
    /// An application-approved tool result that may be returned to a provider in a later request.
    ToolResult(ToolResult),
    /// Terminal stream completion and usage accounting.
    ///
    /// Provider-normalized streams must not emit later events. The pipeline also stops polling
    /// after this event as a defensive boundary.
    Completed(Usage),
}

impl fmt::Debug for AiStreamEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TextDelta(delta) => formatter
                .debug_struct("AiStreamEvent::TextDelta")
                .field("byte_length", &delta.len())
                .finish(),
            Self::ToolCall(call) => formatter
                .debug_tuple("AiStreamEvent::ToolCall")
                .field(call)
                .finish(),
            Self::ToolResult(result) => formatter
                .debug_tuple("AiStreamEvent::ToolResult")
                .field(result)
                .finish(),
            Self::Completed(usage) => formatter
                .debug_tuple("AiStreamEvent::Completed")
                .field(usage)
                .finish(),
        }
    }
}

/// A provider-normalized stream of AI events.
pub type AiEventStream<E> = BoxStream<'static, Result<AiStreamEvent, E>>;

/// Future returned while a provider opens an [`AiEventStream`].
pub type AiEventStreamFuture<E> = BoxFuture<'static, Result<AiEventStream<E>, E>>;

/// Provider-neutral AI client contract.
pub trait AiProvider: Clone + Send + Sync + 'static {
    /// Provider-specific failure.
    type Error: StdError + Send + Sync + 'static;

    /// Performs one non-streaming completion.
    fn complete(
        &self,
        request: ChatRequest,
    ) -> BoxFuture<'static, Result<ChatResponse, Self::Error>>;

    /// Opens a normalized text stream.
    ///
    /// A successful terminal stream ends with [`AiStreamEvent::Completed`]. The pipeline treats a
    /// provider error as terminal too and does not poll for any subsequent events.
    fn stream(&self, request: ChatRequest) -> AiEventStreamFuture<Self::Error>;
}

/// Ordered application hook around one AI request, response, and stream event.
///
/// Advisors receive owned values so they can add authorized context or redact application-owned
/// output before it reaches the next stage. They must not log prompt, completion, or tool content
/// by default. The pipeline applies its request policy after [`AiAdvisor::before_request`] and
/// before provider invocation.
pub trait AiAdvisor: Clone + Send + Sync + 'static {
    /// Error returned by this application's advisor implementation.
    type Error: StdError + Send + Sync + 'static;

    /// Adds or validates application context before the provider sees a request.
    fn before_request(
        &self,
        request: ChatRequest,
    ) -> BoxFuture<'static, Result<ChatRequest, Self::Error>>;

    /// Transforms an application-visible non-streaming response after provider completion.
    fn after_response(
        &self,
        response: ChatResponse,
    ) -> BoxFuture<'static, Result<ChatResponse, Self::Error>>;

    /// Transforms one application-visible stream event after the provider emits it.
    fn on_stream_event(
        &self,
        event: AiStreamEvent,
    ) -> BoxFuture<'static, Result<AiStreamEvent, Self::Error>>;
}

/// Advisor that passes every request, response, and stream event through unchanged.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopAiAdvisor;

impl AiAdvisor for NoopAiAdvisor {
    type Error = std::convert::Infallible;

    fn before_request(
        &self,
        request: ChatRequest,
    ) -> BoxFuture<'static, Result<ChatRequest, Self::Error>> {
        Box::pin(futures_util::future::ready(Ok(request)))
    }

    fn after_response(
        &self,
        response: ChatResponse,
    ) -> BoxFuture<'static, Result<ChatResponse, Self::Error>> {
        Box::pin(futures_util::future::ready(Ok(response)))
    }

    fn on_stream_event(
        &self,
        event: AiStreamEvent,
    ) -> BoxFuture<'static, Result<AiStreamEvent, Self::Error>> {
        Box::pin(futures_util::future::ready(Ok(event)))
    }
}
