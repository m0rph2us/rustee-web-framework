//! Content-free records and failures for the deterministic AI provider fake.

use rustee_ai::ChatRequest;

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
    pub(crate) fn from_request(operation: RecordedAiOperation, request: &ChatRequest) -> Self {
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
