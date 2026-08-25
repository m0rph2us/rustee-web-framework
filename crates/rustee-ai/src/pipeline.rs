//! AI request policy, provider execution pipelines, usage settlement, and telemetry.

use std::{fmt, time::Instant};

use futures_util::StreamExt;
use tracing::Instrument;

mod error;
mod stream;
mod telemetry;
mod usage;

pub use error::{
    AdvisedPipelineError, AdvisedStreamError, AdvisedUsageLedgerPipelineError,
    AdvisedUsageLedgerStreamError, BudgetAdvisorError, PipelineError, PolicyError,
    UsageLedgerPipelineError, UsageLedgerStreamError,
};

pub(crate) use telemetry::record_outcome;
use telemetry::{
    ai_operation_span, observe_stream, record_advised_pipeline_error,
    record_advised_pipeline_result, record_pipeline_error, record_pipeline_result,
    record_request_metadata,
};

use super::{AiAdvisor, AiEventStream, AiProvider, ChatRequest, ChatResponse};

/// Cost and safety bounds applied before a provider receives a request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AiPolicy {
    /// Maximum total Unicode scalar count in provider-bound message and tool-result content.
    pub max_input_characters: usize,
    /// Maximum declared tools.
    pub max_tools: usize,
    /// Maximum application-approved tool results returned to a provider.
    pub max_tool_results: usize,
}

impl Default for AiPolicy {
    fn default() -> Self {
        Self {
            max_input_characters: 100_000,
            max_tools: 16,
            max_tool_results: 16,
        }
    }
}

impl AiPolicy {
    /// Validates a request before the provider call.
    ///
    /// Cardinality limits are checked before provider-bound content is counted, so a request
    /// rejected for too many tools or tool results does not serialize those result values.
    ///
    /// # Errors
    ///
    /// Returns [`PolicyError`] when configured bounds are exceeded.
    pub fn validate(self, request: &ChatRequest) -> Result<(), PolicyError> {
        self.validate_and_measure_input(request)?;
        Ok(())
    }

    fn validate_and_measure_input(self, request: &ChatRequest) -> Result<usize, PolicyError> {
        let tool_count = request.tools().len();
        if tool_count > self.max_tools {
            return Err(PolicyError::TooManyTools {
                limit: self.max_tools,
                actual: tool_count,
            });
        }
        let tool_result_count = request.tool_results().len();
        if tool_result_count > self.max_tool_results {
            return Err(PolicyError::TooManyToolResults {
                limit: self.max_tool_results,
                actual: tool_result_count,
            });
        }
        let actual = request.input_characters();
        if actual > self.max_input_characters {
            return Err(PolicyError::InputTooLarge {
                limit: self.max_input_characters,
                actual,
            });
        }
        Ok(actual)
    }
}

/// Pipeline that enforces policy before one provider call.
///
/// Debug output retains the provider type and policy without invoking provider diagnostics.
#[derive(Clone)]
pub struct AiPipeline<P> {
    provider: P,
    policy: AiPolicy,
}

impl<P> fmt::Debug for AiPipeline<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiPipeline")
            .field("provider_type", &std::any::type_name::<P>())
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl<P> AiPipeline<P> {
    /// Creates a pipeline with default bounds.
    #[must_use]
    pub fn new(provider: P) -> Self {
        Self {
            provider,
            policy: AiPolicy::default(),
        }
    }

    /// Replaces pre-provider request bounds.
    #[must_use]
    pub fn with_policy(mut self, policy: AiPolicy) -> Self {
        self.policy = policy;
        self
    }
}

impl<P: AiProvider> AiPipeline<P> {
    /// Validates and completes a chat request; tool calls remain unexecuted.
    ///
    /// # Errors
    ///
    /// Returns policy or provider failure.
    pub async fn complete(
        &self,
        request: ChatRequest,
    ) -> Result<ChatResponse, PipelineError<P::Error>> {
        let span = ai_operation_span("complete", &request);
        let started_at = Instant::now();
        let request_span = span.clone();
        let result = async {
            let input_characters = self
                .policy
                .validate_and_measure_input(&request)
                .map_err(PipelineError::Policy)?;
            record_request_metadata(&request_span, &request, input_characters);
            self.provider
                .complete(request)
                .await
                .map_err(PipelineError::Provider)
        }
        .instrument(span.clone())
        .await;
        record_pipeline_result(&span, started_at, &result);
        result
    }

    /// Validates and opens an AI text stream.
    ///
    /// # Errors
    ///
    /// Returns policy or provider failure before stream creation.
    pub async fn stream(
        &self,
        request: ChatRequest,
    ) -> Result<AiEventStream<P::Error>, PipelineError<P::Error>> {
        let span = ai_operation_span("stream", &request);
        let started_at = Instant::now();
        let request_span = span.clone();
        let result = async {
            let input_characters = self
                .policy
                .validate_and_measure_input(&request)
                .map_err(PipelineError::Policy)?;
            record_request_metadata(&request_span, &request, input_characters);
            self.provider
                .stream(request)
                .await
                .map_err(PipelineError::Provider)
        }
        .instrument(span.clone())
        .await;
        match result {
            Ok(stream) => {
                record_outcome(&span, started_at, "opened", false);
                Ok(observe_stream(
                    span,
                    started_at,
                    stream::stop_after_terminal_event(stream),
                ))
            }
            Err(error) => {
                record_pipeline_error(&span, started_at, &error);
                Err(error)
            }
        }
    }

    /// Runs an advisor around a non-streaming provider completion.
    ///
    /// # Errors
    ///
    /// Returns an advisor, request-policy, or provider failure. A provider is never called when
    /// the advisor or final request-policy validation fails.
    pub async fn complete_with_advisor<A>(
        &self,
        request: ChatRequest,
        advisor: &A,
    ) -> Result<ChatResponse, AdvisedPipelineError<P::Error, A::Error>>
    where
        A: AiAdvisor,
    {
        let span = ai_operation_span("complete", &request);
        let started_at = Instant::now();
        let request_span = span.clone();
        let result = async {
            let request = advisor
                .before_request(request)
                .await
                .map_err(AdvisedPipelineError::Advisor)?;
            let input_characters = self
                .policy
                .validate_and_measure_input(&request)
                .map_err(AdvisedPipelineError::Policy)?;
            record_request_metadata(&request_span, &request, input_characters);
            let response = self
                .provider
                .complete(request)
                .await
                .map_err(AdvisedPipelineError::Provider)?;
            advisor
                .after_response(response)
                .await
                .map_err(AdvisedPipelineError::Advisor)
        }
        .instrument(span.clone())
        .await;
        record_advised_pipeline_result(&span, started_at, &result);
        result
    }

    /// Runs an advisor around a provider stream.
    ///
    /// The advisor validates or enriches the request before the stream opens, then receives every
    /// application-visible event. Provider stream failures and advisor event failures remain
    /// distinct in the returned stream.
    ///
    /// # Errors
    ///
    /// Returns an advisor, request-policy, or provider-open failure before a stream is returned.
    pub async fn stream_with_advisor<A>(
        &self,
        request: ChatRequest,
        advisor: &A,
    ) -> Result<
        AiEventStream<AdvisedStreamError<P::Error, A::Error>>,
        AdvisedPipelineError<P::Error, A::Error>,
    >
    where
        A: AiAdvisor,
    {
        let span = ai_operation_span("stream", &request);
        let started_at = Instant::now();
        let request_span = span.clone();
        let result = async {
            let request = advisor
                .before_request(request)
                .await
                .map_err(AdvisedPipelineError::Advisor)?;
            let input_characters = self
                .policy
                .validate_and_measure_input(&request)
                .map_err(AdvisedPipelineError::Policy)?;
            record_request_metadata(&request_span, &request, input_characters);
            self.provider
                .stream(request)
                .await
                .map_err(AdvisedPipelineError::Provider)
        }
        .instrument(span.clone())
        .await;
        match result {
            Ok(stream) => {
                let advisor = advisor.clone();
                let stream = stream::stop_after_terminal_event(stream);
                let stream = Box::pin(stream.then(move |event| {
                    let advisor = advisor.clone();
                    async move {
                        let event = event.map_err(AdvisedStreamError::Provider)?;
                        advisor
                            .on_stream_event(event)
                            .await
                            .map_err(AdvisedStreamError::Advisor)
                    }
                }));
                let stream = stream::stop_after_terminal_event(stream);
                record_outcome(&span, started_at, "opened", false);
                Ok(observe_stream(span, started_at, stream))
            }
            Err(error) => {
                record_advised_pipeline_error(&span, started_at, &error);
                Err(error)
            }
        }
    }
}
