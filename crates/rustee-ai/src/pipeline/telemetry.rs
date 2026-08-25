//! Content-free AI pipeline tracing and outcome classification.

use std::time::Instant;

use futures_util::StreamExt;
use tracing::Span;

use super::{
    AdvisedPipelineError, AdvisedUsageLedgerPipelineError, PipelineError, UsageLedgerPipelineError,
};
use crate::{AiEventStream, AiStreamEvent, ChatRequest, ChatResponse, Usage};

pub(super) fn ai_operation_span(operation: &'static str, request: &ChatRequest) -> Span {
    tracing::info_span!(
        "rustee.ai",
        otel.name = "AI request",
        otel.kind = "client",
        ai.operation = operation,
        ai.request.message_count = request.messages().len(),
        ai.request.tool_count = request.tools().len(),
        ai.request.tool_result_count = request.tool_results().len(),
        ai.request.input_characters = tracing::field::Empty,
        ai.usage.input_tokens = tracing::field::Empty,
        ai.usage.output_tokens = tracing::field::Empty,
        ai.outcome = tracing::field::Empty,
        duration_ms = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
    )
}

pub(super) fn record_request_metadata(span: &Span, request: &ChatRequest, input_characters: usize) {
    span.record(
        "ai.request.message_count",
        tracing::field::display(request.messages().len()),
    );
    span.record(
        "ai.request.tool_count",
        tracing::field::display(request.tools().len()),
    );
    span.record(
        "ai.request.tool_result_count",
        tracing::field::display(request.tool_results().len()),
    );
    span.record(
        "ai.request.input_characters",
        tracing::field::display(input_characters),
    );
}

fn record_usage(span: &Span, usage: Usage) {
    span.record(
        "ai.usage.input_tokens",
        tracing::field::display(usage.input_tokens),
    );
    span.record(
        "ai.usage.output_tokens",
        tracing::field::display(usage.output_tokens),
    );
}

fn record_success(span: &Span, started_at: Instant, usage: Usage) {
    record_usage(span, usage);
    record_outcome(span, started_at, "succeeded", false);
}

pub(crate) fn record_outcome(
    span: &Span,
    started_at: Instant,
    outcome: &'static str,
    failed: bool,
) {
    span.record("ai.outcome", outcome);
    span.record("otel.status_code", if failed { "ERROR" } else { "UNSET" });
    span.record(
        "duration_ms",
        tracing::field::display(started_at.elapsed().as_millis()),
    );
}

pub(super) fn observe_stream<E: 'static>(
    span: Span,
    started_at: Instant,
    stream: AiEventStream<E>,
) -> AiEventStream<E> {
    Box::pin(stream.inspect(move |event| match event {
        Ok(AiStreamEvent::Completed(usage)) => record_success(&span, started_at, *usage),
        Err(_) => record_outcome(&span, started_at, "stream_failed", true),
        Ok(_) => {}
    }))
}

pub(super) fn record_pipeline_result<E>(
    span: &Span,
    started_at: Instant,
    result: &Result<ChatResponse, PipelineError<E>>,
) {
    match result {
        Ok(response) => record_success(span, started_at, response.usage()),
        Err(error) => record_pipeline_error(span, started_at, error),
    }
}

pub(super) fn record_pipeline_error<E>(span: &Span, started_at: Instant, error: &PipelineError<E>) {
    match error {
        PipelineError::Policy(_) => record_outcome(span, started_at, "policy_rejected", false),
        PipelineError::Provider(_) => record_outcome(span, started_at, "provider_failed", true),
    }
}

pub(super) fn record_advised_pipeline_result<ProviderError, AdvisorError>(
    span: &Span,
    started_at: Instant,
    result: &Result<ChatResponse, AdvisedPipelineError<ProviderError, AdvisorError>>,
) {
    match result {
        Ok(response) => record_success(span, started_at, response.usage()),
        Err(error) => record_advised_pipeline_error(span, started_at, error),
    }
}

pub(super) fn record_advised_pipeline_error<ProviderError, AdvisorError>(
    span: &Span,
    started_at: Instant,
    error: &AdvisedPipelineError<ProviderError, AdvisorError>,
) {
    match error {
        AdvisedPipelineError::Policy(_) => {
            record_outcome(span, started_at, "policy_rejected", false);
        }
        AdvisedPipelineError::Provider(_) => {
            record_outcome(span, started_at, "provider_failed", true);
        }
        AdvisedPipelineError::Advisor(_) => {
            record_outcome(span, started_at, "advisor_failed", true);
        }
    }
}

pub(super) fn record_usage_ledger_pipeline_result<ProviderError, LedgerError>(
    span: &Span,
    started_at: Instant,
    result: &Result<ChatResponse, UsageLedgerPipelineError<ProviderError, LedgerError>>,
) {
    match result {
        Ok(response) => record_success(span, started_at, response.usage()),
        Err(error) => record_usage_ledger_pipeline_error(span, started_at, error),
    }
}

pub(super) fn record_usage_ledger_pipeline_error<ProviderError, LedgerError>(
    span: &Span,
    started_at: Instant,
    error: &UsageLedgerPipelineError<ProviderError, LedgerError>,
) {
    match error {
        UsageLedgerPipelineError::Policy(_)
        | UsageLedgerPipelineError::ReservationRequestMismatch => {
            record_outcome(span, started_at, "policy_rejected", false);
        }
        UsageLedgerPipelineError::Denied => {
            record_outcome(span, started_at, "budget_denied", false);
        }
        UsageLedgerPipelineError::PendingReconciliation
        | UsageLedgerPipelineError::AlreadySettled => {
            record_outcome(span, started_at, "usage_reconciliation_required", true);
        }
        UsageLedgerPipelineError::Reservation(_) => {
            record_outcome(span, started_at, "usage_reservation_failed", true);
        }
        UsageLedgerPipelineError::Provider(_) => {
            record_outcome(span, started_at, "provider_failed", true);
        }
        UsageLedgerPipelineError::Settlement { .. } => {
            record_outcome(span, started_at, "usage_settlement_failed", true);
        }
    }
}

pub(super) fn record_advised_usage_ledger_pipeline_result<
    ProviderError,
    AdvisorError,
    LedgerError,
>(
    span: &Span,
    started_at: Instant,
    result: &Result<
        ChatResponse,
        AdvisedUsageLedgerPipelineError<ProviderError, AdvisorError, LedgerError>,
    >,
) {
    match result {
        Ok(response) => record_success(span, started_at, response.usage()),
        Err(error) => record_advised_usage_ledger_pipeline_error(span, started_at, error),
    }
}

pub(super) fn record_advised_usage_ledger_pipeline_error<
    ProviderError,
    AdvisorError,
    LedgerError,
>(
    span: &Span,
    started_at: Instant,
    error: &AdvisedUsageLedgerPipelineError<ProviderError, AdvisorError, LedgerError>,
) {
    match error {
        AdvisedUsageLedgerPipelineError::Advisor(_) => {
            record_outcome(span, started_at, "advisor_failed", true);
        }
        AdvisedUsageLedgerPipelineError::ReservationMetadata(_) => {
            record_outcome(span, started_at, "usage_reservation_invalid", false);
        }
        AdvisedUsageLedgerPipelineError::Usage(error) => {
            record_usage_ledger_pipeline_error(span, started_at, error);
        }
    }
}
