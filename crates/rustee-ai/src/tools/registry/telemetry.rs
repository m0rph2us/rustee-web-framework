//! Content-free tool execution spans and terminal outcome recording.

use std::{error::Error as StdError, time::Instant};

use tracing::Span;

use crate::{ToolResult, ToolRisk, record_outcome};

use super::super::audit::{
    AuditedToolRunError, ExecutionAuditedToolRunError, ToolExecutionOutcome,
};
use super::ToolRunError;

pub(super) fn tool_execution_span() -> Span {
    tracing::info_span!(
        "rustee.ai.tool",
        otel.name = "AI tool execution",
        otel.kind = "internal",
        ai.operation = "tool_execute",
        ai.tool.risk = tracing::field::Empty,
        ai.tool.handler_outcome = tracing::field::Empty,
        ai.outcome = tracing::field::Empty,
        duration_ms = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
    )
}

pub(super) fn record_tool_risk(span: &Span, risk: ToolRisk) {
    span.record("ai.tool.risk", tool_risk_name(risk));
}

pub(super) fn record_tool_run_result<ApprovalError>(
    span: &Span,
    started_at: Instant,
    result: &Result<ToolResult, ToolRunError<ApprovalError>>,
) where
    ApprovalError: StdError + Send + Sync + 'static,
{
    match result {
        Ok(_) => record_outcome(span, started_at, "succeeded", false),
        Err(error) => record_tool_run_error(span, started_at, error),
    }
}

pub(super) fn record_audited_tool_run_result<ApprovalError, AuditError>(
    span: &Span,
    started_at: Instant,
    result: &Result<ToolResult, AuditedToolRunError<ApprovalError, AuditError>>,
) where
    ApprovalError: StdError + Send + Sync + 'static,
    AuditError: StdError + Send + Sync + 'static,
{
    match result {
        Ok(_) => record_outcome(span, started_at, "succeeded", false),
        Err(AuditedToolRunError::Run(error)) => record_tool_run_error(span, started_at, error),
        Err(AuditedToolRunError::Audit(_)) => {
            record_outcome(span, started_at, "approval_audit_failed", true);
        }
    }
}

pub(super) fn record_execution_audited_tool_run_result<ApprovalError, AuditError>(
    span: &Span,
    started_at: Instant,
    result: &Result<ToolResult, ExecutionAuditedToolRunError<ApprovalError, AuditError>>,
) where
    ApprovalError: StdError + Send + Sync + 'static,
    AuditError: StdError + Send + Sync + 'static,
{
    match result {
        Ok(_) => record_outcome(span, started_at, "succeeded", false),
        Err(ExecutionAuditedToolRunError::Run(error)) => {
            record_tool_run_error(span, started_at, error);
        }
        Err(ExecutionAuditedToolRunError::ApprovalAudit(_)) => {
            record_outcome(span, started_at, "approval_audit_failed", true);
        }
        Err(ExecutionAuditedToolRunError::OutcomeAudit { event, .. }) => {
            span.record(
                "ai.tool.handler_outcome",
                tool_outcome_name(event.outcome()),
            );
            record_outcome(span, started_at, "outcome_audit_failed", true);
        }
    }
}

fn record_tool_run_error<ApprovalError>(
    span: &Span,
    started_at: Instant,
    error: &ToolRunError<ApprovalError>,
) where
    ApprovalError: StdError + Send + Sync + 'static,
{
    match error {
        ToolRunError::UnknownTool => record_outcome(span, started_at, "unknown_tool", true),
        ToolRunError::Approval(_) => record_outcome(span, started_at, "approval_failed", true),
        ToolRunError::Denied { .. } => record_outcome(span, started_at, "denied", false),
        ToolRunError::Execution(_) => record_outcome(span, started_at, "execution_failed", true),
    }
}

fn tool_risk_name(risk: ToolRisk) -> &'static str {
    match risk {
        ToolRisk::ReadOnly => "read_only",
        ToolRisk::RequiresConfirmation => "requires_confirmation",
        ToolRisk::Privileged => "privileged",
    }
}

fn tool_outcome_name(outcome: ToolExecutionOutcome) -> &'static str {
    match outcome {
        ToolExecutionOutcome::Succeeded => "succeeded",
        ToolExecutionOutcome::Failed => "failed",
    }
}
