//! Content-free tool audit events, durable sink contracts, and reconciliation errors.

use std::{error::Error as StdError, fmt};

use futures_util::future::BoxFuture;

use crate::{AiExecutionContext, ToolCall, ToolExecutionContext, ToolRisk};

use super::registry::ToolRunError;

/// Content-free record written before an approved tool handler is allowed to run.
#[derive(Clone, Eq, PartialEq)]
pub struct ToolApprovalAuditEvent {
    execution: ToolExecutionContext,
    call_id: String,
    tool_name: String,
    risk: ToolRisk,
}

impl ToolApprovalAuditEvent {
    pub(super) fn from_call(
        execution: ToolExecutionContext,
        call: &ToolCall,
        risk: ToolRisk,
    ) -> Self {
        Self {
            execution,
            call_id: call.id().to_owned(),
            tool_name: call.name().to_owned(),
            risk,
        }
    }

    /// Returns the trusted identity associated with this approved action.
    #[must_use]
    pub fn context(&self) -> &AiExecutionContext {
        self.execution.ai()
    }

    /// Returns the execution context shared with the tool handler.
    #[must_use]
    pub fn execution(&self) -> &ToolExecutionContext {
        &self.execution
    }

    /// Returns the application-defined external side-effect idempotency key.
    #[must_use]
    pub fn idempotency_key(&self) -> &str {
        self.execution.idempotency_key()
    }

    /// Returns the provider call identifier for application-owned reconciliation.
    #[must_use]
    pub fn call_id(&self) -> &str {
        &self.call_id
    }

    /// Returns the approved provider-visible tool name.
    #[must_use]
    pub fn tool_name(&self) -> &str {
        &self.tool_name
    }

    /// Returns the declared side-effect classification.
    #[must_use]
    pub const fn risk(&self) -> ToolRisk {
        self.risk
    }
}

impl fmt::Debug for ToolApprovalAuditEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolApprovalAuditEvent")
            .field("execution", &self.execution)
            .field("call_id", &"[REDACTED]")
            .field("tool_name", &self.tool_name)
            .field("risk", &self.risk)
            .finish()
    }
}

/// Application-owned durable audit boundary for a tool approved to execute.
///
/// A sink should persist its record before returning success. The registry invokes it after the
/// approval policy says `Approved` and before the typed handler starts, so an audit failure blocks
/// the side effect. The event never includes model arguments, tool output, prompt, or completion.
pub trait ToolApprovalAuditSink: Clone + Send + Sync + 'static {
    /// Failure type returned by the application's audit store.
    type Error: StdError + Send + Sync + 'static;

    /// Records one approved action before its handler starts.
    fn record_approved(
        &self,
        event: ToolApprovalAuditEvent,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// Content-free terminal state of one tool execution after its approval audit persisted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolExecutionOutcome {
    /// The typed handler returned a serializable result.
    Succeeded,
    /// Argument decoding or the typed handler returned a normalized execution failure.
    Failed,
}

/// Content-free terminal audit record for an approved tool execution.
#[derive(Clone, Eq, PartialEq)]
pub struct ToolExecutionAuditEvent {
    approval: ToolApprovalAuditEvent,
    outcome: ToolExecutionOutcome,
}

impl ToolExecutionAuditEvent {
    pub(super) fn new(approval: ToolApprovalAuditEvent, outcome: ToolExecutionOutcome) -> Self {
        Self { approval, outcome }
    }

    /// Returns the approved action identity and idempotency key shared with the handler.
    #[must_use]
    pub fn approval(&self) -> &ToolApprovalAuditEvent {
        &self.approval
    }

    /// Returns the terminal handler outcome without prompt, arguments, or tool result content.
    #[must_use]
    pub const fn outcome(&self) -> ToolExecutionOutcome {
        self.outcome
    }
}

impl fmt::Debug for ToolExecutionAuditEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolExecutionAuditEvent")
            .field("approval", &self.approval)
            .field("outcome", &self.outcome)
            .finish()
    }
}

/// Durable audit boundary that records both approval and terminal execution outcome.
///
/// The registry writes approval before the handler and outcome after it. An outcome-write failure
/// cannot undo an external side effect, so the registry returns the redacted event in
/// [`ExecutionAuditedToolRunError::OutcomeAudit`] for application-owned retry and reconciliation.
pub trait ToolExecutionAuditSink: ToolApprovalAuditSink {
    /// Records the terminal outcome for a previously approved action.
    fn record_outcome(
        &self,
        event: ToolExecutionAuditEvent,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// Failure while executing a tool with a required pre-execution approval audit.
#[derive(thiserror::Error)]
pub enum AuditedToolRunError<ApprovalError, AuditError>
where
    ApprovalError: StdError + Send + Sync + 'static,
    AuditError: StdError + Send + Sync + 'static,
{
    /// Approval, lookup, argument, or handler execution failed.
    #[error(transparent)]
    Run(ToolRunError<ApprovalError>),
    /// The audit record did not persist, so the handler was not started.
    #[error("AI approved tool audit could not be recorded")]
    Audit(#[source] AuditError),
}

impl<ApprovalError, AuditError> fmt::Debug for AuditedToolRunError<ApprovalError, AuditError>
where
    ApprovalError: StdError + Send + Sync + 'static,
    AuditError: StdError + Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Run(_) => "tool_run_failed",
            Self::Audit(_) => "approval_audit_failed",
        };
        formatter
            .debug_struct("AuditedToolRunError")
            .field("kind", &kind)
            .finish()
    }
}

/// Failure while executing a tool with both approval and terminal outcome audit.
#[derive(thiserror::Error)]
pub enum ExecutionAuditedToolRunError<ApprovalError, AuditError>
where
    ApprovalError: StdError + Send + Sync + 'static,
    AuditError: StdError + Send + Sync + 'static,
{
    /// Approval, lookup, argument, or handler execution failed after any required audit writes.
    #[error(transparent)]
    Run(ToolRunError<ApprovalError>),
    /// Approval audit did not persist, so the handler was not started.
    #[error("AI approved tool audit could not be recorded")]
    ApprovalAudit(#[source] AuditError),
    /// Terminal audit did not persist after the handler ran; reconciliation is required.
    #[error("AI tool outcome audit could not be recorded; reconciliation is required")]
    OutcomeAudit {
        /// Redacted event that identifies the action and observed terminal handler outcome.
        event: ToolExecutionAuditEvent,
        /// Durable audit store failure.
        #[source]
        source: AuditError,
    },
}

impl<ApprovalError, AuditError> fmt::Debug
    for ExecutionAuditedToolRunError<ApprovalError, AuditError>
where
    ApprovalError: StdError + Send + Sync + 'static,
    AuditError: StdError + Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Run(_) => "tool_run_failed",
            Self::ApprovalAudit(_) => "approval_audit_failed",
            Self::OutcomeAudit { .. } => "outcome_audit_failed",
        };
        formatter
            .debug_struct("ExecutionAuditedToolRunError")
            .field("kind", &kind)
            .finish()
    }
}
