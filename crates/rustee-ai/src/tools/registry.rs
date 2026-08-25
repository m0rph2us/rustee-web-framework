//! Application tool registry orchestration, auditing, and execution telemetry.

use std::{collections::BTreeMap, error::Error as StdError, fmt, sync::Arc, time::Instant};

use tracing::{Instrument, Span};

use super::{
    ToolApprovalDecision, ToolApprovalPolicy, ToolExecutionError, ToolExecutor, ToolResult,
    audit::{
        AuditedToolRunError, ExecutionAuditedToolRunError, ToolApprovalAuditEvent,
        ToolApprovalAuditSink, ToolExecutionAuditEvent, ToolExecutionAuditSink,
        ToolExecutionOutcome,
    },
};
use crate::{ToolCall, ToolDefinition, ToolExecutionContext, ToolRisk};

mod telemetry;

use telemetry::{
    record_audited_tool_run_result, record_execution_audited_tool_run_result, record_tool_risk,
    record_tool_run_result, tool_execution_span,
};

/// Registry of application tools that can be advertised to a provider.
#[derive(Default)]
pub struct ToolRegistry {
    tools: BTreeMap<String, Arc<dyn ToolExecutor>>,
}

impl ToolRegistry {
    /// Creates an empty tool registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one tool with a unique provider-visible name.
    ///
    /// # Errors
    ///
    /// Returns [`ToolRegistryError::DuplicateTool`] when a tool with the same name is already
    /// registered.
    pub fn register<T>(&mut self, tool: T) -> Result<(), ToolRegistryError>
    where
        T: ToolExecutor,
    {
        let name = tool.definition().name().to_owned();
        if self.tools.contains_key(&name) {
            return Err(ToolRegistryError::DuplicateTool);
        }
        self.tools.insert(name, Arc::new(tool));
        Ok(())
    }

    /// Returns declared tools in deterministic name order for a provider request.
    #[must_use]
    pub fn definitions(&self) -> impl ExactSizeIterator<Item = &ToolDefinition> {
        self.tools.values().map(|tool| tool.definition())
    }

    /// Executes one requested call only after an application approval policy permits it.
    ///
    /// The policy receives the invocation identity and the handler receives that identity plus an
    /// application-defined idempotency key. The policy also receives the full call, including
    /// untrusted arguments, so it can apply tenant, user-confirmation, and audit rules before JSON
    /// decoding invokes the tool handler.
    ///
    /// # Errors
    ///
    /// Returns a normalized registry, approval, or execution failure without exposing tool output
    /// or a handler's internal error.
    pub async fn execute<P>(
        &self,
        context: ToolExecutionContext,
        call: ToolCall,
        approval: &P,
    ) -> Result<ToolResult, ToolRunError<P::Error>>
    where
        P: ToolApprovalPolicy,
    {
        let span = tool_execution_span();
        let started_at = Instant::now();
        let execution_span = span.clone();
        let result = async {
            let (tool, _) = self
                .authorize(&context, &call, approval, &execution_span)
                .await?;
            let result_value = tool
                .execute(context, call.arguments().clone())
                .await
                .map_err(ToolRunError::Execution)?;
            Ok(ToolResult::from_call(&call, result_value))
        }
        .instrument(span.clone())
        .await;
        record_tool_run_result(&span, started_at, &result);
        result
    }

    /// Executes one approved tool only after its approval audit record persists.
    ///
    /// The audit sink is invoked after approval but before argument decoding or handler execution.
    /// Therefore an [`AuditedToolRunError::Audit`] result guarantees that the tool handler did not
    /// start. The application owns sink retention, reconciliation, and any post-execution audit.
    ///
    /// # Errors
    ///
    /// Returns a normalized approval/execution failure or an audit failure that prevented the
    /// handler from starting.
    pub async fn execute_with_approval_audit<P, A>(
        &self,
        context: ToolExecutionContext,
        call: ToolCall,
        approval: &P,
        audit: &A,
    ) -> Result<ToolResult, AuditedToolRunError<P::Error, A::Error>>
    where
        P: ToolApprovalPolicy,
        A: ToolApprovalAuditSink,
    {
        let span = tool_execution_span();
        let started_at = Instant::now();
        let execution_span = span.clone();
        let result = async {
            let (tool, risk) = self
                .authorize(&context, &call, approval, &execution_span)
                .await
                .map_err(AuditedToolRunError::Run)?;
            audit
                .record_approved(ToolApprovalAuditEvent::from_call(
                    context.clone(),
                    &call,
                    risk,
                ))
                .await
                .map_err(AuditedToolRunError::Audit)?;
            let result_value = tool
                .execute(context, call.arguments().clone())
                .await
                .map_err(ToolRunError::Execution)
                .map_err(AuditedToolRunError::Run)?;
            Ok(ToolResult::from_call(&call, result_value))
        }
        .instrument(span.clone())
        .await;
        record_audited_tool_run_result(&span, started_at, &result);
        result
    }

    /// Executes one tool with both pre-execution approval and post-execution outcome audit.
    ///
    /// The approval event is persisted before argument decoding and handler start. After the
    /// handler resolves, the same application idempotency key and a content-free terminal outcome
    /// are persisted. If that final write fails, the returned error exposes a redacted event for
    /// application-owned retry; it does not claim the side effect was rolled back.
    ///
    /// # Errors
    ///
    /// Returns an approval/handler failure, an approval-audit failure that prevented handler
    /// start, or an outcome-audit failure that requires reconciliation because the handler ran.
    pub async fn execute_with_execution_audit<P, A>(
        &self,
        context: ToolExecutionContext,
        call: ToolCall,
        approval: &P,
        audit: &A,
    ) -> Result<ToolResult, ExecutionAuditedToolRunError<P::Error, A::Error>>
    where
        P: ToolApprovalPolicy,
        A: ToolExecutionAuditSink,
    {
        let span = tool_execution_span();
        let started_at = Instant::now();
        let execution_span = span.clone();
        let result = async {
            let (tool, risk) = self
                .authorize(&context, &call, approval, &execution_span)
                .await
                .map_err(ExecutionAuditedToolRunError::Run)?;
            let approval_event = ToolApprovalAuditEvent::from_call(context.clone(), &call, risk);
            audit
                .record_approved(approval_event.clone())
                .await
                .map_err(ExecutionAuditedToolRunError::ApprovalAudit)?;

            let result = tool.execute(context, call.arguments().clone()).await;
            let outcome = if result.is_ok() {
                ToolExecutionOutcome::Succeeded
            } else {
                ToolExecutionOutcome::Failed
            };
            let outcome_event = ToolExecutionAuditEvent::new(approval_event, outcome);
            audit
                .record_outcome(outcome_event.clone())
                .await
                .map_err(|source| ExecutionAuditedToolRunError::OutcomeAudit {
                    event: outcome_event,
                    source,
                })?;
            let result_value = result
                .map_err(ToolRunError::Execution)
                .map_err(ExecutionAuditedToolRunError::Run)?;
            Ok(ToolResult::from_call(&call, result_value))
        }
        .instrument(span.clone())
        .await;
        record_execution_audited_tool_run_result(&span, started_at, &result);
        result
    }

    /// Selects one registered tool and applies approval before any audit or handler side effect.
    async fn authorize<P>(
        &self,
        context: &ToolExecutionContext,
        call: &ToolCall,
        approval: &P,
        span: &Span,
    ) -> Result<(Arc<dyn ToolExecutor>, ToolRisk), ToolRunError<P::Error>>
    where
        P: ToolApprovalPolicy,
    {
        let tool = self
            .tools
            .get(call.name())
            .cloned()
            .ok_or(ToolRunError::UnknownTool)?;
        let risk = tool.risk();
        record_tool_risk(span, risk);
        match approval
            .approve(context.ai().clone(), call.clone(), risk)
            .await
            .map_err(ToolRunError::Approval)?
        {
            ToolApprovalDecision::Approved => Ok((tool, risk)),
            ToolApprovalDecision::Denied => Err(ToolRunError::Denied { risk }),
        }
    }
}

impl fmt::Debug for ToolRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolRegistry")
            .field("tool_count", &self.tools.len())
            .finish_non_exhaustive()
    }
}

/// Failure while registering an application tool.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ToolRegistryError {
    /// A provider-visible tool name may be registered only once.
    #[error("AI tool name is already registered")]
    DuplicateTool,
}

/// Failure while approving or executing a model-requested tool call.
#[derive(thiserror::Error)]
pub enum ToolRunError<ApprovalError>
where
    ApprovalError: StdError + Send + Sync + 'static,
{
    /// The provider requested a tool that this application did not register.
    #[error("AI requested an unknown tool")]
    UnknownTool,
    /// The application's approval system could not decide whether execution is allowed.
    #[error("AI tool approval failed")]
    Approval(#[source] ApprovalError),
    /// The application deliberately rejected this requested action.
    #[error("AI tool execution was not approved")]
    Denied {
        /// Risk classification of the denied action.
        risk: ToolRisk,
    },
    /// Argument decoding, handler execution, or result serialization failed.
    #[error(transparent)]
    Execution(#[from] ToolExecutionError),
}

impl<ApprovalError> fmt::Debug for ToolRunError<ApprovalError>
where
    ApprovalError: StdError + Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::UnknownTool => "unknown_tool",
            Self::Approval(_) => "approval_failed",
            Self::Denied { .. } => "denied",
            Self::Execution(_) => "execution_failed",
        };
        formatter
            .debug_struct("ToolRunError")
            .field("kind", &kind)
            .finish()
    }
}
