//! Shared AI unit-test facade for focused provider, policy, usage, and tool fixtures.

pub(super) use crate::{
    AdvisedPipelineError, AdvisedStreamError, AdvisedUsageLedgerPipelineError,
    AdvisedUsageLedgerStreamError, AiAdvisor, AiBudgetDecision, AiBudgetPolicy, AiBudgetRequest,
    AiExecutionContext, AiExecutionContextError, AiPipeline, AiPolicy, AiProvider, AiStreamEvent,
    AiUsageLedger, AiUsageReservation, AiUsageReservationDecision, AiUsageReservationError,
    AiUsageSettlement, AuditedToolRunError, BudgetAdvisor, BudgetAdvisorError, ChatMessage,
    ChatRequest, ChatResponse, DenyAllToolApproval, ExecutionAuditedToolRunError,
    MAX_IDEMPOTENCY_KEY_BYTES, MAX_MODEL_ALIAS_BYTES, MAX_SUBJECT_BYTES, MAX_TENANT_BYTES,
    MAX_TOOL_CALL_ID_BYTES, MAX_TOOL_NAME_BYTES, MessageRole, PipelineError, PolicyError,
    ToolApprovalAuditEvent, ToolApprovalAuditSink, ToolApprovalDecision, ToolApprovalPolicy,
    ToolCall, ToolDefinition, ToolError, ToolExecutionAuditEvent, ToolExecutionAuditSink,
    ToolExecutionContext, ToolExecutionContextError, ToolExecutionError, ToolExecutionOutcome,
    ToolExecutor, ToolRegistry, ToolResult, ToolRisk, ToolRunError, Usage,
    UsageLedgerPipelineError, UsageLedgerStreamError,
};
pub(super) use futures_util::{StreamExt, future, stream};
pub(super) use serde::{Deserialize, Serialize};
pub(super) use serde_json::json;
pub(super) use std::{
    convert::Infallible,
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(super) enum TestUsageLedgerError {
    #[error("test usage ledger is unavailable")]
    Unavailable,
}

#[path = "support/advisor.rs"]
mod advisor;
#[path = "support/provider.rs"]
mod provider;
#[path = "support/tools.rs"]
mod tools;
#[path = "support/usage.rs"]
mod usage;

pub(super) use advisor::*;
pub(super) use provider::*;
pub(super) use tools::*;
pub(super) use usage::*;
