//! Provider-neutral AI application contracts.
//!
//! Model output and tool arguments are untrusted input. This crate returns requested tool calls
//! but never executes a side effect automatically.

mod context;
mod pipeline;
mod protocol;
mod provider;
mod tools;
mod usage;

pub use context::{
    AiExecutionContext, AiExecutionContextError, MAX_IDEMPOTENCY_KEY_BYTES, MAX_SUBJECT_BYTES,
    MAX_TENANT_BYTES, ToolExecutionContext, ToolExecutionContextError,
};
pub use pipeline::{
    AdvisedPipelineError, AdvisedStreamError, AdvisedUsageLedgerPipelineError,
    AdvisedUsageLedgerStreamError, AiPipeline, AiPolicy, BudgetAdvisorError, PipelineError,
    PolicyError, UsageLedgerPipelineError, UsageLedgerStreamError,
};
pub use protocol::{
    ChatMessage, ChatRequest, ChatResponse, MAX_MODEL_ALIAS_BYTES, MAX_TOOL_CALL_ID_BYTES,
    MAX_TOOL_NAME_BYTES, MessageRole, ModelAliasError, RequestError, ResponseError,
    StructuredOutputError, ToolCall, ToolDefinition, ToolError, ToolRisk, Usage,
    validate_model_alias,
};
pub use provider::{
    AiAdvisor, AiEventStream, AiEventStreamFuture, AiProvider, AiStreamEvent, NoopAiAdvisor,
};
pub use tools::{
    AuditedToolRunError, DenyAllToolApproval, ExecutionAuditedToolRunError, ToolApprovalAuditEvent,
    ToolApprovalAuditSink, ToolApprovalDecision, ToolApprovalPolicy, ToolExecutionAuditEvent,
    ToolExecutionAuditSink, ToolExecutionError, ToolExecutionOutcome, ToolExecutor, ToolRegistry,
    ToolRegistryError, ToolResult, ToolRunError, TypedTool,
};
pub use usage::{
    AiBudgetDecision, AiBudgetPolicy, AiBudgetRequest, AiUsageLedger, AiUsageReservation,
    AiUsageReservationDecision, AiUsageReservationError, AiUsageSettlement, BudgetAdvisor,
};

pub(crate) use pipeline::record_outcome;

#[cfg(test)]
mod tests;
