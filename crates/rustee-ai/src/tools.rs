//! Application tool execution, approval, audit, and telemetry boundaries.

use std::{error::Error as StdError, fmt, future::Future, marker::PhantomData, sync::Arc};

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{AiExecutionContext, ToolCall, ToolDefinition, ToolExecutionContext, ToolRisk};

mod audit;
mod registry;

pub use audit::{
    AuditedToolRunError, ExecutionAuditedToolRunError, ToolApprovalAuditEvent,
    ToolApprovalAuditSink, ToolExecutionAuditEvent, ToolExecutionAuditSink, ToolExecutionOutcome,
};
pub use registry::{ToolRegistry, ToolRegistryError, ToolRunError};

/// Application implementation of one provider-visible tool.
///
/// Tool arguments are model output and must be decoded and validated by the implementation before
/// a side effect occurs. The registry never invokes this trait without an approval decision.
pub trait ToolExecutor: Send + Sync + 'static {
    /// Returns the provider-visible declaration.
    fn definition(&self) -> &ToolDefinition;

    /// Returns the tool's application-side risk classification.
    fn risk(&self) -> ToolRisk;

    /// Executes validated arguments after approval with trusted identity and an idempotency key.
    fn execute(
        &self,
        context: ToolExecutionContext,
        arguments: Value,
    ) -> BoxFuture<'static, Result<Value, ToolExecutionError>>;
}

/// Typed [`ToolExecutor`] that decodes JSON before invoking an application handler.
pub struct TypedTool<Input, Output, Handler> {
    definition: ToolDefinition,
    risk: ToolRisk,
    handler: Arc<Handler>,
    marker: PhantomData<fn(Input) -> Output>,
}

impl<Input, Output, Handler> TypedTool<Input, Output, Handler> {
    /// Creates a typed tool from a validated declaration, risk classification, and handler.
    #[must_use]
    pub fn new(definition: ToolDefinition, risk: ToolRisk, handler: Handler) -> Self {
        Self {
            definition,
            risk,
            handler: Arc::new(handler),
            marker: PhantomData,
        }
    }
}

impl<Input, Output, Handler> fmt::Debug for TypedTool<Input, Output, Handler> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TypedTool")
            .field("definition", &self.definition)
            .field("risk", &self.risk)
            .field("handler", &"[REDACTED]")
            .finish()
    }
}

impl<Input, Output, Handler, HandlerFuture, HandlerError> ToolExecutor
    for TypedTool<Input, Output, Handler>
where
    Input: serde::de::DeserializeOwned + Send + 'static,
    Output: Serialize + Send + 'static,
    Handler: Fn(ToolExecutionContext, Input) -> HandlerFuture + Send + Sync + 'static,
    HandlerFuture: Future<Output = Result<Output, HandlerError>> + Send + 'static,
    HandlerError: StdError + Send + Sync + 'static,
{
    fn definition(&self) -> &ToolDefinition {
        &self.definition
    }

    fn risk(&self) -> ToolRisk {
        self.risk
    }

    fn execute(
        &self,
        context: ToolExecutionContext,
        arguments: Value,
    ) -> BoxFuture<'static, Result<Value, ToolExecutionError>> {
        let handler = Arc::clone(&self.handler);
        Box::pin(async move {
            let arguments = serde_json::from_value(arguments)
                .map_err(|_| ToolExecutionError::InvalidArguments)?;
            let output = handler(context, arguments)
                .await
                .map_err(|_| ToolExecutionError::HandlerFailed)?;
            serde_json::to_value(output).map_err(|_| ToolExecutionError::InvalidResult)
        })
    }
}

/// One application-side approval decision for a requested tool call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolApprovalDecision {
    /// The request may proceed to typed argument validation and handler execution.
    Approved,
    /// The request must not invoke the handler.
    Denied,
}

/// Policy boundary for user confirmation, tenant authorization, and side-effect approval.
pub trait ToolApprovalPolicy: Clone + Send + Sync + 'static {
    /// Failure type returned by the application's approval system.
    type Error: StdError + Send + Sync + 'static;

    /// Approves or rejects one requested call before its handler can run.
    fn approve(
        &self,
        context: AiExecutionContext,
        call: ToolCall,
        risk: ToolRisk,
    ) -> BoxFuture<'static, Result<ToolApprovalDecision, Self::Error>>;
}

/// Default approval policy that never permits a tool execution.
#[derive(Clone, Copy, Debug, Default)]
pub struct DenyAllToolApproval;

impl ToolApprovalPolicy for DenyAllToolApproval {
    type Error = std::convert::Infallible;

    fn approve(
        &self,
        _: AiExecutionContext,
        _: ToolCall,
        _: ToolRisk,
    ) -> BoxFuture<'static, Result<ToolApprovalDecision, Self::Error>> {
        Box::pin(futures_util::future::ready(Ok(
            ToolApprovalDecision::Denied,
        )))
    }
}

/// Result of an approved tool execution.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct ToolResult {
    call_id: String,
    name: String,
    content: Value,
}

#[derive(Deserialize)]
struct SerializedToolResult {
    call_id: String,
    name: String,
    content: Value,
}

impl<'de> Deserialize<'de> for ToolResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serialized = SerializedToolResult::deserialize(deserializer)?;
        let call = ToolCall::new(serialized.call_id, serialized.name, Value::Null)
            .map_err(serde::de::Error::custom)?;
        Ok(Self::from_call(&call, serialized.content))
    }
}

impl ToolResult {
    fn from_call(call: &ToolCall, content: Value) -> Self {
        Self {
            call_id: call.id().to_owned(),
            name: call.name().to_owned(),
            content,
        }
    }

    /// Returns the provider call ID that this result satisfies.
    #[must_use]
    pub fn call_id(&self) -> &str {
        &self.call_id
    }

    /// Returns the tool name selected by the registry.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns tool output for an application to redact, bound, and send back to a provider.
    #[must_use]
    pub const fn content(&self) -> &Value {
        &self.content
    }
}

impl fmt::Debug for ToolResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolResult")
            .field("call_id", &"[REDACTED]")
            .field("name", &self.name)
            .field("content", &"[REDACTED]")
            .finish()
    }
}

/// Failure returned by the typed tool executor before a tool result is produced.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ToolExecutionError {
    /// Untrusted model arguments did not match the typed input model.
    #[error("AI tool arguments were invalid")]
    InvalidArguments,
    /// The application tool handler failed; its details remain internal.
    #[error("AI tool execution failed")]
    HandlerFailed,
    /// The typed handler output could not be converted to JSON.
    #[error("AI tool result could not be serialized")]
    InvalidResult,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AiExecutionContext, ToolCall, ToolExecutionOutcome};
    use serde_json::json;

    struct LeakyAuditError;

    impl fmt::Debug for LeakyAuditError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("LeakyAuditError(private-audit-store-detail)")
        }
    }

    impl fmt::Display for LeakyAuditError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("private-audit-store-detail")
        }
    }

    impl StdError for LeakyAuditError {}

    #[test]
    fn outcome_audit_diagnostics_redact_adapter_details_and_preserve_source() {
        let context = ToolExecutionContext::new(
            AiExecutionContext::new("tenant", "subject").expect("test context is valid"),
            "tool:action",
        )
        .expect("test tool context is valid");
        let call = ToolCall::new("call", "tool", json!({"id": 7})).expect("test call is valid");
        let event = ToolExecutionAuditEvent::new(
            ToolApprovalAuditEvent::from_call(context, &call, ToolRisk::ReadOnly),
            ToolExecutionOutcome::Succeeded,
        );
        let error =
            ExecutionAuditedToolRunError::<LeakyAuditError, LeakyAuditError>::OutcomeAudit {
                event,
                source: LeakyAuditError,
            };

        assert!(!format!("{error:?}").contains("private-audit-store-detail"));
        assert!(!error.to_string().contains("private-audit-store-detail"));
        assert!(StdError::source(&error).is_some());
    }
}
