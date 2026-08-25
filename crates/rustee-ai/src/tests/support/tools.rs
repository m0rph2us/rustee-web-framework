//! Tool, approval, and audit fixtures.

use super::*;

#[derive(Deserialize)]
struct LookupArguments {
    id: u64,
}

#[derive(Serialize)]
struct LookupResult {
    status: &'static str,
}

#[derive(Clone, Copy)]
pub(in crate::tests) struct ApproveAll;

impl ToolApprovalPolicy for ApproveAll {
    type Error = Infallible;

    fn approve(
        &self,
        _: AiExecutionContext,
        _: ToolCall,
        _: ToolRisk,
    ) -> futures_util::future::BoxFuture<'static, Result<ToolApprovalDecision, Self::Error>> {
        Box::pin(future::ready(Ok(ToolApprovalDecision::Approved)))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(in crate::tests) enum TestAuditError {
    #[error("test audit store is unavailable")]
    Unavailable,
}

#[derive(Clone)]
pub(in crate::tests) struct CapturingAudit {
    pub(in crate::tests) events: Arc<Mutex<Vec<ToolApprovalAuditEvent>>>,
    pub(in crate::tests) should_fail: bool,
}

impl ToolApprovalAuditSink for CapturingAudit {
    type Error = TestAuditError;

    fn record_approved(
        &self,
        event: ToolApprovalAuditEvent,
    ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
        let events = Arc::clone(&self.events);
        let should_fail = self.should_fail;
        Box::pin(async move {
            if should_fail {
                return Err(TestAuditError::Unavailable);
            }
            events
                .lock()
                .expect("test audit lock is available")
                .push(event);
            Ok(())
        })
    }
}

#[derive(Clone)]
pub(in crate::tests) struct CapturingExecutionAudit {
    pub(in crate::tests) approvals: Arc<Mutex<Vec<ToolApprovalAuditEvent>>>,
    pub(in crate::tests) outcomes: Arc<Mutex<Vec<ToolExecutionAuditEvent>>>,
    pub(in crate::tests) fail_approval: bool,
    pub(in crate::tests) fail_outcome: bool,
}

impl ToolApprovalAuditSink for CapturingExecutionAudit {
    type Error = TestAuditError;

    fn record_approved(
        &self,
        event: ToolApprovalAuditEvent,
    ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
        let approvals = Arc::clone(&self.approvals);
        let fail_approval = self.fail_approval;
        Box::pin(async move {
            if fail_approval {
                return Err(TestAuditError::Unavailable);
            }
            approvals
                .lock()
                .expect("test audit lock is available")
                .push(event);
            Ok(())
        })
    }
}

impl ToolExecutionAuditSink for CapturingExecutionAudit {
    fn record_outcome(
        &self,
        event: ToolExecutionAuditEvent,
    ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
        let outcomes = Arc::clone(&self.outcomes);
        let fail_outcome = self.fail_outcome;
        Box::pin(async move {
            if fail_outcome {
                return Err(TestAuditError::Unavailable);
            }
            outcomes
                .lock()
                .expect("test audit lock is available")
                .push(event);
            Ok(())
        })
    }
}

pub(in crate::tests) fn lookup_tool(calls: Arc<AtomicUsize>) -> impl ToolExecutor {
    crate::TypedTool::new(
        ToolDefinition::new("lookup_order", json!({"type":"object"})).unwrap(),
        ToolRisk::ReadOnly,
        move |context: ToolExecutionContext, arguments: LookupArguments| {
            assert_eq!(arguments.id, 7);
            assert_eq!(context.tenant(), "tenant-a");
            assert_eq!(context.subject(), "user-7");
            assert_eq!(context.idempotency_key(), "external:order:7");
            calls.fetch_add(1, Ordering::SeqCst);
            future::ready(Ok::<LookupResult, Infallible>(LookupResult {
                status: "found",
            }))
        },
    )
}

pub(in crate::tests) fn ai_context() -> AiExecutionContext {
    AiExecutionContext::new("tenant-a", "user-7").expect("test context is valid")
}

pub(in crate::tests) fn tool_context() -> ToolExecutionContext {
    ToolExecutionContext::new(ai_context(), "external:order:7")
        .expect("test execution context is valid")
}
