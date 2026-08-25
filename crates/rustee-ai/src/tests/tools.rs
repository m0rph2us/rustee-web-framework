use super::*;

#[test]
fn ai_execution_context_requires_trusted_identity_and_redacts_debug_output() {
    assert_eq!(
        AiExecutionContext::new(" ", "user").unwrap_err(),
        AiExecutionContextError::BlankTenant
    );
    assert_eq!(
        AiExecutionContext::new("tenant", " ").unwrap_err(),
        AiExecutionContextError::BlankSubject
    );
    let debug = format!("{:?}", ai_context());
    assert!(!debug.contains("tenant-a"));
    assert!(!debug.contains("user-7"));
}

#[test]
fn tool_execution_context_requires_an_idempotency_key_and_redacts_debug_output() {
    assert_eq!(
        ToolExecutionContext::new(ai_context(), " ").unwrap_err(),
        ToolExecutionContextError::BlankIdempotencyKey
    );
    let debug = format!("{:?}", tool_context());
    assert!(!debug.contains("tenant-a"));
    assert!(!debug.contains("external:order:7"));
}

#[test]
fn durable_identity_metadata_uses_shared_bounds() {
    let context =
        AiExecutionContext::new("t".repeat(MAX_TENANT_BYTES), "s".repeat(MAX_SUBJECT_BYTES))
            .expect("maximum durable identity values are valid");
    let key = "k".repeat(MAX_IDEMPOTENCY_KEY_BYTES);
    assert!(ToolExecutionContext::new(context.clone(), key.clone()).is_ok());
    assert!(AiUsageReservation::for_request(context.clone(), key.clone(), &request()).is_ok());
    assert!(AiUsageReservation::from_metadata(
        context,
        key,
        "m".repeat(MAX_MODEL_ALIAS_BYTES),
        0,
        0,
        0,
    )
    .is_ok());

    assert_eq!(
        AiExecutionContext::new("t".repeat(MAX_TENANT_BYTES + 1), "subject").unwrap_err(),
        AiExecutionContextError::TenantTooLong
    );
    assert_eq!(
        AiExecutionContext::new("tenant", "subject\0id").unwrap_err(),
        AiExecutionContextError::SubjectContainsNul
    );
    assert_eq!(
        ToolExecutionContext::new(ai_context(), "k".repeat(MAX_IDEMPOTENCY_KEY_BYTES + 1))
            .unwrap_err(),
        ToolExecutionContextError::IdempotencyKeyTooLong
    );
    assert_eq!(
        AiUsageReservation::for_request(ai_context(), "key\0value", &request()).unwrap_err(),
        AiUsageReservationError::IdempotencyKeyContainsNul
    );
    assert_eq!(
        AiUsageReservation::from_metadata(
            ai_context(),
            "usage:1",
            "m".repeat(MAX_MODEL_ALIAS_BYTES + 1),
            0,
            0,
            0,
        )
        .unwrap_err(),
        AiUsageReservationError::ModelAliasTooLong
    );
    assert_eq!(
        AiUsageReservation::from_metadata(ai_context(), "usage:1", "model\0alias", 0, 0, 0)
            .unwrap_err(),
        AiUsageReservationError::ModelAliasContainsNul
    );
}

#[test]
fn tool_names_allow_explicit_remote_namespaces() {
    let definition = ToolDefinition::new("orders.lookup.v1", json!({"type":"object"}))
        .expect("dotted remote tool name is portable");
    let call = ToolCall::new("call-remote-1", "orders.lookup.v1", json!({"id":7}))
        .expect("dotted remote tool call is portable");

    assert_eq!(definition.name(), "orders.lookup.v1");
    assert_eq!(call.name(), "orders.lookup.v1");
}

#[test]
fn tool_metadata_uses_the_shared_durable_limits() {
    assert!(ToolDefinition::new("x".repeat(MAX_TOOL_NAME_BYTES), json!({})).is_ok());
    assert!(
        ToolCall::new(
            "x".repeat(MAX_TOOL_CALL_ID_BYTES),
            "lookup_order",
            json!({}),
        )
        .is_ok()
    );
    assert_eq!(
        ToolDefinition::new("x".repeat(MAX_TOOL_NAME_BYTES + 1), json!({})).unwrap_err(),
        ToolError::ToolNameTooLong
    );
    assert_eq!(
        ToolCall::new(
            "x".repeat(MAX_TOOL_CALL_ID_BYTES + 1),
            "lookup_order",
            json!({}),
        )
        .unwrap_err(),
        ToolError::ToolCallIdTooLong
    );
    assert_eq!(
        ToolCall::new("call\0id", "lookup_order", json!({})).unwrap_err(),
        ToolError::ToolCallIdContainsNul
    );
}

#[test]
fn tool_registry_debug_reports_only_cardinality() {
    let mut registry = ToolRegistry::new();
    registry
        .register(lookup_tool(Arc::new(AtomicUsize::new(0))))
        .unwrap();

    let debug = format!("{registry:?}");
    assert!(debug.contains("tool_count: 1"));
    assert!(!debug.contains("lookup_order"));
}

#[tokio::test]
async fn tool_registry_requires_approval_before_handler_execution() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = ToolRegistry::new();
    registry.register(lookup_tool(Arc::clone(&calls))).unwrap();

    let error = registry
        .execute(
            tool_context(),
            ToolCall::new("call-1", "lookup_order", json!({"id": 7})).unwrap(),
            &DenyAllToolApproval,
        )
        .await
        .unwrap_err();

    assert!(matches!(error, ToolRunError::Denied { .. }));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn denied_execution_audit_skips_audit_sinks_and_the_handler() {
    let calls = Arc::new(AtomicUsize::new(0));
    let approvals = Arc::new(Mutex::new(Vec::new()));
    let outcomes = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ToolRegistry::new();
    registry.register(lookup_tool(Arc::clone(&calls))).unwrap();

    let error = registry
        .execute_with_execution_audit(
            tool_context(),
            ToolCall::new("call-denied-audit", "lookup_order", json!({"id": 7})).unwrap(),
            &DenyAllToolApproval,
            &CapturingExecutionAudit {
                approvals: Arc::clone(&approvals),
                outcomes: Arc::clone(&outcomes),
                fail_approval: false,
                fail_outcome: false,
            },
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        ExecutionAuditedToolRunError::Run(ToolRunError::Denied { .. })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(
        approvals
            .lock()
            .expect("test audit lock is available")
            .is_empty()
    );
    assert!(
        outcomes
            .lock()
            .expect("test audit lock is available")
            .is_empty()
    );
}

#[tokio::test]
async fn typed_tool_rejects_invalid_arguments_before_handler_execution() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = ToolRegistry::new();
    registry.register(lookup_tool(Arc::clone(&calls))).unwrap();

    let error = registry
        .execute(
            tool_context(),
            ToolCall::new("call-2", "lookup_order", json!({"id":"not-a-number"})).unwrap(),
            &ApproveAll,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        ToolRunError::Execution(ToolExecutionError::InvalidArguments)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn approved_typed_tool_returns_redacted_debug_result() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = ToolRegistry::new();
    registry.register(lookup_tool(calls)).unwrap();

    let result = registry
        .execute(
            tool_context(),
            ToolCall::new("call-3", "lookup_order", json!({"id": 7})).unwrap(),
            &ApproveAll,
        )
        .await
        .unwrap();

    assert_eq!(result.content(), &json!({"status":"found"}));
    assert!(!format!("{result:?}").contains("found"));
}

#[tokio::test]
async fn approved_tool_audit_persists_before_the_handler_runs() {
    let calls = Arc::new(AtomicUsize::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ToolRegistry::new();
    registry.register(lookup_tool(Arc::clone(&calls))).unwrap();

    let result = registry
        .execute_with_approval_audit(
            tool_context(),
            ToolCall::new("call-audit-1", "lookup_order", json!({"id": 7})).unwrap(),
            &ApproveAll,
            &CapturingAudit {
                events: Arc::clone(&events),
                should_fail: false,
            },
        )
        .await
        .unwrap();

    assert_eq!(result.content(), &json!({"status":"found"}));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let events = events.lock().expect("test audit lock is available");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].context().tenant(), "tenant-a");
    assert_eq!(events[0].context().subject(), "user-7");
    assert_eq!(events[0].idempotency_key(), "external:order:7");
    assert_eq!(events[0].call_id(), "call-audit-1");
    assert_eq!(events[0].tool_name(), "lookup_order");
    assert_eq!(events[0].risk(), ToolRisk::ReadOnly);
    let debug = format!("{:?}", events[0]);
    assert!(!debug.contains("tenant-a"));
    assert!(!debug.contains("call-audit-1"));
    assert!(!debug.contains("external:order:7"));
}

#[tokio::test]
async fn execution_audit_records_a_terminal_success_after_the_handler() {
    let calls = Arc::new(AtomicUsize::new(0));
    let approvals = Arc::new(Mutex::new(Vec::new()));
    let outcomes = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ToolRegistry::new();
    registry.register(lookup_tool(Arc::clone(&calls))).unwrap();

    let result = registry
        .execute_with_execution_audit(
            tool_context(),
            ToolCall::new("call-execution-1", "lookup_order", json!({"id": 7})).unwrap(),
            &ApproveAll,
            &CapturingExecutionAudit {
                approvals: Arc::clone(&approvals),
                outcomes: Arc::clone(&outcomes),
                fail_approval: false,
                fail_outcome: false,
            },
        )
        .await
        .unwrap();

    assert_eq!(result.content(), &json!({"status":"found"}));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        approvals
            .lock()
            .expect("test audit lock is available")
            .len(),
        1
    );
    let outcomes = outcomes.lock().expect("test audit lock is available");
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].outcome(), ToolExecutionOutcome::Succeeded);
    assert_eq!(outcomes[0].approval().idempotency_key(), "external:order:7");
    assert!(!format!("{:?}", outcomes[0]).contains("external:order:7"));
}

#[tokio::test]
async fn execution_audit_records_a_terminal_failure_before_returning_handler_error() {
    let calls = Arc::new(AtomicUsize::new(0));
    let outcomes = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ToolRegistry::new();
    registry.register(lookup_tool(Arc::clone(&calls))).unwrap();

    let error = registry
        .execute_with_execution_audit(
            tool_context(),
            ToolCall::new("call-execution-2", "lookup_order", json!({"id":"bad"})).unwrap(),
            &ApproveAll,
            &CapturingExecutionAudit {
                approvals: Arc::new(Mutex::new(Vec::new())),
                outcomes: Arc::clone(&outcomes),
                fail_approval: false,
                fail_outcome: false,
            },
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        ExecutionAuditedToolRunError::Run(ToolRunError::Execution(
            ToolExecutionError::InvalidArguments
        ))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let outcomes = outcomes.lock().expect("test audit lock is available");
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].outcome(), ToolExecutionOutcome::Failed);
}

#[tokio::test]
async fn failed_execution_outcome_audit_requires_reconciliation_after_the_handler() {
    let calls = Arc::new(AtomicUsize::new(0));
    let approvals = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ToolRegistry::new();
    registry.register(lookup_tool(Arc::clone(&calls))).unwrap();

    let error = registry
        .execute_with_execution_audit(
            tool_context(),
            ToolCall::new("call-execution-3", "lookup_order", json!({"id": 7})).unwrap(),
            &ApproveAll,
            &CapturingExecutionAudit {
                approvals: Arc::clone(&approvals),
                outcomes: Arc::new(Mutex::new(Vec::new())),
                fail_approval: false,
                fail_outcome: true,
            },
        )
        .await
        .unwrap_err();

    match error {
        ExecutionAuditedToolRunError::OutcomeAudit { event, source } => {
            assert_eq!(source, TestAuditError::Unavailable);
            assert_eq!(event.outcome(), ToolExecutionOutcome::Succeeded);
            assert_eq!(event.approval().call_id(), "call-execution-3");
            assert_eq!(event.approval().idempotency_key(), "external:order:7");
        }
        error => panic!("expected outcome audit failure, received {error:?}"),
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        approvals
            .lock()
            .expect("test audit lock is available")
            .len(),
        1
    );
}

#[tokio::test]
async fn failed_approval_audit_blocks_the_tool_handler() {
    let calls = Arc::new(AtomicUsize::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ToolRegistry::new();
    registry.register(lookup_tool(Arc::clone(&calls))).unwrap();

    let error = registry
        .execute_with_approval_audit(
            tool_context(),
            ToolCall::new("call-audit-2", "lookup_order", json!({"id": 7})).unwrap(),
            &ApproveAll,
            &CapturingAudit {
                events: Arc::clone(&events),
                should_fail: true,
            },
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        AuditedToolRunError::Audit(TestAuditError::Unavailable)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(
        events
            .lock()
            .expect("test audit lock is available")
            .is_empty()
    );
}

#[test]
fn stream_event_debug_output_redacts_model_text_and_tool_arguments() {
    let text = AiStreamEvent::TextDelta("private streamed completion".to_owned());
    let tool = AiStreamEvent::ToolCall(
        ToolCall::new(
            "provider-call-secret",
            "orders.lookup",
            json!({"customer_note":"private tool argument"}),
        )
        .unwrap(),
    );
    let output = format!("{text:?} {tool:?}");

    assert!(!output.contains("private streamed completion"));
    assert!(!output.contains("provider-call-secret"));
    assert!(!output.contains("private tool argument"));
    assert!(output.contains("byte_length"));
    assert!(output.contains("orders.lookup"));
}
