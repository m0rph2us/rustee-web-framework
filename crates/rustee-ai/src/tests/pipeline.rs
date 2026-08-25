use super::*;

#[derive(Deserialize)]
struct Output {
    answer: u64,
}
#[tokio::test]
async fn pipeline_parses_structured_output() {
    assert_eq!(
        AiPipeline::new(Fake)
            .complete(request())
            .await
            .unwrap()
            .parse_json::<Output>()
            .unwrap()
            .answer,
        42
    );
}

#[test]
fn pipeline_debug_does_not_delegate_to_provider_diagnostics() {
    let pipeline = AiPipeline::new(LeakyDebugProvider);

    let debug = format!("{pipeline:?}");
    assert!(debug.contains("provider_type"));
    assert!(!debug.contains("private-provider-credential"));
}

#[test]
fn usage_ledger_diagnostics_redact_adapter_details_and_preserve_sources() {
    let pipeline = UsageLedgerPipelineError::<LeakyDiagnosticError, LeakyDiagnosticError>::Provider(
        LeakyDiagnosticError,
    );
    let stream = UsageLedgerStreamError::<LeakyDiagnosticError, LeakyDiagnosticError>::Provider(
        LeakyDiagnosticError,
    );
    let advised_pipeline = AdvisedUsageLedgerPipelineError::<
        LeakyDiagnosticError,
        LeakyDiagnosticError,
        LeakyDiagnosticError,
    >::Advisor(LeakyDiagnosticError);
    let advised_stream = AdvisedUsageLedgerStreamError::<
        LeakyDiagnosticError,
        LeakyDiagnosticError,
        LeakyDiagnosticError,
    >::Advisor(LeakyDiagnosticError);

    for error in [
        &pipeline as &dyn std::error::Error,
        &stream as &dyn std::error::Error,
        &advised_pipeline as &dyn std::error::Error,
        &advised_stream as &dyn std::error::Error,
    ] {
        assert!(!format!("{error:?}").contains("private-provider-or-ledger-detail"));
        assert!(
            !error
                .to_string()
                .contains("private-provider-or-ledger-detail")
        );
        assert!(std::error::Error::source(error).is_some());
    }
}

#[test]
fn pipeline_diagnostics_redact_adapter_details_and_preserve_sources() {
    let pipeline = PipelineError::Provider(LeakyDiagnosticError);
    let budget = BudgetAdvisorError::Policy(LeakyDiagnosticError);
    let advised_pipeline_provider = AdvisedPipelineError::<
        LeakyDiagnosticError,
        LeakyDiagnosticError,
    >::Provider(LeakyDiagnosticError);
    let advised_pipeline_advisor =
        AdvisedPipelineError::<LeakyDiagnosticError, LeakyDiagnosticError>::Advisor(
            LeakyDiagnosticError,
        );
    let advised_stream_provider =
        AdvisedStreamError::<LeakyDiagnosticError, LeakyDiagnosticError>::Provider(
            LeakyDiagnosticError,
        );
    let advised_stream_advisor =
        AdvisedStreamError::<LeakyDiagnosticError, LeakyDiagnosticError>::Advisor(
            LeakyDiagnosticError,
        );

    for error in [
        &pipeline as &dyn std::error::Error,
        &budget as &dyn std::error::Error,
        &advised_pipeline_provider as &dyn std::error::Error,
        &advised_pipeline_advisor as &dyn std::error::Error,
        &advised_stream_provider as &dyn std::error::Error,
        &advised_stream_advisor as &dyn std::error::Error,
    ] {
        assert!(!format!("{error:?}").contains("private-provider-or-ledger-detail"));
        assert!(
            !error
                .to_string()
                .contains("private-provider-or-ledger-detail")
        );
        assert!(std::error::Error::source(error).is_some());
    }
}

#[test]
fn tool_diagnostics_redact_adapter_details_and_preserve_sources() {
    let tool_run = ToolRunError::Approval(LeakyDiagnosticError);
    let audited = AuditedToolRunError::<LeakyDiagnosticError, LeakyDiagnosticError>::Audit(
        LeakyDiagnosticError,
    );
    let execution_audited = ExecutionAuditedToolRunError::<
        LeakyDiagnosticError,
        LeakyDiagnosticError,
    >::ApprovalAudit(LeakyDiagnosticError);

    for error in [
        &tool_run as &dyn std::error::Error,
        &audited as &dyn std::error::Error,
        &execution_audited as &dyn std::error::Error,
    ] {
        assert!(!format!("{error:?}").contains("private-provider-or-ledger-detail"));
        assert!(
            !error
                .to_string()
                .contains("private-provider-or-ledger-detail")
        );
        assert!(std::error::Error::source(error).is_some());
    }
}

#[test]
fn policy_rejects_input_before_provider() {
    assert!(
        AiPolicy {
            max_input_characters: 2,
            max_tools: 1,
            max_tool_results: 1,
        }
        .validate(&request())
        .is_err()
    );
}

#[test]
fn input_accounting_and_policy_include_provider_bound_tool_result_content() {
    let content = json!({
        "status":"private tool result",
        "locale":"\u{c548}\u{b155}\u{d558}\u{c138}\u{c694}",
    });
    let request = request().with_tool_results([serde_json::from_value::<ToolResult>(json!({
        "call_id":"call_1",
        "name":"lookup_order",
        "content":content,
    }))
    .unwrap()]);
    let actual = "status?".chars().count() + content.to_string().chars().count();

    assert_eq!(request.input_characters(), actual);
    assert_eq!(
        AiBudgetRequest::from_request(&request).input_characters(),
        actual
    );
    assert_eq!(
        AiPolicy {
            max_input_characters: actual - 1,
            max_tools: 1,
            max_tool_results: 1,
        }
        .validate(&request),
        Err(PolicyError::InputTooLarge {
            limit: actual - 1,
            actual,
        })
    );
}

#[test]
fn policy_rejects_tool_result_cardinality_before_input_size() {
    let request = request().with_tool_results([
        serde_json::from_value::<ToolResult>(json!({
            "call_id":"call_1",
            "name":"lookup_order",
            "content":{"private":"x".repeat(1_024)},
        }))
        .unwrap(),
        serde_json::from_value::<ToolResult>(json!({
            "call_id":"call_2",
            "name":"lookup_order",
            "content":{"private":"y".repeat(1_024)},
        }))
        .unwrap(),
    ]);

    assert_eq!(
        AiPolicy {
            max_input_characters: 1,
            max_tools: 1,
            max_tool_results: 1,
        }
        .validate(&request),
        Err(PolicyError::TooManyToolResults {
            limit: 1,
            actual: 2,
        })
    );
}

#[tokio::test]
async fn pipeline_rejects_excessive_tool_results_before_provider_invocation() {
    let invocations = Arc::new(AtomicUsize::new(0));
    let result = AiPipeline::new(CountingProvider {
        invocations: Arc::clone(&invocations),
    })
    .with_policy(AiPolicy {
        max_input_characters: 100,
        max_tools: 1,
        max_tool_results: 1,
    })
    .complete(
        request().with_tool_results([
            serde_json::from_value::<ToolResult>(json!({
                "call_id":"call_1",
                "name":"lookup_order",
                "content":{"status":"one"},
            }))
            .unwrap(),
            serde_json::from_value::<ToolResult>(json!({
                "call_id":"call_2",
                "name":"lookup_order",
                "content":{"status":"two"},
            }))
            .unwrap(),
        ]),
    )
    .await;

    assert!(matches!(
        result,
        Err(PipelineError::Policy(PolicyError::TooManyToolResults {
            limit: 1,
            actual: 2,
        }))
    ));
    assert_eq!(invocations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn advisor_context_is_checked_before_provider_invocation() {
    let invocations = Arc::new(AtomicUsize::new(0));
    let advisor = ContextAdvisor::new();
    let error = AiPipeline::new(CountingProvider {
        invocations: Arc::clone(&invocations),
    })
    .with_policy(AiPolicy {
        max_input_characters: 7,
        max_tools: 1,
        max_tool_results: 1,
    })
    .complete_with_advisor(request(), &advisor)
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        AdvisedPipelineError::Policy(PolicyError::InputTooLarge {
            limit: 7,
            actual: 8,
        })
    ));
    assert_eq!(advisor.before.load(Ordering::SeqCst), 1);
    assert_eq!(invocations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn advisor_runs_for_complete_and_each_stream_event() {
    let invocations = Arc::new(AtomicUsize::new(0));
    let pipeline = AiPipeline::new(CountingProvider {
        invocations: Arc::clone(&invocations),
    });
    let advisor = ContextAdvisor::new();

    let response = pipeline
        .complete_with_advisor(request(), &advisor)
        .await
        .unwrap();
    assert_eq!(response.content(), "complete");

    let events = pipeline
        .stream_with_advisor(request(), &advisor)
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    assert_eq!(events.len(), 2);
    assert_eq!(
        events[0].as_ref().unwrap(),
        &AiStreamEvent::TextDelta("delta!".to_owned())
    );
    assert!(matches!(
        events[1].as_ref().unwrap(),
        AiStreamEvent::Completed(Usage {
            input_tokens: 3,
            output_tokens: 2,
        })
    ));
    assert_eq!(advisor.before.load(Ordering::SeqCst), 2);
    assert_eq!(advisor.response.load(Ordering::SeqCst), 1);
    assert_eq!(advisor.stream.load(Ordering::SeqCst), 2);
    assert_eq!(invocations.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn pipeline_stream_stops_polling_after_completion() {
    let tail_polls = Arc::new(AtomicUsize::new(0));
    let events = AiPipeline::new(PostCompletionProvider {
        tail_polls: Arc::clone(&tail_polls),
    })
    .stream(request())
    .await
    .unwrap()
    .collect::<Vec<_>>()
    .await;

    assert!(matches!(
        events.as_slice(),
        [Ok(AiStreamEvent::Completed(Usage {
            input_tokens: 3,
            output_tokens: 2,
        }))]
    ));
    assert_eq!(tail_polls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn pipeline_stream_stops_polling_after_advisor_failure() {
    let tail_polls = Arc::new(AtomicUsize::new(0));
    let events = AiPipeline::new(PostAdvisorFailureProvider {
        tail_polls: Arc::clone(&tail_polls),
    })
    .stream_with_advisor(request(), &RejectingStreamAdvisor)
    .await
    .unwrap()
    .collect::<Vec<_>>()
    .await;

    assert!(matches!(
        events.as_slice(),
        [Err(AdvisedStreamError::Advisor(
            TestUsageLedgerError::Unavailable
        ))]
    ));
    assert_eq!(tail_polls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn budget_advisor_denies_before_provider_invocation_without_prompt_content() {
    let invocations = Arc::new(AtomicUsize::new(0));
    let admissions = Arc::new(Mutex::new(Vec::new()));
    let advisor = BudgetAdvisor::new(
        ai_context(),
        DenyingBudget {
            admissions: Arc::clone(&admissions),
        },
    );

    let error = AiPipeline::new(CountingProvider {
        invocations: Arc::clone(&invocations),
    })
    .complete_with_advisor(request(), &advisor)
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        AdvisedPipelineError::Advisor(BudgetAdvisorError::Denied)
    ));
    assert_eq!(invocations.load(Ordering::SeqCst), 0);

    let admissions = admissions.lock().expect("test budget lock is available");
    assert_eq!(admissions.len(), 1);
    let (context, request) = &admissions[0];
    assert_eq!(context.tenant(), "tenant-a");
    assert_eq!(context.subject(), "user-7");
    assert_eq!(request.model(), "support.default");
    assert_eq!(request.input_characters(), 7);
    assert_eq!(request.tool_count(), 0);
    assert_eq!(request.tool_result_count(), 0);
    assert!(!format!("{request:?}").contains("status?"));
}
