use super::*;

struct LeakyBudget;

impl std::fmt::Debug for LeakyBudget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("LeakyBudget(private-budget-store-configuration)")
    }
}

#[test]
fn budget_advisor_debug_does_not_delegate_to_policy_diagnostics() {
    let advisor = BudgetAdvisor::new(ai_context(), LeakyBudget);

    let debug = format!("{advisor:?}");

    assert!(debug.contains("budget_type"));
    assert!(!debug.contains("private-budget-store-configuration"));
}

#[tokio::test]
async fn usage_ledger_reserves_before_completion_and_records_actual_usage() {
    let ledger = usage_ledger(AiUsageReservationDecision::Reserved, false);
    let response = AiPipeline::new(Fake)
        .complete_with_usage_ledger(request(), usage_reservation("ai:request:1"), &ledger)
        .await
        .unwrap();

    assert_eq!(response.usage().total_tokens(), 5);
    let reservations = ledger
        .reservations
        .lock()
        .expect("test usage ledger lock is available");
    assert_eq!(reservations.len(), 1);
    assert_eq!(reservations[0].context().tenant(), "tenant-a");
    assert_eq!(reservations[0].idempotency_key(), "ai:request:1");
    assert!(!format!("{:?}", reservations[0]).contains("ai:request:1"));
    drop(reservations);
    let settlements = ledger
        .settlements
        .lock()
        .expect("test usage ledger lock is available");
    assert_eq!(settlements.len(), 1);
    assert_eq!(settlements[0].usage(), response.usage());
}

#[tokio::test]
async fn usage_ledger_blocks_a_pending_retry_before_provider_invocation() {
    let invocations = Arc::new(AtomicUsize::new(0));
    let ledger = usage_ledger(AiUsageReservationDecision::PendingReconciliation, false);
    let error = AiPipeline::new(CountingProvider {
        invocations: Arc::clone(&invocations),
    })
    .complete_with_usage_ledger(request(), usage_reservation("ai:request:pending"), &ledger)
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        UsageLedgerPipelineError::PendingReconciliation
    ));
    assert_eq!(invocations.load(Ordering::SeqCst), 0);
    assert_eq!(
        ledger
            .reservations
            .lock()
            .expect("test usage ledger lock is available")
            .len(),
        1
    );
}

#[tokio::test]
async fn usage_ledger_blocks_non_starting_decisions_before_provider_invocation() {
    for (decision, key) in [
        (AiUsageReservationDecision::Denied, "ai:request:denied"),
        (
            AiUsageReservationDecision::AlreadySettled,
            "ai:request:settled",
        ),
    ] {
        let invocations = Arc::new(AtomicUsize::new(0));
        let ledger = usage_ledger(decision, false);
        let error = AiPipeline::new(CountingProvider {
            invocations: Arc::clone(&invocations),
        })
        .complete_with_usage_ledger(request(), usage_reservation(key), &ledger)
        .await
        .unwrap_err();

        match decision {
            AiUsageReservationDecision::Denied => {
                assert!(matches!(error, UsageLedgerPipelineError::Denied));
            }
            AiUsageReservationDecision::AlreadySettled => {
                assert!(matches!(error, UsageLedgerPipelineError::AlreadySettled));
            }
            AiUsageReservationDecision::Reserved
            | AiUsageReservationDecision::PendingReconciliation => unreachable!(),
        }
        assert_eq!(invocations.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn usage_ledger_settles_stream_only_after_terminal_usage_event() {
    let invocations = Arc::new(AtomicUsize::new(0));
    let ledger = usage_ledger(AiUsageReservationDecision::Reserved, false);
    let events = AiPipeline::new(CountingProvider {
        invocations: Arc::clone(&invocations),
    })
    .stream_with_usage_ledger(request(), usage_reservation("ai:stream:1"), &ledger)
    .await
    .unwrap()
    .collect::<Vec<_>>()
    .await;

    assert_eq!(events.len(), 2);
    assert!(matches!(
        events[1],
        Ok(AiStreamEvent::Completed(Usage {
            input_tokens: 3,
            output_tokens: 2,
        }))
    ));
    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    let settlements = ledger
        .settlements
        .lock()
        .expect("test usage ledger lock is available");
    assert_eq!(settlements.len(), 1);
    assert_eq!(settlements[0].usage().total_tokens(), 5);
}

#[tokio::test]
async fn usage_ledger_stream_stops_polling_after_completion() {
    let tail_polls = Arc::new(AtomicUsize::new(0));
    let ledger = usage_ledger(AiUsageReservationDecision::Reserved, false);
    let events = AiPipeline::new(PostCompletionProvider {
        tail_polls: Arc::clone(&tail_polls),
    })
    .stream_with_usage_ledger(request(), usage_reservation("ai:stream:terminal"), &ledger)
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
    assert_eq!(
        ledger
            .settlements
            .lock()
            .expect("test usage ledger lock is available")
            .len(),
        1
    );
}

#[tokio::test]
async fn usage_ledger_settlement_failure_returns_the_completed_response_without_retrying() {
    let invocations = Arc::new(AtomicUsize::new(0));
    let ledger = usage_ledger(AiUsageReservationDecision::Reserved, true);
    let error = AiPipeline::new(CountingProvider {
        invocations: Arc::clone(&invocations),
    })
    .complete_with_usage_ledger(
        request(),
        usage_reservation("ai:request:reconcile"),
        &ledger,
    )
    .await
    .unwrap_err();

    match error {
        UsageLedgerPipelineError::Settlement { response, source } => {
            assert_eq!(response.content(), "complete");
            assert_eq!(source, TestUsageLedgerError::Unavailable);
        }
        error => panic!("expected settlement failure, received {error:?}"),
    }
    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert!(
        ledger
            .settlements
            .lock()
            .expect("test usage ledger lock is available")
            .is_empty()
    );
}

#[test]
fn usage_reservation_requires_a_stable_key_and_redacts_metadata() {
    assert_eq!(
        AiUsageReservation::for_request(ai_context(), " ", &request()).unwrap_err(),
        AiUsageReservationError::BlankIdempotencyKey
    );
    let reservation = usage_reservation("ai:request:redacted");
    let debug = format!("{reservation:?}");
    assert!(!debug.contains("tenant-a"));
    assert!(!debug.contains("ai:request:redacted"));
    assert!(!debug.contains("support.default"));
}

#[tokio::test]
async fn usage_ledger_stream_settlement_failure_is_returned_after_the_completion_event() {
    let ledger = usage_ledger(AiUsageReservationDecision::Reserved, true);
    let events = AiPipeline::new(CountingProvider {
        invocations: Arc::new(AtomicUsize::new(0)),
    })
    .stream_with_usage_ledger(request(), usage_reservation("ai:stream:reconcile"), &ledger)
    .await
    .unwrap()
    .collect::<Vec<_>>()
    .await;

    assert_eq!(events.len(), 2);
    assert!(matches!(
        events[1],
        Err(UsageLedgerStreamError::Ledger(
            TestUsageLedgerError::Unavailable
        ))
    ));
}

#[tokio::test]
async fn advisor_and_usage_ledger_reserve_the_final_enriched_request_before_completion() {
    let invocations = Arc::new(AtomicUsize::new(0));
    let advisor = ContextAdvisor::new();
    let ledger = usage_ledger(AiUsageReservationDecision::Reserved, false);
    let response = AiPipeline::new(CountingProvider {
        invocations: Arc::clone(&invocations),
    })
    .complete_with_advisor_and_usage_ledger(
        request(),
        &advisor,
        ai_context(),
        "ai:advised:completion",
        &ledger,
    )
    .await
    .unwrap();

    assert_eq!(response.content(), "complete");
    assert_eq!(advisor.before.load(Ordering::SeqCst), 1);
    assert_eq!(advisor.response.load(Ordering::SeqCst), 1);
    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    let reservations = ledger
        .reservations
        .lock()
        .expect("test usage ledger lock is available");
    assert_eq!(reservations.len(), 1);
    assert_eq!(reservations[0].request().input_characters(), 8);
    assert_eq!(reservations[0].idempotency_key(), "ai:advised:completion");
    drop(reservations);
    assert_eq!(
        ledger
            .settlements
            .lock()
            .expect("test usage ledger lock is available")
            .len(),
        1
    );
}

#[tokio::test]
async fn advisor_and_usage_ledger_settle_raw_stream_usage_before_advisor_output() {
    let invocations = Arc::new(AtomicUsize::new(0));
    let advisor = ContextAdvisor::new();
    let ledger = usage_ledger(AiUsageReservationDecision::Reserved, false);
    let events = AiPipeline::new(CountingProvider {
        invocations: Arc::clone(&invocations),
    })
    .stream_with_advisor_and_usage_ledger(
        request(),
        &advisor,
        ai_context(),
        "ai:advised:stream",
        &ledger,
    )
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
        events[1],
        Ok(AiStreamEvent::Completed(Usage {
            input_tokens: 3,
            output_tokens: 2,
        }))
    ));
    assert_eq!(advisor.before.load(Ordering::SeqCst), 1);
    assert_eq!(advisor.stream.load(Ordering::SeqCst), 2);
    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    let settlements = ledger
        .settlements
        .lock()
        .expect("test usage ledger lock is available");
    assert_eq!(settlements.len(), 1);
    assert_eq!(settlements[0].usage().total_tokens(), 5);
}

#[tokio::test]
async fn advisor_and_usage_ledger_reject_blank_provider_keys_before_invocation() {
    let invocations = Arc::new(AtomicUsize::new(0));
    let advisor = ContextAdvisor::new();
    let ledger = usage_ledger(AiUsageReservationDecision::Reserved, false);
    let error = AiPipeline::new(CountingProvider {
        invocations: Arc::clone(&invocations),
    })
    .complete_with_advisor_and_usage_ledger(request(), &advisor, ai_context(), " ", &ledger)
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        AdvisedUsageLedgerPipelineError::ReservationMetadata(
            AiUsageReservationError::BlankIdempotencyKey
        )
    ));
    assert_eq!(advisor.before.load(Ordering::SeqCst), 1);
    assert_eq!(invocations.load(Ordering::SeqCst), 0);
    assert!(
        ledger
            .reservations
            .lock()
            .expect("test usage ledger lock is available")
            .is_empty()
    );
}
