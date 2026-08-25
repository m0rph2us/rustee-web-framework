//! Opt-in `PostgreSQL` durable AI tool audit contract tests.

use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures_util::{future, stream};
use rustee_ai::{
    AiExecutionContext, AiPipeline, AiProvider, AiUsageLedger, AiUsageReservation,
    AiUsageReservationDecision, ChatMessage, ChatRequest, ChatResponse,
    ExecutionAuditedToolRunError, MessageRole, ToolApprovalDecision, ToolApprovalPolicy, ToolCall,
    ToolDefinition, ToolExecutionContext, ToolExecutionOutcome, ToolRegistry, ToolRisk, TypedTool,
    Usage, UsageLedgerPipelineError,
};
use rustee_ai_audit_sqlx::{
    AI_USAGE_LEDGER_MIGRATION_SQL, PendingAuditLimit, PendingUsageLimit, PostgresAiUsageLedger,
    PostgresAiUsageLedgerError, PostgresToolAuditError, PostgresToolAuditSink,
    TOOL_AUDIT_MIGRATION_SQL,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{PgPool, postgres::PgPoolOptions};

fn database_url() -> String {
    std::env::var("RUSTEE_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rustee:rustee@127.0.0.1:5432/rustee".to_owned())
}

async fn pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url())
        .await
        .unwrap()
}

async fn reset_schema(pool: &PgPool) {
    sqlx::raw_sql(TOOL_AUDIT_MIGRATION_SQL)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("TRUNCATE rustee_ai_tool_audit")
        .execute(pool)
        .await
        .unwrap();
}

async fn reset_usage_schema(pool: &PgPool) {
    sqlx::raw_sql(AI_USAGE_LEDGER_MIGRATION_SQL)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("TRUNCATE rustee_ai_usage_ledger")
        .execute(pool)
        .await
        .unwrap();
}

#[derive(Clone, Copy)]
struct ApproveAll;

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

#[derive(Deserialize)]
struct LookupInput {
    id: u64,
}

#[derive(Serialize)]
struct LookupOutput {
    status: &'static str,
}

fn registry(calls: Arc<AtomicUsize>) -> ToolRegistry {
    let mut registry = ToolRegistry::new();
    registry
        .register(TypedTool::new(
            ToolDefinition::new("lookup_order", json!({"type": "object"})).unwrap(),
            ToolRisk::ReadOnly,
            move |_context: ToolExecutionContext, input: LookupInput| {
                assert_eq!(input.id, 7);
                calls.fetch_add(1, Ordering::SeqCst);
                future::ready(Ok::<LookupOutput, Infallible>(LookupOutput {
                    status: "found",
                }))
            },
        ))
        .unwrap();
    registry
}

fn context(idempotency_key: &str) -> ToolExecutionContext {
    ToolExecutionContext::new(
        AiExecutionContext::new("tenant-a", "user-7").unwrap(),
        idempotency_key,
    )
    .unwrap()
}

fn call(call_id: &str, arguments: serde_json::Value) -> ToolCall {
    ToolCall::new(call_id, "lookup_order", arguments).unwrap()
}

fn chat_request() -> ChatRequest {
    ChatRequest::new(
        "support.default",
        [ChatMessage::new(MessageRole::User, "status?").unwrap()],
    )
    .unwrap()
}

#[derive(Clone)]
struct UsageProvider {
    calls: Arc<AtomicUsize>,
}

impl AiProvider for UsageProvider {
    type Error = Infallible;

    fn complete(
        &self,
        _: ChatRequest,
    ) -> futures_util::future::BoxFuture<'static, Result<ChatResponse, Self::Error>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(future::ready(Ok(ChatResponse::new(
            "response-1",
            "resolved-model",
            "completed",
            [],
            Usage {
                input_tokens: 13,
                output_tokens: 8,
            },
        )
        .unwrap())))
    }

    fn stream(&self, _: ChatRequest) -> rustee_ai::AiEventStreamFuture<Self::Error> {
        let stream: rustee_ai::AiEventStream<Self::Error> = Box::pin(stream::empty());
        Box::pin(future::ready(Ok(stream)))
    }
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn audit_sink_persists_reconciliation_state_without_overwriting_conflicts() {
    let pool = pool().await;
    reset_schema(&pool).await;
    let sink = PostgresToolAuditSink::new(pool.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let registry = registry(Arc::clone(&calls));

    registry
        .execute_with_approval_audit(
            context("external:pending:7"),
            call("call-pending", json!({"id": 7})),
            &ApproveAll,
            &sink,
        )
        .await
        .unwrap();
    let pending = sink
        .pending(PendingAuditLimit::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].tenant(), "tenant-a");
    assert_eq!(pending[0].subject(), "user-7");
    assert_eq!(pending[0].idempotency_key(), "external:pending:7");
    assert_eq!(pending[0].call_id(), "call-pending");
    assert_eq!(pending[0].tool_name(), "lookup_order");
    assert_eq!(pending[0].risk(), ToolRisk::ReadOnly);
    let pending_debug = format!("{:?}", pending[0]);
    assert!(!pending_debug.contains("tenant-a"));
    assert!(!pending_debug.contains("external:pending:7"));

    registry
        .execute_with_execution_audit(
            context("external:settled:7"),
            call("call-settled", json!({"id": 7})),
            &ApproveAll,
            &sink,
        )
        .await
        .unwrap();
    registry
        .execute_with_execution_audit(
            context("external:settled:7"),
            call("call-settled", json!({"id": 7})),
            &ApproveAll,
            &sink,
        )
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let outcome = sqlx::query_scalar::<_, String>(
        "SELECT terminal_outcome FROM rustee_ai_tool_audit \
         WHERE tenant = $1 AND idempotency_key = $2",
    )
    .bind("tenant-a")
    .bind("external:settled:7")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(outcome, "succeeded");

    let identity_conflict = registry
        .execute_with_execution_audit(
            context("external:settled:7"),
            call("call-different", json!({"id": 7})),
            &ApproveAll,
            &sink,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        identity_conflict,
        ExecutionAuditedToolRunError::ApprovalAudit(PostgresToolAuditError::IdentityConflict)
    ));

    let outcome_conflict = registry
        .execute_with_execution_audit(
            context("external:settled:7"),
            call("call-settled", json!({"id": "not-a-number"})),
            &ApproveAll,
            &sink,
        )
        .await
        .unwrap_err();
    match outcome_conflict {
        ExecutionAuditedToolRunError::OutcomeAudit { event, source } => {
            assert_eq!(event.outcome(), ToolExecutionOutcome::Failed);
            assert!(matches!(source, PostgresToolAuditError::OutcomeConflict));
        }
        error => panic!("expected terminal outcome conflict, received {error:?}"),
    }
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        sink.pending(PendingAuditLimit::new(10).unwrap())
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn usage_ledger_blocks_duplicate_provider_attempts_and_persists_actual_usage() {
    let pool = pool().await;
    reset_usage_schema(&pool).await;
    let ledger = PostgresAiUsageLedger::new(pool.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let provider = UsageProvider {
        calls: Arc::clone(&calls),
    };
    let request = chat_request();
    let reservation = AiUsageReservation::for_request(
        AiExecutionContext::new("tenant-a", "user-7").unwrap(),
        "ai:usage:1",
        &request,
    )
    .unwrap();

    let response = AiPipeline::new(provider.clone())
        .complete_with_usage_ledger(request.clone(), reservation.clone(), &ledger)
        .await
        .unwrap();
    assert_eq!(response.usage().total_tokens(), 21);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let persisted = sqlx::query_as::<_, (String, i64, i64)>(
        "SELECT status, input_tokens, output_tokens \
         FROM rustee_ai_usage_ledger WHERE tenant = $1 AND idempotency_key = $2",
    )
    .bind("tenant-a")
    .bind("ai:usage:1")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(persisted.0, "completed");
    assert_eq!(persisted.1, 13);
    assert_eq!(persisted.2, 8);

    let duplicate = AiPipeline::new(provider)
        .complete_with_usage_ledger(request, reservation.clone(), &ledger)
        .await
        .unwrap_err();
    assert!(matches!(
        duplicate,
        UsageLedgerPipelineError::AlreadySettled
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        ledger.reserve(reservation.clone()).await.unwrap(),
        AiUsageReservationDecision::AlreadySettled
    );
    let settled_at_before = sqlx::query_scalar::<_, i64>(
        "UPDATE rustee_ai_usage_ledger \
         SET settled_at = TIMESTAMPTZ '2000-01-01 00:00:00+00' \
         WHERE tenant = $1 AND idempotency_key = $2 \
         RETURNING (EXTRACT(EPOCH FROM settled_at) * 1000000)::bigint",
    )
    .bind("tenant-a")
    .bind("ai:usage:1")
    .fetch_one(&pool)
    .await
    .unwrap();
    ledger
        .record_usage(reservation.settlement(Usage {
            input_tokens: 13,
            output_tokens: 8,
        }))
        .await
        .unwrap();
    let settled_at_after = sqlx::query_scalar::<_, i64>(
        "SELECT (EXTRACT(EPOCH FROM settled_at) * 1000000)::bigint \
         FROM rustee_ai_usage_ledger WHERE tenant = $1 AND idempotency_key = $2",
    )
    .bind("tenant-a")
    .bind("ai:usage:1")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(settled_at_after, settled_at_before);
    let usage_conflict = ledger
        .record_usage(reservation.settlement(Usage {
            input_tokens: 14,
            output_tokens: 8,
        }))
        .await
        .unwrap_err();
    assert!(matches!(
        usage_conflict,
        PostgresAiUsageLedgerError::UsageConflict
    ));
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn usage_ledger_exposes_pending_records_for_reconciliation() {
    let pool = pool().await;
    reset_usage_schema(&pool).await;
    let ledger = PostgresAiUsageLedger::new(pool);
    let pending_request = chat_request();
    let pending_reservation = AiUsageReservation::for_request(
        AiExecutionContext::new("tenant-a", "user-7").unwrap(),
        "ai:usage:pending",
        &pending_request,
    )
    .unwrap();
    assert_eq!(
        ledger.reserve(pending_reservation.clone()).await.unwrap(),
        AiUsageReservationDecision::Reserved
    );
    assert_eq!(
        ledger.reserve(pending_reservation.clone()).await.unwrap(),
        AiUsageReservationDecision::PendingReconciliation
    );
    let pending = ledger
        .pending(PendingUsageLimit::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(
        pending[0].reservation().idempotency_key(),
        "ai:usage:pending"
    );
    let pending_debug = format!("{:?}", pending[0]);
    assert!(!pending_debug.contains("tenant-a"));
    assert!(!pending_debug.contains("ai:usage:pending"));

    let identity_conflict = AiUsageReservation::from_metadata(
        AiExecutionContext::new("tenant-a", "user-7").unwrap(),
        "ai:usage:pending",
        "different-model",
        7,
        0,
        0,
    )
    .unwrap();
    assert!(matches!(
        ledger.reserve(identity_conflict).await.unwrap_err(),
        PostgresAiUsageLedgerError::IdentityConflict
    ));
}
