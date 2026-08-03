//! Opt-in `PostgreSQL` durable AI evaluation run-ledger contract tests.

use rustee_ai_eval::{AiEvaluationReference, AiEvaluationRunLedger, AiEvaluationRunReservation};
use rustee_ai_eval_sqlx::{
    AI_EVALUATION_RUN_LEDGER_MIGRATION_SQL, PendingAiEvaluationRunLimit,
    PostgresAiEvaluationRunLedger, PostgresAiEvaluationRunLedgerError,
};
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
    sqlx::raw_sql(AI_EVALUATION_RUN_LEDGER_MIGRATION_SQL)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("TRUNCATE rustee_ai_evaluation_run_ledger")
        .execute(pool)
        .await
        .unwrap();
}

fn reference(catalog_id: &str, run_key: &str) -> AiEvaluationReference {
    AiEvaluationReference::new("tenant-a", catalog_id, run_key).unwrap()
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn evaluation_run_ledger_preserves_pending_state_and_reuses_only_exact_completions() {
    let pool = pool().await;
    reset_schema(&pool).await;
    let ledger = PostgresAiEvaluationRunLedger::new(pool.clone());
    let pending_reference = reference("catalog-7", "run-pending-7");

    assert_eq!(
        ledger.reserve(pending_reference.clone()).await.unwrap(),
        AiEvaluationRunReservation::Reserved
    );
    assert_eq!(
        ledger.reserve(pending_reference.clone()).await.unwrap(),
        AiEvaluationRunReservation::Pending
    );
    let pending = ledger
        .pending(PendingAiEvaluationRunLimit::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].reference(), &pending_reference);
    let pending_debug = format!("{:?}", pending[0]);
    assert!(!pending_debug.contains("tenant-a"));
    assert!(!pending_debug.contains("run-pending-7"));

    ledger
        .record_completed(pending_reference.clone())
        .await
        .unwrap();
    ledger
        .record_completed(pending_reference.clone())
        .await
        .unwrap();
    assert_eq!(
        ledger.reserve(pending_reference.clone()).await.unwrap(),
        AiEvaluationRunReservation::Completed
    );
    assert!(
        ledger
            .pending(PendingAiEvaluationRunLimit::new(10).unwrap())
            .await
            .unwrap()
            .is_empty()
    );

    let reviewed_retry = reference("catalog-7", "run-pending-7-retry-1");
    assert_eq!(
        ledger.reserve(reviewed_retry).await.unwrap(),
        AiEvaluationRunReservation::Reserved
    );

    let conflict = ledger
        .reserve(reference("catalog-other", "run-pending-7"))
        .await
        .unwrap_err();
    assert!(matches!(
        conflict,
        PostgresAiEvaluationRunLedgerError::IdentityConflict
    ));
    let status = sqlx::query_scalar::<_, String>(
        "SELECT status FROM rustee_ai_evaluation_run_ledger WHERE scope = $1 AND run_key = $2",
    )
    .bind("tenant-a")
    .bind("run-pending-7")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "completed");
}
