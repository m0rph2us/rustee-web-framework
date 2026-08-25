//! Opt-in `PostgreSQL` durable AI batch artifact-ledger contract tests.

use rustee_ai_batch::{
    AiBatchArtifactKind, AiBatchArtifactLedger, AiBatchArtifactReference,
    AiBatchArtifactReservation, AiBatchReference, MAX_BATCH_IDENTIFIER_BYTES,
};
use rustee_ai_batch_sqlx::{
    AI_BATCH_ARTIFACT_LEDGER_IDENTIFIER_BOUND_MIGRATION_SQL,
    AI_BATCH_ARTIFACT_LEDGER_MIGRATION_SQL, PendingAiBatchArtifactLimit,
    PostgresAiBatchArtifactLedger, PostgresAiBatchArtifactLedgerError,
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
    sqlx::raw_sql(AI_BATCH_ARTIFACT_LEDGER_MIGRATION_SQL)
        .execute(pool)
        .await
        .unwrap();
    sqlx::raw_sql(AI_BATCH_ARTIFACT_LEDGER_IDENTIFIER_BOUND_MIGRATION_SQL)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("TRUNCATE rustee_ai_batch_artifact_ledger")
        .execute(pool)
        .await
        .unwrap();
}

fn reference(file_id: &str, reconciliation_key: &str) -> AiBatchArtifactReference {
    AiBatchArtifactReference::new(
        AiBatchReference::new("tenant-a", "catalog-7", "run-7").unwrap(),
        AiBatchArtifactKind::Output,
        file_id,
        reconciliation_key,
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn artifact_ledger_preserves_pending_state_and_reuses_only_exact_reconciliations() {
    let pool = pool().await;
    reset_schema(&pool).await;
    let ledger = PostgresAiBatchArtifactLedger::new(pool.clone());
    let pending_reference = reference("file-pending-7", "reconcile-pending-7");

    assert_eq!(
        ledger.reserve(pending_reference.clone()).await.unwrap(),
        AiBatchArtifactReservation::Reserved
    );
    assert_eq!(
        ledger.reserve(pending_reference.clone()).await.unwrap(),
        AiBatchArtifactReservation::Pending
    );
    let pending = ledger
        .pending(PendingAiBatchArtifactLimit::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].reference(), &pending_reference);
    let pending_debug = format!("{:?}", pending[0]);
    assert!(!pending_debug.contains("tenant-a"));
    assert!(!pending_debug.contains("file-pending-7"));

    ledger
        .record_reconciled(pending_reference.clone())
        .await
        .unwrap();
    ledger
        .record_reconciled(pending_reference.clone())
        .await
        .unwrap();
    assert_eq!(
        ledger.reserve(pending_reference.clone()).await.unwrap(),
        AiBatchArtifactReservation::Reconciled
    );
    assert!(
        ledger
            .pending(PendingAiBatchArtifactLimit::new(10).unwrap())
            .await
            .unwrap()
            .is_empty()
    );

    let reviewed_retry = reference("file-pending-7", "reconcile-pending-7-retry-1");
    assert_eq!(
        ledger.reserve(reviewed_retry).await.unwrap(),
        AiBatchArtifactReservation::Reserved
    );

    let conflict = ledger
        .reserve(reference("file-other-7", "reconcile-pending-7"))
        .await
        .unwrap_err();
    assert!(matches!(
        conflict,
        PostgresAiBatchArtifactLedgerError::IdentityConflict
    ));
    let status = sqlx::query_scalar::<_, String>(
        "SELECT status FROM rustee_ai_batch_artifact_ledger \
         WHERE scope = $1 AND reconciliation_key = $2",
    )
    .bind("tenant-a")
    .bind("reconcile-pending-7")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "reconciled");
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn identifier_schema_rejects_oversized_artifact_metadata() {
    let pool = pool().await;
    reset_schema(&pool).await;
    let ledger = PostgresAiBatchArtifactLedger::new(pool.clone());
    let reference = reference("file-bounded-7", "reconcile-bounded-7");
    ledger.reserve(reference.clone()).await.unwrap();

    let error = sqlx::query(
        "UPDATE rustee_ai_batch_artifact_ledger SET provider_file_id = $1 \\
         WHERE scope = $2 AND reconciliation_key = $3",
    )
    .bind("x".repeat(MAX_BATCH_IDENTIFIER_BYTES + 1))
    .bind(reference.batch().scope())
    .bind(reference.reconciliation_key())
    .execute(&pool)
    .await
    .unwrap_err();
    assert!(error.as_database_error().is_some());
}
