//! Optional durable `PostgreSQL` storage for AI batch artifact reconciliation.
//!
//! Applications add [`AI_BATCH_ARTIFACT_LEDGER_MIGRATION_SQL`] and then
//! [`AI_BATCH_ARTIFACT_LEDGER_IDENTIFIER_BOUND_MIGRATION_SQL`] to their normal deployment
//! migration sequence. The ledger persists opaque batch/file references and never provider JSONL
//! rows, prompts, model response bodies, or domain result values. It intentionally does not run
//! migrations, download artifacts, requeue pending work, or erase provider files at startup.

mod model;
mod store;

pub use model::{
    PendingAiBatchArtifact, PendingAiBatchArtifactLimit, PendingAiBatchArtifactLimitError,
};
pub use store::{
    AI_BATCH_ARTIFACT_LEDGER_IDENTIFIER_BOUND_MIGRATION_SQL,
    AI_BATCH_ARTIFACT_LEDGER_MIGRATION_SQL, PostgresAiBatchArtifactLedger,
    PostgresAiBatchArtifactLedgerError,
};

#[cfg(test)]
mod tests {
    use rustee_ai_batch::{
        AiBatchArtifactKind, AiBatchArtifactReference, AiBatchReference, MAX_BATCH_IDENTIFIER_BYTES,
    };

    use super::{
        AI_BATCH_ARTIFACT_LEDGER_IDENTIFIER_BOUND_MIGRATION_SQL, PendingAiBatchArtifact,
        PendingAiBatchArtifactLimit, PendingAiBatchArtifactLimitError,
    };

    #[test]
    fn pending_limit_is_non_zero_and_bounded() {
        assert_eq!(
            PendingAiBatchArtifactLimit::new(0).unwrap_err(),
            PendingAiBatchArtifactLimitError::Zero
        );
        assert_eq!(
            PendingAiBatchArtifactLimit::new(1_001).unwrap_err(),
            PendingAiBatchArtifactLimitError::TooLarge
        );
        assert_eq!(PendingAiBatchArtifactLimit::new(1).unwrap().get(), 1);
    }

    #[test]
    fn pending_artifact_debug_redacts_its_reference() {
        let reference = AiBatchArtifactReference::new(
            AiBatchReference::new("tenant-a", "catalog-7", "run-7").unwrap(),
            AiBatchArtifactKind::Output,
            "file-7",
            "reconcile-7",
        )
        .unwrap();
        let pending = PendingAiBatchArtifact::new(reference);

        let rendered = format!("{pending:?}");
        assert!(!rendered.contains("tenant-a"));
        assert!(!rendered.contains("reconcile-7"));
    }

    #[test]
    fn identifier_bound_migration_matches_the_reference_contract() {
        assert!(
            AI_BATCH_ARTIFACT_LEDGER_IDENTIFIER_BOUND_MIGRATION_SQL
                .contains(&MAX_BATCH_IDENTIFIER_BYTES.to_string(),)
        );
    }
}
