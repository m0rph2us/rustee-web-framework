//! Optional durable `PostgreSQL` run ledger for reference-only AI evaluation.
//!
//! Applications add [`AI_EVALUATION_RUN_LEDGER_MIGRATION_SQL`] to normal deployment migrations.
//! The ledger stores only scope, catalog ID, run key, and status. It never stores evaluation
//! prompts, targets, model completions, grader details, or evaluation reports. It does not start
//! jobs, load catalogs, make provider calls, retry pending evaluations, or delete records.

mod model;
mod store;

pub use model::{
    PendingAiEvaluationRun, PendingAiEvaluationRunLimit, PendingAiEvaluationRunLimitError,
};
pub use store::{PostgresAiEvaluationRunLedger, PostgresAiEvaluationRunLedgerError};

/// Deployment-owned migration for durable AI evaluation run records.
pub const AI_EVALUATION_RUN_LEDGER_MIGRATION_SQL: &str =
    include_str!("../migrations/0001_rustee_ai_evaluation_run_ledger.sql");
