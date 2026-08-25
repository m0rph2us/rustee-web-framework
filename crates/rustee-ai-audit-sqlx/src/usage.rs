//! Durable AI usage-ledger public facade.

mod record;
mod store;

pub use record::{PendingAiUsage, PendingUsageLimit, PendingUsageLimitError};
pub use store::{PostgresAiUsageLedger, PostgresAiUsageLedgerError};

/// The deployment-owned migration for durable AI provider-usage reservations.
pub const AI_USAGE_LEDGER_MIGRATION_SQL: &str =
    include_str!("../migrations/0002_rustee_ai_usage_ledger.sql");
