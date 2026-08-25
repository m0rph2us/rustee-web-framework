//! Optional durable `PostgreSQL` storage for Rustee AI tool audit and usage-ledger events.
//!
//! Applications add [`TOOL_AUDIT_MIGRATION_SQL`] to their normal deployment migration sequence.
//! [`PostgresToolAuditSink`] persists the approval record before a tool handler starts and the
//! terminal result afterwards. It intentionally does not run migrations at application startup or
//! claim rollback for a completed external side effect.

mod audit;
mod usage;

pub use audit::{
    PendingAuditLimit, PendingAuditLimitError, PendingToolAudit, PostgresToolAuditError,
    PostgresToolAuditSink, TOOL_AUDIT_MIGRATION_SQL,
};
pub use usage::{
    AI_USAGE_LEDGER_MIGRATION_SQL, PendingAiUsage, PendingUsageLimit, PendingUsageLimitError,
    PostgresAiUsageLedger, PostgresAiUsageLedgerError,
};

#[cfg(test)]
mod tests {
    use super::{
        PendingAuditLimit, PendingAuditLimitError, PendingUsageLimit, PendingUsageLimitError,
    };

    #[test]
    fn pending_limit_is_non_zero_and_bounded() {
        assert_eq!(
            PendingAuditLimit::new(0).unwrap_err(),
            PendingAuditLimitError::Zero
        );
        assert_eq!(
            PendingAuditLimit::new(1_001).unwrap_err(),
            PendingAuditLimitError::TooLarge
        );
        assert_eq!(PendingAuditLimit::new(1).unwrap().get(), 1);
    }

    #[test]
    fn pending_usage_limit_is_non_zero_and_bounded() {
        assert_eq!(
            PendingUsageLimit::new(0).unwrap_err(),
            PendingUsageLimitError::Zero
        );
        assert_eq!(
            PendingUsageLimit::new(1_001).unwrap_err(),
            PendingUsageLimitError::TooLarge
        );
        assert_eq!(PendingUsageLimit::new(1).unwrap().get(), 1);
    }
}
