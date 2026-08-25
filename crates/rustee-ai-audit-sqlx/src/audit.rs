//! Durable AI tool-audit public facade.

mod record;
mod store;

pub use record::{PendingAuditLimit, PendingAuditLimitError, PendingToolAudit};
pub use store::{PostgresToolAuditError, PostgresToolAuditSink};

/// The deployment-owned migration for durable AI tool audit records.
pub const TOOL_AUDIT_MIGRATION_SQL: &str =
    include_str!("../migrations/0001_rustee_ai_tool_audit.sql");
