use std::{fmt, num::NonZeroUsize};

use rustee_ai::{
    MAX_IDEMPOTENCY_KEY_BYTES, MAX_SUBJECT_BYTES, MAX_TENANT_BYTES, MAX_TOOL_CALL_ID_BYTES,
    MAX_TOOL_NAME_BYTES, ToolRisk,
};

const MAX_PENDING_AUDIT_LIMIT: usize = 1_000;

/// A bounded request for unresolved tool audit records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PendingAuditLimit(NonZeroUsize);

impl PendingAuditLimit {
    /// Creates a non-zero, bounded pending-record query limit.
    ///
    /// # Errors
    ///
    /// Returns [`PendingAuditLimitError`] when `limit` is zero or too large.
    pub fn new(limit: usize) -> Result<Self, PendingAuditLimitError> {
        let limit = NonZeroUsize::new(limit).ok_or(PendingAuditLimitError::Zero)?;
        if limit.get() > MAX_PENDING_AUDIT_LIMIT {
            return Err(PendingAuditLimitError::TooLarge);
        }
        Ok(Self(limit))
    }

    /// Returns the configured number of records.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

impl Default for PendingAuditLimit {
    fn default() -> Self {
        Self(NonZeroUsize::new(100).expect("default pending audit limit is non-zero"))
    }
}

/// Invalid unresolved-audit query limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PendingAuditLimitError {
    /// A reconciliation query must request at least one record.
    #[error("pending AI tool audit limit must be non-zero")]
    Zero,
    /// A reconciliation query would retain too many rows in one response.
    #[error("pending AI tool audit limit exceeds the supported maximum")]
    TooLarge,
}

/// One approved action that has not yet received a durable terminal outcome record.
///
/// This metadata is intentionally returned to application-owned reconciliation code so it can
/// consult an idempotent side-effect provider. Its debug representation redacts all identifiers.
#[derive(Clone, Eq, PartialEq)]
pub struct PendingToolAudit {
    tenant: String,
    subject: String,
    idempotency_key: String,
    call_id: String,
    tool_name: String,
    risk: ToolRisk,
}

impl PendingToolAudit {
    pub(super) fn from_durable_metadata(
        tenant: String,
        subject: String,
        idempotency_key: String,
        call_id: String,
        tool_name: String,
        risk: ToolRisk,
    ) -> Option<Self> {
        valid_durable_metadata(&tenant, &subject, &idempotency_key, &call_id, &tool_name).then_some(
            Self {
                tenant,
                subject,
                idempotency_key,
                call_id,
                tool_name,
                risk,
            },
        )
    }

    /// Returns the tenant scope of the durable action.
    #[must_use]
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// Returns the validated actor identifier of the durable action.
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Returns the application key used to reconcile an external side effect.
    #[must_use]
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    /// Returns the provider call identifier associated with the approved action.
    #[must_use]
    pub fn call_id(&self) -> &str {
        &self.call_id
    }

    /// Returns the application tool name associated with the approved action.
    #[must_use]
    pub fn tool_name(&self) -> &str {
        &self.tool_name
    }

    /// Returns the approved side-effect classification.
    #[must_use]
    pub const fn risk(&self) -> ToolRisk {
        self.risk
    }
}

impl fmt::Debug for PendingToolAudit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingToolAudit")
            .field("tenant", &"[REDACTED]")
            .field("subject", &"[REDACTED]")
            .field("idempotency_key", &"[REDACTED]")
            .field("call_id", &"[REDACTED]")
            .field("tool_name", &"[REDACTED]")
            .field("risk", &self.risk)
            .finish()
    }
}

pub(super) fn valid_durable_metadata(
    tenant: &str,
    subject: &str,
    idempotency_key: &str,
    call_id: &str,
    tool_name: &str,
) -> bool {
    [
        (tenant, MAX_TENANT_BYTES),
        (subject, MAX_SUBJECT_BYTES),
        (idempotency_key, MAX_IDEMPOTENCY_KEY_BYTES),
        (call_id, MAX_TOOL_CALL_ID_BYTES),
        (tool_name, MAX_TOOL_NAME_BYTES),
    ]
    .into_iter()
    .all(|(value, maximum)| {
        !value.trim().is_empty() && !value.contains('\0') && value.len() <= maximum
    })
}

#[cfg(test)]
mod tests {
    use super::{MAX_TOOL_CALL_ID_BYTES, PendingToolAudit, ToolRisk};

    #[test]
    fn reconstruction_rejects_invalid_durable_metadata() {
        assert!(
            PendingToolAudit::from_durable_metadata(
                "tenant-a".to_owned(),
                "subject-a".to_owned(),
                "tool:1".to_owned(),
                "call-1".to_owned(),
                "lookup".to_owned(),
                ToolRisk::ReadOnly,
            )
            .is_some()
        );

        for call_id in [
            " ".to_owned(),
            "bad\0call".to_owned(),
            "x".repeat(MAX_TOOL_CALL_ID_BYTES + 1),
        ] {
            assert!(
                PendingToolAudit::from_durable_metadata(
                    "tenant-a".to_owned(),
                    "subject-a".to_owned(),
                    "tool:1".to_owned(),
                    call_id,
                    "lookup".to_owned(),
                    ToolRisk::ReadOnly,
                )
                .is_none()
            );
        }
    }

    #[test]
    fn debug_redacts_reconciliation_identifiers() {
        let record = PendingToolAudit::from_durable_metadata(
            "tenant-a".to_owned(),
            "subject-a".to_owned(),
            "tool:1".to_owned(),
            "call-1".to_owned(),
            "lookup".to_owned(),
            ToolRisk::ReadOnly,
        )
        .expect("test metadata is valid");

        let output = format!("{record:?}");
        for value in ["tenant-a", "subject-a", "tool:1", "call-1", "lookup"] {
            assert!(!output.contains(value));
        }
    }
}
