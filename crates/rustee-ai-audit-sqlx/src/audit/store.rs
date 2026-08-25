use std::fmt;

use futures_util::future::BoxFuture;
use rustee_ai::{
    ToolApprovalAuditEvent, ToolApprovalAuditSink, ToolExecutionAuditEvent, ToolExecutionAuditSink,
    ToolExecutionOutcome, ToolRisk,
};
use sqlx::{PgPool, Row};

use super::record::{PendingAuditLimit, PendingToolAudit, valid_durable_metadata};

/// Durable `PostgreSQL` implementation of both tool approval and terminal-outcome audit sinks.
///
/// The primary key is `(tenant, idempotency_key)`. Repeating the same approved action or outcome
/// is idempotent. A changed actor, call ID, tool name, risk, or terminal outcome returns an error
/// instead of overwriting the first durable record.
#[derive(Clone)]
pub struct PostgresToolAuditSink {
    pool: PgPool,
}

impl PostgresToolAuditSink {
    /// Creates a durable audit sink from an application-owned `PostgreSQL` pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Returns unresolved approval records in oldest-first order for application reconciliation.
    ///
    /// The caller owns the retry schedule, external-provider lookup, retention, and any manual
    /// review. This method never retries or re-executes a tool handler.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresToolAuditError`] when storage is unavailable or contains an invalid
    /// record.
    pub async fn pending(
        &self,
        limit: PendingAuditLimit,
    ) -> Result<Vec<PendingToolAudit>, PostgresToolAuditError> {
        let limit =
            i64::try_from(limit.get()).map_err(|_| PostgresToolAuditError::InvalidMetadata)?;
        let rows = sqlx::query(
            "SELECT tenant, subject, idempotency_key, call_id, tool_name, risk \
             FROM rustee_ai_tool_audit \
             WHERE terminal_outcome IS NULL \
             ORDER BY approved_at ASC, tenant ASC, idempotency_key ASC \
             LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(PostgresToolAuditError::storage)?;
        rows.iter().map(decode_pending_audit).collect()
    }

    async fn record_approval(
        &self,
        event: &ToolApprovalAuditEvent,
    ) -> Result<(), PostgresToolAuditError> {
        validate_approval_event(event)?;
        let result = sqlx::query(
            "INSERT INTO rustee_ai_tool_audit \
             (tenant, idempotency_key, subject, call_id, tool_name, risk) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (tenant, idempotency_key) DO NOTHING",
        )
        .bind(event.context().tenant())
        .bind(event.idempotency_key())
        .bind(event.context().subject())
        .bind(event.call_id())
        .bind(event.tool_name())
        .bind(tool_risk_name(event.risk()))
        .execute(&self.pool)
        .await
        .map_err(PostgresToolAuditError::storage)?;
        if result.rows_affected() == 1 {
            return Ok(());
        }
        self.verify_same_approval(event).await
    }

    async fn verify_same_approval(
        &self,
        event: &ToolApprovalAuditEvent,
    ) -> Result<(), PostgresToolAuditError> {
        let row = sqlx::query(
            "SELECT subject, call_id, tool_name, risk \
             FROM rustee_ai_tool_audit WHERE tenant = $1 AND idempotency_key = $2",
        )
        .bind(event.context().tenant())
        .bind(event.idempotency_key())
        .fetch_optional(&self.pool)
        .await
        .map_err(PostgresToolAuditError::storage)?;
        let Some(row) = row else {
            return Err(PostgresToolAuditError::MissingApproval);
        };
        let subject = row
            .try_get::<String, _>("subject")
            .map_err(PostgresToolAuditError::storage)?;
        let call_id = row
            .try_get::<String, _>("call_id")
            .map_err(PostgresToolAuditError::storage)?;
        let tool_name = row
            .try_get::<String, _>("tool_name")
            .map_err(PostgresToolAuditError::storage)?;
        let risk = row
            .try_get::<String, _>("risk")
            .map_err(PostgresToolAuditError::storage)?;
        if subject == event.context().subject()
            && call_id == event.call_id()
            && tool_name == event.tool_name()
            && risk == tool_risk_name(event.risk())
        {
            Ok(())
        } else {
            Err(PostgresToolAuditError::IdentityConflict)
        }
    }

    async fn record_terminal_outcome(
        &self,
        event: &ToolExecutionAuditEvent,
    ) -> Result<(), PostgresToolAuditError> {
        let approval = event.approval();
        validate_approval_event(approval)?;
        let outcome = tool_outcome_name(event.outcome());
        let result = sqlx::query(
            "UPDATE rustee_ai_tool_audit \
             SET terminal_outcome = COALESCE(terminal_outcome, $7), \
                 outcome_recorded_at = COALESCE(outcome_recorded_at, clock_timestamp()) \
             WHERE tenant = $1 AND idempotency_key = $2 AND subject = $3 \
               AND call_id = $4 AND tool_name = $5 AND risk = $6 \
               AND (terminal_outcome IS NULL OR terminal_outcome = $7)",
        )
        .bind(approval.context().tenant())
        .bind(approval.idempotency_key())
        .bind(approval.context().subject())
        .bind(approval.call_id())
        .bind(approval.tool_name())
        .bind(tool_risk_name(approval.risk()))
        .bind(outcome)
        .execute(&self.pool)
        .await
        .map_err(PostgresToolAuditError::storage)?;
        if result.rows_affected() == 1 {
            return Ok(());
        }
        self.classify_terminal_conflict(event).await
    }

    async fn classify_terminal_conflict(
        &self,
        event: &ToolExecutionAuditEvent,
    ) -> Result<(), PostgresToolAuditError> {
        let approval = event.approval();
        let row = sqlx::query(
            "SELECT subject, call_id, tool_name, risk, terminal_outcome \
             FROM rustee_ai_tool_audit WHERE tenant = $1 AND idempotency_key = $2",
        )
        .bind(approval.context().tenant())
        .bind(approval.idempotency_key())
        .fetch_optional(&self.pool)
        .await
        .map_err(PostgresToolAuditError::storage)?;
        let Some(row) = row else {
            return Err(PostgresToolAuditError::MissingApproval);
        };
        let subject = row
            .try_get::<String, _>("subject")
            .map_err(PostgresToolAuditError::storage)?;
        let call_id = row
            .try_get::<String, _>("call_id")
            .map_err(PostgresToolAuditError::storage)?;
        let tool_name = row
            .try_get::<String, _>("tool_name")
            .map_err(PostgresToolAuditError::storage)?;
        let risk = row
            .try_get::<String, _>("risk")
            .map_err(PostgresToolAuditError::storage)?;
        if subject != approval.context().subject()
            || call_id != approval.call_id()
            || tool_name != approval.tool_name()
            || risk != tool_risk_name(approval.risk())
        {
            return Err(PostgresToolAuditError::IdentityConflict);
        }
        let stored_outcome = row
            .try_get::<Option<String>, _>("terminal_outcome")
            .map_err(PostgresToolAuditError::storage)?;
        match stored_outcome.as_deref() {
            Some(value) if value == tool_outcome_name(event.outcome()) => Ok(()),
            Some(_) | None => Err(PostgresToolAuditError::OutcomeConflict),
        }
    }
}

impl fmt::Debug for PostgresToolAuditSink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresToolAuditSink")
            .field("pool", &"[REDACTED]")
            .finish()
    }
}

impl ToolApprovalAuditSink for PostgresToolAuditSink {
    type Error = PostgresToolAuditError;

    fn record_approved(
        &self,
        event: ToolApprovalAuditEvent,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let sink = self.clone();
        Box::pin(async move { sink.record_approval(&event).await })
    }
}

impl ToolExecutionAuditSink for PostgresToolAuditSink {
    fn record_outcome(
        &self,
        event: ToolExecutionAuditEvent,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let sink = self.clone();
        Box::pin(async move { sink.record_terminal_outcome(&event).await })
    }
}

/// Sanitized failure from `PostgreSQL` durable tool audit storage.
#[derive(thiserror::Error)]
pub enum PostgresToolAuditError {
    /// One field was not safe for this adapter's bounded durable schema.
    #[error("AI tool audit event has invalid durable metadata")]
    InvalidMetadata,
    /// An idempotency key was reused for a different action identity.
    #[error("AI tool audit identity conflicts with an existing action")]
    IdentityConflict,
    /// A durable terminal result cannot be changed after first persistence.
    #[error("AI tool audit outcome conflicts with the existing terminal result")]
    OutcomeConflict,
    /// The approval record was absent before an outcome or duplicate verification.
    #[error("AI tool audit approval record is missing")]
    MissingApproval,
    /// `PostgreSQL` storage did not complete; source detail remains available to application logs.
    #[error("PostgreSQL AI tool audit storage failed")]
    Storage(#[source] sqlx::Error),
}

impl PostgresToolAuditError {
    fn storage(error: sqlx::Error) -> Self {
        Self::Storage(error)
    }
}

impl fmt::Debug for PostgresToolAuditError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::InvalidMetadata => "InvalidMetadata",
            Self::IdentityConflict => "IdentityConflict",
            Self::OutcomeConflict => "OutcomeConflict",
            Self::MissingApproval => "MissingApproval",
            Self::Storage(_) => "Storage",
        };
        formatter
            .debug_tuple("PostgresToolAuditError")
            .field(&name)
            .finish()
    }
}

fn decode_pending_audit(
    row: &sqlx::postgres::PgRow,
) -> Result<PendingToolAudit, PostgresToolAuditError> {
    let tenant = row
        .try_get("tenant")
        .map_err(PostgresToolAuditError::storage)?;
    let subject = row
        .try_get("subject")
        .map_err(PostgresToolAuditError::storage)?;
    let idempotency_key = row
        .try_get("idempotency_key")
        .map_err(PostgresToolAuditError::storage)?;
    let call_id = row
        .try_get("call_id")
        .map_err(PostgresToolAuditError::storage)?;
    let tool_name = row
        .try_get("tool_name")
        .map_err(PostgresToolAuditError::storage)?;
    let risk = row
        .try_get::<String, _>("risk")
        .map_err(PostgresToolAuditError::storage)?;
    PendingToolAudit::from_durable_metadata(
        tenant,
        subject,
        idempotency_key,
        call_id,
        tool_name,
        parse_tool_risk(&risk)?,
    )
    .ok_or(PostgresToolAuditError::InvalidMetadata)
}

fn validate_approval_event(event: &ToolApprovalAuditEvent) -> Result<(), PostgresToolAuditError> {
    valid_durable_metadata(
        event.context().tenant(),
        event.context().subject(),
        event.idempotency_key(),
        event.call_id(),
        event.tool_name(),
    )
    .then_some(())
    .ok_or(PostgresToolAuditError::InvalidMetadata)
}

fn tool_risk_name(risk: ToolRisk) -> &'static str {
    match risk {
        ToolRisk::ReadOnly => "read_only",
        ToolRisk::RequiresConfirmation => "requires_confirmation",
        ToolRisk::Privileged => "privileged",
    }
}

fn parse_tool_risk(value: &str) -> Result<ToolRisk, PostgresToolAuditError> {
    match value {
        "read_only" => Ok(ToolRisk::ReadOnly),
        "requires_confirmation" => Ok(ToolRisk::RequiresConfirmation),
        "privileged" => Ok(ToolRisk::Privileged),
        _ => Err(PostgresToolAuditError::InvalidMetadata),
    }
}

fn tool_outcome_name(outcome: ToolExecutionOutcome) -> &'static str {
    match outcome {
        ToolExecutionOutcome::Succeeded => "succeeded",
        ToolExecutionOutcome::Failed => "failed",
    }
}
