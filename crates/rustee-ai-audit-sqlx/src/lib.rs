//! Optional durable `PostgreSQL` storage for Rustee AI tool audit and usage-ledger events.
//!
//! Applications add [`TOOL_AUDIT_MIGRATION_SQL`] to their normal deployment migration sequence.
//! [`PostgresToolAuditSink`] persists the approval record before a tool handler starts and the
//! terminal result afterwards. It intentionally does not run migrations at application startup or
//! claim rollback for a completed external side effect.

use std::{fmt, num::NonZeroUsize};

use futures_util::future::BoxFuture;
use rustee_ai::{
    AiExecutionContext, AiUsageLedger, AiUsageReservation, AiUsageReservationDecision,
    AiUsageSettlement, ToolApprovalAuditEvent, ToolApprovalAuditSink, ToolExecutionAuditEvent,
    ToolExecutionAuditSink, ToolExecutionOutcome, ToolRisk,
};
use sqlx::{PgPool, Row};

const MAX_TENANT_BYTES: usize = 255;
const MAX_SUBJECT_BYTES: usize = 255;
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 512;
const MAX_CALL_ID_BYTES: usize = 255;
const MAX_TOOL_NAME_BYTES: usize = 255;
const MAX_MODEL_ALIAS_BYTES: usize = 255;
const MAX_PENDING_AUDIT_LIMIT: usize = 1_000;
const MAX_PENDING_USAGE_LIMIT: usize = 1_000;

/// The deployment-owned migration for durable AI tool audit records.
pub const TOOL_AUDIT_MIGRATION_SQL: &str =
    include_str!("../migrations/0001_rustee_ai_tool_audit.sql");

/// The deployment-owned migration for durable AI provider-usage reservations.
pub const AI_USAGE_LEDGER_MIGRATION_SQL: &str =
    include_str!("../migrations/0002_rustee_ai_usage_ledger.sql");

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

/// A bounded request for pending AI usage reservations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PendingUsageLimit(NonZeroUsize);

impl PendingUsageLimit {
    /// Creates a non-zero, bounded pending-usage query limit.
    ///
    /// # Errors
    ///
    /// Returns [`PendingUsageLimitError`] when `limit` is zero or too large.
    pub fn new(limit: usize) -> Result<Self, PendingUsageLimitError> {
        let limit = NonZeroUsize::new(limit).ok_or(PendingUsageLimitError::Zero)?;
        if limit.get() > MAX_PENDING_USAGE_LIMIT {
            return Err(PendingUsageLimitError::TooLarge);
        }
        Ok(Self(limit))
    }

    /// Returns the configured number of records.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

impl Default for PendingUsageLimit {
    fn default() -> Self {
        Self(NonZeroUsize::new(100).expect("default pending usage limit is non-zero"))
    }
}

/// Invalid pending-usage query limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PendingUsageLimitError {
    /// A reconciliation query must request at least one record.
    #[error("pending AI usage query limit must be non-zero")]
    Zero,
    /// A reconciliation query would retain too many rows in one response.
    #[error("pending AI usage query limit exceeds the supported maximum")]
    TooLarge,
}

/// One provider-attempt reservation with no durable terminal usage.
///
/// Applications use the contained reservation to query an idempotent provider or complete a
/// policy-defined timeout workflow. The debug representation leaves all tenant and request
/// identifiers redacted.
#[derive(Clone, Eq, PartialEq)]
pub struct PendingAiUsage {
    reservation: AiUsageReservation,
}

impl PendingAiUsage {
    /// Returns the pending content-free reservation for application-owned reconciliation.
    #[must_use]
    pub const fn reservation(&self) -> &AiUsageReservation {
        &self.reservation
    }
}

impl fmt::Debug for PendingAiUsage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingAiUsage")
            .field("reservation", &self.reservation)
            .finish()
    }
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

/// Durable `PostgreSQL` usage ledger for idempotent AI provider attempts.
///
/// [`PostgresAiUsageLedger`] atomically inserts a pending reservation before a provider call and
/// persists exact provider-reported [`Usage`] once completion succeeds. It intentionally does not
/// infer pricing, quota periods, or refunds. A provider error or dropped stream remains pending,
/// so application-owned reconciliation can consult an idempotent provider rather than double-call
/// it. New reservations are accepted; an application needing quota denial implements the core
/// [`AiUsageLedger`] boundary with its quota policy in the same durable transaction.
#[derive(Clone)]
pub struct PostgresAiUsageLedger {
    pool: PgPool,
}

impl PostgresAiUsageLedger {
    /// Creates a durable usage ledger from an application-owned `PostgreSQL` pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Returns pending provider attempts in oldest-first order for application reconciliation.
    ///
    /// This method never retries a provider call, guesses usage, or releases quota. The caller
    /// owns its provider lookup, timeout, retention, and manual-review policy.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresAiUsageLedgerError`] when storage is unavailable or contains invalid
    /// durable metadata.
    pub async fn pending(
        &self,
        limit: PendingUsageLimit,
    ) -> Result<Vec<PendingAiUsage>, PostgresAiUsageLedgerError> {
        let limit =
            i64::try_from(limit.get()).map_err(|_| PostgresAiUsageLedgerError::InvalidMetadata)?;
        let rows = sqlx::query(
            "SELECT tenant, subject, idempotency_key, model_alias, input_characters, tool_count, \
             tool_result_count FROM rustee_ai_usage_ledger WHERE status = 'pending' \
             ORDER BY reserved_at ASC, tenant ASC, idempotency_key ASC LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(PostgresAiUsageLedgerError::storage)?;
        rows.into_iter()
            .map(|row| PendingAiUsage::from_row(&row))
            .collect()
    }

    async fn reserve_usage(
        &self,
        reservation: &AiUsageReservation,
    ) -> Result<AiUsageReservationDecision, PostgresAiUsageLedgerError> {
        validate_usage_reservation(reservation)?;
        let request = reservation.request();
        let result = sqlx::query(
            "INSERT INTO rustee_ai_usage_ledger \
             (tenant, idempotency_key, subject, model_alias, input_characters, tool_count, tool_result_count, status) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending') \
             ON CONFLICT (tenant, idempotency_key) DO NOTHING",
        )
        .bind(reservation.context().tenant())
        .bind(reservation.idempotency_key())
        .bind(reservation.context().subject())
        .bind(request.model())
        .bind(to_i64(request.input_characters())?)
        .bind(to_i64(request.tool_count())?)
        .bind(to_i64(request.tool_result_count())?)
        .execute(&self.pool)
        .await
        .map_err(PostgresAiUsageLedgerError::storage)?;
        if result.rows_affected() == 1 {
            return Ok(AiUsageReservationDecision::Reserved);
        }
        self.classify_existing_reservation(reservation).await
    }

    async fn classify_existing_reservation(
        &self,
        reservation: &AiUsageReservation,
    ) -> Result<AiUsageReservationDecision, PostgresAiUsageLedgerError> {
        let row = sqlx::query(
            "SELECT subject, model_alias, input_characters, tool_count, tool_result_count, status \
             FROM rustee_ai_usage_ledger WHERE tenant = $1 AND idempotency_key = $2",
        )
        .bind(reservation.context().tenant())
        .bind(reservation.idempotency_key())
        .fetch_optional(&self.pool)
        .await
        .map_err(PostgresAiUsageLedgerError::storage)?;
        let Some(row) = row else {
            return Err(PostgresAiUsageLedgerError::MissingReservation);
        };
        validate_matching_usage_identity(&row, reservation)?;
        let status = row
            .try_get::<String, _>("status")
            .map_err(PostgresAiUsageLedgerError::storage)?;
        match status.as_str() {
            "pending" => Ok(AiUsageReservationDecision::PendingReconciliation),
            "completed" => Ok(AiUsageReservationDecision::AlreadySettled),
            _ => Err(PostgresAiUsageLedgerError::InvalidMetadata),
        }
    }

    async fn record_terminal_usage(
        &self,
        settlement: &AiUsageSettlement,
    ) -> Result<(), PostgresAiUsageLedgerError> {
        let reservation = settlement.reservation();
        validate_usage_reservation(reservation)?;
        let request = reservation.request();
        let usage = settlement.usage();
        let result = sqlx::query(
            "UPDATE rustee_ai_usage_ledger SET status = 'completed', input_tokens = $8, \
             output_tokens = $9, settled_at = clock_timestamp() \
             WHERE tenant = $1 AND idempotency_key = $2 AND subject = $3 AND model_alias = $4 \
               AND input_characters = $5 AND tool_count = $6 AND tool_result_count = $7 \
               AND (status = 'pending' OR (status = 'completed' AND input_tokens = $8 AND output_tokens = $9))",
        )
        .bind(reservation.context().tenant())
        .bind(reservation.idempotency_key())
        .bind(reservation.context().subject())
        .bind(request.model())
        .bind(to_i64(request.input_characters())?)
        .bind(to_i64(request.tool_count())?)
        .bind(to_i64(request.tool_result_count())?)
        .bind(to_i64_u64(usage.input_tokens)?)
        .bind(to_i64_u64(usage.output_tokens)?)
        .execute(&self.pool)
        .await
        .map_err(PostgresAiUsageLedgerError::storage)?;
        if result.rows_affected() == 1 {
            return Ok(());
        }
        self.classify_usage_settlement(settlement).await
    }

    async fn classify_usage_settlement(
        &self,
        settlement: &AiUsageSettlement,
    ) -> Result<(), PostgresAiUsageLedgerError> {
        let reservation = settlement.reservation();
        let row = sqlx::query(
            "SELECT subject, model_alias, input_characters, tool_count, tool_result_count, status, \
             input_tokens, output_tokens FROM rustee_ai_usage_ledger \
             WHERE tenant = $1 AND idempotency_key = $2",
        )
        .bind(reservation.context().tenant())
        .bind(reservation.idempotency_key())
        .fetch_optional(&self.pool)
        .await
        .map_err(PostgresAiUsageLedgerError::storage)?;
        let Some(row) = row else {
            return Err(PostgresAiUsageLedgerError::MissingReservation);
        };
        validate_matching_usage_identity(&row, reservation)?;
        let status = row
            .try_get::<String, _>("status")
            .map_err(PostgresAiUsageLedgerError::storage)?;
        if status != "completed" {
            return Err(PostgresAiUsageLedgerError::UsageConflict);
        }
        let input_tokens = row
            .try_get::<Option<i64>, _>("input_tokens")
            .map_err(PostgresAiUsageLedgerError::storage)?;
        let output_tokens = row
            .try_get::<Option<i64>, _>("output_tokens")
            .map_err(PostgresAiUsageLedgerError::storage)?;
        let usage = settlement.usage();
        if input_tokens == Some(to_i64_u64(usage.input_tokens)?)
            && output_tokens == Some(to_i64_u64(usage.output_tokens)?)
        {
            Ok(())
        } else {
            Err(PostgresAiUsageLedgerError::UsageConflict)
        }
    }
}

impl fmt::Debug for PostgresAiUsageLedger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresAiUsageLedger")
            .field("pool", &"[REDACTED]")
            .finish()
    }
}

impl AiUsageLedger for PostgresAiUsageLedger {
    type Error = PostgresAiUsageLedgerError;

    fn reserve(
        &self,
        reservation: AiUsageReservation,
    ) -> BoxFuture<'static, Result<AiUsageReservationDecision, Self::Error>> {
        let ledger = self.clone();
        Box::pin(async move { ledger.reserve_usage(&reservation).await })
    }

    fn record_usage(
        &self,
        settlement: AiUsageSettlement,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let ledger = self.clone();
        Box::pin(async move { ledger.record_terminal_usage(&settlement).await })
    }
}

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
        rows.into_iter()
            .map(|row| PendingToolAudit::from_row(&row))
            .collect()
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

/// Sanitized failure from `PostgreSQL` durable AI usage storage.
#[derive(thiserror::Error)]
pub enum PostgresAiUsageLedgerError {
    /// One field was not safe for this adapter's bounded durable schema.
    #[error("AI usage reservation has invalid durable metadata")]
    InvalidMetadata,
    /// An idempotency key was reused for a different provider-attempt identity.
    #[error("AI usage reservation identity conflicts with an existing attempt")]
    IdentityConflict,
    /// A terminal usage value cannot be changed after first persistence.
    #[error("AI usage reservation conflicts with existing terminal usage")]
    UsageConflict,
    /// The reservation was absent before a terminal usage write or duplicate verification.
    #[error("AI usage reservation is missing")]
    MissingReservation,
    /// `PostgreSQL` storage did not complete; source detail remains available to application logs.
    #[error("PostgreSQL AI usage ledger storage failed")]
    Storage(#[source] sqlx::Error),
}

impl PostgresAiUsageLedgerError {
    fn storage(error: sqlx::Error) -> Self {
        Self::Storage(error)
    }
}

impl fmt::Debug for PostgresAiUsageLedgerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::InvalidMetadata => "InvalidMetadata",
            Self::IdentityConflict => "IdentityConflict",
            Self::UsageConflict => "UsageConflict",
            Self::MissingReservation => "MissingReservation",
            Self::Storage(_) => "Storage",
        };
        formatter
            .debug_tuple("PostgresAiUsageLedgerError")
            .field(&name)
            .finish()
    }
}

impl PendingToolAudit {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, PostgresToolAuditError> {
        let risk = row
            .try_get::<String, _>("risk")
            .map_err(PostgresToolAuditError::storage)?;
        Ok(Self {
            tenant: row
                .try_get("tenant")
                .map_err(PostgresToolAuditError::storage)?,
            subject: row
                .try_get("subject")
                .map_err(PostgresToolAuditError::storage)?,
            idempotency_key: row
                .try_get("idempotency_key")
                .map_err(PostgresToolAuditError::storage)?,
            call_id: row
                .try_get("call_id")
                .map_err(PostgresToolAuditError::storage)?,
            tool_name: row
                .try_get("tool_name")
                .map_err(PostgresToolAuditError::storage)?,
            risk: parse_tool_risk(&risk)?,
        })
    }
}

impl PendingAiUsage {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, PostgresAiUsageLedgerError> {
        let input_characters = row
            .try_get::<i64, _>("input_characters")
            .map_err(PostgresAiUsageLedgerError::storage)?;
        let tool_count = row
            .try_get::<i64, _>("tool_count")
            .map_err(PostgresAiUsageLedgerError::storage)?;
        let tool_result_count = row
            .try_get::<i64, _>("tool_result_count")
            .map_err(PostgresAiUsageLedgerError::storage)?;
        let tenant = row
            .try_get::<String, _>("tenant")
            .map_err(PostgresAiUsageLedgerError::storage)?;
        let subject = row
            .try_get::<String, _>("subject")
            .map_err(PostgresAiUsageLedgerError::storage)?;
        let idempotency_key = row
            .try_get::<String, _>("idempotency_key")
            .map_err(PostgresAiUsageLedgerError::storage)?;
        let model = row
            .try_get::<String, _>("model_alias")
            .map_err(PostgresAiUsageLedgerError::storage)?;
        let context = AiExecutionContext::new(tenant, subject)
            .map_err(|_| PostgresAiUsageLedgerError::InvalidMetadata)?;
        let reservation = AiUsageReservation::from_metadata(
            context,
            idempotency_key,
            model,
            usize::try_from(input_characters)
                .map_err(|_| PostgresAiUsageLedgerError::InvalidMetadata)?,
            usize::try_from(tool_count).map_err(|_| PostgresAiUsageLedgerError::InvalidMetadata)?,
            usize::try_from(tool_result_count)
                .map_err(|_| PostgresAiUsageLedgerError::InvalidMetadata)?,
        )
        .map_err(|_| PostgresAiUsageLedgerError::InvalidMetadata)?;
        Ok(Self { reservation })
    }
}

fn validate_approval_event(event: &ToolApprovalAuditEvent) -> Result<(), PostgresToolAuditError> {
    for (value, maximum) in [
        (event.context().tenant(), MAX_TENANT_BYTES),
        (event.context().subject(), MAX_SUBJECT_BYTES),
        (event.idempotency_key(), MAX_IDEMPOTENCY_KEY_BYTES),
        (event.call_id(), MAX_CALL_ID_BYTES),
        (event.tool_name(), MAX_TOOL_NAME_BYTES),
    ] {
        if value.trim().is_empty() || value.contains('\0') || value.len() > maximum {
            return Err(PostgresToolAuditError::InvalidMetadata);
        }
    }
    Ok(())
}

fn validate_usage_reservation(
    reservation: &AiUsageReservation,
) -> Result<(), PostgresAiUsageLedgerError> {
    for (value, maximum) in [
        (reservation.context().tenant(), MAX_TENANT_BYTES),
        (reservation.context().subject(), MAX_SUBJECT_BYTES),
        (reservation.idempotency_key(), MAX_IDEMPOTENCY_KEY_BYTES),
        (reservation.request().model(), MAX_MODEL_ALIAS_BYTES),
    ] {
        if value.trim().is_empty() || value.contains('\0') || value.len() > maximum {
            return Err(PostgresAiUsageLedgerError::InvalidMetadata);
        }
    }
    to_i64(reservation.request().input_characters())?;
    to_i64(reservation.request().tool_count())?;
    to_i64(reservation.request().tool_result_count())?;
    Ok(())
}

fn validate_matching_usage_identity(
    row: &sqlx::postgres::PgRow,
    reservation: &AiUsageReservation,
) -> Result<(), PostgresAiUsageLedgerError> {
    let subject = row
        .try_get::<String, _>("subject")
        .map_err(PostgresAiUsageLedgerError::storage)?;
    let model = row
        .try_get::<String, _>("model_alias")
        .map_err(PostgresAiUsageLedgerError::storage)?;
    let input_characters = row
        .try_get::<i64, _>("input_characters")
        .map_err(PostgresAiUsageLedgerError::storage)?;
    let tool_count = row
        .try_get::<i64, _>("tool_count")
        .map_err(PostgresAiUsageLedgerError::storage)?;
    let tool_result_count = row
        .try_get::<i64, _>("tool_result_count")
        .map_err(PostgresAiUsageLedgerError::storage)?;
    let request = reservation.request();
    if subject == reservation.context().subject()
        && model == request.model()
        && input_characters == to_i64(request.input_characters())?
        && tool_count == to_i64(request.tool_count())?
        && tool_result_count == to_i64(request.tool_result_count())?
    {
        Ok(())
    } else {
        Err(PostgresAiUsageLedgerError::IdentityConflict)
    }
}

fn to_i64(value: usize) -> Result<i64, PostgresAiUsageLedgerError> {
    i64::try_from(value).map_err(|_| PostgresAiUsageLedgerError::InvalidMetadata)
}

fn to_i64_u64(value: u64) -> Result<i64, PostgresAiUsageLedgerError> {
    i64::try_from(value).map_err(|_| PostgresAiUsageLedgerError::InvalidMetadata)
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
