use std::fmt;

use futures_util::future::BoxFuture;
use rustee_ai::{AiUsageLedger, AiUsageReservation, AiUsageReservationDecision, AiUsageSettlement};
use sqlx::{PgPool, Row};

use super::record::{PendingAiUsage, PendingUsageLimit, valid_usage_reservation};

/// Durable `PostgreSQL` usage ledger for idempotent AI provider attempts.
///
/// [`PostgresAiUsageLedger`] atomically inserts a pending reservation before a provider call and
/// persists exact provider-reported `rustee_ai::Usage` once completion succeeds. It intentionally
/// does not infer pricing, quota periods, or refunds. A provider error or dropped stream remains
/// pending, so application-owned reconciliation can consult an idempotent provider rather than
/// double-call it. New reservations are accepted; an application needing quota denial implements
/// the core [`AiUsageLedger`] boundary with its quota policy in the same durable transaction.
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
        rows.iter().map(decode_pending_usage).collect()
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
             output_tokens = $9, settled_at = COALESCE(settled_at, clock_timestamp()) \
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

fn decode_pending_usage(
    row: &sqlx::postgres::PgRow,
) -> Result<PendingAiUsage, PostgresAiUsageLedgerError> {
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
    PendingAiUsage::from_durable_metadata(
        tenant,
        subject,
        idempotency_key,
        model,
        input_characters,
        tool_count,
        tool_result_count,
    )
    .ok_or(PostgresAiUsageLedgerError::InvalidMetadata)
}

fn validate_usage_reservation(
    reservation: &AiUsageReservation,
) -> Result<(), PostgresAiUsageLedgerError> {
    valid_usage_reservation(reservation)
        .then_some(())
        .ok_or(PostgresAiUsageLedgerError::InvalidMetadata)
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
