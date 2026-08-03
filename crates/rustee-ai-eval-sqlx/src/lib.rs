//! Optional durable `PostgreSQL` run ledger for reference-only AI evaluation.
//!
//! Applications add [`AI_EVALUATION_RUN_LEDGER_MIGRATION_SQL`] to normal deployment migrations.
//! The ledger stores only scope, catalog ID, run key, and status. It never stores evaluation
//! prompts, targets, model completions, grader details, or evaluation reports. It does not start
//! jobs, load catalogs, make provider calls, retry pending evaluations, or delete records.

use std::{fmt, num::NonZeroUsize};

use futures_util::future::BoxFuture;
use rustee_ai_eval::{AiEvaluationReference, AiEvaluationRunLedger, AiEvaluationRunReservation};
use sqlx::{PgPool, Row};

const MAX_IDENTIFIER_BYTES: usize = 128;
const MAX_PENDING_LIMIT: usize = 1_000;

/// Deployment-owned migration for durable AI evaluation run records.
pub const AI_EVALUATION_RUN_LEDGER_MIGRATION_SQL: &str =
    include_str!("../migrations/0001_rustee_ai_evaluation_run_ledger.sql");

/// A bounded request for unresolved evaluation runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PendingAiEvaluationRunLimit(NonZeroUsize);

impl PendingAiEvaluationRunLimit {
    /// Creates a non-zero, bounded pending-run query limit.
    ///
    /// # Errors
    ///
    /// Returns [`PendingAiEvaluationRunLimitError`] when `limit` is zero or too large.
    pub fn new(limit: usize) -> Result<Self, PendingAiEvaluationRunLimitError> {
        let limit = NonZeroUsize::new(limit).ok_or(PendingAiEvaluationRunLimitError::Zero)?;
        if limit.get() > MAX_PENDING_LIMIT {
            return Err(PendingAiEvaluationRunLimitError::TooLarge);
        }
        Ok(Self(limit))
    }

    /// Returns the configured number of records.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

impl Default for PendingAiEvaluationRunLimit {
    fn default() -> Self {
        Self(NonZeroUsize::new(100).expect("default pending evaluation limit is non-zero"))
    }
}

/// Invalid pending-evaluation query limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PendingAiEvaluationRunLimitError {
    /// A reconciliation query must request at least one record.
    #[error("pending AI evaluation run limit must be non-zero")]
    Zero,
    /// A reconciliation query would retain too many rows in one response.
    #[error("pending AI evaluation run limit exceeds the supported maximum")]
    TooLarge,
}

/// One evaluation whose prior catalog, provider, grader, or record attempt is ambiguous.
///
/// This metadata is returned only to application-owned reconciliation code. Application owners
/// authorize the scope, inspect provider usage/report sinks, and choose a new run key only when a
/// fresh evaluation is safe.
#[derive(Clone, Eq, PartialEq)]
pub struct PendingAiEvaluationRun {
    reference: AiEvaluationReference,
}

impl PendingAiEvaluationRun {
    /// Returns the content-free reference for application reconciliation.
    #[must_use]
    pub const fn reference(&self) -> &AiEvaluationReference {
        &self.reference
    }
}

impl fmt::Debug for PendingAiEvaluationRun {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingAiEvaluationRun")
            .field("reference", &"[REDACTED]")
            .finish()
    }
}

/// Durable `PostgreSQL` implementation of [`AiEvaluationRunLedger`].
///
/// `(scope, run_key)` is the primary key. Repeating the same exact catalog reference is
/// idempotent; reusing the scoped run key for another catalog fails instead of overwriting the
/// original record. A failure after reservation remains pending so the coordinator cannot reload
/// the suite or repeat provider usage automatically.
#[derive(Clone)]
pub struct PostgresAiEvaluationRunLedger {
    pool: PgPool,
}

impl PostgresAiEvaluationRunLedger {
    /// Creates a durable run ledger from an application-owned `PostgreSQL` pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Returns pending run references in oldest-first order for application reconciliation.
    ///
    /// This method never loads catalogs, calls models, grades completions, changes status, or
    /// schedules a retry.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresAiEvaluationRunLedgerError`] when storage is unavailable or metadata is
    /// invalid.
    pub async fn pending(
        &self,
        limit: PendingAiEvaluationRunLimit,
    ) -> Result<Vec<PendingAiEvaluationRun>, PostgresAiEvaluationRunLedgerError> {
        let limit = i64::try_from(limit.get())
            .map_err(|_| PostgresAiEvaluationRunLedgerError::InvalidMetadata)?;
        let rows = sqlx::query(
            "SELECT scope, catalog_id, run_key \
             FROM rustee_ai_evaluation_run_ledger WHERE status = 'pending' \
             ORDER BY reserved_at ASC, scope ASC, run_key ASC LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(PostgresAiEvaluationRunLedgerError::storage)?;
        rows.into_iter()
            .map(|row| PendingAiEvaluationRun::from_row(&row))
            .collect()
    }

    async fn reserve_run(
        &self,
        reference: AiEvaluationReference,
    ) -> Result<AiEvaluationRunReservation, PostgresAiEvaluationRunLedgerError> {
        validate_reference(&reference)?;
        let result = sqlx::query(
            "INSERT INTO rustee_ai_evaluation_run_ledger (scope, run_key, catalog_id, status) \
             VALUES ($1, $2, $3, 'pending') \
             ON CONFLICT (scope, run_key) DO NOTHING",
        )
        .bind(reference.scope())
        .bind(reference.run_key())
        .bind(reference.catalog_id())
        .execute(&self.pool)
        .await
        .map_err(PostgresAiEvaluationRunLedgerError::storage)?;
        if result.rows_affected() == 1 {
            return Ok(AiEvaluationRunReservation::Reserved);
        }
        self.classify_existing(&reference).await
    }

    async fn classify_existing(
        &self,
        reference: &AiEvaluationReference,
    ) -> Result<AiEvaluationRunReservation, PostgresAiEvaluationRunLedgerError> {
        let row = self.find_existing(reference).await?;
        let status = row
            .try_get::<String, _>("status")
            .map_err(PostgresAiEvaluationRunLedgerError::storage)?;
        match status.as_str() {
            "pending" => Ok(AiEvaluationRunReservation::Pending),
            "completed" => Ok(AiEvaluationRunReservation::Completed),
            _ => Err(PostgresAiEvaluationRunLedgerError::InvalidMetadata),
        }
    }

    async fn record_run_completed(
        &self,
        reference: AiEvaluationReference,
    ) -> Result<(), PostgresAiEvaluationRunLedgerError> {
        validate_reference(&reference)?;
        let result = sqlx::query(
            "UPDATE rustee_ai_evaluation_run_ledger \
             SET status = 'completed', completed_at = COALESCE(completed_at, clock_timestamp()) \
             WHERE scope = $1 AND run_key = $2 AND catalog_id = $3 \
               AND status IN ('pending', 'completed')",
        )
        .bind(reference.scope())
        .bind(reference.run_key())
        .bind(reference.catalog_id())
        .execute(&self.pool)
        .await
        .map_err(PostgresAiEvaluationRunLedgerError::storage)?;
        if result.rows_affected() == 1 {
            return Ok(());
        }
        self.classify_completion_conflict(&reference).await
    }

    async fn classify_completion_conflict(
        &self,
        reference: &AiEvaluationReference,
    ) -> Result<(), PostgresAiEvaluationRunLedgerError> {
        self.find_existing(reference).await?;
        Err(PostgresAiEvaluationRunLedgerError::MissingReservation)
    }

    async fn find_existing(
        &self,
        reference: &AiEvaluationReference,
    ) -> Result<sqlx::postgres::PgRow, PostgresAiEvaluationRunLedgerError> {
        let row = sqlx::query(
            "SELECT catalog_id, status FROM rustee_ai_evaluation_run_ledger \
             WHERE scope = $1 AND run_key = $2",
        )
        .bind(reference.scope())
        .bind(reference.run_key())
        .fetch_optional(&self.pool)
        .await
        .map_err(PostgresAiEvaluationRunLedgerError::storage)?;
        let Some(row) = row else {
            return Err(PostgresAiEvaluationRunLedgerError::MissingReservation);
        };
        validate_matching_reference(&row, reference)?;
        Ok(row)
    }
}

impl fmt::Debug for PostgresAiEvaluationRunLedger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresAiEvaluationRunLedger")
            .field("pool", &"[REDACTED]")
            .finish()
    }
}

impl AiEvaluationRunLedger for PostgresAiEvaluationRunLedger {
    type Error = PostgresAiEvaluationRunLedgerError;

    fn reserve(
        &self,
        reference: AiEvaluationReference,
    ) -> BoxFuture<'static, Result<AiEvaluationRunReservation, Self::Error>> {
        let ledger = self.clone();
        Box::pin(async move { ledger.reserve_run(reference).await })
    }

    fn record_completed(
        &self,
        reference: AiEvaluationReference,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let ledger = self.clone();
        Box::pin(async move { ledger.record_run_completed(reference).await })
    }
}

/// Durable evaluation-run ledger failure with redacted debug output.
#[derive(thiserror::Error)]
pub enum PostgresAiEvaluationRunLedgerError {
    /// Stored identifiers or state were invalid.
    #[error("PostgreSQL AI evaluation run ledger metadata is invalid")]
    InvalidMetadata,
    /// A scoped run key was reused for a different catalog identity.
    #[error("AI evaluation run key conflicts with an existing catalog identity")]
    IdentityConflict,
    /// An exact reservation was absent before completion recording or duplicate verification.
    #[error("AI evaluation run reservation is missing")]
    MissingReservation,
    /// `PostgreSQL` storage did not complete; source detail remains available to application logs.
    #[error("PostgreSQL AI evaluation run ledger storage failed")]
    Storage(#[source] sqlx::Error),
}

impl PostgresAiEvaluationRunLedgerError {
    fn storage(error: sqlx::Error) -> Self {
        Self::Storage(error)
    }
}

impl fmt::Debug for PostgresAiEvaluationRunLedgerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::InvalidMetadata => "InvalidMetadata",
            Self::IdentityConflict => "IdentityConflict",
            Self::MissingReservation => "MissingReservation",
            Self::Storage(_) => "Storage",
        };
        formatter
            .debug_tuple("PostgresAiEvaluationRunLedgerError")
            .field(&name)
            .finish()
    }
}

impl PendingAiEvaluationRun {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, PostgresAiEvaluationRunLedgerError> {
        let scope = row
            .try_get::<String, _>("scope")
            .map_err(PostgresAiEvaluationRunLedgerError::storage)?;
        let catalog_id = row
            .try_get::<String, _>("catalog_id")
            .map_err(PostgresAiEvaluationRunLedgerError::storage)?;
        let run_key = row
            .try_get::<String, _>("run_key")
            .map_err(PostgresAiEvaluationRunLedgerError::storage)?;
        let reference = AiEvaluationReference::new(scope, catalog_id, run_key)
            .map_err(|_| PostgresAiEvaluationRunLedgerError::InvalidMetadata)?;
        Ok(Self { reference })
    }
}

fn validate_reference(
    reference: &AiEvaluationReference,
) -> Result<(), PostgresAiEvaluationRunLedgerError> {
    for value in [
        reference.scope(),
        reference.catalog_id(),
        reference.run_key(),
    ] {
        if value.is_empty() || value.contains('\0') || value.len() > MAX_IDENTIFIER_BYTES {
            return Err(PostgresAiEvaluationRunLedgerError::InvalidMetadata);
        }
    }
    Ok(())
}

fn validate_matching_reference(
    row: &sqlx::postgres::PgRow,
    reference: &AiEvaluationReference,
) -> Result<(), PostgresAiEvaluationRunLedgerError> {
    let catalog_id = row
        .try_get::<String, _>("catalog_id")
        .map_err(PostgresAiEvaluationRunLedgerError::storage)?;
    if catalog_id == reference.catalog_id() {
        Ok(())
    } else {
        Err(PostgresAiEvaluationRunLedgerError::IdentityConflict)
    }
}

#[cfg(test)]
mod tests {
    use rustee_ai_eval::AiEvaluationReference;

    use super::{
        PendingAiEvaluationRun, PendingAiEvaluationRunLimit, PendingAiEvaluationRunLimitError,
    };

    #[test]
    fn pending_limit_is_non_zero_and_bounded() {
        assert_eq!(
            PendingAiEvaluationRunLimit::new(0).unwrap_err(),
            PendingAiEvaluationRunLimitError::Zero
        );
        assert_eq!(
            PendingAiEvaluationRunLimit::new(1_001).unwrap_err(),
            PendingAiEvaluationRunLimitError::TooLarge
        );
        assert_eq!(PendingAiEvaluationRunLimit::new(1).unwrap().get(), 1);
    }

    #[test]
    fn pending_run_debug_redacts_its_reference() {
        let pending = PendingAiEvaluationRun {
            reference: AiEvaluationReference::new("tenant-a", "catalog-7", "run-7").unwrap(),
        };

        let rendered = format!("{pending:?}");
        assert!(!rendered.contains("tenant-a"));
        assert!(!rendered.contains("run-7"));
    }
}
