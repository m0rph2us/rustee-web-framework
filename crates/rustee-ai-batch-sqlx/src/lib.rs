//! Optional durable `PostgreSQL` storage for AI batch artifact reconciliation.
//!
//! Applications add [`AI_BATCH_ARTIFACT_LEDGER_MIGRATION_SQL`] to their normal deployment
//! migration sequence. The ledger persists opaque batch/file references and never provider JSONL
//! rows, prompts, model response bodies, or domain result values. It intentionally does not run
//! migrations, download artifacts, requeue pending work, or erase provider files at startup.

use std::{fmt, num::NonZeroUsize};

use futures_util::future::BoxFuture;
use rustee_ai_batch::{
    AiBatchArtifactKind, AiBatchArtifactLedger, AiBatchArtifactReference,
    AiBatchArtifactReservation, AiBatchReference,
};
use sqlx::{PgPool, Row};

const MAX_IDENTIFIER_BYTES: usize = 128;
const MAX_PENDING_LIMIT: usize = 1_000;

/// The deployment-owned migration for durable AI batch artifact reconciliation records.
pub const AI_BATCH_ARTIFACT_LEDGER_MIGRATION_SQL: &str =
    include_str!("../migrations/0001_rustee_ai_batch_artifact_ledger.sql");

/// A bounded request for unresolved provider artifact reconciliations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PendingAiBatchArtifactLimit(NonZeroUsize);

impl PendingAiBatchArtifactLimit {
    /// Creates a non-zero, bounded pending-artifact query limit.
    ///
    /// # Errors
    ///
    /// Returns [`PendingAiBatchArtifactLimitError`] when `limit` is zero or too large.
    pub fn new(limit: usize) -> Result<Self, PendingAiBatchArtifactLimitError> {
        let limit = NonZeroUsize::new(limit).ok_or(PendingAiBatchArtifactLimitError::Zero)?;
        if limit.get() > MAX_PENDING_LIMIT {
            return Err(PendingAiBatchArtifactLimitError::TooLarge);
        }
        Ok(Self(limit))
    }

    /// Returns the configured number of records.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

impl Default for PendingAiBatchArtifactLimit {
    fn default() -> Self {
        Self(NonZeroUsize::new(100).expect("default pending artifact limit is non-zero"))
    }
}

/// Invalid pending-artifact query limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PendingAiBatchArtifactLimitError {
    /// A reconciliation query must request at least one record.
    #[error("pending AI batch artifact limit must be non-zero")]
    Zero,
    /// A reconciliation query would retain too many rows in one response.
    #[error("pending AI batch artifact limit exceeds the supported maximum")]
    TooLarge,
}

/// One provider artifact whose previous reconciliation attempt is still ambiguous.
///
/// This metadata is deliberately returned only to application-owned reconciliation code. Its
/// debug representation redacts every identifier; applications authorize the batch scope and
/// inspect their row-level domain idempotency ledger before scheduling another attempt.
#[derive(Clone, Eq, PartialEq)]
pub struct PendingAiBatchArtifact {
    reference: AiBatchArtifactReference,
}

impl PendingAiBatchArtifact {
    /// Returns the content-free artifact reference for application reconciliation.
    #[must_use]
    pub const fn reference(&self) -> &AiBatchArtifactReference {
        &self.reference
    }
}

impl fmt::Debug for PendingAiBatchArtifact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingAiBatchArtifact")
            .field("reference", &"[REDACTED]")
            .finish()
    }
}

/// Durable `PostgreSQL` implementation of [`AiBatchArtifactLedger`].
///
/// The primary key is `(scope, reconciliation_key)`. Repeating the same exact reference is
/// idempotent; reusing that durable key for another catalog/run/file/artifact role fails instead
/// of overwriting the first record. A loader, processor, or completion-record failure leaves the
/// row pending. The application owns its retry/DLQ schedule, row-level `custom_id` idempotency,
/// provider result lookup, billing, retention, and erase procedure.
#[derive(Clone)]
pub struct PostgresAiBatchArtifactLedger {
    pool: PgPool,
}

impl PostgresAiBatchArtifactLedger {
    /// Creates a durable artifact ledger from an application-owned `PostgreSQL` pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Returns pending artifacts in oldest-first order for application reconciliation.
    ///
    /// This method never downloads a provider file, decodes a model body, replays a side effect,
    /// changes reservation status, or schedules a retry.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresAiBatchArtifactLedgerError`] when storage is unavailable or metadata is
    /// invalid.
    pub async fn pending(
        &self,
        limit: PendingAiBatchArtifactLimit,
    ) -> Result<Vec<PendingAiBatchArtifact>, PostgresAiBatchArtifactLedgerError> {
        let limit = i64::try_from(limit.get())
            .map_err(|_| PostgresAiBatchArtifactLedgerError::InvalidMetadata)?;
        let rows = sqlx::query(
            "SELECT scope, catalog_id, run_key, artifact_kind, provider_file_id, reconciliation_key \
             FROM rustee_ai_batch_artifact_ledger WHERE status = 'pending' \
             ORDER BY reserved_at ASC, scope ASC, reconciliation_key ASC LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(PostgresAiBatchArtifactLedgerError::storage)?;
        rows.into_iter()
            .map(|row| PendingAiBatchArtifact::from_row(&row))
            .collect()
    }

    async fn reserve_artifact(
        &self,
        reference: &AiBatchArtifactReference,
    ) -> Result<AiBatchArtifactReservation, PostgresAiBatchArtifactLedgerError> {
        validate_reference(reference)?;
        let result = sqlx::query(
            "INSERT INTO rustee_ai_batch_artifact_ledger \
             (scope, reconciliation_key, catalog_id, run_key, artifact_kind, provider_file_id, status) \
             VALUES ($1, $2, $3, $4, $5, $6, 'pending') \
             ON CONFLICT (scope, reconciliation_key) DO NOTHING",
        )
        .bind(reference.batch().scope())
        .bind(reference.reconciliation_key())
        .bind(reference.batch().catalog_id())
        .bind(reference.batch().run_key())
        .bind(artifact_kind_name(reference.kind()))
        .bind(reference.provider_file_id())
        .execute(&self.pool)
        .await
        .map_err(PostgresAiBatchArtifactLedgerError::storage)?;
        if result.rows_affected() == 1 {
            return Ok(AiBatchArtifactReservation::Reserved);
        }
        self.classify_existing(reference).await
    }

    async fn classify_existing(
        &self,
        reference: &AiBatchArtifactReference,
    ) -> Result<AiBatchArtifactReservation, PostgresAiBatchArtifactLedgerError> {
        let row = self.find_existing(reference).await?;
        let status = row
            .try_get::<String, _>("status")
            .map_err(PostgresAiBatchArtifactLedgerError::storage)?;
        match status.as_str() {
            "pending" => Ok(AiBatchArtifactReservation::Pending),
            "reconciled" => Ok(AiBatchArtifactReservation::Reconciled),
            _ => Err(PostgresAiBatchArtifactLedgerError::InvalidMetadata),
        }
    }

    async fn record_artifact_reconciled(
        &self,
        reference: &AiBatchArtifactReference,
    ) -> Result<(), PostgresAiBatchArtifactLedgerError> {
        validate_reference(reference)?;
        let result = sqlx::query(
            "UPDATE rustee_ai_batch_artifact_ledger \
             SET status = 'reconciled', reconciled_at = COALESCE(reconciled_at, clock_timestamp()) \
             WHERE scope = $1 AND reconciliation_key = $2 AND catalog_id = $3 AND run_key = $4 \
               AND artifact_kind = $5 AND provider_file_id = $6 \
               AND status IN ('pending', 'reconciled')",
        )
        .bind(reference.batch().scope())
        .bind(reference.reconciliation_key())
        .bind(reference.batch().catalog_id())
        .bind(reference.batch().run_key())
        .bind(artifact_kind_name(reference.kind()))
        .bind(reference.provider_file_id())
        .execute(&self.pool)
        .await
        .map_err(PostgresAiBatchArtifactLedgerError::storage)?;
        if result.rows_affected() == 1 {
            return Ok(());
        }
        self.classify_record_conflict(reference).await
    }

    async fn classify_record_conflict(
        &self,
        reference: &AiBatchArtifactReference,
    ) -> Result<(), PostgresAiBatchArtifactLedgerError> {
        let row = self.find_existing(reference).await?;
        let status = row
            .try_get::<String, _>("status")
            .map_err(PostgresAiBatchArtifactLedgerError::storage)?;
        if matches!(status.as_str(), "pending" | "reconciled") {
            Err(PostgresAiBatchArtifactLedgerError::MissingReservation)
        } else {
            Err(PostgresAiBatchArtifactLedgerError::InvalidMetadata)
        }
    }

    async fn find_existing(
        &self,
        reference: &AiBatchArtifactReference,
    ) -> Result<sqlx::postgres::PgRow, PostgresAiBatchArtifactLedgerError> {
        let row = sqlx::query(
            "SELECT catalog_id, run_key, artifact_kind, provider_file_id, status \
             FROM rustee_ai_batch_artifact_ledger WHERE scope = $1 AND reconciliation_key = $2",
        )
        .bind(reference.batch().scope())
        .bind(reference.reconciliation_key())
        .fetch_optional(&self.pool)
        .await
        .map_err(PostgresAiBatchArtifactLedgerError::storage)?;
        let Some(row) = row else {
            return Err(PostgresAiBatchArtifactLedgerError::MissingReservation);
        };
        validate_matching_reference(&row, reference)?;
        Ok(row)
    }
}

impl fmt::Debug for PostgresAiBatchArtifactLedger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresAiBatchArtifactLedger")
            .field("pool", &"[REDACTED]")
            .finish()
    }
}

impl AiBatchArtifactLedger for PostgresAiBatchArtifactLedger {
    type Error = PostgresAiBatchArtifactLedgerError;

    fn reserve(
        &self,
        reference: AiBatchArtifactReference,
    ) -> BoxFuture<'static, Result<AiBatchArtifactReservation, Self::Error>> {
        let ledger = self.clone();
        Box::pin(async move { ledger.reserve_artifact(&reference).await })
    }

    fn record_reconciled(
        &self,
        reference: AiBatchArtifactReference,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let ledger = self.clone();
        Box::pin(async move { ledger.record_artifact_reconciled(&reference).await })
    }
}

/// Sanitized failure from `PostgreSQL` durable batch-artifact storage.
#[derive(thiserror::Error)]
pub enum PostgresAiBatchArtifactLedgerError {
    /// One field was not safe for this adapter's bounded durable schema.
    #[error("AI batch artifact reference has invalid durable metadata")]
    InvalidMetadata,
    /// A reconciliation key was reused for another artifact identity.
    #[error("AI batch artifact reconciliation key conflicts with an existing artifact")]
    IdentityConflict,
    /// The exact artifact reservation was absent before recording or duplicate verification.
    #[error("AI batch artifact reservation is missing")]
    MissingReservation,
    /// `PostgreSQL` storage did not complete; source detail remains available to application logs.
    #[error("PostgreSQL AI batch artifact ledger storage failed")]
    Storage(#[source] sqlx::Error),
}

impl PostgresAiBatchArtifactLedgerError {
    fn storage(error: sqlx::Error) -> Self {
        Self::Storage(error)
    }
}

impl fmt::Debug for PostgresAiBatchArtifactLedgerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::InvalidMetadata => "InvalidMetadata",
            Self::IdentityConflict => "IdentityConflict",
            Self::MissingReservation => "MissingReservation",
            Self::Storage(_) => "Storage",
        };
        formatter
            .debug_tuple("PostgresAiBatchArtifactLedgerError")
            .field(&name)
            .finish()
    }
}

impl PendingAiBatchArtifact {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, PostgresAiBatchArtifactLedgerError> {
        let scope = row
            .try_get::<String, _>("scope")
            .map_err(PostgresAiBatchArtifactLedgerError::storage)?;
        let catalog_id = row
            .try_get::<String, _>("catalog_id")
            .map_err(PostgresAiBatchArtifactLedgerError::storage)?;
        let run_key = row
            .try_get::<String, _>("run_key")
            .map_err(PostgresAiBatchArtifactLedgerError::storage)?;
        let kind = row
            .try_get::<String, _>("artifact_kind")
            .map_err(PostgresAiBatchArtifactLedgerError::storage)?;
        let provider_file_id = row
            .try_get::<String, _>("provider_file_id")
            .map_err(PostgresAiBatchArtifactLedgerError::storage)?;
        let reconciliation_key = row
            .try_get::<String, _>("reconciliation_key")
            .map_err(PostgresAiBatchArtifactLedgerError::storage)?;
        let batch = AiBatchReference::new(scope, catalog_id, run_key)
            .map_err(|_| PostgresAiBatchArtifactLedgerError::InvalidMetadata)?;
        let reference = AiBatchArtifactReference::new(
            batch,
            parse_artifact_kind(&kind)?,
            provider_file_id,
            reconciliation_key,
        )
        .map_err(|_| PostgresAiBatchArtifactLedgerError::InvalidMetadata)?;
        Ok(Self { reference })
    }
}

fn validate_reference(
    reference: &AiBatchArtifactReference,
) -> Result<(), PostgresAiBatchArtifactLedgerError> {
    for value in [
        reference.batch().scope(),
        reference.batch().catalog_id(),
        reference.batch().run_key(),
        reference.provider_file_id(),
        reference.reconciliation_key(),
    ] {
        if value.is_empty() || value.contains('\0') || value.len() > MAX_IDENTIFIER_BYTES {
            return Err(PostgresAiBatchArtifactLedgerError::InvalidMetadata);
        }
    }
    Ok(())
}

fn validate_matching_reference(
    row: &sqlx::postgres::PgRow,
    reference: &AiBatchArtifactReference,
) -> Result<(), PostgresAiBatchArtifactLedgerError> {
    let catalog_id = row
        .try_get::<String, _>("catalog_id")
        .map_err(PostgresAiBatchArtifactLedgerError::storage)?;
    let run_key = row
        .try_get::<String, _>("run_key")
        .map_err(PostgresAiBatchArtifactLedgerError::storage)?;
    let kind = row
        .try_get::<String, _>("artifact_kind")
        .map_err(PostgresAiBatchArtifactLedgerError::storage)?;
    let provider_file_id = row
        .try_get::<String, _>("provider_file_id")
        .map_err(PostgresAiBatchArtifactLedgerError::storage)?;
    if catalog_id == reference.batch().catalog_id()
        && run_key == reference.batch().run_key()
        && kind == artifact_kind_name(reference.kind())
        && provider_file_id == reference.provider_file_id()
    {
        Ok(())
    } else {
        Err(PostgresAiBatchArtifactLedgerError::IdentityConflict)
    }
}

fn artifact_kind_name(kind: AiBatchArtifactKind) -> &'static str {
    match kind {
        AiBatchArtifactKind::Output => "output",
        AiBatchArtifactKind::Error => "error",
    }
}

fn parse_artifact_kind(
    value: &str,
) -> Result<AiBatchArtifactKind, PostgresAiBatchArtifactLedgerError> {
    match value {
        "output" => Ok(AiBatchArtifactKind::Output),
        "error" => Ok(AiBatchArtifactKind::Error),
        _ => Err(PostgresAiBatchArtifactLedgerError::InvalidMetadata),
    }
}

#[cfg(test)]
mod tests {
    use rustee_ai_batch::{AiBatchArtifactKind, AiBatchArtifactReference, AiBatchReference};

    use super::{
        PendingAiBatchArtifact, PendingAiBatchArtifactLimit, PendingAiBatchArtifactLimitError,
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
        let pending = PendingAiBatchArtifact { reference };

        let rendered = format!("{pending:?}");
        assert!(!rendered.contains("tenant-a"));
        assert!(!rendered.contains("reconcile-7"));
    }
}
