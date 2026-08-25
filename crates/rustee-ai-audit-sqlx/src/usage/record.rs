use std::{fmt, num::NonZeroUsize};

use rustee_ai::{
    AiExecutionContext, AiUsageReservation, MAX_IDEMPOTENCY_KEY_BYTES, MAX_MODEL_ALIAS_BYTES,
    MAX_SUBJECT_BYTES, MAX_TENANT_BYTES,
};

const MAX_PENDING_USAGE_LIMIT: usize = 1_000;

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
    pub(super) fn from_durable_metadata(
        tenant: String,
        subject: String,
        idempotency_key: String,
        model: String,
        input_characters: i64,
        tool_count: i64,
        tool_result_count: i64,
    ) -> Option<Self> {
        let context = AiExecutionContext::new(tenant, subject).ok()?;
        let reservation = AiUsageReservation::from_metadata(
            context,
            idempotency_key,
            model,
            usize::try_from(input_characters).ok()?,
            usize::try_from(tool_count).ok()?,
            usize::try_from(tool_result_count).ok()?,
        )
        .ok()?;
        valid_usage_reservation(&reservation).then_some(Self { reservation })
    }

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

pub(super) fn valid_usage_reservation(reservation: &AiUsageReservation) -> bool {
    [
        (reservation.context().tenant(), MAX_TENANT_BYTES),
        (reservation.context().subject(), MAX_SUBJECT_BYTES),
        (reservation.idempotency_key(), MAX_IDEMPOTENCY_KEY_BYTES),
        (reservation.request().model(), MAX_MODEL_ALIAS_BYTES),
    ]
    .into_iter()
    .all(|(value, maximum)| {
        !value.trim().is_empty() && !value.contains('\0') && value.len() <= maximum
    }) && i64::try_from(reservation.request().input_characters()).is_ok()
        && i64::try_from(reservation.request().tool_count()).is_ok()
        && i64::try_from(reservation.request().tool_result_count()).is_ok()
}

#[cfg(test)]
mod tests {
    use super::{MAX_MODEL_ALIAS_BYTES, PendingAiUsage};

    #[test]
    fn pending_reconstruction_reapplies_durable_metadata_limits() {
        let pending = PendingAiUsage::from_durable_metadata(
            "tenant-a".to_owned(),
            "subject-a".to_owned(),
            "usage:1".to_owned(),
            "model-a".to_owned(),
            12,
            1,
            0,
        )
        .expect("test metadata is valid");
        assert_eq!(pending.reservation().request().model(), "model-a");

        for (tenant, key, model) in [
            (
                "tenant\0a".to_owned(),
                "usage:1".to_owned(),
                "model-a".to_owned(),
            ),
            (
                "tenant-a".to_owned(),
                "usage\0:1".to_owned(),
                "model-a".to_owned(),
            ),
            (
                "tenant-a".to_owned(),
                "usage:1".to_owned(),
                "m".repeat(MAX_MODEL_ALIAS_BYTES + 1),
            ),
        ] {
            assert!(
                PendingAiUsage::from_durable_metadata(
                    tenant,
                    "subject-a".to_owned(),
                    key,
                    model,
                    12,
                    1,
                    0,
                )
                .is_none()
            );
        }

        for (input_characters, tool_count, tool_result_count) in
            [(-1, 1, 0), (1, -1, 0), (1, 1, -1)]
        {
            assert!(
                PendingAiUsage::from_durable_metadata(
                    "tenant-a".to_owned(),
                    "subject-a".to_owned(),
                    "usage:1".to_owned(),
                    "model-a".to_owned(),
                    input_characters,
                    tool_count,
                    tool_result_count,
                )
                .is_none()
            );
        }
    }

    #[test]
    fn pending_debug_redacts_reconciliation_identifiers() {
        let pending = PendingAiUsage::from_durable_metadata(
            "tenant-a".to_owned(),
            "subject-a".to_owned(),
            "usage:1".to_owned(),
            "model-a".to_owned(),
            12,
            1,
            0,
        )
        .expect("test metadata is valid");

        let output = format!("{pending:?}");
        for value in ["tenant-a", "subject-a", "usage:1", "model-a"] {
            assert!(!output.contains(value));
        }
    }
}
