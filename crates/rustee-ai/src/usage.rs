//! Content-free AI budget admission and durable usage-accounting contracts.

use std::{error::Error as StdError, fmt};

use futures_util::future::BoxFuture;

use super::{
    AiExecutionContext, ChatRequest, Usage,
    context::{IdempotencyKeyError, validate_idempotency_key},
    protocol::{ModelAliasError, validate_model_alias},
};

mod budget;

pub use budget::{AiBudgetDecision, AiBudgetPolicy, AiBudgetRequest, BudgetAdvisor};

/// One trusted, idempotent provider-attempt reservation for usage accounting.
///
/// The application creates this before the provider call from verified identity and a stable
/// request key. The key identifies one semantic provider attempt, rather than an HTTP retry. It
/// never contains prompt or completion content, and its debug representation redacts identity,
/// the key, and the model alias.
#[derive(Clone, Eq, PartialEq)]
pub struct AiUsageReservation {
    context: AiExecutionContext,
    idempotency_key: String,
    request: AiBudgetRequest,
}

impl AiUsageReservation {
    /// Creates a content-free reservation for one chat request.
    ///
    /// # Errors
    ///
    /// Returns [`AiUsageReservationError`] when `idempotency_key` is invalid.
    pub fn for_request(
        context: AiExecutionContext,
        idempotency_key: impl Into<String>,
        request: &ChatRequest,
    ) -> Result<Self, AiUsageReservationError> {
        let idempotency_key = idempotency_key.into();
        validate_usage_idempotency_key(&idempotency_key)?;
        Ok(Self {
            context,
            idempotency_key,
            request: AiBudgetRequest::from_request(request),
        })
    }

    /// Reconstructs a reservation from previously persisted content-free metadata.
    ///
    /// This is intended for a durable ledger's reconciliation query. Applications must use only
    /// metadata that was originally written by a trusted reservation path.
    ///
    /// # Errors
    ///
    /// Returns [`AiUsageReservationError`] when the idempotency key or model alias is invalid.
    pub fn from_metadata(
        context: AiExecutionContext,
        idempotency_key: impl Into<String>,
        model: impl Into<String>,
        input_characters: usize,
        tool_count: usize,
        tool_result_count: usize,
    ) -> Result<Self, AiUsageReservationError> {
        let idempotency_key = idempotency_key.into();
        validate_usage_idempotency_key(&idempotency_key)?;
        let model = model.into();
        validate_usage_model_alias(&model)?;
        Ok(Self {
            context,
            idempotency_key,
            request: AiBudgetRequest::from_metadata(
                model,
                input_characters,
                tool_count,
                tool_result_count,
            ),
        })
    }

    /// Returns the verified tenant and subject scope of the provider attempt.
    #[must_use]
    pub const fn context(&self) -> &AiExecutionContext {
        &self.context
    }

    /// Returns the application-owned idempotency key for this semantic provider attempt.
    #[must_use]
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    /// Returns content-free request metadata captured when the reservation was created.
    #[must_use]
    pub const fn request(&self) -> &AiBudgetRequest {
        &self.request
    }

    /// Creates the terminal provider-usage record for this reservation.
    #[must_use]
    pub fn settlement(&self, usage: Usage) -> AiUsageSettlement {
        AiUsageSettlement {
            reservation: self.clone(),
            usage,
        }
    }
}

impl fmt::Debug for AiUsageReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiUsageReservation")
            .field("context", &self.context)
            .field("idempotency_key", &"[REDACTED]")
            .field("request", &self.request)
            .finish()
    }
}

/// Invalid application metadata for a provider-usage reservation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AiUsageReservationError {
    /// A provider attempt must have a stable application-owned idempotency key.
    #[error("AI usage reservation idempotency key must not be blank")]
    BlankIdempotencyKey,
    /// Key was longer than durable AI metadata supports.
    #[error("AI usage reservation idempotency key exceeded the supported length")]
    IdempotencyKeyTooLong,
    /// Key contained a NUL byte.
    #[error("AI usage reservation idempotency key must not contain a NUL byte")]
    IdempotencyKeyContainsNul,
    /// A durable usage reservation must retain the deployment-owned model alias.
    #[error("AI usage reservation model alias must not be blank")]
    BlankModel,
    /// Alias was longer than durable usage metadata supports.
    #[error("AI usage reservation model alias exceeded the supported length")]
    ModelAliasTooLong,
    /// Alias contained a NUL byte.
    #[error("AI usage reservation model alias must not contain a NUL byte")]
    ModelAliasContainsNul,
}

fn validate_usage_idempotency_key(idempotency_key: &str) -> Result<(), AiUsageReservationError> {
    validate_idempotency_key(idempotency_key).map_err(|error| match error {
        IdempotencyKeyError::Blank => AiUsageReservationError::BlankIdempotencyKey,
        IdempotencyKeyError::TooLong => AiUsageReservationError::IdempotencyKeyTooLong,
        IdempotencyKeyError::ContainsNul => AiUsageReservationError::IdempotencyKeyContainsNul,
    })
}

fn validate_usage_model_alias(model: &str) -> Result<(), AiUsageReservationError> {
    validate_model_alias(model).map_err(|error| match error {
        ModelAliasError::Blank => AiUsageReservationError::BlankModel,
        ModelAliasError::TooLong => AiUsageReservationError::ModelAliasTooLong,
        ModelAliasError::ContainsNul => AiUsageReservationError::ModelAliasContainsNul,
    })
}

/// Content-free terminal usage reported by a provider for one reservation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AiUsageSettlement {
    reservation: AiUsageReservation,
    usage: Usage,
}

impl AiUsageSettlement {
    /// Returns the reservation being settled.
    #[must_use]
    pub const fn reservation(&self) -> &AiUsageReservation {
        &self.reservation
    }

    /// Returns the provider-reported token usage to record durably.
    #[must_use]
    pub const fn usage(&self) -> Usage {
        self.usage
    }
}

/// A usage-ledger decision before a provider call starts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AiUsageReservationDecision {
    /// This caller owns the reservation and may start exactly one provider attempt.
    Reserved,
    /// The application deliberately refused this attempt before provider invocation.
    Denied,
    /// A previous attempt with this key has no durable terminal usage and must be reconciled.
    PendingReconciliation,
    /// A previous attempt with this key already has durable terminal usage.
    AlreadySettled,
}

/// Application-owned durable reservation and actual-usage boundary.
///
/// A ledger atomically decides whether this caller may make a provider attempt and later records
/// provider-reported [`Usage`]. A provider transport error or a dropped stream deliberately does
/// not produce an automatic refund: delivery may have reached the provider, so the reservation
/// remains pending for an application-owned provider lookup, timeout policy, or manual review.
///
/// Implementations that enforce tenant or actor quota should make admission and reservation one
/// durable transaction. [`AiUsageReservationDecision::Reserved`] is the only decision that lets
/// [`crate::AiPipeline::complete_with_usage_ledger`] or
/// [`crate::AiPipeline::stream_with_usage_ledger`] call a provider.
pub trait AiUsageLedger: Clone + Send + Sync + 'static {
    /// Failure type returned by the application's durable ledger.
    type Error: StdError + Send + Sync + 'static;

    /// Atomically reserves one provider attempt or returns a non-starting decision.
    fn reserve(
        &self,
        reservation: AiUsageReservation,
    ) -> BoxFuture<'static, Result<AiUsageReservationDecision, Self::Error>>;

    /// Records actual usage after a provider completes successfully.
    ///
    /// This operation must be replay-safe for the same reservation and usage, and must reject a
    /// changed identity or changed terminal usage rather than overwriting a prior record.
    fn record_usage(
        &self,
        settlement: AiUsageSettlement,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}
