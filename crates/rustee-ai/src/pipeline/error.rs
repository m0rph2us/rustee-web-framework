//! Sanitized request-policy, provider, advisor, and usage-ledger lifecycle errors.

use std::fmt;

use crate::{protocol::ChatResponse, usage::AiUsageReservationError};

/// Request policy failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PolicyError {
    /// Input exceeded the configured character bound.
    #[error("AI input has {actual} characters, exceeding the limit of {limit}")]
    InputTooLarge {
        /// Configured limit.
        limit: usize,
        /// Observed count.
        actual: usize,
    },
    /// Too many tool declarations were supplied.
    #[error("AI request has {actual} tools, exceeding the limit of {limit}")]
    TooManyTools {
        /// Configured limit.
        limit: usize,
        /// Observed count.
        actual: usize,
    },
    /// Too many approved tool results were supplied.
    #[error("AI request has {actual} tool results, exceeding the limit of {limit}")]
    TooManyToolResults {
        /// Configured limit.
        limit: usize,
        /// Observed count.
        actual: usize,
    },
}

/// Policy or provider failure.
///
/// Its display and debug forms retain only the failure category. A provider source remains
/// available through [`std::error::Error::source`] for trusted handling.
#[derive(thiserror::Error)]
pub enum PipelineError<E> {
    /// Request was rejected before provider invocation.
    #[error(transparent)]
    Policy(PolicyError),
    /// Provider failed.
    #[error("AI provider failed")]
    Provider(#[source] E),
}

impl<E> fmt::Debug for PipelineError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Policy(_) => formatter.write_str("PipelineError::Policy"),
            Self::Provider(_) => formatter.write_str("PipelineError::Provider"),
        }
    }
}

/// Failure while a budget advisor admits one provider request.
///
/// Its display and debug forms retain only the failure category. The policy source remains
/// available through [`std::error::Error::source`] for trusted handling.
#[derive(thiserror::Error)]
pub enum BudgetAdvisorError<E> {
    /// The application deliberately refused this request before provider invocation.
    #[error("AI request exceeded the application budget")]
    Denied,
    /// The application budget store or policy could not decide safely.
    #[error("AI budget policy failed")]
    Policy(#[source] E),
}

impl<E> fmt::Debug for BudgetAdvisorError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Denied => formatter.write_str("BudgetAdvisorError::Denied"),
            Self::Policy(_) => formatter.write_str("BudgetAdvisorError::Policy"),
        }
    }
}

/// Failure while a usage-ledger pipeline reserves or settles one provider attempt.
///
/// Its display and debug forms retain only a safe lifecycle category. Provider and ledger sources
/// remain available through [`std::error::Error::source`] for trusted reconciliation handling.
#[derive(thiserror::Error)]
pub enum UsageLedgerPipelineError<ProviderError, LedgerError> {
    /// The request exceeded the pipeline's explicit bounds before a reservation was created.
    #[error(transparent)]
    Policy(PolicyError),
    /// The supplied reservation does not describe the request that would reach the provider.
    #[error("AI usage reservation does not match the provider request")]
    ReservationRequestMismatch,
    /// The durable ledger could not decide whether the provider may start.
    #[error("AI usage reservation could not be recorded")]
    Reservation(#[source] LedgerError),
    /// The application deliberately refused this attempt before provider invocation.
    #[error("AI request exceeded the application budget")]
    Denied,
    /// A prior attempt may have reached the provider but has no durable terminal usage yet.
    #[error("AI usage reservation requires reconciliation before another provider attempt")]
    PendingReconciliation,
    /// A prior attempt already has durable terminal usage, so this call must not be repeated.
    #[error("AI usage reservation is already settled")]
    AlreadySettled,
    /// The provider failed after reservation; usage remains pending for reconciliation.
    #[error("AI provider failed")]
    Provider(#[source] ProviderError),
    /// The provider completed, but its actual usage was not durably recorded.
    #[error("AI provider usage could not be recorded; reconciliation is required")]
    Settlement {
        /// The completed response. Reuse it or reconcile the ledger; do not repeat the provider.
        response: ChatResponse,
        /// Durable ledger failure.
        #[source]
        source: LedgerError,
    },
}

impl<ProviderError, LedgerError> fmt::Debug
    for UsageLedgerPipelineError<ProviderError, LedgerError>
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Policy(_) => formatter.write_str("UsageLedgerPipelineError::Policy"),
            Self::ReservationRequestMismatch => {
                formatter.write_str("UsageLedgerPipelineError::ReservationRequestMismatch")
            }
            Self::Reservation(_) => formatter.write_str("UsageLedgerPipelineError::Reservation"),
            Self::Denied => formatter.write_str("UsageLedgerPipelineError::Denied"),
            Self::PendingReconciliation => {
                formatter.write_str("UsageLedgerPipelineError::PendingReconciliation")
            }
            Self::AlreadySettled => formatter.write_str("UsageLedgerPipelineError::AlreadySettled"),
            Self::Provider(_) => formatter.write_str("UsageLedgerPipelineError::Provider"),
            Self::Settlement { .. } => formatter.write_str("UsageLedgerPipelineError::Settlement"),
        }
    }
}

/// Provider or ledger failure emitted after a usage-ledger stream has opened.
///
/// Its display and debug forms retain only a safe lifecycle category. Provider and ledger sources
/// remain available through [`std::error::Error::source`] for trusted reconciliation handling.
#[derive(thiserror::Error)]
pub enum UsageLedgerStreamError<ProviderError, LedgerError> {
    /// The provider emitted a stream failure; usage remains pending for reconciliation.
    #[error("AI provider stream failed")]
    Provider(#[source] ProviderError),
    /// The provider completed, but terminal usage could not be persisted.
    #[error("AI provider stream usage could not be recorded; reconciliation is required")]
    Ledger(#[source] LedgerError),
    /// The provider emitted more than one terminal completion event.
    #[error("AI provider stream emitted multiple completion events")]
    DuplicateCompletion,
}

impl<ProviderError, LedgerError> fmt::Debug for UsageLedgerStreamError<ProviderError, LedgerError> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Provider(_) => formatter.write_str("UsageLedgerStreamError::Provider"),
            Self::Ledger(_) => formatter.write_str("UsageLedgerStreamError::Ledger"),
            Self::DuplicateCompletion => {
                formatter.write_str("UsageLedgerStreamError::DuplicateCompletion")
            }
        }
    }
}

/// Failure while an advisor and usage ledger wrap a non-streaming provider call.
///
/// Its display and debug forms retain only a safe lifecycle category. Advisor and nested usage
/// sources remain available through [`std::error::Error::source`] for trusted handling.
#[derive(thiserror::Error)]
pub enum AdvisedUsageLedgerPipelineError<ProviderError, AdvisorError, LedgerError> {
    /// The application advisor could not enrich, validate, or transform the call.
    #[error("AI advisor failed")]
    Advisor(#[source] AdvisorError),
    /// The stable provider-attempt metadata was invalid before a reservation could be written.
    #[error("AI usage reservation metadata was invalid")]
    ReservationMetadata(AiUsageReservationError),
    /// Policy, durable ledger, or provider lifecycle failure.
    #[error(transparent)]
    Usage(UsageLedgerPipelineError<ProviderError, LedgerError>),
}

impl<ProviderError, AdvisorError, LedgerError> fmt::Debug
    for AdvisedUsageLedgerPipelineError<ProviderError, AdvisorError, LedgerError>
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Advisor(_) => formatter.write_str("AdvisedUsageLedgerPipelineError::Advisor"),
            Self::ReservationMetadata(_) => {
                formatter.write_str("AdvisedUsageLedgerPipelineError::ReservationMetadata")
            }
            Self::Usage(_) => formatter.write_str("AdvisedUsageLedgerPipelineError::Usage"),
        }
    }
}

/// Provider, ledger, or advisor failure emitted after an advised usage-ledger stream opens.
///
/// Its display and debug forms retain only a safe lifecycle category. Advisor and nested usage
/// sources remain available through [`std::error::Error::source`] for trusted handling.
#[derive(thiserror::Error)]
pub enum AdvisedUsageLedgerStreamError<ProviderError, AdvisorError, LedgerError> {
    /// Provider or durable-usage lifecycle failure.
    #[error(transparent)]
    Usage(UsageLedgerStreamError<ProviderError, LedgerError>),
    /// Application stream-event processing failure.
    #[error("AI advisor stream processing failed")]
    Advisor(#[source] AdvisorError),
}

impl<ProviderError, AdvisorError, LedgerError> fmt::Debug
    for AdvisedUsageLedgerStreamError<ProviderError, AdvisorError, LedgerError>
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage(_) => formatter.write_str("AdvisedUsageLedgerStreamError::Usage"),
            Self::Advisor(_) => formatter.write_str("AdvisedUsageLedgerStreamError::Advisor"),
        }
    }
}

/// Failure while an advisor wraps a non-streaming provider call.
///
/// Its display and debug forms retain only the failure category. Provider and advisor sources
/// remain available through [`std::error::Error::source`] for trusted handling.
#[derive(thiserror::Error)]
pub enum AdvisedPipelineError<ProviderError, AdvisorError> {
    /// The final advisor-produced request exceeded an explicit pipeline bound.
    #[error(transparent)]
    Policy(PolicyError),
    /// The provider rejected or could not complete the request.
    #[error("AI provider failed")]
    Provider(#[source] ProviderError),
    /// Application advisor enrichment, validation, or response processing failed.
    #[error("AI advisor failed")]
    Advisor(#[source] AdvisorError),
}

impl<ProviderError, AdvisorError> fmt::Debug for AdvisedPipelineError<ProviderError, AdvisorError> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Policy(_) => formatter.write_str("AdvisedPipelineError::Policy"),
            Self::Provider(_) => formatter.write_str("AdvisedPipelineError::Provider"),
            Self::Advisor(_) => formatter.write_str("AdvisedPipelineError::Advisor"),
        }
    }
}

/// Provider or advisor failure emitted after an advised stream has opened.
///
/// Its display and debug forms retain only the failure category. Provider and advisor sources
/// remain available through [`std::error::Error::source`] for trusted handling.
#[derive(thiserror::Error)]
pub enum AdvisedStreamError<ProviderError, AdvisorError> {
    /// The provider emitted a stream failure.
    #[error("AI provider stream failed")]
    Provider(#[source] ProviderError),
    /// Application advisor stream processing failed.
    #[error("AI advisor stream processing failed")]
    Advisor(#[source] AdvisorError),
}

impl<ProviderError, AdvisorError> fmt::Debug for AdvisedStreamError<ProviderError, AdvisorError> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Provider(_) => formatter.write_str("AdvisedStreamError::Provider"),
            Self::Advisor(_) => formatter.write_str("AdvisedStreamError::Advisor"),
        }
    }
}
