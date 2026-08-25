//! Provider-neutral retry policy and settlement actions.

use std::time::Duration;

/// A provider-neutral retry policy for failures after a delivery attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    /// Maximum total deliveries, including the first delivery.
    pub max_deliveries: u16,
    /// Delay before the first retry.
    pub initial_backoff: Duration,
    /// Maximum retry delay after exponential backoff.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_deliveries: 5,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_mins(5),
        }
    }
}

impl RetryPolicy {
    /// Returns whether this policy has a usable delivery budget and ordered retry delays.
    ///
    /// A valid policy allows at least one total delivery, uses a non-zero first retry delay, and
    /// never caps retries below that first delay. Providers may impose additional constraints for
    /// their native delay representation.
    #[must_use]
    pub fn is_valid(self) -> bool {
        self.max_deliveries != 0
            && !self.initial_backoff.is_zero()
            && self.initial_backoff <= self.max_backoff
    }

    /// Chooses a retry or dead-letter action after a failed one-based delivery attempt.
    ///
    /// An invalid policy fails closed to [`DeliveryAction::DeadLetter`] so direct callers cannot
    /// produce an unbounded or immediate retry when they bypass provider startup validation.
    #[must_use]
    pub fn after_failure(self, attempt: u16) -> DeliveryAction {
        if !self.is_valid() || attempt == 0 || attempt >= self.max_deliveries {
            return DeliveryAction::DeadLetter;
        }

        let exponent = u32::from(attempt.saturating_sub(1));
        let multiplier = 2_u32.saturating_pow(exponent);
        let delay = self
            .initial_backoff
            .saturating_mul(multiplier)
            .min(self.max_backoff);
        DeliveryAction::Retry {
            next_attempt: attempt.saturating_add(1),
            delay,
        }
    }
}

/// The next provider delivery action after handling a job.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryAction {
    /// Acknowledge only after the handler's side effect has completed successfully.
    Acknowledge,
    /// Retry after an explicit delay, preserving the next delivery attempt number.
    Retry {
        /// One-based delivery attempt number to use on the retry.
        next_attempt: u16,
        /// Minimum delay before another delivery attempt.
        delay: Duration,
    },
    /// Move the message to a provider-specific dead-letter path without automatic replay.
    DeadLetter,
}
