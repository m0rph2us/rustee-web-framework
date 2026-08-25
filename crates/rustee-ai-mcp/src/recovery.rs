//! Shared bounded automatic-recovery policy for MCP transports.

use std::time::Duration;

const MAX_AUTOMATIC_RECOVERY_ATTEMPTS: usize = 8;
const MAX_AUTOMATIC_RECOVERY_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
pub(super) struct AutomaticRecovery {
    pub(super) max_attempts: usize,
    pub(super) initial_backoff: Duration,
    pub(super) max_backoff: Duration,
}

impl AutomaticRecovery {
    pub(super) fn new(
        max_attempts: usize,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Result<Self, AutomaticRecoveryPolicyError> {
        if max_attempts == 0 {
            return Err(AutomaticRecoveryPolicyError::ZeroAttempts);
        }
        if max_attempts > MAX_AUTOMATIC_RECOVERY_ATTEMPTS {
            return Err(AutomaticRecoveryPolicyError::AttemptLimit);
        }
        if initial_backoff.is_zero() || max_backoff.is_zero() {
            return Err(AutomaticRecoveryPolicyError::ZeroBackoff);
        }
        if max_backoff < initial_backoff {
            return Err(AutomaticRecoveryPolicyError::InvalidBackoff);
        }
        if max_backoff > MAX_AUTOMATIC_RECOVERY_BACKOFF {
            return Err(AutomaticRecoveryPolicyError::BackoffLimit);
        }
        Ok(Self {
            max_attempts,
            initial_backoff,
            max_backoff,
        })
    }

    pub(super) fn delay_for(self, attempt: usize) -> Duration {
        let mut delay = self.initial_backoff;
        for _ in 0..attempt {
            delay = delay.saturating_mul(2).min(self.max_backoff);
        }
        delay
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum AutomaticRecoveryPolicyError {
    ZeroAttempts,
    AttemptLimit,
    ZeroBackoff,
    InvalidBackoff,
    BackoffLimit,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::AutomaticRecovery;

    #[test]
    fn delay_is_exponential_and_capped() {
        let recovery =
            AutomaticRecovery::new(4, Duration::from_millis(2), Duration::from_millis(5))
                .expect("valid recovery policy");

        assert_eq!(recovery.delay_for(0), Duration::from_millis(2));
        assert_eq!(recovery.delay_for(1), Duration::from_millis(4));
        assert_eq!(recovery.delay_for(2), Duration::from_millis(5));
        assert_eq!(recovery.delay_for(3), Duration::from_millis(5));
    }
}
