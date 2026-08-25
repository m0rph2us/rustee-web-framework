//! Bounded relay configuration and aggregate pass reports.

use std::time::Duration;

use crate::{LeaseConfig, MAX_LEASE_DURATION, MIN_POSTGRES_INTERVAL};

const MAX_RELAY_IDLE_DELAY: Duration = Duration::from_hours(1);

/// Outbox relay settings, including its claim lease and retry delay after publish failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelayConfig {
    pub(super) lease: LeaseConfig,
    pub(super) retry_delay: Duration,
}

impl RelayConfig {
    /// Creates relay settings with an explicit retry delay.
    ///
    /// A zero delay makes a failed row eligible immediately. Any non-zero delay must use
    /// `PostgreSQL`'s millisecond resolution so a publish failure cannot expose an invalid
    /// storage configuration after the provider was already called.
    ///
    /// # Errors
    ///
    /// Returns [`RelayConfigError::InvalidRetryDelay`] when a non-zero delay is below one
    /// millisecond or any delay is longer than one hour.
    pub fn new(lease: LeaseConfig, retry_delay: Duration) -> Result<Self, RelayConfigError> {
        if (!retry_delay.is_zero() && retry_delay < MIN_POSTGRES_INTERVAL)
            || retry_delay > MAX_LEASE_DURATION
        {
            return Err(RelayConfigError::InvalidRetryDelay);
        }
        Ok(Self { lease, retry_delay })
    }

    /// Returns the bounded row claim configuration.
    #[must_use]
    pub const fn lease(&self) -> LeaseConfig {
        self.lease
    }

    /// Returns the delay used after the publisher reports failure.
    #[must_use]
    pub const fn retry_delay(&self) -> Duration {
        self.retry_delay
    }
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self::new(LeaseConfig::default(), Duration::from_secs(1))
            .expect("default outbox relay configuration is valid")
    }
}

/// Invalid outbox relay configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RelayConfigError {
    /// The retry timing was not an immediate retry or a bounded `PostgreSQL` interval.
    #[error("outbox retry delay must be zero or at least 1 millisecond, and at most one hour")]
    InvalidRetryDelay,
}

/// Explicit polling settings for [`crate::EventOutboxRelay::run_until`] and
/// [`crate::JobOutboxRelay::run_until`].
///
/// This config does not start a background task. The application chooses where to await the
/// relay, supplies its shutdown future, and owns readiness, supervision, and metric export.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelayLoopConfig {
    pub(super) idle_delay: Duration,
}

impl RelayLoopConfig {
    /// Creates relay loop settings with a bounded delay after an empty pass.
    ///
    /// # Errors
    ///
    /// Returns [`RelayLoopConfigError`] when the delay is zero or longer than one hour.
    pub fn new(idle_delay: Duration) -> Result<Self, RelayLoopConfigError> {
        if idle_delay.is_zero() {
            return Err(RelayLoopConfigError::ZeroIdleDelay);
        }
        if idle_delay > MAX_RELAY_IDLE_DELAY {
            return Err(RelayLoopConfigError::IdleDelayTooLong);
        }
        Ok(Self { idle_delay })
    }

    /// Returns the delay inserted only after a relay pass claims no rows.
    #[must_use]
    pub const fn idle_delay(&self) -> Duration {
        self.idle_delay
    }
}

impl Default for RelayLoopConfig {
    fn default() -> Self {
        Self::new(Duration::from_secs(1)).expect("default outbox relay loop configuration is valid")
    }
}

/// Invalid explicit outbox relay loop settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RelayLoopConfigError {
    /// An empty-pass delay of zero would create a database polling loop.
    #[error("outbox relay idle delay must be greater than zero")]
    ZeroIdleDelay,
    /// The bounded polling delay exceeded the supported operational interval.
    #[error("outbox relay idle delay must be at most one hour")]
    IdleDelayTooLong,
}

/// Per-pass relay counts. Provider failure ends the pass after scheduling its retry.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RelayReport {
    /// Number of rows leased at the start of the pass.
    pub claimed: usize,
    /// Number of rows confirmed after broker acknowledgement.
    pub published: usize,
    /// Number of failed rows successfully released for a later retry.
    pub retry_scheduled: usize,
    /// Number of confirmation or retry operations that lost their lease ownership.
    pub lease_lost: usize,
}

/// Aggregate counts collected while an explicit relay loop was running.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RelayLoopReport {
    /// Number of bounded relay passes completed before shutdown.
    pub passes: usize,
    /// Total rows claimed across completed passes.
    pub claimed: usize,
    /// Total rows confirmed after broker acknowledgement across completed passes.
    pub published: usize,
    /// Total failed rows successfully released for a later retry across completed passes.
    pub retry_scheduled: usize,
    /// Total confirmation or retry operations that lost lease ownership across completed passes.
    pub lease_lost: usize,
}
impl RelayLoopReport {
    pub(super) fn record(&mut self, report: RelayReport) {
        self.passes = self.passes.saturating_add(1);
        self.claimed = self.claimed.saturating_add(report.claimed);
        self.published = self.published.saturating_add(report.published);
        self.retry_scheduled = self.retry_scheduled.saturating_add(report.retry_scheduled);
        self.lease_lost = self.lease_lost.saturating_add(report.lease_lost);
    }
}
