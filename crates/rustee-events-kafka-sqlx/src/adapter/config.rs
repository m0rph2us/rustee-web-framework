use std::{num::NonZeroU16, time::Duration};

/// Bounded fixed delay applied before a failed Kafka event is released to its retry topic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KafkaDelayedRetryDelay(Duration);

impl KafkaDelayedRetryDelay {
    /// Creates a delay of at most 366 days.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaDelayedRetryDelayError::InvalidDelay`] for values below one millisecond,
    /// with fractional milliseconds, or above the durable database bound.
    pub fn new(delay: Duration) -> Result<Self, KafkaDelayedRetryDelayError> {
        if delay < Duration::from_millis(1)
            || !delay.subsec_nanos().is_multiple_of(1_000_000)
            || delay > Duration::from_secs(366 * 24 * 60 * 60)
        {
            return Err(KafkaDelayedRetryDelayError::InvalidDelay);
        }
        Ok(Self(delay))
    }

    pub(super) fn milliseconds(self) -> i64 {
        i64::try_from(self.0.as_millis()).expect("validated delay fits i64")
    }
}

/// Invalid delayed retry delay.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum KafkaDelayedRetryDelayError {
    /// The delay was not a whole positive `PostgreSQL` millisecond within the fixed bound.
    #[error("Kafka delayed retry delay must be whole milliseconds between 1 and 366 days")]
    InvalidDelay,
}

/// Explicit lease and retry timing for a `PostgreSQL` delayed-retry relay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KafkaDelayedRetryRelayConfig {
    lease: KafkaDelayedRetryDelay,
    retry_after_failure: KafkaDelayedRetryDelay,
}

impl KafkaDelayedRetryRelayConfig {
    /// Creates relay timing from validated positive, bounded durations.
    #[must_use]
    pub const fn new(
        lease: KafkaDelayedRetryDelay,
        retry_after_failure: KafkaDelayedRetryDelay,
    ) -> Self {
        Self {
            lease,
            retry_after_failure,
        }
    }

    pub(super) const fn lease(self) -> KafkaDelayedRetryDelay {
        self.lease
    }

    pub(super) const fn retry_after_failure(self) -> KafkaDelayedRetryDelay {
        self.retry_after_failure
    }
}

const MAX_READINESS_TIMEOUT: Duration = Duration::from_secs(60);

/// Bounded per-dependency timeout for the delayed-retry relay readiness check.
///
/// The framework does not register a health route or decide whether a Kafka delayed-retry relay
/// is required for a deployment. The application calls this check from its chosen readiness
/// policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KafkaDelayedRetryReadinessConfig {
    database_timeout: Duration,
    kafka_timeout: Duration,
}

impl KafkaDelayedRetryReadinessConfig {
    /// Creates explicit bounded timeout settings for the `PostgreSQL` and Kafka checks.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaDelayedRetryReadinessConfigError`] when either timeout is zero or longer
    /// than one minute.
    pub fn new(
        database_timeout: Duration,
        kafka_timeout: Duration,
    ) -> Result<Self, KafkaDelayedRetryReadinessConfigError> {
        if database_timeout.is_zero() || kafka_timeout.is_zero() {
            return Err(KafkaDelayedRetryReadinessConfigError::ZeroTimeout);
        }
        if database_timeout > MAX_READINESS_TIMEOUT || kafka_timeout > MAX_READINESS_TIMEOUT {
            return Err(KafkaDelayedRetryReadinessConfigError::TimeoutTooLong);
        }
        Ok(Self {
            database_timeout,
            kafka_timeout,
        })
    }

    /// Returns the timeout used for `PostgreSQL` retry-table access.
    #[must_use]
    pub const fn database_timeout(self) -> Duration {
        self.database_timeout
    }

    /// Returns the timeout used for retry and dead-letter Kafka topic metadata.
    #[must_use]
    pub const fn kafka_timeout(self) -> Duration {
        self.kafka_timeout
    }
}

impl Default for KafkaDelayedRetryReadinessConfig {
    fn default() -> Self {
        Self::new(Duration::from_secs(5), Duration::from_secs(5))
            .expect("default Kafka delayed-retry readiness configuration is valid")
    }
}

/// Invalid explicit delayed-retry readiness timeout settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum KafkaDelayedRetryReadinessConfigError {
    /// A zero timeout cannot bound a dependency check.
    #[error("Kafka delayed retry readiness timeouts must be greater than zero")]
    ZeroTimeout,
    /// The timeout exceeded the supported operational interval.
    #[error("Kafka delayed retry readiness timeouts must be at most one minute")]
    TimeoutTooLong,
}

/// Sanitized delayed-retry readiness failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum KafkaDelayedRetryReadinessError {
    /// `PostgreSQL` could not query the delayed-retry table before its configured timeout.
    #[error("Kafka delayed retry PostgreSQL readiness check failed")]
    Database,
    /// Kafka could not return retry and dead-letter topic metadata before its configured timeout.
    #[error("Kafka delayed retry Kafka readiness check failed")]
    Kafka,
}

const MAX_RELAY_IDLE_DELAY: Duration = Duration::from_hours(1);
const MAX_RELAY_BATCH_SIZE: u16 = 100;

/// A non-zero, bounded number of delayed-retry rows claimed in one relay pass.
///
/// Durable retry payloads and optional partition keys are each limited to one MiB, so this keeps
/// a single `PostgreSQL` fetch within a predictable memory budget.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KafkaDelayedRetryRelayBatchSize(NonZeroU16);

impl KafkaDelayedRetryRelayBatchSize {
    /// Creates a batch size of at most 100 delayed-retry rows.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaDelayedRetryRelayBatchSizeError`] when `batch_size` is zero or too large.
    pub fn new(batch_size: u16) -> Result<Self, KafkaDelayedRetryRelayBatchSizeError> {
        let batch_size =
            NonZeroU16::new(batch_size).ok_or(KafkaDelayedRetryRelayBatchSizeError::Zero)?;
        if batch_size.get() > MAX_RELAY_BATCH_SIZE {
            return Err(KafkaDelayedRetryRelayBatchSizeError::TooLarge);
        }
        Ok(Self(batch_size))
    }

    /// Returns the number of rows claimed in one pass.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

/// Invalid Kafka delayed-retry relay batch size.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum KafkaDelayedRetryRelayBatchSizeError {
    /// An empty batch cannot make relay progress.
    #[error("Kafka delayed retry relay batch size must be greater than zero")]
    Zero,
    /// A batch would retain too many potentially large payloads in one query result.
    #[error("Kafka delayed retry relay batch size must be at most 100")]
    TooLarge,
}

/// Explicit polling settings for a delayed-retry relay loop.
///
/// This configuration does not start a background task. The application chooses where to await
/// the relay, supplies its shutdown future, and owns readiness, supervision, and metric export.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KafkaDelayedRetryRelayLoopConfig {
    batch_size: KafkaDelayedRetryRelayBatchSize,
    idle_delay: Duration,
}

impl KafkaDelayedRetryRelayLoopConfig {
    /// Creates bounded relay-loop settings with a delay after an empty pass.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaDelayedRetryRelayLoopConfigError`] when `idle_delay` is zero or longer
    /// than one hour. Constructing `batch_size` validates its own non-zero upper bound.
    pub fn new(
        batch_size: KafkaDelayedRetryRelayBatchSize,
        idle_delay: Duration,
    ) -> Result<Self, KafkaDelayedRetryRelayLoopConfigError> {
        if idle_delay.is_zero() {
            return Err(KafkaDelayedRetryRelayLoopConfigError::ZeroIdleDelay);
        }
        if idle_delay > MAX_RELAY_IDLE_DELAY {
            return Err(KafkaDelayedRetryRelayLoopConfigError::IdleDelayTooLong);
        }
        Ok(Self {
            batch_size,
            idle_delay,
        })
    }

    /// Returns the maximum number of due rows claimed by each pass.
    #[must_use]
    pub const fn batch_size(self) -> KafkaDelayedRetryRelayBatchSize {
        self.batch_size
    }

    /// Returns the delay inserted only after a pass publishes no rows.
    #[must_use]
    pub const fn idle_delay(self) -> Duration {
        self.idle_delay
    }
}

impl Default for KafkaDelayedRetryRelayLoopConfig {
    fn default() -> Self {
        Self::new(
            KafkaDelayedRetryRelayBatchSize::new(100)
                .expect("default Kafka relay batch size is valid"),
            Duration::from_secs(1),
        )
        .expect("default Kafka relay loop configuration is valid")
    }
}

/// Invalid explicit Kafka delayed-retry relay-loop settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum KafkaDelayedRetryRelayLoopConfigError {
    /// An empty-pass delay of zero would create a database polling loop.
    #[error("Kafka delayed retry relay idle delay must be greater than zero")]
    ZeroIdleDelay,
    /// The bounded polling delay exceeded the supported operational interval.
    #[error("Kafka delayed retry relay idle delay must be at most one hour")]
    IdleDelayTooLong,
}

/// Aggregate counts collected while an explicit Kafka delayed-retry relay loop was running.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct KafkaDelayedRetryRelayLoopReport {
    /// Number of bounded relay passes completed before shutdown.
    pub passes: usize,
    /// Total records confirmed after Kafka acknowledgement across completed passes.
    pub published: usize,
}

impl KafkaDelayedRetryRelayLoopReport {
    pub(super) fn record(&mut self, published: u16) {
        self.passes = self.passes.saturating_add(1);
        self.published = self.published.saturating_add(usize::from(published));
    }
}

#[cfg(test)]
mod tests {
    use super::{
        KafkaDelayedRetryDelay, KafkaDelayedRetryReadinessConfig,
        KafkaDelayedRetryReadinessConfigError, KafkaDelayedRetryRelayBatchSize,
        KafkaDelayedRetryRelayBatchSizeError, KafkaDelayedRetryRelayConfig,
        KafkaDelayedRetryRelayLoopConfig, KafkaDelayedRetryRelayLoopConfigError,
    };
    use std::time::Duration;

    #[test]
    fn timing_is_positive_and_bounded() {
        assert!(KafkaDelayedRetryDelay::new(Duration::ZERO).is_err());
        assert!(KafkaDelayedRetryDelay::new(Duration::from_nanos(1)).is_err());
        assert!(
            KafkaDelayedRetryDelay::new(Duration::from_millis(1) + Duration::from_nanos(1))
                .is_err()
        );
        assert!(KafkaDelayedRetryDelay::new(Duration::from_secs(366 * 24 * 60 * 60 + 1)).is_err());
        let delay = KafkaDelayedRetryDelay::new(Duration::from_millis(1)).unwrap();
        assert_eq!(delay.milliseconds(), 1);

        let lease = KafkaDelayedRetryDelay::new(Duration::from_secs(30)).unwrap();
        let retry = KafkaDelayedRetryDelay::new(Duration::from_secs(1)).unwrap();
        let config = KafkaDelayedRetryRelayConfig::new(lease, retry);
        assert_eq!(config.lease().milliseconds(), 30_000);
        assert_eq!(config.retry_after_failure().milliseconds(), 1_000);
    }

    #[test]
    fn readiness_and_loop_configuration_are_bounded_and_explicit() {
        assert!(matches!(
            KafkaDelayedRetryReadinessConfig::new(Duration::ZERO, Duration::from_secs(1)),
            Err(KafkaDelayedRetryReadinessConfigError::ZeroTimeout)
        ));
        assert!(matches!(
            KafkaDelayedRetryReadinessConfig::new(Duration::from_secs(1), Duration::from_secs(61)),
            Err(KafkaDelayedRetryReadinessConfigError::TimeoutTooLong)
        ));
        let readiness = KafkaDelayedRetryReadinessConfig::new(
            Duration::from_millis(5),
            Duration::from_millis(7),
        )
        .unwrap();
        assert_eq!(readiness.database_timeout(), Duration::from_millis(5));
        assert_eq!(readiness.kafka_timeout(), Duration::from_millis(7));

        assert_eq!(
            KafkaDelayedRetryRelayBatchSize::new(0).unwrap_err(),
            KafkaDelayedRetryRelayBatchSizeError::Zero
        );
        assert_eq!(
            KafkaDelayedRetryRelayBatchSize::new(101).unwrap_err(),
            KafkaDelayedRetryRelayBatchSizeError::TooLarge
        );
        let batch_size = KafkaDelayedRetryRelayBatchSize::new(8).unwrap();
        assert_eq!(batch_size.get(), 8);
        assert!(matches!(
            KafkaDelayedRetryRelayLoopConfig::new(batch_size, Duration::ZERO),
            Err(KafkaDelayedRetryRelayLoopConfigError::ZeroIdleDelay)
        ));
        assert!(matches!(
            KafkaDelayedRetryRelayLoopConfig::new(batch_size, Duration::from_secs(60 * 60 + 1)),
            Err(KafkaDelayedRetryRelayLoopConfigError::IdleDelayTooLong)
        ));
        let config =
            KafkaDelayedRetryRelayLoopConfig::new(batch_size, Duration::from_millis(1)).unwrap();
        assert_eq!(config.batch_size(), batch_size);
        assert_eq!(config.idle_delay(), Duration::from_millis(1));
    }
}
