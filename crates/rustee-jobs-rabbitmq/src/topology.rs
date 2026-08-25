//! Validated deployment topology configuration for `RabbitMQ` job routes.

use std::{fmt, time::Duration};

use rustee_jobs::RetryPolicy;

const MAX_AMQP_SHORT_STRING_BYTES: usize = 255;
const DEFAULT_PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);

/// Settings for publishing jobs through a deployment-provisioned direct exchange.
///
/// Its `Debug` output keeps deployment exchange and routing identifiers redacted.
#[derive(Clone, Eq, PartialEq)]
pub struct RabbitMqPublisherConfig {
    exchange: String,
    routing_key: String,
    publish_timeout: Duration,
}

impl RabbitMqPublisherConfig {
    /// Creates a direct-exchange route for durable job publishing.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidIdentifier`] for blank, whitespace-containing, or oversized
    /// AMQP names.
    pub fn new(
        exchange: impl Into<String>,
        routing_key: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        let exchange = exchange.into();
        let routing_key = routing_key.into();
        validate_identifier(&exchange)?;
        validate_identifier(&routing_key)?;
        Ok(Self {
            exchange,
            routing_key,
            publish_timeout: DEFAULT_PUBLISH_TIMEOUT,
        })
    }

    /// Sets the bounded time allowed for a broker publisher confirmation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroDuration`] when `publish_timeout` is zero.
    pub fn with_publish_timeout(mut self, publish_timeout: Duration) -> Result<Self, ConfigError> {
        if publish_timeout.is_zero() {
            return Err(ConfigError::ZeroDuration);
        }
        self.publish_timeout = publish_timeout;
        Ok(self)
    }

    /// Returns the deployment-provisioned direct exchange name.
    #[must_use]
    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    /// Returns the direct-exchange routing key.
    #[must_use]
    pub fn routing_key(&self) -> &str {
        &self.routing_key
    }

    pub(crate) const fn publish_timeout(&self) -> Duration {
        self.publish_timeout
    }
}

impl fmt::Debug for RabbitMqPublisherConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RabbitMqPublisherConfig")
            .field("exchange", &"[REDACTED]")
            .field("exchange_length", &self.exchange.len())
            .field("routing_key", &"[REDACTED]")
            .field("routing_key_length", &self.routing_key.len())
            .field("publish_timeout", &self.publish_timeout)
            .finish()
    }
}

/// The pre-provisioned `RabbitMQ` 4.3 quorum-queue delayed-retry policy.
///
/// `RabbitMQ` applies `min(minimum_delay * delivery_count, maximum_delay)` after a returned
/// delivery. Rustee therefore accepts this provider only for a compatible bounded
/// [`RetryPolicy`]: `minimum_delay == initial_backoff`, `maximum_delay == max_backoff`, and
/// `maximum_delay <= 3 * minimum_delay`. That range makes `RabbitMQ`'s linear sequence equal to
/// the core policy's capped exponential sequence for every retry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RabbitMqNativeRetryConfig {
    minimum_delay: Duration,
    maximum_delay: Duration,
}

impl RabbitMqNativeRetryConfig {
    /// Describes the deployment-owned `delayed-retry-min` and `delayed-retry-max` policy values.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidRetryRange`] when either delay cannot be represented as a
    /// positive whole-millisecond value or the minimum exceeds the maximum.
    pub fn new(minimum_delay: Duration, maximum_delay: Duration) -> Result<Self, ConfigError> {
        if minimum_delay.as_millis() == 0
            || !minimum_delay.subsec_nanos().is_multiple_of(1_000_000)
            || !maximum_delay.subsec_nanos().is_multiple_of(1_000_000)
            || minimum_delay > maximum_delay
        {
            return Err(ConfigError::InvalidRetryRange);
        }
        Ok(Self {
            minimum_delay,
            maximum_delay,
        })
    }

    /// Returns the policy's first-return delay.
    #[must_use]
    pub const fn minimum_delay(self) -> Duration {
        self.minimum_delay
    }

    /// Returns the policy's capped delay.
    #[must_use]
    pub const fn maximum_delay(self) -> Duration {
        self.maximum_delay
    }

    pub(crate) fn matches(self, retry_policy: RetryPolicy) -> bool {
        retry_policy.is_valid()
            && retry_policy.initial_backoff == self.minimum_delay
            && retry_policy.max_backoff == self.maximum_delay
            && self.maximum_delay <= self.minimum_delay.saturating_mul(3)
    }

    pub(crate) fn delay_for(self, next_attempt: u16) -> Duration {
        let retries_before_delivery = u32::from(next_attempt.saturating_sub(1));
        let delay = self.minimum_delay.saturating_mul(retries_before_delivery);
        delay.min(self.maximum_delay)
    }
}

/// Consumer, native delayed-retry, and dead-letter routes for one `RabbitMQ` job worker.
///
/// Its `Debug` output keeps deployment queue and routing identifiers redacted.
#[derive(Clone, Eq, PartialEq)]
pub struct RabbitMqWorkerConfig {
    queue: String,
    consumer_tag: String,
    native_retry: RabbitMqNativeRetryConfig,
    dead_letter_exchange: String,
    dead_letter_routing_key: String,
    publish_timeout: Duration,
}

impl RabbitMqWorkerConfig {
    /// Creates settings for a pre-provisioned `RabbitMQ` 4.3 quorum queue and dead-letter route.
    ///
    /// The queue must already have the matching native delayed-retry policy (`failed`,
    /// `delayed-retry-min`, and `delayed-retry-max`) and a broker-native DLX for delivery-limit
    /// failures. The adapter only uses its explicit direct exchange for poison messages and
    /// exhausted Rustee retries; it never creates queues, exchanges, bindings, or policies.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] for unsafe AMQP identifiers or an invalid native retry range.
    pub fn new(
        queue: impl Into<String>,
        consumer_tag: impl Into<String>,
        native_retry: RabbitMqNativeRetryConfig,
        dead_letter_exchange: impl Into<String>,
        dead_letter_routing_key: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        let queue = queue.into();
        let consumer_tag = consumer_tag.into();
        let dead_letter_exchange = dead_letter_exchange.into();
        let dead_letter_routing_key = dead_letter_routing_key.into();
        for identifier in [
            &queue,
            &consumer_tag,
            &dead_letter_exchange,
            &dead_letter_routing_key,
        ] {
            validate_identifier(identifier)?;
        }
        Ok(Self {
            queue,
            consumer_tag,
            native_retry,
            dead_letter_exchange,
            dead_letter_routing_key,
            publish_timeout: DEFAULT_PUBLISH_TIMEOUT,
        })
    }

    /// Sets the bounded time allowed for dead-letter publish confirmations.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroDuration`] when `publish_timeout` is zero.
    pub fn with_publish_timeout(mut self, publish_timeout: Duration) -> Result<Self, ConfigError> {
        if publish_timeout.is_zero() {
            return Err(ConfigError::ZeroDuration);
        }
        self.publish_timeout = publish_timeout;
        Ok(self)
    }

    /// Returns the deployment-provisioned source queue.
    #[must_use]
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// Returns the explicit consumer tag used by this worker.
    #[must_use]
    pub fn consumer_tag(&self) -> &str {
        &self.consumer_tag
    }

    /// Returns the expected deployment-owned native retry policy values.
    #[must_use]
    pub const fn native_retry(&self) -> RabbitMqNativeRetryConfig {
        self.native_retry
    }

    /// Returns the direct exchange used for poison messages and exhausted retries.
    #[must_use]
    pub fn dead_letter_exchange(&self) -> &str {
        &self.dead_letter_exchange
    }

    /// Returns the dead-letter direct-exchange routing key.
    #[must_use]
    pub fn dead_letter_routing_key(&self) -> &str {
        &self.dead_letter_routing_key
    }

    pub(crate) const fn publish_timeout(&self) -> Duration {
        self.publish_timeout
    }
}

impl fmt::Debug for RabbitMqWorkerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RabbitMqWorkerConfig")
            .field("queue", &"[REDACTED]")
            .field("queue_length", &self.queue.len())
            .field("consumer_tag", &"[REDACTED]")
            .field("consumer_tag_length", &self.consumer_tag.len())
            .field("native_retry", &self.native_retry)
            .field("dead_letter_exchange", &"[REDACTED]")
            .field(
                "dead_letter_exchange_length",
                &self.dead_letter_exchange.len(),
            )
            .field("dead_letter_routing_key", &"[REDACTED]")
            .field(
                "dead_letter_routing_key_length",
                &self.dead_letter_routing_key.len(),
            )
            .field("publish_timeout", &self.publish_timeout)
            .finish()
    }
}

/// Invalid `RabbitMQ` job-provider configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConfigError {
    /// An AMQP short string was blank, had whitespace, or exceeded the protocol bound.
    #[error("RabbitMQ job identifier must be a non-empty AMQP short string without whitespace")]
    InvalidIdentifier,
    /// A connection or broker confirmation timeout was zero.
    #[error("RabbitMQ timeout must be non-zero")]
    ZeroDuration,
    /// A native delayed-retry policy must use a positive millisecond minimum no greater than its
    /// maximum.
    #[error(
        "RabbitMQ native retry delays must be whole positive milliseconds with minimum not exceeding maximum"
    )]
    InvalidRetryRange,
}

fn validate_identifier(value: &str) -> Result<(), ConfigError> {
    if value.is_empty()
        || value.len() > MAX_AMQP_SHORT_STRING_BYTES
        || value.chars().any(char::is_whitespace)
    {
        return Err(ConfigError::InvalidIdentifier);
    }
    Ok(())
}
