//! Validated Kafka producer configuration and shared consumer configuration admission.

use std::{collections::BTreeMap, fmt, time::Duration};

mod consumer;

pub use consumer::KafkaConsumerConfig;

const DEFAULT_DELIVERY_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_DELIVERY_TIMEOUT: Duration = Duration::from_millis(i32::MAX as u64);
const MAX_BOOTSTRAP_SERVERS_BYTES: usize = 16 * 1024;
/// Maximum UTF-8 byte length admitted for one Kafka topic name.
///
/// The bound is intentionally no larger than Kafka's broker topic-name limit. Rustee applies it
/// before native client construction so invalid routing identifiers do not reach librdkafka.
pub const MAX_TOPIC_BYTES: usize = 249;
/// Maximum native librdkafka properties admitted by one Rustee configuration.
pub const MAX_NATIVE_OPTION_COUNT: usize = 64;
/// Maximum UTF-8 byte length of one native librdkafka property name.
pub const MAX_NATIVE_OPTION_KEY_BYTES: usize = 256;
/// Maximum UTF-8 byte length of one native librdkafka property value.
pub const MAX_NATIVE_OPTION_VALUE_BYTES: usize = 64 * 1024;

/// Configuration for an acknowledged `Kafka` event producer.
///
/// Its `Debug` output keeps connection and deployment-routing values redacted.
#[derive(Clone, Eq, PartialEq)]
pub struct KafkaConfig {
    bootstrap_servers: String,
    topic: String,
    queue_timeout: Duration,
    delivery_timeout: Duration,
    options: BTreeMap<String, String>,
}

impl KafkaConfig {
    /// Creates a producer configuration with `acks=all` and finite queue timeout.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidBootstrapServers`] when the broker value is blank,
    /// oversized, or contains a NUL byte, or [`ConfigError::InvalidTopic`] when `topic` is blank,
    /// oversized, contains a NUL byte, or contains whitespace.
    pub fn new(
        bootstrap_servers: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        let bootstrap_servers = bootstrap_servers.into();
        let topic = topic.into();
        validate_bootstrap_servers(&bootstrap_servers)?;
        validate_topic_name(&topic)?;
        Ok(Self {
            bootstrap_servers,
            topic,
            queue_timeout: Duration::from_secs(5),
            delivery_timeout: DEFAULT_DELIVERY_TIMEOUT,
            options: BTreeMap::new(),
        })
    }

    /// Sets the bounded time spent waiting for local producer queue capacity.
    #[must_use]
    pub fn with_queue_timeout(mut self, timeout: Duration) -> Self {
        self.queue_timeout = timeout;
        self
    }

    /// Sets the bounded time allowed for broker delivery acknowledgement.
    ///
    /// This is applied as librdkafka's <code>message.timeout.ms</code> after native options, so
    /// an unavailable broker cannot make one publish await indefinitely.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidDeliveryTimeout`] when `timeout` is below one millisecond,
    /// has fractional milliseconds, or cannot be represented by librdkafka's millisecond setting.
    pub fn with_delivery_timeout(mut self, timeout: Duration) -> Result<Self, ConfigError> {
        if timeout.as_millis() == 0
            || !timeout.subsec_nanos().is_multiple_of(1_000_000)
            || timeout > MAX_DELIVERY_TIMEOUT
        {
            return Err(ConfigError::InvalidDeliveryTimeout);
        }
        self.delivery_timeout = timeout;
        Ok(self)
    }

    /// Returns the broker delivery acknowledgement deadline.
    #[must_use]
    pub const fn delivery_timeout(&self) -> Duration {
        self.delivery_timeout
    }

    /// Adds a native librdkafka client option such as TLS or SASL configuration.
    ///
    /// A configuration accepts at most [`MAX_NATIVE_OPTION_COUNT`] options. Names and values are
    /// bounded and omitted from [`fmt::Debug`] output. The typed bootstrap server and `acks=all`
    /// delivery invariant are applied after these options.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidNativeOption`] when a name or value exceeds its bound or
    /// contains a NUL byte, or [`ConfigError::NativeOptionLimit`] when adding a new name would
    /// exceed the configuration's option limit. Replacing an existing option does not consume an
    /// additional entry.
    pub fn with_option(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        insert_native_option(&mut self.options, key.into(), value.into())?;
        Ok(self)
    }

    /// Returns the explicit destination topic.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    #[cfg(feature = "rdkafka")]
    pub(super) fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    #[cfg(feature = "rdkafka")]
    pub(super) const fn queue_timeout(&self) -> Duration {
        self.queue_timeout
    }

    #[cfg(feature = "rdkafka")]
    pub(super) fn options(&self) -> &BTreeMap<String, String> {
        &self.options
    }
}

impl fmt::Debug for KafkaConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaConfig")
            .field("bootstrap_servers", &"[REDACTED]")
            .field("topic", &"[REDACTED]")
            .field("topic_length", &self.topic.len())
            .field("queue_timeout", &self.queue_timeout)
            .field("delivery_timeout", &self.delivery_timeout)
            .field("native_option_count", &self.options.len())
            .finish()
    }
}

/// Invalid Kafka event producer or consumer configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConfigError {
    /// Bootstrap servers must be present, bounded, and free from NUL bytes.
    #[error("Kafka bootstrap servers must be non-empty, bounded, and free from NUL bytes")]
    InvalidBootstrapServers,
    /// Topic names must be bounded, present, and free from NUL and whitespace characters.
    #[error("Kafka event topic must be bounded, non-blank, and free from NUL and whitespace")]
    InvalidTopic,
    /// Consumer groups must be present and free from whitespace.
    #[error("Kafka event consumer group must not be blank or contain whitespace")]
    InvalidGroupId,
    /// A retry topic matched the consumer's primary source topic.
    #[error("Kafka retry topic must differ from the source topic")]
    RetryTopicMatchesSource,
    /// Retry and dead-letter topic names must identify different destinations.
    #[error("Kafka retry and dead-letter topics must differ")]
    RetryTopicMatchesDeadLetter,
    /// The configured broker delivery deadline was not a whole positive millisecond in
    /// librdkafka's supported range.
    #[error("Kafka broker delivery timeout must be whole milliseconds between 1 and 2147483647")]
    InvalidDeliveryTimeout,
    /// A native librdkafka option name or value exceeded its supported bound or contained a NUL byte.
    #[error("Kafka native client option name or value was invalid")]
    InvalidNativeOption,
    /// The configuration would exceed its bounded native librdkafka option count.
    #[error("Kafka native client option count exceeded its limit")]
    NativeOptionLimit,
}

pub(super) fn insert_native_option(
    options: &mut BTreeMap<String, String>,
    key: String,
    value: String,
) -> Result<(), ConfigError> {
    if key.is_empty()
        || key.len() > MAX_NATIVE_OPTION_KEY_BYTES
        || key.contains('\0')
        || value.len() > MAX_NATIVE_OPTION_VALUE_BYTES
        || value.contains('\0')
    {
        return Err(ConfigError::InvalidNativeOption);
    }
    if !options.contains_key(&key) && options.len() == MAX_NATIVE_OPTION_COUNT {
        return Err(ConfigError::NativeOptionLimit);
    }
    options.insert(key, value);
    Ok(())
}

pub(super) fn validate_bootstrap_servers(bootstrap_servers: &str) -> Result<(), ConfigError> {
    if bootstrap_servers.trim().is_empty()
        || bootstrap_servers.len() > MAX_BOOTSTRAP_SERVERS_BYTES
        || bootstrap_servers.contains('\0')
    {
        return Err(ConfigError::InvalidBootstrapServers);
    }
    Ok(())
}

/// Validates a Kafka topic name for configuration and durable-routing boundaries.
///
/// The name must be non-blank, free from NUL and whitespace characters, and no longer than
/// [`MAX_TOPIC_BYTES`] UTF-8 bytes.
///
/// # Errors
///
/// Returns [`ConfigError::InvalidTopic`] when the topic is outside this bounded grammar.
pub fn validate_topic_name(topic: &str) -> Result<(), ConfigError> {
    if topic.trim().is_empty()
        || topic.len() > MAX_TOPIC_BYTES
        || topic.contains('\0')
        || topic.chars().any(char::is_whitespace)
    {
        return Err(ConfigError::InvalidTopic);
    }
    Ok(())
}

pub(super) fn validate_group_id(group_id: &str) -> Result<(), ConfigError> {
    if group_id.trim().is_empty() || group_id.chars().any(char::is_whitespace) {
        return Err(ConfigError::InvalidGroupId);
    }
    Ok(())
}
