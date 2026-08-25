//! Manual-commit Kafka consumer configuration.

use std::{collections::BTreeMap, fmt};

use super::{
    ConfigError, insert_native_option, validate_bootstrap_servers, validate_group_id,
    validate_topic_name,
};

/// Configuration for one manual-commit `Kafka` event consumer group.
///
/// Its `Debug` output keeps connection and deployment-routing values redacted.
#[derive(Clone, Eq, PartialEq)]
pub struct KafkaConsumerConfig {
    bootstrap_servers: String,
    topic: String,
    group_id: String,
    retry_topic: Option<String>,
    options: BTreeMap<String, String>,
}

impl KafkaConsumerConfig {
    /// Creates a consumer configuration that disables automatic offset commits and stores.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when the broker value is invalid, or `topic` or `group_id` is
    /// blank or contains whitespace.
    pub fn new(
        bootstrap_servers: impl Into<String>,
        topic: impl Into<String>,
        group_id: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        let bootstrap_servers = bootstrap_servers.into();
        let topic = topic.into();
        let group_id = group_id.into();
        validate_bootstrap_servers(&bootstrap_servers)?;
        validate_topic_name(&topic)?;
        validate_group_id(&group_id)?;
        Ok(Self {
            bootstrap_servers,
            topic,
            group_id,
            retry_topic: None,
            options: BTreeMap::new(),
        })
    }

    /// Subscribes this consumer group to one retry topic in addition to its source topic.
    ///
    /// Retry records retain the original event envelope and are handled by the same typed handler.
    /// A retry topic cannot equal the source topic because that would make the failure route
    /// indistinguishable from an unbounded source redelivery loop.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidTopic`] for a blank, oversized, NUL-containing, or
    /// whitespace-containing topic name, or
    /// [`ConfigError::RetryTopicMatchesSource`] when it equals the source topic.
    pub fn with_retry_topic(mut self, retry_topic: impl Into<String>) -> Result<Self, ConfigError> {
        let retry_topic = retry_topic.into();
        validate_topic_name(&retry_topic)?;
        if retry_topic == self.topic {
            return Err(ConfigError::RetryTopicMatchesSource);
        }
        self.retry_topic = Some(retry_topic);
        Ok(self)
    }

    /// Adds a native librdkafka client option such as TLS or SASL configuration.
    ///
    /// A configuration accepts at most [`super::MAX_NATIVE_OPTION_COUNT`] options. Names and
    /// values are bounded and omitted from [`fmt::Debug`] output. The typed bootstrap server,
    /// group, and manual commit/store invariants are applied after these options.
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

    /// Returns the subscribed topic.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the stable consumer-group identifier.
    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the optional retry topic subscribed by this consumer group.
    #[must_use]
    pub fn retry_topic(&self) -> Option<&str> {
        self.retry_topic.as_deref()
    }

    #[cfg(feature = "rdkafka")]
    pub(crate) fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    #[cfg(feature = "rdkafka")]
    pub(crate) fn options(&self) -> &BTreeMap<String, String> {
        &self.options
    }

    #[cfg(feature = "rdkafka")]
    pub(crate) fn subscription_topics(&self) -> Vec<&str> {
        let mut topics = vec![self.topic.as_str()];
        if let Some(retry_topic) = self.retry_topic.as_deref() {
            topics.push(retry_topic);
        }
        topics
    }
}

impl fmt::Debug for KafkaConsumerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaConsumerConfig")
            .field("bootstrap_servers", &"[REDACTED]")
            .field("topic", &"[REDACTED]")
            .field("topic_length", &self.topic.len())
            .field("group_id", &"[REDACTED]")
            .field("group_id_length", &self.group_id.len())
            .field(
                "retry_topic",
                &self.retry_topic.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "retry_topic_length",
                &self.retry_topic.as_ref().map(String::len),
            )
            .field("native_option_count", &self.options.len())
            .finish()
    }
}
