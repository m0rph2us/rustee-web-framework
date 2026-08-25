//! Redis response-cache configuration and bounded key construction.

use std::fmt;

use rustee_ai_cache::AiCacheKey;
use rustee_redis::is_valid_key_namespace;

/// Default versioned Redis key namespace for AI response-cache entries.
pub const DEFAULT_NAMESPACE: &str = "rustee:ai:response-cache:v1";

/// Default upper limit for a serialized cached response.
pub const DEFAULT_MAX_ENTRY_BYTES: usize = 256 * 1024;

const MAX_ENTRY_BYTES: usize = 1024 * 1024;

/// Redis-backed exact-key response cache.
///
/// Its `Debug` output exposes only the configured namespace length, never the key prefix.
#[derive(Clone)]
pub struct RedisAiResponseCache {
    pub(crate) connection: rustee_redis::redis::aio::ConnectionManager,
    namespace: String,
    pub(crate) max_entry_bytes: usize,
}

impl RedisAiResponseCache {
    /// Creates an adapter under [`DEFAULT_NAMESPACE`].
    #[must_use]
    pub fn new(connection: rustee_redis::redis::aio::ConnectionManager) -> Self {
        Self {
            connection,
            namespace: DEFAULT_NAMESPACE.to_owned(),
            max_entry_bytes: DEFAULT_MAX_ENTRY_BYTES,
        }
    }

    /// Replaces the Redis namespace with a bounded versioned namespace.
    ///
    /// # Errors
    ///
    /// Returns [`RedisAiResponseCacheConfigError::InvalidNamespace`] for blank, unsafe, or
    /// oversized namespaces.
    pub fn with_namespace(
        connection: rustee_redis::redis::aio::ConnectionManager,
        namespace: impl Into<String>,
    ) -> Result<Self, RedisAiResponseCacheConfigError> {
        let namespace = namespace.into();
        if !is_valid_key_namespace(&namespace) {
            return Err(RedisAiResponseCacheConfigError::InvalidNamespace);
        }
        Ok(Self {
            connection,
            namespace,
            max_entry_bytes: DEFAULT_MAX_ENTRY_BYTES,
        })
    }

    /// Sets the serialized response size bound before Redis writes.
    ///
    /// # Errors
    ///
    /// Returns [`RedisAiResponseCacheConfigError::InvalidMaxEntryBytes`] outside the one-byte to
    /// one-mebibyte bound.
    pub fn with_max_entry_bytes(
        mut self,
        max_entry_bytes: usize,
    ) -> Result<Self, RedisAiResponseCacheConfigError> {
        if !valid_max_entry_bytes(max_entry_bytes) {
            return Err(RedisAiResponseCacheConfigError::InvalidMaxEntryBytes);
        }
        self.max_entry_bytes = max_entry_bytes;
        Ok(self)
    }

    /// Returns the backend key namespace without any response content.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Returns the maximum serialized response size admitted for one cache write.
    #[must_use]
    pub const fn max_entry_bytes(&self) -> usize {
        self.max_entry_bytes
    }

    pub(crate) fn key(&self, key: &AiCacheKey) -> String {
        format!("{}:{}:{}", self.namespace, key.scope(), key.fingerprint())
    }
}

impl fmt::Debug for RedisAiResponseCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisAiResponseCache")
            .field("namespace", &"[REDACTED]")
            .field("namespace_length", &self.namespace.len())
            .field("max_entry_bytes", &self.max_entry_bytes)
            .finish_non_exhaustive()
    }
}

/// Invalid public Redis response-cache configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RedisAiResponseCacheConfigError {
    /// Redis namespace must stay a bounded key prefix without whitespace or brace syntax.
    #[error(
        "Redis AI cache namespace must use bounded ASCII letters, digits, colon, underscore, hyphen, or dot"
    )]
    InvalidNamespace,
    /// The stored serialized response has an explicit, bounded upper limit.
    #[error("Redis AI cache maximum entry size must be between one byte and one mebibyte")]
    InvalidMaxEntryBytes,
}

fn valid_max_entry_bytes(max_entry_bytes: usize) -> bool {
    (1..=MAX_ENTRY_BYTES).contains(&max_entry_bytes)
}

#[cfg(test)]
mod tests {
    use rustee_redis::is_valid_key_namespace;

    use super::{DEFAULT_NAMESPACE, RedisAiResponseCacheConfigError, valid_max_entry_bytes};

    #[test]
    fn namespace_and_entry_size_admission_stay_bounded() {
        assert_eq!(DEFAULT_NAMESPACE, "rustee:ai:response-cache:v1");
        assert!(is_valid_key_namespace("tenant-a:ai-cache:v2"));
        assert!(!is_valid_key_namespace("tenant a"));
        assert!(!is_valid_key_namespace("cache{slot}"));
        assert_eq!(
            (!valid_max_entry_bytes(0))
                .then_some(RedisAiResponseCacheConfigError::InvalidMaxEntryBytes),
            Some(RedisAiResponseCacheConfigError::InvalidMaxEntryBytes),
        );
        assert_eq!(
            (!valid_max_entry_bytes(1024 * 1024 + 1))
                .then_some(RedisAiResponseCacheConfigError::InvalidMaxEntryBytes),
            Some(RedisAiResponseCacheConfigError::InvalidMaxEntryBytes),
        );
        assert!(valid_max_entry_bytes(1024));
    }
}
