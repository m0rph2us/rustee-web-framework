//! Redis adapter for explicit Rustee AI response-cache entries.
//!
//! Redis retains serialized response content. Deployments must separately choose encrypted
//! transport/storage, tenant erase, credential rotation, and retention controls. This adapter
//! accepts only the opaque exact keys and non-tool-call entries validated by `rustee-ai-cache`.
//! It neither hashes prompts nor offers SCAN/wildcard namespace deletion.

use std::{fmt, time::Duration};

use futures_util::future::BoxFuture;
use rustee_ai::ChatResponse;
use rustee_ai_cache::{AiCacheEligibilityError, AiCacheEntry, AiCacheKey, AiResponseCache};
use rustee_redis::{CacheError, redis::RedisError};

/// Default versioned Redis key namespace for AI response-cache entries.
pub const DEFAULT_NAMESPACE: &str = "rustee:ai:response-cache:v1";

/// Default upper limit for a serialized cached response.
pub const DEFAULT_MAX_ENTRY_BYTES: usize = 256 * 1024;

const MAX_ENTRY_BYTES: usize = 1024 * 1024;

/// Redis-backed exact-key response cache.
#[derive(Clone)]
pub struct RedisAiResponseCache {
    connection: rustee_redis::redis::aio::ConnectionManager,
    namespace: String,
    max_entry_bytes: usize,
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
        if !valid_namespace(&namespace) {
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

    fn key(&self, key: &AiCacheKey) -> String {
        format!("{}:{}:{}", self.namespace, key.scope(), key.fingerprint())
    }
}

impl fmt::Debug for RedisAiResponseCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisAiResponseCache")
            .field("namespace", &self.namespace)
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

/// Sanitized Redis response-cache failure.
#[derive(Debug, thiserror::Error)]
pub enum RedisAiResponseCacheError {
    /// Redis read or JSON decode failed.
    #[error("Redis AI response cache read failed")]
    Read(#[source] CacheError),
    /// Redis held a response that violated the cache eligibility boundary.
    #[error("Redis AI response cache entry was not eligible")]
    IneligibleEntry(#[source] AiCacheEligibilityError),
    /// Serialization could not establish an entry size before the write.
    #[error("Redis AI response cache serialization failed")]
    Serialize(#[source] serde_json::Error),
    /// The serialized response exceeded the configured bounded storage limit.
    #[error("Redis AI response cache entry exceeded its size limit")]
    EntryTooLarge,
    /// Redis write or JSON encoding failed.
    #[error("Redis AI response cache write failed")]
    Write(#[source] CacheError),
    /// The adapter cannot represent a zero direct-store TTL in Redis seconds.
    #[error("Redis AI response cache TTL must be greater than zero")]
    ZeroTtl,
    /// Exact-key invalidation could not complete.
    #[error("Redis AI response cache invalidation failed")]
    Invalidate(#[source] RedisError),
}

impl AiResponseCache for RedisAiResponseCache {
    type Error = RedisAiResponseCacheError;

    fn get(
        &self,
        key: AiCacheKey,
    ) -> BoxFuture<'static, Result<Option<AiCacheEntry>, Self::Error>> {
        let connection = self.connection.clone();
        let key = self.key(&key);
        Box::pin(async move {
            let response = rustee_redis::get_json::<ChatResponse>(&connection, &key)
                .await
                .map_err(RedisAiResponseCacheError::Read)?;
            response
                .map(AiCacheEntry::new)
                .transpose()
                .map_err(RedisAiResponseCacheError::IneligibleEntry)
        })
    }

    fn put(
        &self,
        key: AiCacheKey,
        entry: AiCacheEntry,
        ttl: Duration,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let connection = self.connection.clone();
        let key = self.key(&key);
        let max_entry_bytes = self.max_entry_bytes;
        Box::pin(async move {
            let ttl_seconds = ttl_seconds(ttl)?;
            let response = entry.into_response();
            let encoded =
                serde_json::to_vec(&response).map_err(RedisAiResponseCacheError::Serialize)?;
            if encoded.len() > max_entry_bytes {
                return Err(RedisAiResponseCacheError::EntryTooLarge);
            }
            rustee_redis::set_json(&connection, &key, &response, ttl_seconds)
                .await
                .map_err(RedisAiResponseCacheError::Write)
        })
    }

    fn invalidate(&self, key: AiCacheKey) -> BoxFuture<'static, Result<(), Self::Error>> {
        let connection = self.connection.clone();
        let key = self.key(&key);
        Box::pin(async move {
            rustee_redis::delete(&connection, &key)
                .await
                .map_err(RedisAiResponseCacheError::Invalidate)
        })
    }
}

fn ttl_seconds(ttl: Duration) -> Result<u64, RedisAiResponseCacheError> {
    if ttl.is_zero() {
        return Err(RedisAiResponseCacheError::ZeroTtl);
    }
    Ok(ttl
        .as_secs()
        .saturating_add(u64::from(ttl.subsec_nanos() != 0)))
}

fn valid_namespace(namespace: &str) -> bool {
    !namespace.is_empty()
        && namespace.len() <= 128
        && namespace
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'_' | b'-' | b'.'))
}

fn valid_max_entry_bytes(max_entry_bytes: usize) -> bool {
    (1..=MAX_ENTRY_BYTES).contains(&max_entry_bytes)
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_NAMESPACE, RedisAiResponseCacheConfigError, ttl_seconds, valid_max_entry_bytes,
        valid_namespace,
    };
    use std::time::Duration;

    #[test]
    fn namespace_size_and_ttl_conversion_stay_bounded() {
        assert_eq!(DEFAULT_NAMESPACE, "rustee:ai:response-cache:v1");
        assert!(valid_namespace("tenant-a:ai-cache:v2"));
        assert!(!valid_namespace("tenant a"));
        assert!(!valid_namespace("cache{slot}"));
        assert_eq!(ttl_seconds(Duration::from_millis(1)).unwrap(), 1);
        assert!(ttl_seconds(Duration::ZERO).is_err());
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
