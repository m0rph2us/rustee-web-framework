//! Redis persistence for bounded AI response-cache entries.

use std::{fmt, time::Duration};

use futures_util::future::BoxFuture;
use rustee_ai::ChatResponse;
use rustee_ai_cache::{
    AiCacheEligibilityError, AiCacheEntry, AiCacheKey, AiResponseCache, MAX_CACHE_TTL,
};
use rustee_redis::CacheError;

use crate::RedisAiResponseCache;

/// Sanitized Redis response-cache failure.
#[derive(thiserror::Error)]
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
    /// The serialized response exceeded the configured bounded cache limit.
    #[error("Redis AI response cache entry exceeded its size limit")]
    EntryTooLarge,
    /// Redis write or JSON encoding failed.
    #[error("Redis AI response cache write failed")]
    Write(#[source] CacheError),
    /// The adapter cannot represent a zero direct-store TTL in Redis seconds.
    #[error("Redis AI response cache TTL must be greater than zero")]
    ZeroTtl,
    /// Long-lived retention must use an application-specific adapter and ADR.
    #[error("Redis AI response cache TTL must not exceed one day")]
    TtlExceedsMaximum,
    /// Exact-key invalidation could not complete.
    #[error("Redis AI response cache invalidation failed")]
    Invalidate(#[source] CacheError),
}

impl fmt::Debug for RedisAiResponseCacheError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Read(_) => "read_failed",
            Self::IneligibleEntry(_) => "ineligible_entry",
            Self::Serialize(_) => "serialization_failed",
            Self::EntryTooLarge => "entry_too_large",
            Self::Write(_) => "write_failed",
            Self::ZeroTtl => "zero_ttl",
            Self::TtlExceedsMaximum => "ttl_exceeds_maximum",
            Self::Invalidate(_) => "invalidation_failed",
        };
        formatter
            .debug_struct("RedisAiResponseCacheError")
            .field("kind", &kind)
            .finish()
    }
}

impl AiResponseCache for RedisAiResponseCache {
    type Error = RedisAiResponseCacheError;

    fn get(
        &self,
        key: AiCacheKey,
    ) -> BoxFuture<'static, Result<Option<AiCacheEntry>, Self::Error>> {
        let connection = self.connection.clone();
        let key = self.key(&key);
        let max_entry_bytes = self.max_entry_bytes;
        Box::pin(async move {
            let response =
                rustee_redis::get_json_bounded::<ChatResponse>(&connection, &key, max_entry_bytes)
                    .await
                    .map_err(cache_read_error)?;
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
            rustee_redis::set_json_bounded(
                &connection,
                &key,
                &response,
                ttl_seconds,
                max_entry_bytes,
            )
            .await
            .map_err(cache_write_error)
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

fn cache_read_error(error: CacheError) -> RedisAiResponseCacheError {
    match error {
        CacheError::ValueTooLarge => RedisAiResponseCacheError::EntryTooLarge,
        error => RedisAiResponseCacheError::Read(error),
    }
}

fn cache_write_error(error: CacheError) -> RedisAiResponseCacheError {
    match error {
        CacheError::ZeroTtl => RedisAiResponseCacheError::ZeroTtl,
        CacheError::ValueTooLarge => RedisAiResponseCacheError::EntryTooLarge,
        CacheError::Json(error) => RedisAiResponseCacheError::Serialize(error),
        error => RedisAiResponseCacheError::Write(error),
    }
}

fn ttl_seconds(ttl: Duration) -> Result<u64, RedisAiResponseCacheError> {
    if ttl.is_zero() {
        return Err(RedisAiResponseCacheError::ZeroTtl);
    }
    if ttl > MAX_CACHE_TTL {
        return Err(RedisAiResponseCacheError::TtlExceedsMaximum);
    }
    Ok(ttl
        .as_secs()
        .saturating_add(u64::from(ttl.subsec_nanos() != 0)))
}

#[cfg(test)]
mod tests {
    use std::{error::Error as StdError, time::Duration};

    use rustee_redis::{CacheError, redis};

    use super::{RedisAiResponseCacheError, cache_read_error, cache_write_error, ttl_seconds};

    #[test]
    fn ttl_conversion_and_bounded_cache_write_errors_stay_explicit() {
        assert_eq!(ttl_seconds(Duration::from_millis(1)).unwrap(), 1);
        assert!(ttl_seconds(Duration::ZERO).is_err());
        assert!(matches!(
            ttl_seconds(Duration::from_secs(24 * 60 * 60 + 1)),
            Err(RedisAiResponseCacheError::TtlExceedsMaximum)
        ));
        assert!(matches!(
            cache_read_error(CacheError::ValueTooLarge),
            RedisAiResponseCacheError::EntryTooLarge
        ));
        let read_json_error = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        assert!(matches!(
            cache_read_error(CacheError::Json(read_json_error)),
            RedisAiResponseCacheError::Read(CacheError::Json(_))
        ));
        assert!(matches!(
            cache_write_error(CacheError::ValueTooLarge),
            RedisAiResponseCacheError::EntryTooLarge
        ));
        assert!(matches!(
            cache_write_error(CacheError::ZeroTtl),
            RedisAiResponseCacheError::ZeroTtl
        ));
        assert!(matches!(
            cache_write_error(CacheError::EntryExists),
            RedisAiResponseCacheError::Write(CacheError::EntryExists)
        ));
        let serialization_error = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        assert!(matches!(
            cache_write_error(CacheError::Json(serialization_error)),
            RedisAiResponseCacheError::Serialize(_)
        ));
    }

    #[test]
    fn cache_adapter_diagnostics_redact_redis_details_and_preserve_sources() {
        let read = RedisAiResponseCacheError::Read(CacheError::Redis(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "private-redis-cache-read-detail",
        ))));
        let invalidate =
            RedisAiResponseCacheError::Invalidate(CacheError::Redis(redis::RedisError::from((
                redis::ErrorKind::InvalidClientConfig,
                "private-redis-cache-invalidation-detail",
            ))));

        for error in [&read as &dyn StdError, &invalidate as &dyn StdError] {
            assert!(!format!("{error:?}").contains("private-redis-cache-read-detail"));
            assert!(!format!("{error:?}").contains("private-redis-cache-invalidation-detail"));
            assert!(
                !error
                    .to_string()
                    .contains("private-redis-cache-read-detail")
            );
            assert!(
                !error
                    .to_string()
                    .contains("private-redis-cache-invalidation-detail")
            );
            assert!(StdError::source(error).is_some());
        }
    }
}
