//! Bounded local response-cache adapter for development and deterministic tests.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use futures_util::future::BoxFuture;

use crate::{AiCacheConfigError, AiCacheEntry, AiCacheKey, AiResponseCache, MAX_CACHE_TTL};

const MAX_IN_MEMORY_ENTRIES: usize = 10_000;

/// Bounded in-memory cache intended for deterministic local development and tests.
///
/// It does not provide distributed invalidation, encryption, or eviction. Production deployments
/// should use an application-reviewed durable adapter with tenant erase and retention procedures.
#[derive(Clone)]
pub struct InMemoryAiResponseCache {
    state: Arc<Mutex<InMemoryState>>,
    capacity: usize,
}

#[derive(Default)]
struct InMemoryState {
    entries: BTreeMap<AiCacheKey, InMemoryEntry>,
}

#[derive(Clone)]
struct InMemoryEntry {
    entry: AiCacheEntry,
    expires_at: Instant,
}

impl InMemoryAiResponseCache {
    /// Creates an in-memory cache with a fixed maximum entry count.
    ///
    /// # Errors
    ///
    /// Returns [`AiCacheConfigError::InvalidInMemoryCapacity`] outside the one-to-10,000 bound.
    pub fn new(capacity: usize) -> Result<Self, AiCacheConfigError> {
        if !(1..=MAX_IN_MEMORY_ENTRIES).contains(&capacity) {
            return Err(AiCacheConfigError::InvalidInMemoryCapacity);
        }
        Ok(Self {
            state: Arc::new(Mutex::new(InMemoryState::default())),
            capacity,
        })
    }

    /// Returns the fixed maximum number of unexpired entries.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }
}

impl fmt::Debug for InMemoryAiResponseCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let entries = self.state.lock().ok().map(|state| state.entries.len());
        formatter
            .debug_struct("InMemoryAiResponseCache")
            .field("capacity", &self.capacity)
            .field("retained_entries", &entries)
            .finish()
    }
}

/// In-memory cache storage failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InMemoryAiResponseCacheError {
    /// A cache TTL must stay explicit and positive even for direct store calls.
    #[error("AI in-memory cache TTL must be greater than zero")]
    ZeroTtl,
    /// Long-lived retention must use an application-specific adapter and ADR.
    #[error("AI in-memory cache TTL must not exceed one day")]
    TtlExceedsMaximum,
    /// The TTL cannot be represented as a future monotonic instant on this platform.
    #[error("AI in-memory cache TTL is outside the supported expiration range")]
    TtlOutOfRange,
    /// The cache is full and deliberately does not evict a live entry implicitly.
    #[error("AI in-memory cache capacity is exhausted")]
    CapacityExhausted,
    /// A poisoned lock prevents retaining or returning application data.
    #[error("AI in-memory cache state is unavailable")]
    StateUnavailable,
}

impl AiResponseCache for InMemoryAiResponseCache {
    type Error = InMemoryAiResponseCacheError;

    fn get(
        &self,
        key: AiCacheKey,
    ) -> BoxFuture<'static, Result<Option<AiCacheEntry>, Self::Error>> {
        let state = self.state.clone();
        Box::pin(async move {
            let mut state = state
                .lock()
                .map_err(|_| InMemoryAiResponseCacheError::StateUnavailable)?;
            let now = Instant::now();
            let result = state
                .entries
                .get(&key)
                .and_then(|entry| (entry.expires_at > now).then(|| entry.entry.clone()));
            if result.is_none() {
                state.entries.remove(&key);
            }
            Ok(result)
        })
    }

    fn put(
        &self,
        key: AiCacheKey,
        entry: AiCacheEntry,
        ttl: Duration,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let state = self.state.clone();
        let capacity = self.capacity;
        Box::pin(async move {
            if ttl.is_zero() {
                return Err(InMemoryAiResponseCacheError::ZeroTtl);
            }
            if ttl > MAX_CACHE_TTL {
                return Err(InMemoryAiResponseCacheError::TtlExceedsMaximum);
            }
            let mut state = state
                .lock()
                .map_err(|_| InMemoryAiResponseCacheError::StateUnavailable)?;
            let now = Instant::now();
            let expires_at = now
                .checked_add(ttl)
                .ok_or(InMemoryAiResponseCacheError::TtlOutOfRange)?;
            state.entries.retain(|_, value| value.expires_at > now);
            if !state.entries.contains_key(&key) && state.entries.len() == capacity {
                return Err(InMemoryAiResponseCacheError::CapacityExhausted);
            }
            state
                .entries
                .insert(key, InMemoryEntry { entry, expires_at });
            Ok(())
        })
    }

    fn invalidate(&self, key: AiCacheKey) -> BoxFuture<'static, Result<(), Self::Error>> {
        let state = self.state.clone();
        Box::pin(async move {
            let mut state = state
                .lock()
                .map_err(|_| InMemoryAiResponseCacheError::StateUnavailable)?;
            state.entries.remove(&key);
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread, time::Duration};

    use rustee_ai::{ChatResponse, Usage};

    use crate::{AiCacheKey, AiResponseCache};

    use super::{AiCacheEntry, InMemoryAiResponseCache, InMemoryAiResponseCacheError};

    fn key() -> AiCacheKey {
        AiCacheKey::new(
            "tenant-a.cache-v1",
            "0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f",
        )
        .expect("test cache key must be valid")
    }

    fn entry() -> AiCacheEntry {
        AiCacheEntry::new(
            ChatResponse::new(
                "cached",
                "provider-model",
                "cached response",
                [],
                Usage::default(),
            )
            .expect("test cache response must be valid"),
        )
        .expect("test cache entry must be valid")
    }

    #[tokio::test]
    async fn ttl_exceeding_the_shared_cache_policy_is_rejected_without_a_write() {
        let cache = InMemoryAiResponseCache::new(1).expect("test cache capacity must be valid");
        let key = key();

        assert_eq!(
            cache
                .put(key.clone(), entry(), Duration::MAX)
                .await
                .unwrap_err(),
            InMemoryAiResponseCacheError::TtlExceedsMaximum
        );
        assert!(cache.get(key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn poisoned_state_fails_closed_and_does_not_masquerade_as_empty_in_debug() {
        let cache = InMemoryAiResponseCache::new(1).expect("test cache capacity must be valid");
        let state = Arc::clone(&cache.state);
        let poison = thread::spawn(move || {
            let _guard = state.lock().expect("new cache lock must be available");
            panic!("test must poison local AI cache state");
        });
        assert!(poison.join().is_err());

        assert_eq!(
            cache.get(key()).await.unwrap_err(),
            InMemoryAiResponseCacheError::StateUnavailable
        );
        assert_eq!(
            cache.invalidate(key()).await.unwrap_err(),
            InMemoryAiResponseCacheError::StateUnavailable
        );
        assert!(format!("{cache:?}").contains("retained_entries: None"));
    }
}
