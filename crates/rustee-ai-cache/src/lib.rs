//! Explicit, tenant-scoped AI response-cache contracts.
//!
//! The application derives an opaque SHA-256 fingerprint after it has authorized and normalized a
//! request. This crate never hashes, serializes, logs, or otherwise retains a prompt in order to
//! construct a cache key. Cache lookup is limited to non-streaming requests without declared
//! tools or prior tool results, and responses containing model tool calls are never stored.
//!
//! Cache read failure handling is explicit. A cache write failure is reported as a status after a
//! successful provider completion; it never turns into an automatic provider retry.

mod completion;
mod contracts;
mod in_memory;

pub use completion::{
    AiCacheInvalidationError, AiCacheStatus, AiCachedCompletion, AiCachedCompletionError,
    AiCachedResponse,
};
pub use contracts::{
    AiCacheConfig, AiCacheConfigError, AiCacheEligibilityError, AiCacheEntry, AiCacheExecutor,
    AiCacheKey, AiCacheReadFailurePolicy, AiResponseCache, DEFAULT_CACHE_TTL, MAX_CACHE_TTL,
};
pub use in_memory::{InMemoryAiResponseCache, InMemoryAiResponseCacheError};

#[cfg(test)]
mod tests;
