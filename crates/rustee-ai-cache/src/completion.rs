//! Cache-aware completion orchestration and its explicit status/error contracts.

use std::fmt;

use rustee_ai::{ChatRequest, ChatResponse};

use crate::{
    AiCacheConfig, AiCacheEligibilityError, AiCacheEntry, AiCacheExecutor, AiCacheKey,
    AiCacheReadFailurePolicy, AiResponseCache,
};

/// Status of one completion's explicit cache interaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AiCacheStatus {
    /// A validated cached response was returned and no provider call occurred.
    Hit,
    /// The provider completed once and its eligible response was stored.
    MissStored,
    /// The provider completed once but its eligible response could not be stored.
    MissStoreFailed,
    /// The request was intentionally not eligible for cache lookup or storage.
    BypassedIneligibleRequest,
    /// A cache read failed under the explicit bypass policy; one provider call followed without a
    /// cache write.
    BypassedReadFailure,
    /// The provider returned a model tool call, so the response was not stored.
    MissIneligibleResponse,
}

/// One response with its cache outcome.
#[derive(Clone)]
pub struct AiCachedResponse {
    response: ChatResponse,
    cache_status: AiCacheStatus,
}

impl AiCachedResponse {
    /// Returns the application-visible completion.
    #[must_use]
    pub const fn response(&self) -> &ChatResponse {
        &self.response
    }

    /// Consumes the result and returns its completion.
    #[must_use]
    pub fn into_response(self) -> ChatResponse {
        self.response
    }

    /// Returns the explicit cache outcome for usage and observability accounting.
    #[must_use]
    pub const fn cache_status(&self) -> AiCacheStatus {
        self.cache_status
    }
}

impl fmt::Debug for AiCachedResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiCachedResponse")
            .field("response", &self.response)
            .field("cache_status", &self.cache_status)
            .finish()
    }
}

/// Explicit cache adapter around an application-governed completion executor.
#[derive(Clone)]
pub struct AiCachedCompletion<E, C> {
    executor: E,
    cache: C,
    config: AiCacheConfig,
}

impl<E, C> AiCachedCompletion<E, C> {
    /// Creates one cache adapter with a validated policy.
    #[must_use]
    pub const fn new(executor: E, cache: C, config: AiCacheConfig) -> Self {
        Self {
            executor,
            cache,
            config,
        }
    }

    /// Returns the explicit cache configuration.
    #[must_use]
    pub const fn config(&self) -> AiCacheConfig {
        self.config
    }
}

impl<E, C> fmt::Debug for AiCachedCompletion<E, C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiCachedCompletion")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl<E, C> AiCachedCompletion<E, C>
where
    E: AiCacheExecutor,
    C: AiResponseCache,
{
    /// Completes one request through an explicit cache key.
    ///
    /// The caller must derive `key` after authorization and include tenant, model, prompt/data
    /// revision, and policy versions in its scope or fingerprint. This method does not cache tool
    /// interaction requests/responses, streaming calls, provider failures, or cache errors.
    ///
    /// # Errors
    ///
    /// Returns an executor failure, a fail-closed read failure, or an impossible unsafe cache
    /// entry. Cache write failures are returned as [`AiCacheStatus::MissStoreFailed`] after the
    /// one completed provider response and never trigger a retry.
    pub async fn complete(
        &self,
        request: ChatRequest,
        key: AiCacheKey,
    ) -> Result<AiCachedResponse, AiCachedCompletionError<E::Error, C::Error>> {
        if request_eligibility(&request).is_err() {
            return self
                .complete_without_cache(request, AiCacheStatus::BypassedIneligibleRequest)
                .await
                .map_err(|source| AiCachedCompletionError::Executor { source });
        }

        match self.cache.get(key.clone()).await {
            Ok(Some(entry)) => {
                return Ok(AiCachedResponse {
                    response: entry.into_response(),
                    cache_status: AiCacheStatus::Hit,
                });
            }
            Ok(None) => {}
            Err(source) => match self.config.read_failure_policy() {
                AiCacheReadFailurePolicy::FailClosed => {
                    return Err(AiCachedCompletionError::CacheRead { source });
                }
                AiCacheReadFailurePolicy::Bypass => {
                    return self
                        .complete_without_cache(request, AiCacheStatus::BypassedReadFailure)
                        .await
                        .map_err(|source| AiCachedCompletionError::Executor { source });
                }
            },
        }

        let response = self
            .executor
            .complete_for_cache(request)
            .await
            .map_err(|source| AiCachedCompletionError::Executor { source })?;
        let entry = match AiCacheEntry::new(response.clone()) {
            Ok(entry) => entry,
            Err(AiCacheEligibilityError::ResponseContainsToolCalls) => {
                return Ok(AiCachedResponse {
                    response,
                    cache_status: AiCacheStatus::MissIneligibleResponse,
                });
            }
            Err(error) => return Err(AiCachedCompletionError::UnsafeCacheEntry { error }),
        };
        let cache_status = match self.cache.put(key, entry, self.config.ttl()).await {
            Ok(()) => AiCacheStatus::MissStored,
            Err(_) => AiCacheStatus::MissStoreFailed,
        };
        Ok(AiCachedResponse {
            response,
            cache_status,
        })
    }

    /// Invalidates one exact key; broad tenant deletion remains application-owned.
    ///
    /// # Errors
    ///
    /// Returns a sanitized backend failure when the exact deletion cannot be completed.
    pub async fn invalidate(
        &self,
        key: AiCacheKey,
    ) -> Result<(), AiCacheInvalidationError<C::Error>> {
        self.cache
            .invalidate(key)
            .await
            .map_err(|source| AiCacheInvalidationError { source })
    }

    async fn complete_without_cache(
        &self,
        request: ChatRequest,
        cache_status: AiCacheStatus,
    ) -> Result<AiCachedResponse, E::Error> {
        let response = self.executor.complete_for_cache(request).await?;
        Ok(AiCachedResponse {
            response,
            cache_status,
        })
    }
}

/// One sanitized cache-adapter failure.
#[derive(thiserror::Error)]
pub enum AiCachedCompletionError<ExecutorError, CacheError> {
    /// The application-governed executor failed. No cache or provider retry occurred here.
    #[error("AI cached completion executor failed")]
    Executor {
        /// Application completion failure.
        #[source]
        source: ExecutorError,
    },
    /// The cache read failed under the explicit fail-closed policy, so no provider call occurred.
    #[error("AI response cache read failed")]
    CacheRead {
        /// Backend cache failure.
        #[source]
        source: CacheError,
    },
    /// An entry somehow bypassed the cache eligibility contract.
    #[error("AI response cache entry was not eligible")]
    UnsafeCacheEntry {
        /// Rejected eligibility condition.
        #[source]
        error: AiCacheEligibilityError,
    },
}

impl<ExecutorError, CacheError> fmt::Debug for AiCachedCompletionError<ExecutorError, CacheError> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Executor { .. } => formatter.write_str("AiCachedCompletionError::Executor"),
            Self::CacheRead { .. } => formatter.write_str("AiCachedCompletionError::CacheRead"),
            Self::UnsafeCacheEntry { .. } => {
                formatter.write_str("AiCachedCompletionError::UnsafeCacheEntry")
            }
        }
    }
}

/// A sanitized exact-key cache invalidation failure.
#[derive(thiserror::Error)]
#[error("AI response cache invalidation failed")]
pub struct AiCacheInvalidationError<CacheError> {
    /// Backend cache failure.
    #[source]
    pub source: CacheError,
}

impl<CacheError> fmt::Debug for AiCacheInvalidationError<CacheError> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AiCacheInvalidationError")
    }
}

fn request_eligibility(request: &ChatRequest) -> Result<(), AiCacheEligibilityError> {
    if !request.tools().is_empty() {
        return Err(AiCacheEligibilityError::RequestDeclaresTools);
    }
    if !request.tool_results().is_empty() {
        return Err(AiCacheEligibilityError::RequestContainsToolResults);
    }
    Ok(())
}
