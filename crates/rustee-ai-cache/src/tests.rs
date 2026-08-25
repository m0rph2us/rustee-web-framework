use std::{fmt, time::Duration};

use futures_util::future::BoxFuture;
use rustee_ai::{
    AiPipeline, ChatMessage, ChatRequest, ChatResponse, MessageRole, ToolDefinition, Usage,
};
use rustee_ai_test::RecordedAiProvider;

use super::{
    AiCacheConfig, AiCacheConfigError, AiCacheEntry, AiCacheInvalidationError, AiCacheKey,
    AiCacheReadFailurePolicy, AiCacheStatus, AiCachedCompletion, AiCachedCompletionError,
    AiResponseCache, InMemoryAiResponseCache,
};

#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("test cache unavailable")]
enum TestCacheError {
    Unavailable,
}

struct LeakyDiagnosticError;

impl fmt::Debug for LeakyDiagnosticError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LeakyDiagnosticError(private-cache-credential)")
    }
}

impl fmt::Display for LeakyDiagnosticError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("private-cache-credential")
    }
}

impl std::error::Error for LeakyDiagnosticError {}

#[derive(Clone, Copy)]
struct ReadFailingCache;

impl AiResponseCache for ReadFailingCache {
    type Error = TestCacheError;

    fn get(
        &self,
        _key: AiCacheKey,
    ) -> BoxFuture<'static, Result<Option<AiCacheEntry>, Self::Error>> {
        Box::pin(async { Err(TestCacheError::Unavailable) })
    }

    fn put(
        &self,
        _key: AiCacheKey,
        _entry: AiCacheEntry,
        _ttl: Duration,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(async { panic!("a bypassed cache read must not attempt a cache write") })
    }

    fn invalidate(&self, _key: AiCacheKey) -> BoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone, Copy)]
struct WriteFailingCache;

impl AiResponseCache for WriteFailingCache {
    type Error = TestCacheError;

    fn get(
        &self,
        _key: AiCacheKey,
    ) -> BoxFuture<'static, Result<Option<AiCacheEntry>, Self::Error>> {
        Box::pin(async { Ok(None) })
    }

    fn put(
        &self,
        _key: AiCacheKey,
        _entry: AiCacheEntry,
        _ttl: Duration,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(async { Err(TestCacheError::Unavailable) })
    }

    fn invalidate(&self, _key: AiCacheKey) -> BoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(async { Ok(()) })
    }
}

fn request() -> ChatRequest {
    ChatRequest::new(
        "support.default",
        [ChatMessage::new(MessageRole::User, "private customer prompt").unwrap()],
    )
    .unwrap()
}

fn response(id: &str, content: &str) -> ChatResponse {
    ChatResponse::new(
        id,
        "provider-model",
        content,
        [],
        Usage {
            input_tokens: 3,
            output_tokens: 5,
        },
    )
    .unwrap()
}

fn key() -> AiCacheKey {
    AiCacheKey::new(
        "tenant-a.model-v3.policy-v2",
        "0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f",
    )
    .unwrap()
}

#[test]
fn cache_key_and_retention_are_bounded_and_opaque() {
    assert!(AiCacheKey::new("tenant-a", "f".repeat(64)).is_ok());
    assert!(AiCacheKey::new("tenant a", "f".repeat(64)).is_err());
    assert!(AiCacheKey::new("tenant-a", "prompt content").is_err());
    assert_eq!(
        AiCacheConfig::new(Duration::ZERO).unwrap_err(),
        AiCacheConfigError::ZeroTtl
    );
    assert_eq!(
        AiCacheConfig::new(Duration::from_secs(24 * 60 * 60 + 1)).unwrap_err(),
        AiCacheConfigError::TtlExceedsMaximum
    );

    let key = key();
    let debug = format!("{key:?}");
    assert!(!debug.contains(key.scope()));
    assert!(!debug.contains(key.fingerprint()));
}

#[tokio::test]
async fn cache_hit_never_invokes_the_provider_and_debug_stays_redacted() {
    let cache = InMemoryAiResponseCache::new(2).unwrap();
    cache
        .put(
            key(),
            AiCacheEntry::new(response("cached", "private cached answer")).unwrap(),
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    let provider = RecordedAiProvider::new();
    let completion = AiCachedCompletion::new(
        AiPipeline::new(provider.clone()),
        cache,
        AiCacheConfig::default(),
    );

    let result = completion.complete(request(), key()).await.unwrap();

    assert_eq!(result.cache_status(), AiCacheStatus::Hit);
    assert_eq!(result.response().content(), "private cached answer");
    assert!(provider.recorded_requests().is_empty());
    let debug = format!("{result:?}");
    assert!(!debug.contains("private cached answer"));
    assert!(!debug.contains("private customer prompt"));
}

#[tokio::test]
async fn miss_stores_once_and_later_hit_preserves_usage_without_a_second_call() {
    let cache = InMemoryAiResponseCache::new(2).unwrap();
    let provider = RecordedAiProvider::new();
    provider.queue_completion(response("fresh", "first answer"));
    let completion = AiCachedCompletion::new(
        AiPipeline::new(provider.clone()),
        cache,
        AiCacheConfig::new(Duration::from_secs(60)).unwrap(),
    );

    let first = completion.complete(request(), key()).await.unwrap();
    let second = completion.complete(request(), key()).await.unwrap();

    assert_eq!(first.cache_status(), AiCacheStatus::MissStored);
    assert_eq!(second.cache_status(), AiCacheStatus::Hit);
    assert_eq!(second.response().usage().total_tokens(), 8);
    assert_eq!(provider.recorded_requests().len(), 1);
    completion.invalidate(key()).await.unwrap();
}

#[tokio::test]
async fn tool_interaction_requests_and_responses_are_never_cached() {
    let cache = InMemoryAiResponseCache::new(2).unwrap();
    let provider = RecordedAiProvider::new();
    provider.queue_completion(response("one", "first answer"));
    let completion = AiCachedCompletion::new(
        AiPipeline::new(provider.clone()),
        cache,
        AiCacheConfig::default(),
    );
    let request = request().with_tools([ToolDefinition::new(
        "lookup",
        serde_json::json!({"type": "object"}),
    )
    .unwrap()]);

    let result = completion.complete(request, key()).await.unwrap();

    assert_eq!(
        result.cache_status(),
        AiCacheStatus::BypassedIneligibleRequest
    );
    assert_eq!(provider.recorded_requests().len(), 1);
    assert!(
        AiCacheEntry::new(
            ChatResponse::new(
                "tool-response",
                "provider-model",
                "",
                [rustee_ai::ToolCall::new("call-1", "lookup", serde_json::json!({})).unwrap()],
                Usage::default(),
            )
            .unwrap(),
        )
        .is_err()
    );
}

#[tokio::test]
async fn fail_closed_cache_read_does_not_call_the_provider() {
    let provider = RecordedAiProvider::new();
    provider.queue_completion(response("should-not-run", "response"));
    let completion = AiCachedCompletion::new(
        AiPipeline::new(provider.clone()),
        ReadFailingCache,
        AiCacheConfig::default().with_read_failure_policy(AiCacheReadFailurePolicy::FailClosed),
    );

    let error = completion.complete(request(), key()).await.unwrap_err();

    assert!(matches!(error, AiCachedCompletionError::CacheRead { .. }));
    assert!(provider.recorded_requests().is_empty());
}

#[tokio::test]
async fn bypassed_cache_read_calls_the_provider_without_a_cache_write() {
    let provider = RecordedAiProvider::new();
    provider.queue_completion(response("read-bypassed", "completed once"));
    let completion = AiCachedCompletion::new(
        AiPipeline::new(provider.clone()),
        ReadFailingCache,
        AiCacheConfig::default(),
    );

    let result = completion.complete(request(), key()).await.unwrap();

    assert_eq!(result.cache_status(), AiCacheStatus::BypassedReadFailure);
    assert_eq!(result.response().content(), "completed once");
    assert_eq!(provider.recorded_requests().len(), 1);
}

#[test]
fn cache_error_debug_output_redacts_backend_diagnostics() {
    let completion =
        AiCachedCompletionError::<LeakyDiagnosticError, LeakyDiagnosticError>::CacheRead {
            source: LeakyDiagnosticError,
        };
    let invalidation = AiCacheInvalidationError {
        source: LeakyDiagnosticError,
    };

    for error in [&completion as &dyn std::error::Error, &invalidation] {
        assert!(std::error::Error::source(error).is_some());
        assert!(!format!("{error:?}").contains("private-cache-credential"));
        assert!(!error.to_string().contains("private-cache-credential"));
    }
}

#[tokio::test]
async fn cache_write_failure_returns_the_completed_response_without_provider_retry() {
    let provider = RecordedAiProvider::new();
    provider.queue_completion(response("write-failed", "completed once"));
    let completion = AiCachedCompletion::new(
        AiPipeline::new(provider.clone()),
        WriteFailingCache,
        AiCacheConfig::default(),
    );

    let result = completion.complete(request(), key()).await.unwrap();

    assert_eq!(result.cache_status(), AiCacheStatus::MissStoreFailed);
    assert_eq!(result.response().content(), "completed once");
    assert_eq!(provider.recorded_requests().len(), 1);
}
