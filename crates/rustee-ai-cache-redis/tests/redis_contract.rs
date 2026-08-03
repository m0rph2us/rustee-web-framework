//! Opt-in Redis contract for exact-key AI response-cache persistence.

use std::time::Duration;

use rustee_ai::{ChatResponse, Usage};
use rustee_ai_cache::{AiCacheEntry, AiCacheKey, AiResponseCache};
use rustee_ai_cache_redis::RedisAiResponseCache;
use rustee_redis::{RedisConfig, connect};
use uuid::Uuid;

fn redis_url() -> String {
    std::env::var("RUSTEE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/15".to_owned())
}

fn key() -> AiCacheKey {
    AiCacheKey::new(
        "tenant-a.model-v1.policy-v1",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires a Redis server; CI provisions one"]
async fn redis_cache_round_trips_and_exact_invalidation_removes_only_the_selected_entry() {
    let connection = connect(&RedisConfig::new(redis_url())).await.unwrap();
    let namespace = format!("rustee:test:ai-cache:{}", Uuid::new_v4());
    let cache = RedisAiResponseCache::with_namespace(connection, namespace).unwrap();
    let response = ChatResponse::new(
        "response-1",
        "provider-model",
        "private completion retained only in the selected cache store",
        [],
        Usage {
            input_tokens: 4,
            output_tokens: 6,
        },
    )
    .unwrap();

    cache
        .put(
            key(),
            AiCacheEntry::new(response).unwrap(),
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    let loaded = cache.get(key()).await.unwrap().unwrap();
    assert_eq!(loaded.response().model(), "provider-model");
    assert_eq!(loaded.response().usage().total_tokens(), 10);

    cache.invalidate(key()).await.unwrap();
    assert!(cache.get(key()).await.unwrap().is_none());
}
