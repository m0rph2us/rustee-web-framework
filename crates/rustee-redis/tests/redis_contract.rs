//! Opt-in Redis cache contract tests. Run with a disposable Redis instance at `RUSTEE_REDIS_URL`.

use std::time::{Duration, Instant};

use rustee_redis::{
    RedisConfig, RedisConnectError, connect, delete, get_json, readiness, set_json, take_json,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CachedValue {
    owner: String,
    revision: u64,
}

fn redis_url() -> String {
    std::env::var("RUSTEE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/15".to_owned())
}

#[tokio::test]
#[ignore = "requires a stopped Redis 7 server; CI controls the container lifecycle"]
async fn outage_fails_within_the_connect_deadline_and_redacts_the_endpoint() {
    if std::env::var("RUSTEE_REDIS_EXPECT_OUTAGE").as_deref() != Ok("1") {
        return;
    }

    let config = RedisConfig::new(redis_url())
        .with_connect_timeout(Duration::from_millis(500))
        .unwrap();
    let started = Instant::now();
    let error = connect(&config).await.unwrap_err();

    assert_eq!(error, RedisConnectError::Connection);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "stopped Redis connection exceeded the bounded deadline"
    );
    let displayed = error.to_string();
    assert!(!displayed.contains("127.0.0.1"));
    assert!(!displayed.contains("6379"));
}

#[tokio::test]
#[ignore = "requires a Redis server; CI provisions one"]
async fn cache_helpers_round_trip_and_atomically_consume_namespaced_json() {
    let connection = connect(&RedisConfig::new(redis_url())).await.unwrap();
    readiness(&connection).await.unwrap();
    let key = format!("rustee:test:cache:{}", Uuid::new_v4());
    let value = CachedValue {
        owner: "cache-contract".to_owned(),
        revision: 7,
    };

    set_json(&connection, &key, &value, 60).await.unwrap();
    assert_eq!(
        get_json(&connection, &key).await.unwrap(),
        Some(value.clone())
    );
    assert_eq!(take_json(&connection, &key).await.unwrap(), Some(value));
    assert_eq!(
        take_json::<CachedValue>(&connection, &key).await.unwrap(),
        None
    );

    set_json(
        &connection,
        &key,
        &CachedValue {
            owner: "delete-contract".to_owned(),
            revision: 8,
        },
        60,
    )
    .await
    .unwrap();
    delete(&connection, &key).await.unwrap();
    assert_eq!(
        get_json::<CachedValue>(&connection, &key).await.unwrap(),
        None
    );
}
