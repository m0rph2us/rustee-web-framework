//! Opt-in Redis cache contract tests. Run with a disposable Redis instance at `RUSTEE_REDIS_URL`.

use std::time::{Duration, Instant};

use rustee_redis::redis;
use rustee_redis::{
    CacheError, RedisConfig, RedisConnectError, RedisReadinessError, connect, delete, get_json,
    get_json_bounded, readiness, set_json, set_json_bounded, set_json_bounded_if_absent,
    set_json_if_absent, take_json, take_json_bounded,
};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
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
    readiness(&connection, Duration::from_secs(1))
        .await
        .unwrap();
    let key = format!("rustee:test:cache:{}", Uuid::new_v4());
    assert_eq!(
        get_json_bounded::<CachedValue>(&connection, &key, 64)
            .await
            .unwrap(),
        None
    );
    let mut raw_connection = connection.clone();
    redis::cmd("SET")
        .arg(&key)
        .arg("")
        .arg("EX")
        .arg(60)
        .query_async::<()>(&mut raw_connection)
        .await
        .unwrap();
    assert!(matches!(
        get_json_bounded::<CachedValue>(&connection, &key, 64).await,
        Err(CacheError::Json(_))
    ));
    delete(&connection, &key).await.unwrap();
    let value = CachedValue {
        owner: "cache-contract".to_owned(),
        revision: 7,
    };

    set_json(&connection, &key, &value, 60).await.unwrap();
    assert_eq!(
        get_json(&connection, &key).await.unwrap(),
        Some(value.clone())
    );
    assert_eq!(
        take_json(&connection, &key).await.unwrap(),
        Some(value.clone())
    );
    assert_eq!(
        take_json::<CachedValue>(&connection, &key).await.unwrap(),
        None
    );

    set_json_bounded(&connection, &key, &value, 60, 64)
        .await
        .unwrap();
    assert_eq!(
        get_json_bounded(&connection, &key, 64).await.unwrap(),
        Some(value.clone())
    );
    assert!(matches!(
        set_json_bounded(
            &connection,
            &key,
            &CachedValue {
                owner: "x".repeat(128),
                revision: 9,
            },
            60,
            32,
        )
        .await,
        Err(CacheError::ValueTooLarge)
    ));
    assert_eq!(
        get_json_bounded(&connection, &key, 64).await.unwrap(),
        Some(value)
    );

    set_json(
        &connection,
        &key,
        &CachedValue {
            owner: "x".repeat(128),
            revision: 9,
        },
        60,
    )
    .await
    .unwrap();
    assert!(matches!(
        get_json_bounded::<CachedValue>(&connection, &key, 32).await,
        Err(CacheError::ValueTooLarge)
    ));

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

#[tokio::test]
#[ignore = "requires a Redis server; CI provisions one"]
async fn cache_writes_reject_redis_unrepresentable_ttls_before_storing() {
    let connection = connect(&RedisConfig::new(redis_url())).await.unwrap();
    readiness(&connection, Duration::from_secs(1))
        .await
        .unwrap();
    let key = format!("rustee:test:cache:ttl-range:{}", Uuid::new_v4());
    let value = CachedValue {
        owner: "ttl".to_owned(),
        revision: 0,
    };

    assert!(matches!(
        set_json(&connection, &key, &value, u64::MAX).await,
        Err(CacheError::TtlOutOfRange)
    ));
    assert_eq!(
        get_json::<CachedValue>(&connection, &key).await.unwrap(),
        None
    );
}

#[tokio::test]
#[ignore = "requires a Redis server; CI provisions one"]
async fn bounded_take_consumes_small_values_and_preserves_oversized_values() {
    let connection = connect(&RedisConfig::new(redis_url())).await.unwrap();
    readiness(&connection, Duration::from_secs(1))
        .await
        .unwrap();
    let key = format!("rustee:test:cache:bounded-take:{}", Uuid::new_v4());
    let small = CachedValue {
        owner: "bounded-take".to_owned(),
        revision: 11,
    };

    set_json_bounded(&connection, &key, &small, 60, 64)
        .await
        .unwrap();
    assert_eq!(
        take_json_bounded(&connection, &key, 64).await.unwrap(),
        Some(small)
    );
    assert_eq!(
        get_json::<CachedValue>(&connection, &key).await.unwrap(),
        None
    );

    let oversized = CachedValue {
        owner: "x".repeat(128),
        revision: 12,
    };
    set_json(&connection, &key, &oversized, 60).await.unwrap();
    assert!(matches!(
        take_json_bounded::<CachedValue>(&connection, &key, 32).await,
        Err(CacheError::ValueTooLarge)
    ));
    assert_eq!(get_json(&connection, &key).await.unwrap(), Some(oversized));
    delete(&connection, &key).await.unwrap();
}

#[tokio::test]
#[ignore = "requires a Redis server; CI provisions one"]
async fn bounded_read_reports_size_for_a_multibyte_utf8_prefix() {
    let connection = connect(&RedisConfig::new(redis_url())).await.unwrap();
    readiness(&connection, Duration::from_secs(1))
        .await
        .unwrap();
    let key = format!("rustee:test:cache:bounded-utf8:{}", Uuid::new_v4());

    set_json(&connection, &key, &"\u{AC00}", 60).await.unwrap();
    assert!(matches!(
        get_json_bounded::<String>(&connection, &key, 2).await,
        Err(CacheError::ValueTooLarge)
    ));
    delete(&connection, &key).await.unwrap();
}

#[tokio::test]
#[ignore = "requires a Redis server; CI provisions one"]
async fn bounded_create_only_write_keeps_existing_values_and_rejects_oversized_values() {
    let connection = connect(&RedisConfig::new(redis_url())).await.unwrap();
    readiness(&connection, Duration::from_secs(1))
        .await
        .unwrap();
    let key = format!("rustee:test:cache:bounded-create-only:{}", Uuid::new_v4());
    let first = CachedValue {
        owner: "first".to_owned(),
        revision: 13,
    };
    let replacement = CachedValue {
        owner: "replacement".to_owned(),
        revision: 14,
    };

    set_json_bounded_if_absent(&connection, &key, &first, 60, 64)
        .await
        .unwrap();
    assert!(matches!(
        set_json_bounded_if_absent(&connection, &key, &replacement, 60, 64).await,
        Err(CacheError::EntryExists)
    ));
    assert_eq!(get_json(&connection, &key).await.unwrap(), Some(first));
    delete(&connection, &key).await.unwrap();

    assert!(matches!(
        set_json_bounded_if_absent(
            &connection,
            &key,
            &CachedValue {
                owner: "x".repeat(128),
                revision: 15,
            },
            60,
            32,
        )
        .await,
        Err(CacheError::ValueTooLarge)
    ));
    assert_eq!(
        get_json::<CachedValue>(&connection, &key).await.unwrap(),
        None
    );
}

#[tokio::test]
#[ignore = "requires a Redis server; CI provisions one"]
async fn create_only_write_preserves_exactly_one_concurrent_value() {
    let connection = connect(&RedisConfig::new(redis_url())).await.unwrap();
    readiness(&connection, Duration::from_secs(1))
        .await
        .unwrap();
    let key = format!("rustee:test:cache:create-only:{}", Uuid::new_v4());
    let first = CachedValue {
        owner: "first-writer".to_owned(),
        revision: 1,
    };
    let second = CachedValue {
        owner: "second-writer".to_owned(),
        revision: 2,
    };

    let (first_result, second_result) = tokio::join!(
        set_json_if_absent(&connection, &key, &first, 60),
        set_json_if_absent(&connection, &key, &second, 60),
    );
    assert!(matches!(
        (&first_result, &second_result),
        (Ok(()), Err(CacheError::EntryExists)) | (Err(CacheError::EntryExists), Ok(()))
    ));

    let stored = get_json::<CachedValue>(&connection, &key)
        .await
        .unwrap()
        .expect("one create-only writer must retain its value");
    assert!(stored == first || stored == second);
    delete(&connection, &key).await.unwrap();
}

#[tokio::test]
#[ignore = "requires Redis 7 CLIENT PAUSE permission; CI verifies the bounded readiness deadline"]
async fn readiness_fails_within_the_deadline_when_redis_is_paused() {
    if std::env::var("RUSTEE_REDIS_EXPECT_PAUSE").as_deref() != Ok("1") {
        return;
    }

    let connection = connect(&RedisConfig::new(redis_url())).await.unwrap();
    let mut admin = connection.clone();
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(1_500)
        .arg("ALL")
        .query_async::<()>(&mut admin)
        .await
        .unwrap();

    let timeout = Duration::from_millis(500);
    let started = Instant::now();
    let error = readiness(&connection, timeout).await.unwrap_err();
    assert!(matches!(error, RedisReadinessError::Timeout(value) if value == timeout));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "paused Redis readiness exceeded the configured deadline"
    );
    let rendered = format!("{error:?}");
    assert!(!rendered.contains("127.0.0.1"));
    assert!(!rendered.contains("6379"));

    // The pause is server-global; let Redis resume before a later local contract shares the fixture.
    sleep(Duration::from_millis(1_100)).await;
}
