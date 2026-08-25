//! Opt-in Redis fixed-window contract. Run with a disposable Redis instance at
//! `RUSTEE_REDIS_URL`.

use std::time::Duration;

use futures_util::future::join_all;
use rustee_rate_limit::{FixedWindow, RateLimitKey, RateLimitStore};
use rustee_rate_limit_redis::RedisFixedWindowStore;
use rustee_redis::{RedisConfig, connect, readiness};
use uuid::Uuid;

fn redis_url() -> String {
    std::env::var("RUSTEE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/15".to_owned())
}

#[tokio::test]
#[ignore = "requires a Redis server; CI provisions one"]
async fn fixed_window_rate_limit_is_atomic_and_bounded() {
    let connection = connect(&RedisConfig::new(redis_url())).await.unwrap();
    readiness(&connection, std::time::Duration::from_secs(1))
        .await
        .unwrap();
    let store = RedisFixedWindowStore::new(connection, "rustee:rate-limit:contract:v1").unwrap();
    let key = RateLimitKey::new(format!("client-{}", Uuid::new_v4().simple())).unwrap();
    let policy = FixedWindow::new(2, Duration::from_secs(30)).unwrap();

    let requests = join_all((0..12).map(|_| store.check(key.clone(), policy))).await;
    let decisions = requests.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(
        decisions
            .iter()
            .filter(|decision| decision.is_allowed())
            .count(),
        2
    );
    assert_eq!(
        decisions
            .iter()
            .filter(|decision| !decision.is_allowed())
            .count(),
        10
    );
    assert!(
        decisions
            .iter()
            .all(|decision| decision.reset_after() > Duration::ZERO)
    );
    assert!(
        decisions
            .iter()
            .all(|decision| decision.is_allowed() || decision.remaining() == 0)
    );
}
