use std::{num::NonZeroU16, time::Duration};

use rustee_events_kafka::{KafkaConfig, KafkaFailurePublisher, KafkaRetryConfig};
use sqlx::postgres::PgPoolOptions;

use super::PostgresKafkaDelayedRetryRelay;
use crate::{
    KafkaDelayedRetryDelay, KafkaDelayedRetryRelayConfig, KafkaDelayedRetryRelayLoopConfig,
    KafkaDelayedRetryRelayLoopReport,
};

#[tokio::test]
async fn relay_loop_observes_an_immediate_shutdown_before_touching_postgres() {
    let producer_config = KafkaConfig::new("127.0.0.1:1", "events.source").unwrap();
    let retry =
        KafkaRetryConfig::new("events.retry", "events.dlq", NonZeroU16::new(2).unwrap()).unwrap();
    let publisher = KafkaFailurePublisher::connect(&producer_config, retry).unwrap();
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://rustee:rustee@127.0.0.1:1/rustee")
        .unwrap();
    let relay = PostgresKafkaDelayedRetryRelay::new(
        pool,
        publisher,
        KafkaDelayedRetryRelayConfig::new(
            KafkaDelayedRetryDelay::new(Duration::from_secs(1)).unwrap(),
            KafkaDelayedRetryDelay::new(Duration::from_secs(1)).unwrap(),
        ),
    );
    let report = relay
        .run_until(KafkaDelayedRetryRelayLoopConfig::default(), async {})
        .await
        .unwrap();
    assert_eq!(report, KafkaDelayedRetryRelayLoopReport::default());
}

#[tokio::test]
async fn relay_debug_does_not_delegate_to_pool_or_publisher_diagnostics() {
    let producer = KafkaConfig::new("127.0.0.1:1", "tenant.acme.events.source").unwrap();
    let retry = KafkaRetryConfig::new(
        "tenant.acme.events.retry",
        "tenant.acme.events.dlq",
        NonZeroU16::new(2).unwrap(),
    )
    .unwrap();
    let publisher = KafkaFailurePublisher::connect(&producer, retry).unwrap();
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://rustee:rustee@127.0.0.1:1/rustee")
        .unwrap();
    let relay = PostgresKafkaDelayedRetryRelay::new(
        pool,
        publisher,
        KafkaDelayedRetryRelayConfig::new(
            KafkaDelayedRetryDelay::new(Duration::from_secs(1)).unwrap(),
            KafkaDelayedRetryDelay::new(Duration::from_secs(1)).unwrap(),
        ),
    );

    let debug = format!("{relay:?}");
    for exposed in [
        "127.0.0.1",
        "tenant.acme.events.source",
        "tenant.acme.events.retry",
        "tenant.acme.events.dlq",
    ] {
        assert!(!debug.contains(exposed));
    }
    assert!(debug.contains("[REDACTED]"));
    assert!(debug.contains("config"));
}
