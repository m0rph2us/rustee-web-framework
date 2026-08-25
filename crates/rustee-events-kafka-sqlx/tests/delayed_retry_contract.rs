//! Opt-in `Kafka` plus `PostgreSQL` contract for durable delayed event retries.

#![cfg(feature = "rdkafka")]

use std::{io, num::NonZeroU16, sync::Arc, time::Duration};

use rustee_events::{Event, EventClient, EventEnvelope};
use rustee_events_kafka::{
    KafkaConfig, KafkaConsumerConfig, KafkaEventConsumer, KafkaFailurePublisher, KafkaPublisher,
    KafkaRetryConfig,
};
use rustee_events_kafka_sqlx::{
    KAFKA_DELAYED_RETRY_MIGRATION_SQL, KafkaDelayedRetryBacklog, KafkaDelayedRetryDelay,
    KafkaDelayedRetryReadinessConfig, KafkaDelayedRetryRelayBatchSize,
    KafkaDelayedRetryRelayConfig, PostgresKafkaDelayedRetryRelay, PostgresKafkaDelayedRetryRouter,
};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::{
    sync::{Notify, oneshot},
    time::{sleep, timeout},
};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ContractEvent {
    sequence: u8,
}

impl Event for ContractEvent {
    const TYPE: &'static str = "rustee.kafka.delayed-retry.contract.v1";
    const VERSION: u16 = 1;
}

fn broker_url() -> String {
    std::env::var("RUSTEE_KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "127.0.0.1:9092".into())
}

fn database_url() -> String {
    std::env::var("RUSTEE_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rustee:rustee@127.0.0.1:5432/rustee".into())
}

fn consumer_config(topic: &str, group_id: &str) -> KafkaConsumerConfig {
    KafkaConsumerConfig::new(broker_url(), topic, group_id)
        .unwrap()
        .with_option("auto.offset.reset", "earliest")
        .unwrap()
        .with_option("session.timeout.ms", "6000")
        .unwrap()
}

async fn pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url())
        .await
        .unwrap()
}

async fn reset_schema(pool: &PgPool) {
    sqlx::raw_sql(KAFKA_DELAYED_RETRY_MIGRATION_SQL)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("TRUNCATE rustee_kafka_delayed_retries")
        .execute(pool)
        .await
        .unwrap();
}

async fn ready_publisher(topic: &str) -> KafkaPublisher {
    let publisher = KafkaPublisher::connect(
        &KafkaConfig::new(broker_url(), topic)
            .unwrap()
            .with_queue_timeout(Duration::from_secs(10)),
    )
    .unwrap();
    for _ in 0..60 {
        if publisher.readiness(Duration::from_secs(1)).is_ok() {
            return publisher;
        }
        sleep(Duration::from_millis(500)).await;
    }
    panic!("Kafka broker did not become ready");
}

async fn wait_for_staged_retry(pool: &PgPool) {
    for _ in 0..60 {
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM rustee_kafka_delayed_retries")
            .fetch_one(pool)
            .await
            .unwrap();
        if count == 1 {
            return;
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("failed Kafka event was not persisted for delayed retry");
}

fn assert_backlog(backlog: KafkaDelayedRetryBacklog, unpublished: u64, due: u64, leased: u64) {
    assert_eq!(
        backlog,
        KafkaDelayedRetryBacklog {
            unpublished,
            due,
            leased,
            oldest_due_age: None,
        }
    );
}

async fn observe_retry_delivery(topic: &str, group_id: &str) {
    let retry_seen = Arc::new(Notify::new());
    let retry_consumer = KafkaEventConsumer::connect(&consumer_config(topic, group_id)).unwrap();
    let retry_handler_seen = Arc::clone(&retry_seen);
    let (retry_shutdown_tx, retry_shutdown_rx) = oneshot::channel();
    let retry_worker = tokio::spawn(async move {
        retry_consumer
            .run_until::<ContractEvent, _, _>(
                move |event: ContractEvent, _| {
                    let seen = Arc::clone(&retry_handler_seen);
                    async move {
                        assert_eq!(event.sequence, 21);
                        seen.notify_one();
                        Ok::<(), io::Error>(())
                    }
                },
                async move {
                    let _ = retry_shutdown_rx.await;
                },
            )
            .await
    });
    timeout(Duration::from_secs(30), retry_seen.notified())
        .await
        .expect("delayed retry was not delivered to Kafka");
    retry_shutdown_tx.send(()).unwrap();
    timeout(Duration::from_secs(30), retry_worker)
        .await
        .expect("retry observer did not stop")
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = "requires Kafka and PostgreSQL containers; CI provisions both"]
async fn failure_is_staged_before_commit_and_relayed_only_after_its_database_delay() {
    let pool = pool().await;
    reset_schema(&pool).await;

    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("rustee-delayed-retry-{suffix}");
    let retry_topic = format!("rustee-delayed-retry-{suffix}.retry");
    let dead_letter_topic = format!("rustee-delayed-retry-{suffix}.dlq");
    let group_id = format!("rustee-delayed-retry-{suffix}");
    let publisher = ready_publisher(&topic).await;
    let producer_config = KafkaConfig::new(broker_url(), &topic)
        .unwrap()
        .with_queue_timeout(Duration::from_secs(10));
    let retry = KafkaRetryConfig::new(
        &retry_topic,
        &dead_letter_topic,
        NonZeroU16::new(3).unwrap(),
    )
    .unwrap();
    let _ = ready_publisher(&retry_topic).await;
    let _ = ready_publisher(&dead_letter_topic).await;
    let failure_publisher = KafkaFailurePublisher::connect(&producer_config, retry).unwrap();
    let delay = KafkaDelayedRetryDelay::new(Duration::from_millis(750)).unwrap();
    let router =
        PostgresKafkaDelayedRetryRouter::new(pool.clone(), failure_publisher.clone(), delay);

    let source_consumer = KafkaEventConsumer::connect(
        &consumer_config(&topic, &group_id)
            .with_retry_topic(&retry_topic)
            .unwrap(),
    )
    .unwrap();
    let (source_shutdown_tx, source_shutdown_rx) = oneshot::channel();
    let source_worker = tokio::spawn(async move {
        source_consumer
            .run_with_failure_routing::<ContractEvent, _, _>(
                |event: ContractEvent, _| async move {
                    assert_eq!(event.sequence, 21);
                    Err::<(), io::Error>(io::Error::other("intentional delayed-retry failure"))
                },
                &router,
                async move {
                    let _ = source_shutdown_rx.await;
                },
            )
            .await
    });

    EventClient::new(publisher)
        .publish(&EventEnvelope::new(ContractEvent { sequence: 21 }, "account-21").unwrap())
        .await
        .unwrap();
    wait_for_staged_retry(&pool).await;

    let staged: (i32, String, String, bool) = sqlx::query_as(
        "SELECT retry_attempt, failure_kind, retry_topic, available_at > clock_timestamp() FROM rustee_kafka_delayed_retries",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(staged.0, 2);
    assert_eq!(staged.1, "handler");
    assert_eq!(staged.2, retry_topic);
    assert!(staged.3);

    source_shutdown_tx.send(()).unwrap();
    timeout(Duration::from_secs(30), source_worker)
        .await
        .expect("source consumer did not stop")
        .unwrap()
        .unwrap();

    let relay_config = KafkaDelayedRetryRelayConfig::new(
        KafkaDelayedRetryDelay::new(Duration::from_secs(10)).unwrap(),
        KafkaDelayedRetryDelay::new(Duration::from_millis(50)).unwrap(),
    );
    let relay = PostgresKafkaDelayedRetryRelay::new(pool.clone(), failure_publisher, relay_config);
    relay
        .readiness(
            KafkaDelayedRetryReadinessConfig::new(Duration::from_secs(1), Duration::from_secs(1))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_backlog(relay.backlog().await.unwrap(), 1, 0, 0);
    let batch_size = KafkaDelayedRetryRelayBatchSize::new(8).unwrap();
    assert_eq!(relay.relay_once(batch_size).await.unwrap(), 0);

    sleep(Duration::from_millis(800)).await;
    assert_eq!(relay.relay_once(batch_size).await.unwrap(), 1);
    observe_retry_delivery(&retry_topic, &format!("{group_id}-retry-observer")).await;
    let relay_status: (bool, i32, bool) = sqlx::query_as(
        "SELECT published_at IS NOT NULL, relay_attempt, lease_token IS NULL FROM rustee_kafka_delayed_retries",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(relay_status, (true, 1, true));
    assert_backlog(relay.backlog().await.unwrap(), 0, 0, 0);
}

#[tokio::test]
#[ignore = "CI stops Kafka before this bounded delayed-retry readiness outage contract"]
async fn readiness_fails_within_its_kafka_timeout_without_endpoint_details() {
    let pool = pool().await;
    reset_schema(&pool).await;

    let suffix = Uuid::new_v4().simple().to_string();
    let producer_config = KafkaConfig::new(broker_url(), format!("rustee-readiness-{suffix}"))
        .unwrap()
        .with_queue_timeout(Duration::from_secs(1));
    let failure_publisher = KafkaFailurePublisher::connect(
        &producer_config,
        KafkaRetryConfig::new(
            format!("rustee-readiness-{suffix}.retry"),
            format!("rustee-readiness-{suffix}.dlq"),
            NonZeroU16::new(2).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let relay = PostgresKafkaDelayedRetryRelay::new(
        pool,
        failure_publisher,
        KafkaDelayedRetryRelayConfig::new(
            KafkaDelayedRetryDelay::new(Duration::from_secs(1)).unwrap(),
            KafkaDelayedRetryDelay::new(Duration::from_secs(1)).unwrap(),
        ),
    );

    let error = timeout(
        Duration::from_secs(2),
        relay.readiness(
            KafkaDelayedRetryReadinessConfig::new(
                Duration::from_secs(1),
                Duration::from_millis(500),
            )
            .unwrap(),
        ),
    )
    .await
    .expect("Kafka readiness did not honor its configured timeout")
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Kafka delayed retry Kafka readiness check failed"
    );
}
