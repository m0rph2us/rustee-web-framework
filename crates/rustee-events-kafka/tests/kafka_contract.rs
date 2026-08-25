//! Opt-in real Kafka producer and consumer-group contract.

#![cfg(feature = "rdkafka")]

use std::{
    io,
    num::NonZeroU16,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use rustee_events::{Event, EventClient, EventDeliveryOutcome, EventEnvelope, PublishError};
use rustee_events_kafka::rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    error::RDKafkaErrorCode,
};
use rustee_events_kafka::{
    KafkaConfig, KafkaConsumerConfig, KafkaError, KafkaEventConsumer, KafkaFailurePublisher,
    KafkaLagSnapshotLimit, KafkaPublisher, KafkaRetryConfig,
};
use rustee_events_observability::EventMetrics;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Notify, oneshot},
    time::{Instant, sleep, timeout},
};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ContractEvent {
    sequence: u8,
}

impl Event for ContractEvent {
    const TYPE: &'static str = "rustee.kafka.contract.v1";
    const VERSION: u16 = 1;
}

fn broker_url() -> String {
    std::env::var("RUSTEE_KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "127.0.0.1:9092".to_owned())
}

fn consumer_config(topic: &str, group_id: &str) -> KafkaConsumerConfig {
    KafkaConsumerConfig::new(broker_url(), topic, group_id)
        .unwrap()
        .with_option("auto.offset.reset", "earliest")
        .unwrap()
        .with_option("session.timeout.ms", "6000")
        .unwrap()
}

async fn provision_topics(topics: &[&str]) {
    let admin = ClientConfig::new()
        .set("bootstrap.servers", broker_url())
        .create::<AdminClient<DefaultClientContext>>()
        .expect("Kafka admin client configuration failed");
    let requests = topics
        .iter()
        .map(|topic| NewTopic::new(topic, 1, TopicReplication::Fixed(1)))
        .collect::<Vec<_>>();
    let results = admin
        .create_topics(
            &requests,
            &AdminOptions::new()
                .request_timeout(Some(Duration::from_secs(10)))
                .operation_timeout(Some(Duration::from_secs(10))),
        )
        .await
        .expect("Kafka topic provisioning request failed");
    for result in results {
        match result {
            Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((topic, error)) => panic!("Kafka topic provisioning failed for {topic}: {error}"),
        }
    }
}

async fn ready_publisher(topic: &str) -> KafkaPublisher {
    provision_topics(&[topic]).await;
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

async fn publish(publisher: &KafkaPublisher, sequence: u8) {
    EventClient::new(publisher.clone())
        .publish(&EventEnvelope::new(ContractEvent { sequence }, "contract-key").unwrap())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires a stopped Kafka broker; CI controls the container lifecycle"]
async fn broker_outage_fails_within_the_delivery_deadline_and_redacts_the_endpoint() {
    if std::env::var("RUSTEE_KAFKA_EXPECT_OUTAGE").as_deref() != Ok("1") {
        return;
    }

    let publisher = KafkaPublisher::connect(
        &KafkaConfig::new(broker_url(), "rustee.outage.contract")
            .unwrap()
            .with_delivery_timeout(Duration::from_millis(500))
            .unwrap(),
    )
    .unwrap();
    let started = Instant::now();
    let error = EventClient::new(publisher)
        .publish(&EventEnvelope::new(ContractEvent { sequence: 0 }, "outage-key").unwrap())
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        PublishError::Provider(KafkaError::Delivery)
    ));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "stopped Kafka broker delivery exceeded the bounded deadline"
    );
    let displayed = error.to_string();
    assert!(!displayed.contains("127.0.0.1"));
    assert!(!displayed.contains("9092"));
}

#[tokio::test]
#[ignore = "requires a stopped Kafka broker; CI controls the container lifecycle"]
async fn consumer_readiness_fails_within_its_timeout_and_redacts_the_endpoint() {
    if std::env::var("RUSTEE_KAFKA_EXPECT_OUTAGE").as_deref() != Ok("1") {
        return;
    }

    let consumer = KafkaEventConsumer::connect(&consumer_config(
        "rustee.consumer-readiness.contract",
        "consumer-readiness-contract",
    ))
    .unwrap();
    let started = Instant::now();
    let error = consumer.readiness(Duration::from_millis(500)).unwrap_err();

    assert_eq!(error, KafkaError::Readiness);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "stopped Kafka broker consumer readiness exceeded its bounded timeout"
    );
    let displayed = error.to_string();
    assert!(!displayed.contains("127.0.0.1"));
    assert!(!displayed.contains("9092"));
}

async fn wait_for_group_members(consumer: &KafkaEventConsumer, expected_members: usize) {
    for _ in 0..60 {
        if matches!(
            consumer.group_member_count(Duration::from_secs(1)),
            Ok(member_count) if member_count == expected_members
        ) {
            return;
        }
        sleep(Duration::from_millis(250)).await;
    }
    panic!("Kafka consumer group did not reach {expected_members} members");
}

async fn wait_for_zero_lag(consumer: &KafkaEventConsumer) {
    for _ in 0..60 {
        if let Ok(snapshot) = consumer.lag_snapshot_with_limit(
            KafkaLagSnapshotLimit::new(NonZeroU16::new(8).unwrap()),
            Duration::from_secs(1),
        ) && !snapshot.is_empty()
            && snapshot.iter().all(|partition| partition.lag() == Some(0))
        {
            return;
        }
        sleep(Duration::from_millis(250)).await;
    }
    panic!("Kafka consumer lag did not reach zero");
}

async fn wait_for_delivery_outcome(
    metrics: &EventMetrics,
    outcome: EventDeliveryOutcome,
    expected: u64,
) {
    for _ in 0..60 {
        if metrics.snapshot().outcome("apache_kafka", outcome) == expected {
            return;
        }
        sleep(Duration::from_millis(250)).await;
    }
    panic!("Kafka delivery observer did not record {outcome:?}={expected}");
}

fn assert_delivery_outcomes(metrics: &EventMetrics, expected: &[(EventDeliveryOutcome, u64)]) {
    let snapshot = metrics.snapshot();
    for &(outcome, count) in expected {
        assert_eq!(snapshot.outcome("apache_kafka", outcome), count);
    }
}

async fn assert_successful_delivery_is_committed(
    publisher: &KafkaPublisher,
    topic: &str,
    group_id: &str,
) {
    let seen = Arc::new(Notify::new());
    let shutdown = Arc::new(Notify::new());
    let metrics = EventMetrics::new();
    let consumer = KafkaEventConsumer::connect(&consumer_config(topic, group_id)).unwrap();
    consumer.readiness(Duration::from_secs(5)).unwrap();
    let consumer = consumer.with_delivery_observer(Arc::new(metrics.clone()));
    let handler_seen = Arc::clone(&seen);
    let shutdown_wait = Arc::clone(&shutdown);
    let worker = tokio::spawn(async move {
        consumer
            .run_until::<ContractEvent, _, _>(
                move |event: ContractEvent, _| {
                    let seen = Arc::clone(&handler_seen);
                    async move {
                        assert_eq!(event.sequence, 1);
                        seen.notify_one();
                        Ok::<(), io::Error>(())
                    }
                },
                async move { shutdown_wait.notified().await },
            )
            .await
    });
    publish(publisher, 1).await;
    timeout(Duration::from_secs(30), seen.notified())
        .await
        .expect("successful Kafka handler was not called");
    shutdown.notify_one();
    worker.await.unwrap().unwrap();
    assert_delivery_outcomes(
        &metrics,
        &[
            (EventDeliveryOutcome::Acknowledged, 1),
            (EventDeliveryOutcome::Unsettled, 0),
        ],
    );
}

#[tokio::test]
#[ignore = "requires a Kafka broker; CI provisions one"]
async fn consumer_commits_only_after_success_and_retries_uncommitted_records() {
    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("rustee-contract-{suffix}");
    let group_id = format!("rustee-contract-{suffix}");
    let publisher = ready_publisher(&topic).await;
    assert_successful_delivery_is_committed(&publisher, &topic, &group_id).await;

    let replay_count = Arc::new(AtomicUsize::new(0));
    let replay_count_handler = Arc::clone(&replay_count);
    KafkaEventConsumer::connect(&consumer_config(&topic, &group_id))
        .unwrap()
        .run_until::<ContractEvent, _, _>(
            move |_: ContractEvent, _| {
                let count = Arc::clone(&replay_count_handler);
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), io::Error>(())
                }
            },
            sleep(Duration::from_secs(2)),
        )
        .await
        .unwrap();
    assert_eq!(replay_count.load(Ordering::SeqCst), 0);

    let failed_consumer = KafkaEventConsumer::connect(&consumer_config(&topic, &group_id)).unwrap();
    let failed_worker = tokio::spawn(async move {
        failed_consumer
            .run_until::<ContractEvent, _, _>(
                |event: ContractEvent, _| async move {
                    assert_eq!(event.sequence, 2);
                    Err::<(), io::Error>(io::Error::other("intentional contract failure"))
                },
                std::future::pending(),
            )
            .await
    });
    publish(&publisher, 2).await;
    assert_eq!(
        timeout(Duration::from_secs(30), failed_worker)
            .await
            .expect("failed Kafka handler was not called")
            .unwrap()
            .unwrap_err(),
        KafkaError::Handler
    );

    let recovery_seen = Arc::new(Notify::new());
    let recovery_shutdown = Arc::new(Notify::new());
    let recovery_consumer =
        KafkaEventConsumer::connect(&consumer_config(&topic, &group_id)).unwrap();
    let recovery_handler_seen = Arc::clone(&recovery_seen);
    let recovery_shutdown_wait = Arc::clone(&recovery_shutdown);
    let recovery_worker = tokio::spawn(async move {
        recovery_consumer
            .run_until::<ContractEvent, _, _>(
                move |event: ContractEvent, _| {
                    let seen = Arc::clone(&recovery_handler_seen);
                    async move {
                        assert_eq!(event.sequence, 2);
                        seen.notify_one();
                        Ok::<(), io::Error>(())
                    }
                },
                async move { recovery_shutdown_wait.notified().await },
            )
            .await
    });
    timeout(Duration::from_secs(30), recovery_seen.notified())
        .await
        .expect("uncommitted Kafka record was not redelivered");
    recovery_shutdown.notify_one();
    recovery_worker.await.unwrap().unwrap();
}

#[tokio::test]
#[ignore = "requires a Kafka broker; CI provisions one"]
async fn shutdown_drains_an_active_handler_and_commits_its_record() {
    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("rustee-drain-{suffix}");
    let group_id = format!("rustee-drain-{suffix}");
    let publisher = ready_publisher(&topic).await;

    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let consumer = KafkaEventConsumer::connect(&consumer_config(&topic, &group_id)).unwrap();
    let handler_started = Arc::clone(&started);
    let handler_release = Arc::clone(&release);
    let worker = tokio::spawn(async move {
        consumer
            .run_until::<ContractEvent, _, _>(
                move |event: ContractEvent, _| {
                    let started = Arc::clone(&handler_started);
                    let release = Arc::clone(&handler_release);
                    async move {
                        assert_eq!(event.sequence, 3);
                        started.notify_one();
                        release.notified().await;
                        Ok::<(), io::Error>(())
                    }
                },
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
    });
    publish(&publisher, 3).await;
    timeout(Duration::from_secs(30), started.notified())
        .await
        .expect("Kafka drain handler was not called");
    shutdown_tx.send(()).unwrap();
    tokio::task::yield_now().await;
    assert!(!worker.is_finished());
    release.notify_one();
    timeout(Duration::from_secs(30), worker)
        .await
        .expect("Kafka worker did not finish draining")
        .unwrap()
        .unwrap();

    let replay_count = Arc::new(AtomicUsize::new(0));
    let replay_count_handler = Arc::clone(&replay_count);
    KafkaEventConsumer::connect(&consumer_config(&topic, &group_id))
        .unwrap()
        .run_until::<ContractEvent, _, _>(
            move |_: ContractEvent, _| {
                let count = Arc::clone(&replay_count_handler);
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), io::Error>(())
                }
            },
            sleep(Duration::from_secs(2)),
        )
        .await
        .unwrap();
    assert_eq!(replay_count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
#[ignore = "requires a Kafka broker; CI provisions one"]
async fn consumer_group_rebalances_a_quiescent_partition_to_the_remaining_member() {
    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("rustee-rebalance-{suffix}");
    let group_id = format!("rustee-rebalance-{suffix}");
    let publisher = ready_publisher(&topic).await;

    let first_seen = Arc::new(Notify::new());
    let (first_shutdown_tx, first_shutdown_rx) = oneshot::channel();
    let first_consumer =
        Arc::new(KafkaEventConsumer::connect(&consumer_config(&topic, &group_id)).unwrap());
    let first_worker_consumer = Arc::clone(&first_consumer);
    let first_handler_seen = Arc::clone(&first_seen);
    let first_worker = tokio::spawn(async move {
        first_worker_consumer
            .run_until::<ContractEvent, _, _>(
                move |event: ContractEvent, _| {
                    let seen = Arc::clone(&first_handler_seen);
                    async move {
                        assert_eq!(event.sequence, 4);
                        seen.notify_one();
                        Ok::<(), io::Error>(())
                    }
                },
                async move {
                    let _ = first_shutdown_rx.await;
                },
            )
            .await
    });
    publish(&publisher, 4).await;
    timeout(Duration::from_secs(30), first_seen.notified())
        .await
        .expect("first Kafka group member did not receive its record");

    let second_seen = Arc::new(Notify::new());
    let (second_shutdown_tx, second_shutdown_rx) = oneshot::channel();
    let second_consumer =
        Arc::new(KafkaEventConsumer::connect(&consumer_config(&topic, &group_id)).unwrap());
    let second_worker_consumer = Arc::clone(&second_consumer);
    let second_handler_seen = Arc::clone(&second_seen);
    let second_worker = tokio::spawn(async move {
        second_worker_consumer
            .run_until::<ContractEvent, _, _>(
                move |event: ContractEvent, _| {
                    let seen = Arc::clone(&second_handler_seen);
                    async move {
                        assert_eq!(event.sequence, 5);
                        seen.notify_one();
                        Ok::<(), io::Error>(())
                    }
                },
                async move {
                    let _ = second_shutdown_rx.await;
                },
            )
            .await
    });
    wait_for_group_members(&second_consumer, 2).await;
    first_shutdown_tx.send(()).unwrap();
    timeout(Duration::from_secs(30), first_worker)
        .await
        .expect("first Kafka group member did not stop")
        .unwrap()
        .unwrap();
    drop(first_consumer);
    wait_for_group_members(&second_consumer, 1).await;

    publish(&publisher, 5).await;
    timeout(Duration::from_secs(30), second_seen.notified())
        .await
        .expect("remaining Kafka group member did not receive the reassigned partition");
    second_shutdown_tx.send(()).unwrap();
    timeout(Duration::from_secs(30), second_worker)
        .await
        .expect("remaining Kafka group member did not stop")
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = "requires a Kafka broker; CI provisions one"]
async fn rebalance_redelivers_an_in_flight_record_when_its_original_commit_fails() {
    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("rustee-in-flight-{suffix}");
    let group_id = format!("rustee-in-flight-{suffix}");
    let publisher = ready_publisher(&topic).await;

    let first_started = Arc::new(Notify::new());
    let first_release = Arc::new(Notify::new());
    let first_metrics = EventMetrics::new();
    let first_consumer = Arc::new(
        KafkaEventConsumer::connect(
            &consumer_config(&topic, &group_id)
                .with_option("max.poll.interval.ms", "1000")
                .unwrap(),
        )
        .unwrap()
        .with_delivery_observer(Arc::new(first_metrics.clone())),
    );
    let first_worker_consumer = Arc::clone(&first_consumer);
    let first_handler_started = Arc::clone(&first_started);
    let first_handler_release = Arc::clone(&first_release);
    let first_worker = tokio::spawn(async move {
        first_worker_consumer
            .run_until::<ContractEvent, _, _>(
                move |event: ContractEvent, _| {
                    let started = Arc::clone(&first_handler_started);
                    let release = Arc::clone(&first_handler_release);
                    async move {
                        assert_eq!(event.sequence, 6);
                        started.notify_one();
                        release.notified().await;
                        Ok::<(), io::Error>(())
                    }
                },
                std::future::pending(),
            )
            .await
    });
    publish(&publisher, 6).await;
    timeout(Duration::from_secs(30), first_started.notified())
        .await
        .expect("first Kafka handler did not start");

    let second_seen = Arc::new(Notify::new());
    let second_release = Arc::new(Notify::new());
    let (second_shutdown_tx, second_shutdown_rx) = oneshot::channel();
    let second_consumer =
        Arc::new(KafkaEventConsumer::connect(&consumer_config(&topic, &group_id)).unwrap());
    let second_worker_consumer = Arc::clone(&second_consumer);
    let second_handler_seen = Arc::clone(&second_seen);
    let second_handler_release = Arc::clone(&second_release);
    let second_worker = tokio::spawn(async move {
        second_worker_consumer
            .run_until::<ContractEvent, _, _>(
                move |event: ContractEvent, _| {
                    let seen = Arc::clone(&second_handler_seen);
                    let release = Arc::clone(&second_handler_release);
                    async move {
                        assert_eq!(event.sequence, 6);
                        seen.notify_one();
                        release.notified().await;
                        Ok::<(), io::Error>(())
                    }
                },
                async move {
                    let _ = second_shutdown_rx.await;
                },
            )
            .await
    });
    wait_for_group_members(&second_consumer, 2).await;
    wait_for_group_members(&second_consumer, 1).await;
    timeout(Duration::from_secs(30), second_seen.notified())
        .await
        .expect("in-flight Kafka record was not redelivered after rebalance");

    first_release.notify_one();
    assert_eq!(
        timeout(Duration::from_secs(30), first_worker)
            .await
            .expect("original Kafka worker did not report its failed commit")
            .unwrap()
            .unwrap_err(),
        KafkaError::Commit
    );
    wait_for_delivery_outcome(&first_metrics, EventDeliveryOutcome::Unsettled, 1).await;
    drop(first_consumer);
    second_release.notify_one();
    second_shutdown_tx.send(()).unwrap();
    timeout(Duration::from_secs(30), second_worker)
        .await
        .expect("recovery Kafka worker did not stop")
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = "requires a Kafka broker; CI provisions one"]
async fn lag_snapshot_reports_zero_after_a_committed_record() {
    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("rustee-lag-{suffix}");
    let group_id = format!("rustee-lag-{suffix}");
    let publisher = ready_publisher(&topic).await;
    let seen = Arc::new(Notify::new());
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let consumer =
        Arc::new(KafkaEventConsumer::connect(&consumer_config(&topic, &group_id)).unwrap());
    let worker_consumer = Arc::clone(&consumer);
    let handler_seen = Arc::clone(&seen);
    let worker = tokio::spawn(async move {
        worker_consumer
            .run_until::<ContractEvent, _, _>(
                move |event: ContractEvent, _| {
                    let seen = Arc::clone(&handler_seen);
                    async move {
                        assert_eq!(event.sequence, 7);
                        seen.notify_one();
                        Ok::<(), io::Error>(())
                    }
                },
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
    });
    publish(&publisher, 7).await;
    timeout(Duration::from_secs(30), seen.notified())
        .await
        .expect("Kafka lag contract handler was not called");
    wait_for_zero_lag(&consumer).await;
    shutdown_tx.send(()).unwrap();
    timeout(Duration::from_secs(30), worker)
        .await
        .expect("Kafka lag contract worker did not stop")
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = "requires a Kafka broker; CI provisions one"]
async fn failure_routing_retries_then_delivers_the_terminal_event_to_its_dead_letter_topic() {
    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("rustee-retry-{suffix}");
    let retry_topic = format!("rustee-retry-{suffix}.retry");
    let dead_letter_topic = format!("rustee-retry-{suffix}.dlq");
    let group_id = format!("rustee-retry-{suffix}");
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
    provision_topics(&[&retry_topic, &dead_letter_topic]).await;
    let failure_publisher = KafkaFailurePublisher::connect(&producer_config, retry).unwrap();
    failure_publisher.readiness(Duration::from_secs(5)).unwrap();

    let attempts = Arc::new(AtomicUsize::new(0));
    let metrics = EventMetrics::new();
    let (source_shutdown_tx, source_shutdown_rx) = oneshot::channel();
    let retry_consumer = KafkaEventConsumer::connect(
        &consumer_config(&topic, &group_id)
            .with_retry_topic(&retry_topic)
            .unwrap(),
    )
    .unwrap()
    .with_delivery_observer(Arc::new(metrics.clone()));
    let retry_attempts = Arc::clone(&attempts);
    let retry_failure_publisher = failure_publisher.clone();
    let retry_worker = tokio::spawn(async move {
        retry_consumer
            .run_with_failure_routing::<ContractEvent, _, _>(
                move |event: ContractEvent, _| {
                    let attempts = Arc::clone(&retry_attempts);
                    async move {
                        assert_eq!(event.sequence, 8);
                        attempts.fetch_add(1, Ordering::SeqCst);
                        Err::<(), io::Error>(io::Error::other("intentional retry contract failure"))
                    }
                },
                &retry_failure_publisher,
                async move {
                    let _ = source_shutdown_rx.await;
                },
            )
            .await
    });

    let dead_letter_seen = Arc::new(Notify::new());
    let (dead_letter_shutdown_tx, dead_letter_shutdown_rx) = oneshot::channel();
    let dead_letter_consumer = KafkaEventConsumer::connect(&consumer_config(
        &dead_letter_topic,
        &format!("{group_id}-dlq"),
    ))
    .unwrap();
    let dead_letter_handler_seen = Arc::clone(&dead_letter_seen);
    let dead_letter_worker = tokio::spawn(async move {
        dead_letter_consumer
            .run_until::<ContractEvent, _, _>(
                move |event: ContractEvent, _| {
                    let seen = Arc::clone(&dead_letter_handler_seen);
                    async move {
                        assert_eq!(event.sequence, 8);
                        seen.notify_one();
                        Ok::<(), io::Error>(())
                    }
                },
                async move {
                    let _ = dead_letter_shutdown_rx.await;
                },
            )
            .await
    });

    publish(&publisher, 8).await;
    timeout(Duration::from_secs(30), dead_letter_seen.notified())
        .await
        .expect("terminal Kafka event was not delivered to the dead-letter topic");
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    wait_for_delivery_outcome(&metrics, EventDeliveryOutcome::DeadLettered, 1).await;
    assert_delivery_outcomes(
        &metrics,
        &[
            (EventDeliveryOutcome::Retried, 2),
            (EventDeliveryOutcome::Unsettled, 0),
        ],
    );

    source_shutdown_tx.send(()).unwrap();
    dead_letter_shutdown_tx.send(()).unwrap();
    timeout(Duration::from_secs(30), retry_worker)
        .await
        .expect("Kafka retry worker did not stop")
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(30), dead_letter_worker)
        .await
        .expect("Kafka dead-letter worker did not stop")
        .unwrap()
        .unwrap();
}
