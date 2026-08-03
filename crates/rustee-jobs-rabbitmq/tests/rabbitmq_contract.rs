//! Opt-in `RabbitMQ` 4.3 quorum-queue job contracts.

use std::{io, sync::Arc, time::Duration};

use lapin::{
    Connection, ConnectionProperties, ExchangeKind,
    options::{
        BasicAckOptions, BasicGetOptions, ExchangeDeclareOptions, ExchangeDeleteOptions,
        QueueBindOptions, QueueDeclareOptions, QueueDeleteOptions,
    },
    types::{AMQPValue, FieldTable},
};
use rustee_jobs::{
    Job, JobContext, JobDeliveryOutcome, JobEnvelope, JobPublisher, JobRegistry, RetryPolicy,
    WorkerConfig,
};
use rustee_jobs_observability::JobMetrics;
use rustee_jobs_rabbitmq::{
    RabbitMqConnection, RabbitMqConnectionConfig, RabbitMqError, RabbitMqNativeRetryConfig,
    RabbitMqPublisher, RabbitMqPublisherConfig, RabbitMqWorker, RabbitMqWorkerConfig,
};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Mutex, Notify, oneshot},
    time::{Instant, sleep, timeout},
};
use uuid::Uuid;

const RETRY_DELAY: Duration = Duration::from_millis(25);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ContractJob {
    value: u8,
}

impl Job for ContractJob {
    const NAME: &'static str = "rustee.contract.rabbitmq";
    const VERSION: u16 = 1;
}

struct Fixture {
    raw: Connection,
    connection: RabbitMqConnection,
    exchange: String,
    routing_key: String,
    queue: String,
    consumer_tag: String,
    dead_letter_exchange: String,
    dead_letter_routing_key: String,
    dead_letter_queue: String,
}

impl Fixture {
    async fn new(case: &str) -> Self {
        let suffix = Uuid::new_v4().simple().to_string();
        let prefix = format!("rustee.contract.{case}.{suffix}");
        let exchange = format!("{prefix}.jobs");
        let routing_key = "job".to_owned();
        let queue = format!("{prefix}.queue");
        let consumer_tag = format!("{prefix}.worker");
        let dead_letter_exchange = format!("{prefix}.dlx");
        let dead_letter_routing_key = "dead-letter".to_owned();
        let dead_letter_queue = format!("{prefix}.dlq");
        let url = rabbitmq_url();
        let raw = Connection::connect(&url, ConnectionProperties::default())
            .await
            .unwrap();
        let connection = RabbitMqConnectionConfig::new(url).connect().await.unwrap();
        let fixture = Self {
            raw,
            connection,
            exchange,
            routing_key,
            queue,
            consumer_tag,
            dead_letter_exchange,
            dead_letter_routing_key,
            dead_letter_queue,
        };
        fixture.provision().await;
        fixture
    }

    async fn provision(&self) {
        let channel = self.raw.create_channel().await.unwrap();
        channel
            .exchange_declare(
                self.exchange.as_str().into(),
                ExchangeKind::Direct,
                ExchangeDeclareOptions {
                    durable: true,
                    ..ExchangeDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();
        channel
            .exchange_declare(
                self.dead_letter_exchange.as_str().into(),
                ExchangeKind::Direct,
                ExchangeDeclareOptions {
                    durable: true,
                    ..ExchangeDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();

        let mut source_arguments = FieldTable::default();
        source_arguments.insert(
            "x-queue-type".into(),
            AMQPValue::LongString("quorum".into()),
        );
        source_arguments.insert(
            "x-delayed-retry-type".into(),
            AMQPValue::LongString("failed".into()),
        );
        source_arguments.insert(
            "x-delayed-retry-min".into(),
            AMQPValue::LongInt(RETRY_DELAY.as_millis().try_into().unwrap()),
        );
        source_arguments.insert(
            "x-delayed-retry-max".into(),
            AMQPValue::LongInt(RETRY_DELAY.as_millis().try_into().unwrap()),
        );
        source_arguments.insert("x-delivery-limit".into(), AMQPValue::LongInt(10));
        source_arguments.insert(
            "x-dead-letter-exchange".into(),
            AMQPValue::LongString(self.dead_letter_exchange.as_str().into()),
        );
        source_arguments.insert(
            "x-dead-letter-routing-key".into(),
            AMQPValue::LongString(self.dead_letter_routing_key.as_str().into()),
        );
        channel
            .queue_declare(
                self.queue.as_str().into(),
                QueueDeclareOptions {
                    durable: true,
                    ..QueueDeclareOptions::default()
                },
                source_arguments,
            )
            .await
            .unwrap();
        channel
            .queue_declare(
                self.dead_letter_queue.as_str().into(),
                QueueDeclareOptions {
                    durable: true,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();
        channel
            .queue_bind(
                self.queue.as_str().into(),
                self.exchange.as_str().into(),
                self.routing_key.as_str().into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        channel
            .queue_bind(
                self.dead_letter_queue.as_str().into(),
                self.dead_letter_exchange.as_str().into(),
                self.dead_letter_routing_key.as_str().into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
    }

    async fn publisher(&self) -> RabbitMqPublisher {
        RabbitMqPublisher::new(
            self.connection.clone(),
            RabbitMqPublisherConfig::new(self.exchange.clone(), self.routing_key.clone()).unwrap(),
        )
        .await
        .unwrap()
    }

    fn worker(&self) -> RabbitMqWorker {
        Self::worker_for_connection(
            self.connection.clone(),
            self.queue.clone(),
            self.consumer_tag.clone(),
            self.dead_letter_exchange.clone(),
            self.dead_letter_routing_key.clone(),
        )
    }

    async fn independent_worker(&self) -> RabbitMqWorker {
        Self::worker_for_connection(
            RabbitMqConnectionConfig::new(rabbitmq_url())
                .connect()
                .await
                .unwrap(),
            self.queue.clone(),
            format!("{}.crash", self.consumer_tag),
            self.dead_letter_exchange.clone(),
            self.dead_letter_routing_key.clone(),
        )
    }

    fn worker_for_connection(
        connection: RabbitMqConnection,
        queue: String,
        consumer_tag: String,
        dead_letter_exchange: String,
        dead_letter_routing_key: String,
    ) -> RabbitMqWorker {
        let native_retry = RabbitMqNativeRetryConfig::new(RETRY_DELAY, RETRY_DELAY).unwrap();
        RabbitMqWorker::new(
            connection,
            RabbitMqWorkerConfig::new(
                queue,
                consumer_tag,
                native_retry,
                dead_letter_exchange,
                dead_letter_routing_key,
            )
            .unwrap(),
        )
    }

    async fn enqueue(&self, envelope: &JobEnvelope<ContractJob>) {
        self.publisher()
            .await
            .publish(envelope.message().unwrap())
            .await
            .unwrap();
    }

    async fn await_dead_letter(&self) -> Vec<u8> {
        let channel = self.raw.create_channel().await.unwrap();
        for _ in 0..100 {
            if let Some(message) = channel
                .basic_get(
                    self.dead_letter_queue.as_str().into(),
                    BasicGetOptions::default(),
                )
                .await
                .unwrap()
            {
                message.ack(BasicAckOptions::default()).await.unwrap();
                return message.data.clone();
            }
            sleep(Duration::from_millis(25)).await;
        }
        panic!("timed out waiting for RabbitMQ dead-letter delivery");
    }

    async fn cleanup(&self) {
        let channel = self.raw.create_channel().await.unwrap();
        let _ = channel
            .queue_delete(self.queue.as_str().into(), QueueDeleteOptions::default())
            .await;
        let _ = channel
            .queue_delete(
                self.dead_letter_queue.as_str().into(),
                QueueDeleteOptions::default(),
            )
            .await;
        let _ = channel
            .exchange_delete(
                self.exchange.as_str().into(),
                ExchangeDeleteOptions::default(),
            )
            .await;
        let _ = channel
            .exchange_delete(
                self.dead_letter_exchange.as_str().into(),
                ExchangeDeleteOptions::default(),
            )
            .await;
    }
}

fn rabbitmq_url() -> String {
    std::env::var("RUSTEE_RABBITMQ_URL")
        .unwrap_or_else(|_| "amqp://guest:guest@127.0.0.1:5672/%2f".to_owned())
}

fn retry_policy(max_deliveries: u16) -> RetryPolicy {
    RetryPolicy {
        max_deliveries,
        initial_backoff: RETRY_DELAY,
        max_backoff: RETRY_DELAY,
    }
}

#[tokio::test]
#[ignore = "requires a stopped RabbitMQ 4.3 broker; CI controls the container lifecycle"]
async fn broker_outage_fails_within_the_connect_deadline_and_redacts_the_endpoint() {
    if std::env::var("RUSTEE_RABBITMQ_EXPECT_OUTAGE").as_deref() != Ok("1") {
        return;
    }

    let started = Instant::now();
    let error = RabbitMqConnectionConfig::new(rabbitmq_url())
        .with_connect_timeout(Duration::from_millis(500))
        .unwrap()
        .connect()
        .await
        .unwrap_err();

    assert_eq!(error, RabbitMqError::Connect);
    assert!(started.elapsed() < Duration::from_secs(2));
    let rendered = error.to_string();
    assert!(!rendered.contains("guest"));
    assert!(!rendered.contains("127.0.0.1"));
    assert!(!rendered.contains("5672"));
}

#[tokio::test]
#[ignore = "requires RabbitMQ 4.3 with a reachable AMQP port; CI provisions one"]
async fn successful_handler_acknowledges_a_manual_delivery() {
    let fixture = Fixture::new("success").await;
    let metrics = JobMetrics::new();
    let worker = fixture
        .worker()
        .with_delivery_observer(Arc::new(metrics.clone()));
    worker.readiness().await.unwrap();
    let completed = Arc::new(Notify::new());
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let handler = {
        let completed = completed.clone();
        let attempts = attempts.clone();
        move |_job: ContractJob, context: JobContext| {
            let completed = completed.clone();
            let attempts = attempts.clone();
            async move {
                attempts.lock().await.push(context.attempt());
                completed.notify_one();
                Ok::<(), io::Error>(())
            }
        }
    };
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let worker_task = tokio::spawn(async move {
        worker
            .run_until::<ContractJob, _, _>(
                handler,
                WorkerConfig::default(),
                retry_policy(2),
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
    });

    fixture
        .enqueue(&JobEnvelope::new(ContractJob { value: 7 }))
        .await;
    timeout(Duration::from_secs(5), completed.notified())
        .await
        .unwrap();
    shutdown_tx.send(()).unwrap();
    worker_task.await.unwrap().unwrap();
    assert_eq!(attempts.lock().await.as_slice(), &[1]);
    assert_eq!(
        metrics
            .snapshot()
            .outcome("rabbitmq", JobDeliveryOutcome::Acknowledged),
        1
    );
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires RabbitMQ 4.3 with a reachable AMQP port; CI provisions one"]
async fn failed_handler_uses_native_delay_then_dead_letters_after_the_budget() {
    let fixture = Fixture::new("retry").await;
    let metrics = JobMetrics::new();
    let worker = fixture
        .worker()
        .with_delivery_observer(Arc::new(metrics.clone()));
    let invocation_times = Arc::new(Mutex::new(Vec::new()));
    let handler = {
        let invocation_times = invocation_times.clone();
        move |_job: ContractJob, _context: JobContext| {
            let invocation_times = invocation_times.clone();
            async move {
                invocation_times.lock().await.push(Instant::now());
                Err::<(), _>(io::Error::other("retry"))
            }
        }
    };
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let worker_task = tokio::spawn(async move {
        worker
            .run_until::<ContractJob, _, _>(
                handler,
                WorkerConfig::default(),
                retry_policy(2),
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
    });

    let expected = JobEnvelope::new(ContractJob { value: 9 });
    fixture.enqueue(&expected).await;
    let payload = fixture.await_dead_letter().await;
    shutdown_tx.send(()).unwrap();
    worker_task.await.unwrap().unwrap();
    assert_eq!(
        JobEnvelope::<ContractJob>::decode(&payload).unwrap(),
        expected
    );
    let invocation_times = invocation_times.lock().await;
    assert_eq!(invocation_times.len(), 2);
    assert!(
        invocation_times[1].duration_since(invocation_times[0])
            >= RETRY_DELAY.saturating_sub(Duration::from_millis(5))
    );
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.outcome("rabbitmq", JobDeliveryOutcome::Retried), 1);
    assert_eq!(
        snapshot.outcome("rabbitmq", JobDeliveryOutcome::DeadLettered),
        1
    );
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires RabbitMQ 4.3 with a reachable AMQP port; CI provisions one"]
async fn aborted_worker_connection_redelivers_an_unacknowledged_delivery() {
    let fixture = Fixture::new("connection-loss").await;
    let first_attempt = Arc::new(Notify::new());
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let first_worker = fixture.independent_worker().await;
    let first_handler = {
        let first_attempt = Arc::clone(&first_attempt);
        let attempts = Arc::clone(&attempts);
        move |_job: ContractJob, context: JobContext| {
            let first_attempt = Arc::clone(&first_attempt);
            let attempts = Arc::clone(&attempts);
            async move {
                attempts.lock().await.push(context.attempt());
                first_attempt.notify_one();
                std::future::pending::<Result<(), io::Error>>().await
            }
        }
    };
    let first_task = tokio::spawn(async move {
        first_worker
            .run_until::<ContractJob, _, _>(
                first_handler,
                WorkerConfig::default(),
                retry_policy(2),
                std::future::pending(),
            )
            .await
    });

    fixture
        .enqueue(&JobEnvelope::new(ContractJob { value: 13 }))
        .await;
    timeout(Duration::from_secs(5), first_attempt.notified())
        .await
        .expect("first worker should receive the delivery before connection loss");
    first_task.abort();
    assert!(first_task.await.unwrap_err().is_cancelled());

    let recovered = Arc::new(Notify::new());
    let recovery_worker = fixture.worker();
    let recovery_handler = {
        let recovered = Arc::clone(&recovered);
        let attempts = Arc::clone(&attempts);
        move |_job: ContractJob, context: JobContext| {
            let recovered = Arc::clone(&recovered);
            let attempts = Arc::clone(&attempts);
            async move {
                attempts.lock().await.push(context.attempt());
                recovered.notify_one();
                Ok::<(), io::Error>(())
            }
        }
    };
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let recovery_task = tokio::spawn(async move {
        recovery_worker
            .run_until::<ContractJob, _, _>(
                recovery_handler,
                WorkerConfig::default(),
                retry_policy(2),
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
    });
    timeout(Duration::from_secs(5), recovered.notified())
        .await
        .expect("replacement worker should receive the unacknowledged delivery");
    shutdown_tx.send(()).unwrap();
    recovery_task.await.unwrap().unwrap();
    assert_eq!(attempts.lock().await.as_slice(), &[1, 2]);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires RabbitMQ 4.3 with a reachable AMQP port; CI provisions one"]
async fn unknown_registry_job_dead_letters_without_retrying() {
    let fixture = Fixture::new("registry-dlq").await;
    let worker = fixture.worker();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let worker_task = tokio::spawn(async move {
        worker
            .run_registry_until(
                JobRegistry::new(),
                WorkerConfig::default(),
                retry_policy(2),
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
    });

    let expected = JobEnvelope::new(ContractJob { value: 3 });
    fixture.enqueue(&expected).await;
    let payload = fixture.await_dead_letter().await;
    shutdown_tx.send(()).unwrap();
    worker_task.await.unwrap().unwrap();
    assert_eq!(
        JobEnvelope::<ContractJob>::decode(&payload).unwrap(),
        expected
    );
    fixture.cleanup().await;
}
