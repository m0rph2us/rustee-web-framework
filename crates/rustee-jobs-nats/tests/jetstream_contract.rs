use std::{
    convert::Infallible,
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::StreamExt;
use rustee_jobs::{
    Job, JobClient, JobContext, JobDeliveryOutcome, JobEnvelope, JobId, JobRegistry, RetryPolicy,
    WorkerConfig,
};
use rustee_jobs_nats::{JetStreamPublisher, JetStreamWorker, NatsConfig, NatsError, async_nats};
use rustee_jobs_observability::JobMetrics;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::Notify,
    time::{Instant, timeout},
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ContractJob {
    value: u64,
}

impl Job for ContractJob {
    const NAME: &'static str = "contract.job";
    const VERSION: u16 = 1;
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct UnregisteredContractJob {
    value: u64,
}

impl Job for UnregisteredContractJob {
    const NAME: &'static str = "contract.unregistered";
    const VERSION: u16 = 1;
}

#[derive(Debug)]
struct ExpectedFailure;

impl fmt::Display for ExpectedFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("expected handler failure")
    }
}

impl std::error::Error for ExpectedFailure {}

struct Fixture {
    context: async_nats::jetstream::Context,
    consumer: async_nats::jetstream::consumer::PullConsumer,
    stream_name: String,
    dead_letter_stream_name: String,
    subject: String,
    dead_letter_subject: String,
}

impl Fixture {
    async fn new(label: &str, max_deliver: i64) -> Self {
        let suffix = unique(label);
        let stream_name = format!("RUSTEE_{suffix}");
        let dead_letter_stream_name = format!("RUSTEE_DLQ_{suffix}");
        let subject = format!("jobs.contract.{suffix}");
        let dead_letter_subject = format!("jobs.contract.dlq.{suffix}");
        let client = async_nats::connect(nats_url()).await.unwrap();
        let context = async_nats::jetstream::new(client);
        let stream = context
            .create_stream(async_nats::jetstream::stream::Config {
                name: stream_name.clone(),
                subjects: vec![subject.clone()],
                max_messages: 32,
                ..Default::default()
            })
            .await
            .unwrap();
        context
            .create_stream(async_nats::jetstream::stream::Config {
                name: dead_letter_stream_name.clone(),
                subjects: vec![dead_letter_subject.clone()],
                max_messages: 32,
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer = stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                durable_name: Some(format!("worker_{suffix}")),
                ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                ack_wait: Duration::from_secs(2),
                max_deliver,
                max_ack_pending: 4,
                ..Default::default()
            })
            .await
            .unwrap();
        Self {
            context,
            consumer,
            stream_name,
            dead_letter_stream_name,
            subject,
            dead_letter_subject,
        }
    }

    async fn enqueue<J>(&self, envelope: &JobEnvelope<J>)
    where
        J: Job,
    {
        let publisher =
            JetStreamPublisher::new(self.context.clone(), self.subject.clone()).unwrap();
        JobClient::new(publisher).enqueue(envelope).await.unwrap();
    }

    fn worker(&self) -> JetStreamWorker {
        JetStreamWorker::new(
            self.context.clone(),
            self.consumer.clone(),
            self.dead_letter_subject.clone(),
        )
        .unwrap()
    }

    async fn dead_letter_consumer(&self) -> async_nats::jetstream::consumer::PullConsumer {
        let stream = self
            .context
            .get_stream(&self.dead_letter_stream_name)
            .await
            .unwrap();
        stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                durable_name: Some(format!("dlq_{}", unique("reader"))),
                ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                ..Default::default()
            })
            .await
            .unwrap()
    }

    async fn cleanup(&self) {
        let _ = self.context.delete_stream(&self.stream_name).await;
        let _ = self
            .context
            .delete_stream(&self.dead_letter_stream_name)
            .await;
    }
}

fn nats_url() -> String {
    std::env::var("RUSTEE_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_owned())
}

fn unique(label: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{label}_{nanos}")
}

fn envelope(value: u64) -> JobEnvelope<ContractJob> {
    JobEnvelope::with_metadata(JobId::new(), ContractJob { value }, 123)
}

#[tokio::test]
#[ignore = "requires a stopped JetStream server; CI controls the container lifecycle"]
async fn broker_outage_fails_within_the_connect_deadline_and_redacts_the_endpoint() {
    if std::env::var("RUSTEE_NATS_EXPECT_OUTAGE").as_deref() != Ok("1") {
        return;
    }

    let config = NatsConfig::new(nats_url(), "jobs.contract.outage")
        .unwrap()
        .with_connect_timeout(Duration::from_millis(500))
        .unwrap();
    let started = Instant::now();
    let error = JetStreamPublisher::connect(&config).await.unwrap_err();

    assert_eq!(error, NatsError::Connect);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "stopped NATS connection exceeded the bounded deadline"
    );
    let displayed = error.to_string();
    assert!(!displayed.contains("127.0.0.1"));
    assert!(!displayed.contains("4222"));
}

#[tokio::test]
#[ignore = "requires a JetStream server at RUSTEE_NATS_URL"]
async fn successful_handler_acknowledges_the_durable_delivery() {
    let fixture = Fixture::new("success", 3).await;
    let completed = Arc::new(Notify::new());
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let handler = {
        let completed = Arc::clone(&completed);
        let attempts = Arc::clone(&attempts);
        move |job: ContractJob, context: JobContext| {
            let completed = Arc::clone(&completed);
            let attempts = Arc::clone(&attempts);
            async move {
                assert_eq!(job.value, 7);
                attempts.lock().unwrap().push(context.attempt());
                completed.notify_one();
                Ok::<_, Infallible>(())
            }
        }
    };
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let metrics = JobMetrics::new();
    let worker = fixture
        .worker()
        .with_delivery_observer(Arc::new(metrics.clone()));
    let task = tokio::spawn(async move {
        worker
            .run_until::<ContractJob, _, _>(
                handler,
                WorkerConfig::default(),
                RetryPolicy::default(),
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
    });

    fixture.enqueue(&envelope(7)).await;
    timeout(Duration::from_secs(3), completed.notified())
        .await
        .unwrap();
    shutdown_tx.send(()).unwrap();
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(*attempts.lock().unwrap(), vec![1]);
    assert_eq!(
        metrics
            .snapshot()
            .outcome("nats_jetstream", JobDeliveryOutcome::Acknowledged),
        1
    );
    let info = fixture.consumer.get_info().await.unwrap();
    assert_eq!(info.num_ack_pending, 0);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a JetStream server at RUSTEE_NATS_URL"]
async fn failed_handler_retries_then_relays_to_the_dead_letter_stream() {
    let fixture = Fixture::new("retry", 2).await;
    let second_attempt = Arc::new(Notify::new());
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let handler = {
        let second_attempt = Arc::clone(&second_attempt);
        let attempts = Arc::clone(&attempts);
        move |_job: ContractJob, context: JobContext| {
            let second_attempt = Arc::clone(&second_attempt);
            let attempts = Arc::clone(&attempts);
            async move {
                attempts.lock().unwrap().push(context.attempt());
                if context.attempt() == 2 {
                    second_attempt.notify_one();
                }
                Err::<(), _>(ExpectedFailure)
            }
        }
    };
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let metrics = JobMetrics::new();
    let worker = fixture
        .worker()
        .with_delivery_observer(Arc::new(metrics.clone()));
    let task = tokio::spawn(async move {
        worker
            .run_until::<ContractJob, _, _>(
                handler,
                WorkerConfig::default(),
                RetryPolicy {
                    max_deliveries: 2,
                    initial_backoff: Duration::from_millis(25),
                    max_backoff: Duration::from_millis(25),
                },
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
    });
    let expected = envelope(9);
    let expected_bytes = expected.encode().unwrap();
    fixture.enqueue(&expected).await;
    timeout(Duration::from_secs(3), second_attempt.notified())
        .await
        .unwrap();

    let dead_letters = fixture.dead_letter_consumer().await;
    let mut messages = dead_letters
        .fetch()
        .max_messages(1)
        .expires(Duration::from_secs(2))
        .messages()
        .await
        .unwrap();
    let message = timeout(Duration::from_secs(3), messages.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(message.payload.as_ref(), expected_bytes.as_slice());
    assert_eq!(
        message
            .headers
            .as_ref()
            .unwrap()
            .get_last("Rustee-Delivery-Attempt")
            .unwrap()
            .as_str(),
        "2"
    );
    message.ack().await.unwrap();
    shutdown_tx.send(()).unwrap();
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(*attempts.lock().unwrap(), vec![1, 2]);
    let snapshot = metrics.snapshot();
    assert_eq!(
        snapshot.outcome("nats_jetstream", JobDeliveryOutcome::Retried),
        1
    );
    assert_eq!(
        snapshot.outcome("nats_jetstream", JobDeliveryOutcome::DeadLettered),
        1
    );
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a JetStream server at RUSTEE_NATS_URL"]
async fn shutdown_drains_an_active_handler_before_the_worker_returns() {
    let fixture = Fixture::new("drain", 3).await;
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let handler = {
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        move |_job: ContractJob, _context: JobContext| {
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            async move {
                started.notify_one();
                release.notified().await;
                Ok::<_, Infallible>(())
            }
        }
    };
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let worker = fixture.worker();
    let task = tokio::spawn(async move {
        worker
            .run_until::<ContractJob, _, _>(
                handler,
                WorkerConfig {
                    concurrency: std::num::NonZeroUsize::new(1).unwrap(),
                    drain_timeout: Duration::from_secs(2),
                },
                RetryPolicy::default(),
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
    });

    fixture.enqueue(&envelope(11)).await;
    timeout(Duration::from_secs(3), started.notified())
        .await
        .unwrap();
    shutdown_tx.send(()).unwrap();
    tokio::task::yield_now().await;
    assert!(!task.is_finished());
    release.notify_one();
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a JetStream server at RUSTEE_NATS_URL"]
async fn registry_worker_dispatches_registered_jobs_and_dead_letters_unknown_jobs() {
    let fixture = Fixture::new("registry", 3).await;
    let registered_seen = Arc::new(Notify::new());
    let handler_seen = Arc::clone(&registered_seen);
    let mut registry = JobRegistry::new();
    registry
        .register::<ContractJob, _>(move |job: ContractJob, context: JobContext| {
            let seen = Arc::clone(&handler_seen);
            async move {
                assert_eq!(job.value, 13);
                assert_eq!(context.attempt(), 1);
                seen.notify_one();
                Ok::<_, std::convert::Infallible>(())
            }
        })
        .unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let worker = fixture.worker();
    let task = tokio::spawn(async move {
        worker
            .run_registry_until(
                registry,
                WorkerConfig::default(),
                RetryPolicy::default(),
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
    });

    fixture.enqueue(&envelope(13)).await;
    timeout(Duration::from_secs(3), registered_seen.notified())
        .await
        .unwrap();

    let unknown =
        JobEnvelope::with_metadata(JobId::new(), UnregisteredContractJob { value: 14 }, 123);
    let expected_unknown_payload = unknown.encode().unwrap();
    fixture.enqueue(&unknown).await;
    let dead_letters = fixture.dead_letter_consumer().await;
    let mut messages = dead_letters
        .fetch()
        .max_messages(1)
        .expires(Duration::from_secs(2))
        .messages()
        .await
        .unwrap();
    let message = timeout(Duration::from_secs(3), messages.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        message.payload.as_ref(),
        expected_unknown_payload.as_slice()
    );
    assert_eq!(
        message
            .headers
            .as_ref()
            .unwrap()
            .get_last("Rustee-Delivery-Attempt")
            .unwrap()
            .as_str(),
        "1"
    );
    message.ack().await.unwrap();

    shutdown_tx.send(()).unwrap();
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let info = fixture.consumer.get_info().await.unwrap();
    assert_eq!(info.num_ack_pending, 0);
    fixture.cleanup().await;
}
