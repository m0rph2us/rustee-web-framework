//! Opt-in Redis Streams durable-job contracts. Run with Redis 7 at `RUSTEE_REDIS_URL`.

use std::{
    convert::Infallible,
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use rustee_jobs::{
    Job, JobClient, JobContext, JobDeliveryOutcome, JobEnvelope, JobId, JobRegistry, RetryPolicy,
    WorkerConfig,
};
use rustee_jobs_observability::JobMetrics;
use rustee_jobs_redis::{
    RedisStreamsError, RedisStreamsPublisher, RedisStreamsWorker, RedisStreamsWorkerConfig, redis,
    redis::AsyncCommands,
};
use rustee_redis::{RedisConfig, connect};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::Notify,
    time::{Instant, timeout},
};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ContractJob {
    value: u64,
}

impl Job for ContractJob {
    const NAME: &'static str = "rustee.redis.contract.job";
    const VERSION: u16 = 1;
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct UnknownContractJob {
    value: u64,
}

impl Job for UnknownContractJob {
    const NAME: &'static str = "rustee.redis.contract.unknown";
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
    connection: redis::aio::ConnectionManager,
    stream: String,
    group: String,
    dead_letter_stream: String,
}

impl Fixture {
    async fn new(label: &str) -> Self {
        let connection = connect(&RedisConfig::new(redis_url())).await.unwrap();
        let suffix = Uuid::new_v4();
        let stream = format!("rustee:test:jobs:{label}:{suffix}");
        let group = format!("workers-{suffix}");
        let dead_letter_stream = format!("rustee:test:jobs:dlq:{label}:{suffix}");
        let mut setup = connection.clone();
        create_stream(&mut setup, &stream).await;
        create_stream(&mut setup, &dead_letter_stream).await;
        redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&stream)
            .arg(&group)
            .arg("$")
            .query_async::<()>(&mut setup)
            .await
            .unwrap();
        Self {
            connection,
            stream,
            group,
            dead_letter_stream,
        }
    }

    fn publisher(&self) -> RedisStreamsPublisher {
        RedisStreamsPublisher::new(self.connection.clone(), self.stream.clone()).unwrap()
    }

    fn worker(&self, consumer: &str) -> RedisStreamsWorker {
        let config = RedisStreamsWorkerConfig::new(
            self.stream.clone(),
            self.group.clone(),
            consumer,
            self.dead_letter_stream.clone(),
        )
        .unwrap()
        .with_block_timeout(Duration::from_millis(10))
        .unwrap()
        .with_reclaim_interval(Duration::from_millis(10))
        .unwrap()
        .with_reclaim_idle(Duration::from_millis(25))
        .unwrap();
        RedisStreamsWorker::new(self.connection.clone(), config)
    }

    async fn enqueue<J>(&self, envelope: &JobEnvelope<J>)
    where
        J: Job,
    {
        JobClient::new(self.publisher())
            .enqueue(envelope)
            .await
            .unwrap();
    }

    async fn dead_letters(&self) -> redis::streams::StreamRangeReply {
        let mut connection = self.connection.clone();
        connection
            .xrange_all(&self.dead_letter_stream)
            .await
            .unwrap()
    }

    async fn cleanup(&self) {
        let config = RedisStreamsWorkerConfig::new(
            self.stream.clone(),
            self.group.clone(),
            "cleanup",
            self.dead_letter_stream.clone(),
        )
        .unwrap();
        let mut connection = self.connection.clone();
        redis::cmd("DEL")
            .arg(&self.stream)
            .arg(&self.dead_letter_stream)
            .arg(config.retry_schedule_key())
            .arg(config.retry_payload_key())
            .arg(config.retry_attempt_key())
            .query_async::<usize>(&mut connection)
            .await
            .unwrap();
    }
}

fn redis_url() -> String {
    std::env::var("RUSTEE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/15".to_owned())
}

fn envelope(value: u64) -> JobEnvelope<ContractJob> {
    JobEnvelope::with_metadata(JobId::new(), ContractJob { value }, 123)
}

async fn create_stream(connection: &mut redis::aio::ConnectionManager, stream: &str) {
    redis::cmd("XADD")
        .arg(stream)
        .arg("*")
        .arg("seed")
        .arg("0")
        .query_async::<String>(connection)
        .await
        .unwrap();
}

async fn wait_for_dead_letter(fixture: &Fixture) -> redis::streams::StreamId {
    timeout(Duration::from_secs(3), async {
        loop {
            let reply = fixture.dead_letters().await;
            if let Some(entry) = reply.ids.into_iter().nth(1) {
                return entry;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires a Redis 7 server at RUSTEE_REDIS_URL"]
async fn successful_handler_acknowledges_the_consumer_group_delivery() {
    let fixture = Fixture::new("success").await;
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
        .worker("success-worker")
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
            .outcome("redis_streams", JobDeliveryOutcome::Acknowledged),
        1
    );
    let mut connection = fixture.connection.clone();
    let pending: redis::streams::StreamPendingReply = connection
        .xpending(&fixture.stream, &fixture.group)
        .await
        .unwrap();
    assert_eq!(pending.count(), 0);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a Redis 7 server at RUSTEE_REDIS_URL"]
async fn failed_handler_retries_after_delay_then_routes_to_the_dead_letter_stream() {
    let fixture = Fixture::new("retry").await;
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
        .worker("retry-worker")
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
    let expected_payload = expected.encode().unwrap();
    fixture.enqueue(&expected).await;
    timeout(Duration::from_secs(3), second_attempt.notified())
        .await
        .unwrap();
    let dead_letter = wait_for_dead_letter(&fixture).await;
    assert_eq!(
        dead_letter.get::<Vec<u8>>("payload").unwrap(),
        expected_payload
    );
    assert_eq!(dead_letter.get::<u16>("attempt"), Some(2));

    shutdown_tx.send(()).unwrap();
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(*attempts.lock().unwrap(), vec![1, 2]);
    let snapshot = metrics.snapshot();
    assert_eq!(
        snapshot.outcome("redis_streams", JobDeliveryOutcome::Retried),
        1
    );
    assert_eq!(
        snapshot.outcome("redis_streams", JobDeliveryOutcome::DeadLettered),
        1
    );
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a Redis 7 server at RUSTEE_REDIS_URL"]
async fn registry_worker_reclaims_an_abandoned_pending_delivery_with_a_new_attempt() {
    let fixture = Fixture::new("reclaim").await;
    let expected = envelope(11);
    fixture.enqueue(&expected).await;
    let mut abandoned = fixture.connection.clone();
    let options = redis::streams::StreamReadOptions::default()
        .group(&fixture.group, "abandoned-worker")
        .count(1);
    let reply: redis::streams::StreamReadReply = abandoned
        .xread_options(&[fixture.stream.as_str()], &[">"], &options)
        .await
        .unwrap();
    assert_eq!(reply.keys[0].ids.len(), 1);

    let observed = Arc::new(Notify::new());
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let mut registry = JobRegistry::new();
    registry
        .register::<ContractJob, _>({
            let observed = Arc::clone(&observed);
            let attempts = Arc::clone(&attempts);
            move |job: ContractJob, context: JobContext| {
                let observed = Arc::clone(&observed);
                let attempts = Arc::clone(&attempts);
                async move {
                    assert_eq!(job.value, 11);
                    attempts.lock().unwrap().push(context.attempt());
                    observed.notify_one();
                    Ok::<_, Infallible>(())
                }
            }
        })
        .unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let worker = fixture.worker("reclaim-worker");
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

    timeout(Duration::from_secs(3), observed.notified())
        .await
        .unwrap();
    shutdown_tx.send(()).unwrap();
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(*attempts.lock().unwrap(), vec![2]);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a Redis 7 server at RUSTEE_REDIS_URL"]
async fn registry_worker_dead_letters_an_unknown_job_without_retrying_it() {
    let fixture = Fixture::new("registry-dlq").await;
    let mut registry = JobRegistry::new();
    registry
        .register::<ContractJob, _>(|_job: ContractJob, _context: JobContext| async {
            Ok::<_, Infallible>(())
        })
        .unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let worker = fixture.worker("registry-worker");
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

    let unknown = JobEnvelope::with_metadata(JobId::new(), UnknownContractJob { value: 13 }, 123);
    let expected_payload = unknown.encode().unwrap();
    fixture.enqueue(&unknown).await;
    let dead_letter = wait_for_dead_letter(&fixture).await;
    assert_eq!(
        dead_letter.get::<Vec<u8>>("payload").unwrap(),
        expected_payload
    );
    assert_eq!(dead_letter.get::<u16>("attempt"), Some(1));

    shutdown_tx.send(()).unwrap();
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires Redis 7 CLIENT PAUSE permission; CI verifies the bounded command deadline"]
async fn paused_redis_fails_within_the_operation_deadline_without_endpoint_details() {
    if std::env::var("RUSTEE_REDIS_JOBS_EXPECT_PAUSE").as_deref() != Ok("1") {
        return;
    }
    let fixture = Fixture::new("operation-deadline").await;
    let publisher = fixture
        .publisher()
        .with_operation_timeout(Duration::from_millis(500))
        .unwrap();
    let mut admin = fixture.connection.clone();
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(1_500)
        .arg("ALL")
        .query_async::<()>(&mut admin)
        .await
        .unwrap();

    let started = Instant::now();
    let error = publisher.readiness().await.unwrap_err();
    assert_eq!(error, RedisStreamsError::Readiness);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "paused Redis operation exceeded the adapter deadline"
    );
    let detail = format!("{error:?}");
    assert!(!detail.contains("127.0.0.1"));
    assert!(!detail.contains("6379"));

    let mut connection = fixture.connection.clone();
    let response = timeout(
        Duration::from_secs(3),
        redis::cmd("PING").query_async::<String>(&mut connection),
    )
    .await
    .expect("Redis should resume before fixture cleanup")
    .unwrap();
    assert_eq!(response, "PONG");
    fixture.cleanup().await;
}
