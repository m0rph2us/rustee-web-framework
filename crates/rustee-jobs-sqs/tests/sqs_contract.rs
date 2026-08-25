use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_sqs::{
    Client,
    types::{Message, MessageSystemAttributeName, QueueAttributeName},
};
use aws_smithy_http_client::Builder as HttpClientBuilder;
use aws_types::region::Region;
use rustee_jobs::{
    Job, JobContext, JobDeliveryOutcome, JobEnvelope, JobPublisher, JobRegistry, RetryPolicy,
    WorkerConfig,
};
use rustee_jobs_observability::JobMetrics;
use rustee_jobs_sqs::{
    SqsError, SqsPublisher, SqsQueueKind, SqsQueueTarget, SqsWorker, SqsWorkerConfig,
};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::watch,
    time::{Instant, sleep, timeout},
};

const TEST_TIMEOUT: Duration = Duration::from_secs(15);
static NEXT_QUEUE: AtomicUsize = AtomicUsize::new(1);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct WelcomeEmail {
    user_id: u64,
}

impl Job for WelcomeEmail {
    const NAME: &'static str = "email.welcome";
    const VERSION: u16 = 1;
}

#[derive(Clone)]
struct Topology {
    source: SqsQueueTarget,
    dead_letter: SqsQueueTarget,
}

#[tokio::test]
#[ignore = "requires LocalStack SQS at RUSTEE_LOCALSTACK_URL; CI provisions one"]
async fn successful_handler_deletes_the_source_receipt() {
    let client = localstack_client().await;
    let topology = create_topology(&client, SqsQueueKind::Standard).await;
    let publisher = SqsPublisher::new(client.clone(), topology.source.clone());
    publisher.readiness().await.unwrap();
    publisher
        .publish(
            JobEnvelope::new(WelcomeEmail { user_id: 7 })
                .message()
                .unwrap(),
        )
        .await
        .unwrap();

    let config = worker_config(topology);
    let metrics = JobMetrics::new();
    let worker =
        SqsWorker::new(client.clone(), config).with_delivery_observer(Arc::new(metrics.clone()));
    worker.readiness().await.unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handled = Arc::new(AtomicUsize::new(0));
    let handler_handled = handled.clone();
    timeout(
        TEST_TIMEOUT,
        worker.run_until::<WelcomeEmail, _, _>(
            move |_job: WelcomeEmail, context: JobContext| {
                let shutdown_tx = shutdown_tx.clone();
                let handled = handler_handled.clone();
                async move {
                    assert_eq!(context.attempt(), 1);
                    handled.fetch_add(1, Ordering::SeqCst);
                    let _ = shutdown_tx.send(true);
                    Ok::<(), Infallible>(())
                }
            },
            RetryPolicy::default(),
            one_worker(),
            shutdown_when_set(shutdown_rx),
        ),
    )
    .await
    .expect("worker should stop")
    .unwrap();
    assert_eq!(handled.load(Ordering::SeqCst), 1);
    assert_eq!(
        metrics
            .snapshot()
            .outcome("amazon_sqs", JobDeliveryOutcome::Acknowledged),
        1
    );
    assert!(
        receive_one(&client, worker.config().source().queue_url())
            .await
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires LocalStack SQS at RUSTEE_LOCALSTACK_URL; CI provisions one"]
async fn handler_failure_uses_visibility_retry_then_direct_dead_letter() {
    let client = localstack_client().await;
    let topology = create_topology(&client, SqsQueueKind::Standard).await;
    let publisher = SqsPublisher::new(client.clone(), topology.source.clone());
    publisher
        .publish(
            JobEnvelope::new(WelcomeEmail { user_id: 11 })
                .message()
                .unwrap(),
        )
        .await
        .unwrap();

    let expected_dead_letter = topology.dead_letter.queue_url().to_owned();
    let metrics = JobMetrics::new();
    let worker = SqsWorker::new(client.clone(), worker_config(topology))
        .with_delivery_observer(Arc::new(metrics.clone()));
    worker.readiness().await.unwrap();
    let retry_policy = RetryPolicy {
        max_deliveries: 2,
        initial_backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(1),
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let attempts = Arc::new(AtomicUsize::new(0));
    let handler_attempts = attempts.clone();
    timeout(
        TEST_TIMEOUT,
        worker.run_until::<WelcomeEmail, _, _>(
            move |_job: WelcomeEmail, context: JobContext| {
                let shutdown_tx = shutdown_tx.clone();
                let attempts = handler_attempts.clone();
                async move {
                    let current = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    assert_eq!(context.attempt(), u16::try_from(current).unwrap());
                    if current == 2 {
                        let _ = shutdown_tx.send(true);
                    }
                    Err::<(), _>(std::io::Error::other("expected contract failure"))
                }
            },
            retry_policy,
            one_worker(),
            shutdown_when_set(shutdown_rx),
        ),
    )
    .await
    .expect("worker should stop")
    .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    let snapshot = metrics.snapshot();
    assert_eq!(
        snapshot.outcome("amazon_sqs", JobDeliveryOutcome::Retried),
        1
    );
    assert_eq!(
        snapshot.outcome("amazon_sqs", JobDeliveryOutcome::DeadLettered),
        1
    );
    let dead_letter = wait_for_message(&client, &expected_dead_letter).await;
    assert!(dead_letter.body().unwrap().contains("email.welcome"));
    assert!(
        receive_one(&client, worker.config().source().queue_url())
            .await
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires LocalStack SQS at RUSTEE_LOCALSTACK_URL; CI provisions one"]
async fn unknown_registry_job_is_sent_directly_to_the_dead_letter_queue() {
    let client = localstack_client().await;
    let topology = create_topology(&client, SqsQueueKind::Standard).await;
    let publisher = SqsPublisher::new(client.clone(), topology.source.clone());
    publisher
        .publish(
            JobEnvelope::new(WelcomeEmail { user_id: 19 })
                .message()
                .unwrap(),
        )
        .await
        .unwrap();

    let dead_letter_url = topology.dead_letter.queue_url().to_owned();
    let worker = SqsWorker::new(client.clone(), worker_config(topology));
    worker.readiness().await.unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn({
        let worker = worker.clone();
        async move {
            worker
                .run_registry_until(
                    JobRegistry::new(),
                    RetryPolicy::default(),
                    one_worker(),
                    shutdown_when_set(shutdown_rx),
                )
                .await
        }
    });
    let dead_letter = wait_for_message(&client, &dead_letter_url).await;
    assert!(dead_letter.body().unwrap().contains("email.welcome"));
    let _ = shutdown_tx.send(true);
    timeout(TEST_TIMEOUT, task)
        .await
        .expect("registry worker should stop")
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = "requires LocalStack SQS at RUSTEE_LOCALSTACK_URL; CI provisions one"]
async fn fifo_publisher_sets_the_message_group_and_deduplication_id() {
    let client = localstack_client().await;
    let topology = create_topology(&client, SqsQueueKind::fifo("account-7").unwrap()).await;
    let publisher = SqsPublisher::new(client.clone(), topology.source.clone());
    publisher.readiness().await.unwrap();
    publisher
        .publish(
            JobEnvelope::new(WelcomeEmail { user_id: 23 })
                .message()
                .unwrap(),
        )
        .await
        .unwrap();

    let message = wait_for_message_with_fifo_attributes(&client, topology.source.queue_url()).await;
    let attributes = message.attributes().unwrap();
    assert_eq!(
        attributes.get(&MessageSystemAttributeName::MessageGroupId),
        Some(&"account-7".to_owned())
    );
    assert!(
        attributes
            .get(&MessageSystemAttributeName::MessageDeduplicationId)
            .is_some_and(|value| !value.is_empty())
    );
}

#[tokio::test]
#[ignore = "requires a stopped LocalStack SQS endpoint; CI verifies the bounded outage contract"]
async fn stopped_localstack_fails_within_the_request_deadline_without_endpoint_details() {
    if std::env::var("RUSTEE_SQS_EXPECT_OUTAGE").as_deref() != Ok("1") {
        return;
    }
    let endpoint = localstack_endpoint();
    let target = SqsQueueTarget::new(
        format!("{endpoint}/000000000000/rustee-jobs-sqs-outage"),
        SqsQueueKind::Standard,
    )
    .unwrap();
    let publisher = SqsPublisher::new(localstack_client().await, target)
        .with_request_timeout(Duration::from_millis(500))
        .unwrap();
    let started = Instant::now();
    let error = publisher.readiness().await.unwrap_err();
    assert_eq!(error, SqsError::Readiness);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "stopped LocalStack request exceeded the adapter deadline"
    );
    let detail = format!("{error:?}");
    assert!(!detail.contains("127.0.0.1"));
    assert!(!detail.contains("4566"));
}

async fn localstack_client() -> Client {
    let endpoint = localstack_endpoint();
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("test", "test", None, None, "localstack"))
        .http_client(HttpClientBuilder::new().build_http())
        .endpoint_url(endpoint)
        .load()
        .await;
    Client::new(&config)
}

fn localstack_endpoint() -> String {
    std::env::var("RUSTEE_LOCALSTACK_URL")
        .expect("RUSTEE_LOCALSTACK_URL must point at the LocalStack edge endpoint")
}

async fn create_topology(client: &Client, kind: SqsQueueKind) -> Topology {
    let suffix = NEXT_QUEUE.fetch_add(1, Ordering::SeqCst);
    let prefix = format!("rustee-jobs-sqs-{}-{suffix}", std::process::id());
    let dead_letter_name = queue_name(&prefix, "dlq", &kind);
    let source_name = queue_name(&prefix, "source", &kind);
    let dead_letter_url = create_queue(client, &dead_letter_name, &kind, None).await;
    let dead_letter_arn = queue_arn(client, &dead_letter_url).await;
    let redrive = serde_json::json!({
        "deadLetterTargetArn": dead_letter_arn,
        "maxReceiveCount": "5",
    })
    .to_string();
    let source_url = create_queue(client, &source_name, &kind, Some(redrive)).await;
    Topology {
        source: SqsQueueTarget::new(source_url, kind.clone()).unwrap(),
        dead_letter: SqsQueueTarget::new(dead_letter_url, kind).unwrap(),
    }
}

async fn create_queue(
    client: &Client,
    name: &str,
    kind: &SqsQueueKind,
    redrive: Option<String>,
) -> String {
    let mut request = client.create_queue().queue_name(name);
    if kind.is_fifo() {
        request = request.attributes(QueueAttributeName::FifoQueue, "true");
    }
    if let Some(redrive) = redrive {
        request = request.attributes(QueueAttributeName::RedrivePolicy, redrive);
    }
    request
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_owned()
}

async fn queue_arn(client: &Client, queue_url: &str) -> String {
    client
        .get_queue_attributes()
        .queue_url(queue_url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap()
        .attributes()
        .unwrap()
        .get(&QueueAttributeName::QueueArn)
        .unwrap()
        .to_owned()
}

fn worker_config(topology: Topology) -> SqsWorkerConfig {
    SqsWorkerConfig::new(topology.source, topology.dead_letter, 5)
        .unwrap()
        .with_long_poll(Duration::from_secs(1))
        .unwrap()
        .with_request_timeout(Duration::from_secs(2))
        .unwrap()
        .with_heartbeat_interval(Duration::from_secs(1))
        .unwrap()
        .with_visibility_timeout(Duration::from_secs(4))
        .unwrap()
}

fn one_worker() -> WorkerConfig {
    WorkerConfig {
        concurrency: std::num::NonZeroUsize::new(1).unwrap(),
        drain_timeout: Duration::from_secs(5),
    }
}

async fn shutdown_when_set(mut receiver: watch::Receiver<bool>) {
    loop {
        if *receiver.borrow() {
            return;
        }
        if receiver.changed().await.is_err() {
            return;
        }
    }
}

async fn wait_for_message(client: &Client, queue_url: &str) -> Message {
    wait_for_message_with_attributes(client, queue_url, false).await
}

async fn wait_for_message_with_fifo_attributes(client: &Client, queue_url: &str) -> Message {
    wait_for_message_with_attributes(client, queue_url, true).await
}

async fn wait_for_message_with_attributes(
    client: &Client,
    queue_url: &str,
    fifo_attributes: bool,
) -> Message {
    let deadline = Instant::now() + TEST_TIMEOUT;
    loop {
        let mut request = client
            .receive_message()
            .queue_url(queue_url)
            .max_number_of_messages(1)
            .wait_time_seconds(1);
        if fifo_attributes {
            request = request
                .message_system_attribute_names(MessageSystemAttributeName::MessageGroupId)
                .message_system_attribute_names(MessageSystemAttributeName::MessageDeduplicationId);
        }
        let received = request.send().await.unwrap();
        if let Some(message) = received.messages().first() {
            return message.clone();
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for SQS delivery"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

async fn receive_one(client: &Client, queue_url: &str) -> Option<Message> {
    client
        .receive_message()
        .queue_url(queue_url)
        .max_number_of_messages(1)
        .wait_time_seconds(1)
        .send()
        .await
        .unwrap()
        .messages()
        .first()
        .cloned()
}

fn queue_name(prefix: &str, suffix: &str, kind: &SqsQueueKind) -> String {
    match kind {
        SqsQueueKind::Standard => format!("{prefix}-{suffix}"),
        SqsQueueKind::Fifo { .. } => format!("{prefix}-{suffix}.fifo"),
    }
}
