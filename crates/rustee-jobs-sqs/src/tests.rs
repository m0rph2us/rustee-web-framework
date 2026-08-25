use std::time::Duration;

use aws_sdk_sqs::{
    Client,
    types::{Message, MessageSystemAttributeName},
};
use aws_smithy_http_client::Builder as HttpClientBuilder;
use rustee_jobs::RetryPolicy;
use tokio::{sync::oneshot, task::JoinSet};

use crate::{
    ConfigError, SqsDelivery, SqsError, SqsPublisher, SqsQueueKind, SqsQueueTarget,
    SqsWorkerConfig,
    worker::{SaturatedWorkerWait, validate_retry_policy, wait_for_task_or_shutdown},
};

#[test]
fn queue_target_rejects_non_queue_urls_embedded_credentials_and_invalid_fifo_groups() {
    assert_eq!(
        SqsQueueTarget::new(
            "https://key:secret@sqs.us-east-1.amazonaws.com/123/jobs",
            SqsQueueKind::Standard,
        )
        .unwrap_err(),
        ConfigError::InvalidQueueUrl
    );
    for queue_url in [
        "https://sqs.us-east-1.amazonaws.com",
        "https://sqs.us-east-1.amazonaws.com/123/jobs?temporary-route=true",
        "https://sqs.us-east-1.amazonaws.com/123/jobs#temporary-route",
    ] {
        assert_eq!(
            SqsQueueTarget::new(queue_url, SqsQueueKind::Standard).unwrap_err(),
            ConfigError::InvalidQueueUrl
        );
    }
    assert_eq!(
        SqsQueueKind::fifo("not allowed space").unwrap_err(),
        ConfigError::InvalidFifoMessageGroup
    );
}

#[test]
fn queue_configuration_debug_redacts_deployment_routing_values() {
    let source = SqsQueueTarget::new(
        "https://sqs.ap-northeast-2.amazonaws.com/012345678901/payments.fifo",
        SqsQueueKind::fifo("tenant/acme-payments").unwrap(),
    )
    .unwrap();
    let dead_letter = SqsQueueTarget::new(
        "https://sqs.ap-northeast-2.amazonaws.com/012345678901/payments-dlq.fifo",
        SqsQueueKind::fifo("tenant/acme-payments").unwrap(),
    )
    .unwrap();

    let target_debug = format!("{source:?}");
    let worker_debug = format!(
        "{:?}",
        SqsWorkerConfig::new(source, dead_letter, 5).unwrap()
    );
    for debug in [target_debug, worker_debug] {
        assert!(debug.contains("[REDACTED]"));
        assert!(debug.contains("Fifo"));
        assert!(!debug.contains("012345678901"));
        assert!(!debug.contains("tenant/acme-payments"));
    }
}

#[test]
fn worker_requires_matching_source_and_dead_letter_queue_kinds() {
    let source =
        SqsQueueTarget::new("http://localhost:4566/000/jobs", SqsQueueKind::Standard).unwrap();
    let dead_letter = SqsQueueTarget::new(
        "http://localhost:4566/000/jobs-dlq.fifo",
        SqsQueueKind::fifo("jobs").unwrap(),
    )
    .unwrap();
    assert_eq!(
        SqsWorkerConfig::new(source, dead_letter, 5).unwrap_err(),
        ConfigError::QueueKindMismatch
    );
}

#[test]
fn worker_rejects_unsafe_lease_configuration() {
    let source =
        SqsQueueTarget::new("http://localhost:4566/000/jobs", SqsQueueKind::Standard).unwrap();
    let dead_letter =
        SqsQueueTarget::new("http://localhost:4566/000/jobs-dlq", SqsQueueKind::Standard).unwrap();
    let config = SqsWorkerConfig::new(source, dead_letter, 5).unwrap();
    assert_eq!(
        config
            .clone()
            .with_heartbeat_interval(Duration::from_mins(2))
            .unwrap_err(),
        ConfigError::InvalidHeartbeatInterval
    );
    assert_eq!(
        config
            .clone()
            .with_heartbeat_interval(Duration::from_secs(95))
            .unwrap_err(),
        ConfigError::InvalidHeartbeatInterval
    );
    assert_eq!(
        config
            .clone()
            .with_request_timeout(Duration::ZERO)
            .unwrap_err(),
        ConfigError::ZeroRequestTimeout
    );
    assert_eq!(
        config
            .clone()
            .with_request_timeout(Duration::from_secs(20))
            .unwrap_err(),
        ConfigError::RequestTimeoutNotLongerThanLongPoll
    );
    assert_eq!(
        config
            .clone()
            .with_handler_timeout(Duration::from_secs(43_055))
            .unwrap_err(),
        ConfigError::InvalidHandlerTimeout
    );
    assert!(
        config
            .clone()
            .with_handler_timeout(Duration::from_secs(43_054))
            .is_ok()
    );
    assert_eq!(
        config
            .with_long_poll(Duration::from_millis(500))
            .unwrap_err(),
        ConfigError::InvalidLongPoll
    );
}

#[test]
fn publisher_request_timeout_cannot_be_zero() {
    let client = Client::from_conf(
        aws_sdk_sqs::Config::builder()
            .http_client(HttpClientBuilder::new().build_http())
            .build(),
    );
    let target =
        SqsQueueTarget::new("http://localhost:4566/000/jobs", SqsQueueKind::Standard).unwrap();
    assert_eq!(
        SqsPublisher::new(client, target)
            .with_request_timeout(Duration::ZERO)
            .unwrap_err(),
        ConfigError::ZeroRequestTimeout
    );
}

#[test]
fn delivery_uses_approximate_receive_count_and_redacts_provider_identifiers() {
    let message = Message::builder()
        .body("{\"name\":\"email.welcome\"}")
        .message_id("message-1")
        .receipt_handle("secret-receipt")
        .attributes(MessageSystemAttributeName::ApproximateReceiveCount, "3")
        .build();
    let delivery = SqsDelivery::from_message(&message).unwrap();
    assert_eq!(delivery.attempt(), 3);
    assert_eq!(delivery.payload(), br#"{"name":"email.welcome"}"#);
    let debug = format!("{delivery:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("secret-receipt"));
    assert!(!debug.contains("message-1"));
    assert!(!debug.contains("email.welcome"));
}

#[test]
fn delivery_rejects_missing_or_invalid_provider_metadata() {
    for (body, receipt_handle, message_id, attempt) in [
        (None, Some("receipt"), Some("message"), Some("1")),
        (Some(""), Some("receipt"), Some("message"), Some("1")),
        (Some("payload"), None, Some("message"), Some("1")),
        (Some("payload"), Some(""), Some("message"), Some("1")),
        (Some("payload"), Some("receipt"), None, Some("1")),
        (Some("payload"), Some("receipt"), Some(""), Some("1")),
        (Some("payload"), Some("receipt"), Some("message"), None),
        (Some("payload"), Some("receipt"), Some("message"), Some("0")),
        (
            Some("payload"),
            Some("receipt"),
            Some("message"),
            Some("not-a-number"),
        ),
        (
            Some("payload"),
            Some("receipt"),
            Some("message"),
            Some("65536"),
        ),
    ] {
        assert_eq!(
            SqsDelivery::from_message(&message_with_metadata(
                body,
                receipt_handle,
                message_id,
                attempt,
            ))
            .unwrap_err(),
            SqsError::DeliveryMetadata
        );
    }
    assert_eq!(
        SqsDelivery::from_message(&message_with_metadata(
            Some(&"a".repeat(1_048_577)),
            Some("receipt"),
            Some("message"),
            Some("1"),
        ))
        .unwrap_err(),
        SqsError::DeliveryMetadata
    );
}

fn message_with_metadata(
    body: Option<&str>,
    receipt_handle: Option<&str>,
    message_id: Option<&str>,
    attempt: Option<&str>,
) -> Message {
    let mut message = Message::builder();
    if let Some(body) = body {
        message = message.body(body);
    }
    if let Some(receipt_handle) = receipt_handle {
        message = message.receipt_handle(receipt_handle);
    }
    if let Some(message_id) = message_id {
        message = message.message_id(message_id);
    }
    if let Some(attempt) = attempt {
        message = message.attributes(MessageSystemAttributeName::ApproximateReceiveCount, attempt);
    }
    message.build()
}

#[test]
fn retry_policy_requires_whole_seconds_within_redrive_budget() {
    let valid = RetryPolicy::default();
    assert_eq!(validate_retry_policy(valid, 5), Ok(()));
    assert_eq!(
        validate_retry_policy(
            RetryPolicy {
                initial_backoff: Duration::from_millis(1_500),
                ..valid
            },
            5,
        ),
        Err(SqsError::RetryPolicyMismatch)
    );
    assert_eq!(
        validate_retry_policy(
            RetryPolicy {
                max_deliveries: 6,
                ..valid
            },
            5,
        ),
        Err(SqsError::RetryPolicyMismatch)
    );
    assert_eq!(
        validate_retry_policy(
            RetryPolicy {
                initial_backoff: Duration::from_secs(2),
                max_backoff: Duration::from_secs(1),
                ..valid
            },
            5,
        ),
        Err(SqsError::RetryPolicyMismatch)
    );
}

#[tokio::test]
async fn saturated_worker_observes_shutdown_without_waiting_for_a_task() {
    let mut tasks = JoinSet::new();
    tasks.spawn(async { std::future::pending::<Result<(), SqsError>>().await });
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let mut shutdown = Box::pin(async move {
        let _ = shutdown_receiver.await;
    });
    shutdown_sender.send(()).unwrap();

    assert_eq!(
        wait_for_task_or_shutdown(&mut tasks, &mut shutdown)
            .await
            .unwrap(),
        SaturatedWorkerWait::Shutdown
    );
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}
