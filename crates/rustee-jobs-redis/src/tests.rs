use std::time::Duration;

use tokio::{sync::oneshot, task::JoinSet};

use rustee_jobs::RetryPolicy;

use crate::{
    ConfigError, RedisStreamsError, RedisStreamsWorkerConfig,
    worker::{drain_tasks, validate_retry_policy},
};

struct DropNotifier(Option<oneshot::Sender<()>>);

impl Drop for DropNotifier {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[test]
fn worker_configuration_scopes_retry_storage_and_rejects_unsafe_values() {
    let config = RedisStreamsWorkerConfig::new("jobs", "email", "worker-a", "jobs.dlq").unwrap();
    assert_eq!(
        config.retry_schedule_key(),
        "rustee:jobs:retry:v1:4:jobs:5:email:schedule"
    );
    assert_eq!(
        config.retry_payload_key(),
        "rustee:jobs:retry:v1:4:jobs:5:email:payload"
    );
    assert_eq!(
        config.retry_attempt_key(),
        "rustee:jobs:retry:v1:4:jobs:5:email:attempt"
    );
    let delimiter_in_stream = RedisStreamsWorkerConfig::new(
        "jobs:rustee:jobs:email",
        "worker-a",
        "consumer-left",
        "jobs:rustee:jobs:email:dlq",
    )
    .unwrap();
    let delimiter_in_group = RedisStreamsWorkerConfig::new(
        "jobs",
        "email:rustee:jobs:worker-a",
        "consumer-right",
        "jobs.dlq",
    )
    .unwrap();
    assert_ne!(
        delimiter_in_stream.retry_schedule_key(),
        delimiter_in_group.retry_schedule_key()
    );
    assert_ne!(
        delimiter_in_stream.retry_payload_key(),
        delimiter_in_group.retry_payload_key()
    );
    assert_ne!(
        delimiter_in_stream.retry_attempt_key(),
        delimiter_in_group.retry_attempt_key()
    );
    assert_eq!(
        RedisStreamsWorkerConfig::new("jobs", "workers", "worker-a", "jobs").unwrap_err(),
        ConfigError::DeadLetterMatchesSource
    );
    assert_eq!(
        config
            .clone()
            .with_reclaim_idle(Duration::ZERO)
            .unwrap_err(),
        ConfigError::ZeroDuration
    );
    assert_eq!(
        config
            .clone()
            .with_block_timeout(Duration::from_nanos(1))
            .unwrap_err(),
        ConfigError::DurationOutOfRange
    );
    assert_eq!(
        config
            .clone()
            .with_reclaim_idle(Duration::from_nanos(1))
            .unwrap_err(),
        ConfigError::DurationOutOfRange
    );
    assert_eq!(
        config
            .clone()
            .with_block_timeout(Duration::from_millis(1) + Duration::from_nanos(1))
            .unwrap_err(),
        ConfigError::DurationOutOfRange
    );
    assert_eq!(
        config
            .clone()
            .with_reclaim_idle(Duration::from_millis(1) + Duration::from_nanos(1))
            .unwrap_err(),
        ConfigError::DurationOutOfRange
    );
    assert_eq!(
        config
            .clone()
            .with_operation_timeout(Duration::ZERO)
            .unwrap_err(),
        ConfigError::ZeroDuration
    );
    assert_eq!(
        config
            .with_operation_timeout(Duration::from_millis(250))
            .unwrap_err(),
        ConfigError::OperationTimeoutNotLongerThanBlock
    );
}

#[test]
fn worker_configuration_debug_redacts_deployment_routing_values() {
    let config = RedisStreamsWorkerConfig::new(
        "tenant-acme:payments:jobs",
        "production-payments",
        "worker-us-east-1a-17",
        "tenant-acme:payments:jobs:dlq",
    )
    .unwrap();

    let debug = format!("{config:?}");
    for value in [
        "tenant-acme:payments:jobs",
        "production-payments",
        "worker-us-east-1a-17",
        "tenant-acme:payments:jobs:dlq",
        "retry:schedule",
        "retry:payload",
        "retry:attempt",
    ] {
        assert!(!debug.contains(value));
    }
    assert!(debug.contains("[REDACTED]"));
    assert!(debug.contains("stream_length"));
    assert!(debug.contains("retry_attempt_key_length"));
    assert!(debug.contains("operation_timeout"));
}

#[test]
fn retry_policy_rejects_an_empty_delivery_budget_before_readiness() {
    assert_eq!(
        validate_retry_policy(RetryPolicy {
            max_deliveries: 0,
            ..RetryPolicy::default()
        }),
        Err(RedisStreamsError::RetryPolicy)
    );
    assert_eq!(
        validate_retry_policy(RetryPolicy {
            initial_backoff: Duration::from_nanos(1),
            max_backoff: Duration::from_millis(1),
            ..RetryPolicy::default()
        }),
        Err(RedisStreamsError::RetryPolicy)
    );
    assert_eq!(
        validate_retry_policy(RetryPolicy {
            initial_backoff: Duration::from_millis(1) + Duration::from_nanos(1),
            max_backoff: Duration::from_millis(2),
            ..RetryPolicy::default()
        }),
        Err(RedisStreamsError::RetryPolicy)
    );
}

#[tokio::test]
async fn timed_out_drain_reaps_aborted_handler_tasks() {
    let mut tasks = JoinSet::new();
    let (started_sender, started) = oneshot::channel();
    let (finished_sender, mut finished) = oneshot::channel();
    tasks.spawn(async move {
        let _notifier = DropNotifier(Some(finished_sender));
        let _ = started_sender.send(());
        std::future::pending::<()>().await;
        Ok::<(), RedisStreamsError>(())
    });
    started
        .await
        .expect("the handler task must start before draining");

    assert_eq!(
        drain_tasks(&mut tasks, Duration::ZERO).await,
        Err(RedisStreamsError::DrainTimeout)
    );
    assert!(tasks.is_empty());
    assert_eq!(finished.try_recv(), Ok(()));
}
