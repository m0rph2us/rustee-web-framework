use std::time::Duration;

use lapin::{
    BasicProperties,
    message::Delivery,
    types::{AMQPValue, FieldTable},
};
use rustee_jobs::RetryPolicy;
use tokio::{sync::oneshot, task::JoinSet};

use crate::{
    ConfigError, RabbitMqConnectionConfig, RabbitMqDelivery, RabbitMqError,
    RabbitMqNativeRetryConfig, RabbitMqPublisherConfig, RabbitMqWorkerConfig,
    delivery::ACQUIRED_COUNT_HEADER,
    publisher::persistent_properties,
    worker::{bounded_readiness, drain_tasks, validate_readiness_timeout},
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
fn connection_configuration_redacts_connection_secrets() {
    let config = RabbitMqConnectionConfig::new("amqp://user:password@localhost:5672/%2f");
    assert!(!format!("{config:?}").contains("password"));
}

#[test]
fn connection_configuration_requires_a_non_zero_deadline() {
    let error = RabbitMqConnectionConfig::new("amqp://localhost:5672/%2f")
        .with_connect_timeout(Duration::ZERO)
        .unwrap_err();
    assert_eq!(error, ConfigError::ZeroDuration);
}

#[tokio::test]
async fn malformed_connection_url_fails_before_network_and_stays_redacted() {
    let config = RabbitMqConnectionConfig::new("not-amqp://private-user:private-password");
    let error = config.validate().unwrap_err();

    assert_eq!(error, RabbitMqError::InvalidConnectionConfig);
    assert!(!format!("{error:?} {error}").contains("private-password"));
    assert_eq!(
        config.connect().await.unwrap_err(),
        RabbitMqError::InvalidConnectionConfig
    );
}

#[test]
fn topology_configuration_debug_redacts_deployment_routing_values() {
    let publisher = RabbitMqPublisherConfig::new("tenant.acme.jobs", "payment.approved").unwrap();
    let worker = RabbitMqWorkerConfig::new(
        "tenant.acme.jobs.q",
        "worker.us-east-1a.17",
        RabbitMqNativeRetryConfig::new(Duration::from_secs(1), Duration::from_secs(3)).unwrap(),
        "tenant.acme.jobs.dlx",
        "payment.failed",
    )
    .unwrap();

    let debug = format!("{publisher:?}{worker:?}");
    for value in [
        "tenant.acme.jobs",
        "payment.approved",
        "tenant.acme.jobs.q",
        "worker.us-east-1a.17",
        "tenant.acme.jobs.dlx",
        "payment.failed",
    ] {
        assert!(!debug.contains(value));
    }
    assert!(debug.contains("[REDACTED]"));
    assert!(debug.contains("routing_key_length"));
    assert!(debug.contains("consumer_tag_length"));
    assert!(debug.contains("native_retry"));
}

#[test]
fn readiness_requires_a_non_zero_deadline() {
    assert_eq!(
        validate_readiness_timeout(Duration::ZERO),
        Err(RabbitMqError::InvalidReadinessTimeout)
    );
}

#[tokio::test]
async fn readiness_deadline_cancels_nonresponsive_work() {
    let deadline = Duration::from_millis(10);
    let started = tokio::time::Instant::now();
    let error = bounded_readiness(
        deadline,
        std::future::pending::<Result<(), RabbitMqError>>(),
    )
    .await
    .unwrap_err();

    assert_eq!(error, RabbitMqError::ReadinessTimeout);
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn native_retry_configuration_rejects_an_invalid_range() {
    let error = RabbitMqNativeRetryConfig::new(Duration::ZERO, Duration::from_secs(1)).unwrap_err();
    assert_eq!(error, ConfigError::InvalidRetryRange);

    let error = RabbitMqNativeRetryConfig::new(Duration::from_nanos(1), Duration::from_millis(1))
        .unwrap_err();
    assert_eq!(error, ConfigError::InvalidRetryRange);

    let error = RabbitMqNativeRetryConfig::new(
        Duration::from_millis(1) + Duration::from_nanos(1),
        Duration::from_millis(2),
    )
    .unwrap_err();
    assert_eq!(error, ConfigError::InvalidRetryRange);

    let error = RabbitMqNativeRetryConfig::new(
        Duration::from_millis(1),
        Duration::from_millis(2) + Duration::from_nanos(1),
    )
    .unwrap_err();
    assert_eq!(error, ConfigError::InvalidRetryRange);
}

#[test]
fn persistent_properties_do_not_invent_retry_headers() {
    let properties = persistent_properties("job-id");
    assert!(properties.headers().is_none());
    assert_eq!(
        properties
            .message_id()
            .as_ref()
            .map(lapin::types::ShortString::as_str),
        Some("job-id")
    );
}

#[test]
fn delivery_attempt_uses_the_broker_acquired_count() {
    let mut delivery = Delivery::mock(1, "".into(), "jobs.email".into(), false, vec![]);
    delivery.properties = BasicProperties::default();
    assert_eq!(
        RabbitMqDelivery::new(delivery).delivery_attempt().unwrap(),
        1
    );

    let mut headers = FieldTable::default();
    headers.insert(ACQUIRED_COUNT_HEADER.into(), AMQPValue::ShortUInt(2));
    let mut counted = Delivery::mock(2, "".into(), "jobs.email".into(), false, vec![]);
    counted.properties = BasicProperties::default().with_headers(headers);
    assert_eq!(
        RabbitMqDelivery::new(counted).delivery_attempt().unwrap(),
        2
    );

    let mut invalid_headers = FieldTable::default();
    invalid_headers.insert(
        ACQUIRED_COUNT_HEADER.into(),
        AMQPValue::LongString("two".into()),
    );
    let mut invalid = Delivery::mock(3, "".into(), "jobs.email".into(), false, vec![]);
    invalid.properties = BasicProperties::default().with_headers(invalid_headers);
    assert!(RabbitMqDelivery::new(invalid).delivery_attempt().is_err());
}

#[test]
fn native_linear_retry_is_accepted_only_when_it_matches_the_capped_core_policy() {
    let native =
        RabbitMqNativeRetryConfig::new(Duration::from_millis(10), Duration::from_millis(30))
            .unwrap();
    let compatible = RetryPolicy {
        max_deliveries: 5,
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(30),
    };
    assert!(native.matches(compatible));
    assert_eq!(native.delay_for(2), Duration::from_millis(10));
    assert_eq!(native.delay_for(3), Duration::from_millis(20));
    assert_eq!(native.delay_for(4), Duration::from_millis(30));

    let incompatible = RetryPolicy {
        max_backoff: Duration::from_millis(40),
        ..compatible
    };
    assert!(!native.matches(incompatible));
    assert!(!native.matches(RetryPolicy {
        max_deliveries: 0,
        ..compatible
    }));
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
        Ok::<(), RabbitMqError>(())
    });
    started
        .await
        .expect("the handler task must start before draining");

    assert_eq!(
        drain_tasks(&mut tasks, Duration::ZERO).await,
        Err(RabbitMqError::DrainTimeout)
    );
    assert!(tasks.is_empty());
    assert_eq!(finished.try_recv(), Ok(()));
}
