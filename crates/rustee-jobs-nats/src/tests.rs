use std::time::Duration;

use tokio::{sync::oneshot, task::JoinSet};

use rustee_jobs::RetryPolicy;

use crate::{
    ConfigError, NatsConfig, NatsError,
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
fn configuration_debug_redacts_connection_and_routing_values() {
    let config = NatsConfig::new(
        "nats://user:password@localhost:4222",
        "tenant.acme.payment.jobs",
    )
    .unwrap();
    let debug = format!("{config:?}");

    assert!(!debug.contains("password"));
    assert!(!debug.contains("tenant.acme.payment.jobs"));
    assert!(debug.contains("[REDACTED]"));
    assert!(debug.contains("subject_length"));
}

#[test]
fn configuration_rejects_subscription_wildcards() {
    let error = NatsConfig::new("nats://localhost:4222", "jobs.>").unwrap_err();
    assert_eq!(error, ConfigError::InvalidSubject);
}

#[test]
fn configuration_rejects_invalid_server_urls_before_connecting() {
    let error = NatsConfig::new("https://private-nats.invalid", "jobs.email").unwrap_err();

    assert_eq!(error, ConfigError::InvalidServerUrl);
    assert!(!format!("{error:?} {error}").contains("private-nats.invalid"));
}

#[test]
fn configuration_requires_a_non_zero_connect_deadline() {
    let error = NatsConfig::new("nats://localhost:4222", "jobs.email")
        .unwrap()
        .with_connect_timeout(Duration::ZERO)
        .unwrap_err();
    assert_eq!(error, ConfigError::ZeroConnectTimeout);
}

#[test]
fn configuration_requires_a_non_zero_request_deadline() {
    let error = NatsConfig::new("nats://localhost:4222", "jobs.email")
        .unwrap()
        .with_request_timeout(Duration::ZERO)
        .unwrap_err();

    assert_eq!(error, ConfigError::ZeroRequestTimeout);

    let timeout = Duration::from_millis(250);
    let config = NatsConfig::new("nats://localhost:4222", "jobs.email")
        .unwrap()
        .with_request_timeout(timeout)
        .unwrap();
    assert_eq!(config.request_timeout(), timeout);
}

#[test]
fn retry_policy_rejects_an_empty_delivery_budget_before_connecting() {
    assert_eq!(
        validate_retry_policy(RetryPolicy {
            max_deliveries: 0,
            ..RetryPolicy::default()
        }),
        Err(NatsError::RetryPolicy)
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
        Ok::<(), NatsError>(())
    });
    started
        .await
        .expect("the handler task must start before draining");

    assert_eq!(
        drain_tasks(&mut tasks, Duration::ZERO).await,
        Err(NatsError::DrainTimeout)
    );
    assert!(tasks.is_empty());
    assert_eq!(finished.try_recv(), Ok(()));
}
