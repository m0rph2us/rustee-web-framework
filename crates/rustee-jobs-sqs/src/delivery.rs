use std::{fmt, future::Future, time::Duration};

use aws_sdk_sqs::{
    Client,
    types::{Message, MessageSystemAttributeName},
};
use rustee_jobs::{DeliveryAction, JobDeliveryOutcome};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Instant, Interval, MissedTickBehavior, interval_at, timeout},
};

use crate::{
    SqsError, SqsWorkerConfig,
    config::duration_seconds,
    publisher::{MAX_MESSAGE_BYTES, send_payload},
};

/// A received `SQS` delivery without exposing provider receipt or message identifiers.
pub struct SqsDelivery {
    payload: Vec<u8>,
    receipt_handle: String,
    message_id: String,
    attempt: u16,
}

impl SqsDelivery {
    /// Returns the serialized Rustee envelope body.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Returns `SQS`'s one-based approximate receive count used as the provider attempt.
    #[must_use]
    pub const fn attempt(&self) -> u16 {
        self.attempt
    }

    pub(crate) fn from_message(message: &Message) -> Result<Self, SqsError> {
        let payload = message
            .body()
            .ok_or(SqsError::DeliveryMetadata)?
            .as_bytes()
            .to_vec();
        if payload.is_empty() || payload.len() > MAX_MESSAGE_BYTES {
            return Err(SqsError::DeliveryMetadata);
        }
        let receipt_handle = message
            .receipt_handle()
            .filter(|value| !value.is_empty())
            .ok_or(SqsError::DeliveryMetadata)?
            .to_owned();
        let message_id = message
            .message_id()
            .filter(|value| !value.is_empty())
            .ok_or(SqsError::DeliveryMetadata)?
            .to_owned();
        let attempt = message
            .attributes()
            .and_then(|attributes| {
                attributes.get(&MessageSystemAttributeName::ApproximateReceiveCount)
            })
            .ok_or(SqsError::DeliveryMetadata)?
            .parse::<u16>()
            .ok()
            .filter(|attempt| *attempt > 0)
            .ok_or(SqsError::DeliveryMetadata)?;
        Ok(Self {
            payload,
            receipt_handle,
            message_id,
            attempt,
        })
    }
}

impl fmt::Debug for SqsDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqsDelivery")
            .field("payload_bytes", &self.payload.len())
            .field("receipt_handle", &"[REDACTED]")
            .field("message_id", &"[REDACTED]")
            .field("attempt", &self.attempt)
            .finish()
    }
}

pub(crate) async fn run_with_lease<F>(
    client: &Client,
    config: &SqsWorkerConfig,
    delivery: &SqsDelivery,
    handler: F,
    timeout_action: DeliveryAction,
) -> Result<DeliveryAction, SqsError>
where
    F: Future<Output = Result<DeliveryAction, SqsError>> + Send,
{
    let (stop_tx, stop_rx) = watch::channel(false);
    let heartbeat_client = client.clone();
    let heartbeat_config = config.clone();
    let receipt_handle = delivery.receipt_handle.clone();
    let heartbeat = VisibilityHeartbeat::new(
        stop_tx,
        tokio::spawn(async move {
            renew_visibility(
                &heartbeat_client,
                &heartbeat_config,
                receipt_handle,
                stop_rx,
            )
            .await
        }),
    );
    let handler_result = timeout(config.handler_timeout(), handler).await;
    heartbeat.stop().await?;
    match handler_result {
        Ok(action) => action,
        Err(_) => Ok(timeout_action),
    }
}

struct VisibilityHeartbeat {
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<Result<(), SqsError>>>,
}

impl VisibilityHeartbeat {
    fn new(stop: watch::Sender<bool>, task: JoinHandle<Result<(), SqsError>>) -> Self {
        Self {
            stop,
            task: Some(task),
        }
    }

    async fn stop(mut self) -> Result<(), SqsError> {
        let _ = self.stop.send(true);
        let Some(task) = self.task.take() else {
            return Ok(());
        };
        task.await.map_err(|_| SqsError::WorkerTask)?
    }
}

impl Drop for VisibilityHeartbeat {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn renew_visibility(
    client: &Client,
    config: &SqsWorkerConfig,
    receipt_handle: String,
    mut stop: watch::Receiver<bool>,
) -> Result<(), SqsError> {
    let mut heartbeat = interval_at(
        Instant::now() + config.heartbeat_interval(),
        config.heartbeat_interval(),
    );
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        if stop_requested_or_wait_for_heartbeat(&mut stop, &mut heartbeat).await {
            return Ok(());
        }
        timeout(
            config.request_timeout(),
            client
                .change_message_visibility()
                .queue_url(config.source().queue_url())
                .receipt_handle(&receipt_handle)
                .visibility_timeout(
                    duration_seconds(config.visibility_timeout()).expect("validated"),
                )
                .send(),
        )
        .await
        .map_err(|_| SqsError::VisibilityLease)?
        .map_err(|_| SqsError::VisibilityLease)?;
    }
}

async fn stop_requested_or_wait_for_heartbeat(
    stop: &mut watch::Receiver<bool>,
    heartbeat: &mut Interval,
) -> bool {
    tokio::select! {
        // Do not start one final provider request once delivery settlement has stopped renewal.
        biased;
        changed = stop.changed() => changed.is_err() || *stop.borrow(),
        _ = heartbeat.tick() => false,
    }
}

pub(crate) async fn settle_delivery(
    client: &Client,
    config: &SqsWorkerConfig,
    delivery: &SqsDelivery,
    action: DeliveryAction,
) -> Result<(), SqsError> {
    match action {
        DeliveryAction::Acknowledge => {
            delete_delivery(
                client,
                config.source().queue_url(),
                &delivery.receipt_handle,
                config.request_timeout(),
            )
            .await
        }
        DeliveryAction::Retry { delay, .. } => timeout(
            config.request_timeout(),
            client
                .change_message_visibility()
                .queue_url(config.source().queue_url())
                .receipt_handle(&delivery.receipt_handle)
                .visibility_timeout(
                    duration_seconds(delay).map_err(|()| SqsError::RetryPolicyMismatch)?,
                )
                .send(),
        )
        .await
        .map_err(|_| SqsError::RetryVisibility)?
        .map(|_| ())
        .map_err(|_| SqsError::RetryVisibility),
        DeliveryAction::DeadLetter => {
            let payload = String::from_utf8(delivery.payload.clone())
                .map_err(|_| SqsError::InvalidMessageBody)?;
            send_payload(
                client,
                config.dead_letter(),
                payload,
                &delivery.message_id,
                config.request_timeout(),
            )
            .await
            .map_err(|()| SqsError::DeadLetterPublish)?;
            delete_delivery(
                client,
                config.source().queue_url(),
                &delivery.receipt_handle,
                config.request_timeout(),
            )
            .await
        }
    }
}

pub(crate) const fn outcome_for_action(action: DeliveryAction) -> JobDeliveryOutcome {
    match action {
        DeliveryAction::Acknowledge => JobDeliveryOutcome::Acknowledged,
        DeliveryAction::Retry { .. } => JobDeliveryOutcome::Retried,
        DeliveryAction::DeadLetter => JobDeliveryOutcome::DeadLettered,
    }
}

async fn delete_delivery(
    client: &Client,
    queue_url: &str,
    receipt_handle: &str,
    request_timeout: Duration,
) -> Result<(), SqsError> {
    timeout(
        request_timeout,
        client
            .delete_message()
            .queue_url(queue_url)
            .receipt_handle(receipt_handle)
            .send(),
    )
    .await
    .map_err(|_| SqsError::Delete)?
    .map(|_| ())
    .map_err(|_| SqsError::Delete)
}

#[cfg(test)]
mod tests {
    use tokio::{sync::oneshot, time::timeout};

    use super::{
        Duration, Instant, SqsError, VisibilityHeartbeat, interval_at,
        stop_requested_or_wait_for_heartbeat, watch,
    };

    struct DropNotifier(Option<oneshot::Sender<()>>);

    impl Drop for DropNotifier {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[tokio::test]
    async fn dropped_visibility_heartbeat_stops_and_aborts_renewal_task() {
        let (stop, mut stopped) = watch::channel(false);
        let (started_sender, started) = oneshot::channel();
        let (finished_sender, finished) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _notifier = DropNotifier(Some(finished_sender));
            let _ = started_sender.send(());
            std::future::pending::<()>().await;
            Ok::<(), SqsError>(())
        });
        started
            .await
            .expect("the renewal task must start before cancellation is tested");

        drop(VisibilityHeartbeat::new(stop, task));

        timeout(Duration::from_secs(1), stopped.changed())
            .await
            .expect("dropping the heartbeat must signal its renewal task")
            .expect("the heartbeat stop sender remains alive while it is dropped");
        assert!(*stopped.borrow());
        timeout(Duration::from_secs(1), finished)
            .await
            .expect("dropping the heartbeat must abort its renewal task")
            .expect("the aborted renewal task must release its resources");
    }

    #[tokio::test]
    async fn stop_signal_wins_over_a_due_visibility_heartbeat() {
        let (stop_sender, mut stop) = watch::channel(false);
        let _ = stop_sender.send(true);
        let mut heartbeat = interval_at(Instant::now(), Duration::from_secs(1));

        assert!(stop_requested_or_wait_for_heartbeat(&mut stop, &mut heartbeat).await);
    }
}
