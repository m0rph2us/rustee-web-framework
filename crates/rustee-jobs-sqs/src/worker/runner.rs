//! Bounded SQS long-poll task supervision and graceful draining.

use std::{future::Future, num::NonZeroU16, pin::Pin, sync::Arc, time::Duration};

use aws_sdk_sqs::{
    Client,
    types::{Message, MessageSystemAttributeName},
};
use futures_util::future::BoxFuture;
use rustee_jobs::{
    JobDeliveryObservation, JobDeliveryObserver, JobDeliveryOutcome, RetryPolicy, WorkerConfig,
};
use tokio::{task::JoinSet, time::timeout};

use crate::{
    SqsError, SqsWorkerConfig,
    config::{MAX_VISIBILITY_SECONDS, duration_seconds, validate_whole_seconds},
};

pub(super) async fn run<Shutdown, Processor>(
    client: &Client,
    config: &SqsWorkerConfig,
    observer: Arc<dyn JobDeliveryObserver>,
    worker_config: WorkerConfig,
    shutdown: Shutdown,
    processor: Processor,
) -> Result<(), SqsError>
where
    Shutdown: Future<Output = ()> + Send,
    Processor: Fn(Message) -> BoxFuture<'static, Result<(u16, JobDeliveryOutcome), SqsError>>
        + Clone
        + Send
        + Sync
        + 'static,
{
    let mut shutdown = Box::pin(shutdown);
    let mut tasks = JoinSet::new();
    let run_result = loop {
        let available = worker_config.concurrency.get().saturating_sub(tasks.len());
        if available == 0 {
            match wait_for_task_or_shutdown(&mut tasks, &mut shutdown).await? {
                SaturatedWorkerWait::Shutdown => break Ok(()),
                SaturatedWorkerWait::CapacityAvailable => continue,
            }
        }
        let max_number_of_messages = i32::try_from(available.min(10)).expect("bounded at 10");
        let receive = client
            .receive_message()
            .queue_url(config.source().queue_url())
            .max_number_of_messages(max_number_of_messages)
            .wait_time_seconds(duration_seconds(config.long_poll()).expect("validated"))
            .visibility_timeout(duration_seconds(config.visibility_timeout()).expect("validated"))
            .message_system_attribute_names(MessageSystemAttributeName::ApproximateReceiveCount)
            .send();
        tokio::select! {
            () = &mut shutdown => break Ok(()),
            Some(result) = tasks.join_next(), if !tasks.is_empty() => match result {
                Ok(Ok(())) => {},
                Ok(Err(error)) => break Err(error),
                Err(_) => break Err(SqsError::WorkerTask),
            },
            received = timeout(config.request_timeout(), receive) => {
                let received = received
                    .map_err(|_| SqsError::Receive)?
                    .map_err(|_| SqsError::Receive)?;
                for message in received.messages() {
                    let processor = processor.clone();
                    let observer = Arc::clone(&observer);
                    let message = message.clone();
                    tasks.spawn(async move {
                        let observation = JobDeliveryObservation::start(observer, "amazon_sqs");
                        match processor(message).await {
                            Ok((attempt, outcome)) => {
                                observation.finish(NonZeroU16::new(attempt), outcome);
                                Ok(())
                            }
                            Err(error) => {
                                observation.finish(None, JobDeliveryOutcome::Unsettled);
                                Err(error)
                            }
                        }
                    });
                }
            }
        }
    };
    let drain_result = drain_tasks(&mut tasks, worker_config.drain_timeout).await;
    run_result?;
    drain_result
}

pub(crate) fn validate_retry_policy(
    retry_policy: RetryPolicy,
    expected_redrive_max_receive_count: u16,
) -> Result<(), SqsError> {
    if !retry_policy.is_valid()
        || retry_policy.max_deliveries > expected_redrive_max_receive_count
        || validate_whole_seconds(retry_policy.initial_backoff, 1, MAX_VISIBILITY_SECONDS).is_err()
        || validate_whole_seconds(retry_policy.max_backoff, 1, MAX_VISIBILITY_SECONDS).is_err()
    {
        return Err(SqsError::RetryPolicyMismatch);
    }
    Ok(())
}

async fn drain_tasks(
    tasks: &mut JoinSet<Result<(), SqsError>>,
    drain_timeout: Duration,
) -> Result<(), SqsError> {
    let drained = timeout(drain_timeout, async {
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error),
                Err(_) => return Err(SqsError::WorkerTask),
            }
        }
        Ok(())
    })
    .await;
    if let Ok(result) = drained {
        result
    } else {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        Err(SqsError::DrainTimeout)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SaturatedWorkerWait {
    Shutdown,
    CapacityAvailable,
}

pub(crate) async fn wait_for_task_or_shutdown<Shutdown>(
    tasks: &mut JoinSet<Result<(), SqsError>>,
    shutdown: &mut Pin<Box<Shutdown>>,
) -> Result<SaturatedWorkerWait, SqsError>
where
    Shutdown: Future<Output = ()> + Send,
{
    tokio::select! {
        () = shutdown.as_mut() => Ok(SaturatedWorkerWait::Shutdown),
        result = tasks.join_next() => match result {
            Some(Ok(Ok(()))) | None => Ok(SaturatedWorkerWait::CapacityAvailable),
            Some(Ok(Err(error))) => Err(error),
            Some(Err(_)) => Err(SqsError::WorkerTask),
        },
    }
}
