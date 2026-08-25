//! Shutdown-aware Redis worker supervision, handler execution, and task draining.

use std::{future::Future, num::NonZeroU16, sync::Arc, time::Duration};

use futures_util::future::BoxFuture;
use rustee_jobs::{
    JobDeliveryObservation, JobDeliveryObserver, JobDeliveryOutcome, RetryPolicy, WorkerConfig,
};
use tokio::{
    task::JoinSet,
    time::{MissedTickBehavior, interval, timeout},
};

use crate::{
    RedisStreamsError, config::nonzero_duration_to_millis, delivery::RedisStreamsDelivery,
};

use super::RedisStreamsWorker;

impl RedisStreamsWorker {
    pub(super) async fn run_with<F, P>(
        &self,
        worker_config: WorkerConfig,
        retry_policy: RetryPolicy,
        shutdown: F,
        process: P,
    ) -> Result<(), RedisStreamsError>
    where
        F: Future<Output = ()> + Send,
        P: Fn(
                RedisStreamsDelivery,
                RetryPolicy,
            )
                -> BoxFuture<'static, Result<(u16, JobDeliveryOutcome), RedisStreamsError>>
            + Clone
            + Send
            + Sync
            + 'static,
    {
        validate_retry_policy(retry_policy)?;
        self.readiness().await?;
        let mut shutdown = Box::pin(shutdown);
        let mut maintenance = interval(self.config.reclaim_interval());
        maintenance.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut tasks = JoinSet::new();

        let run_result = loop {
            let available = worker_config.concurrency.get().saturating_sub(tasks.len());
            tokio::select! {
                () = &mut shutdown => break Ok(()),
                Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                    match result {
                        Ok(Ok(())) => {},
                        Ok(Err(error)) => break Err(error),
                        Err(_) => break Err(RedisStreamsError::WorkerTask),
                    }
                }
                _ = maintenance.tick(), if available > 0 => {
                    self.promote_due_retries(available.min(self.config.batch_size()))
                        .await?;
                    let deliveries = self
                        .reclaim_pending(available.min(self.config.batch_size()))
                        .await?;
                    for delivery in deliveries {
                        spawn_delivery_task(
                            &mut tasks,
                            process.clone(),
                            Arc::clone(&self.observer),
                            delivery,
                            retry_policy,
                        );
                    }
                }
                deliveries = self.read_new(available), if available > 0 => {
                    for delivery in deliveries? {
                        spawn_delivery_task(
                            &mut tasks,
                            process.clone(),
                            Arc::clone(&self.observer),
                            delivery,
                            retry_policy,
                        );
                    }
                }
            }
        };

        let drain_result = drain_tasks(&mut tasks, worker_config.drain_timeout).await;
        run_result?;
        drain_result
    }
}

pub(crate) fn validate_retry_policy(retry_policy: RetryPolicy) -> Result<(), RedisStreamsError> {
    if retry_policy.is_valid()
        && nonzero_duration_to_millis(retry_policy.initial_backoff).is_ok()
        && nonzero_duration_to_millis(retry_policy.max_backoff).is_ok()
    {
        Ok(())
    } else {
        Err(RedisStreamsError::RetryPolicy)
    }
}

fn spawn_delivery_task<P>(
    tasks: &mut JoinSet<Result<(), RedisStreamsError>>,
    process: P,
    observer: Arc<dyn JobDeliveryObserver>,
    delivery: RedisStreamsDelivery,
    retry_policy: RetryPolicy,
) where
    P: Fn(
            RedisStreamsDelivery,
            RetryPolicy,
        ) -> BoxFuture<'static, Result<(u16, JobDeliveryOutcome), RedisStreamsError>>
        + Send
        + 'static,
{
    tasks.spawn(async move {
        let observation = JobDeliveryObservation::start(observer, "redis_streams");
        match process(delivery, retry_policy).await {
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

pub(crate) async fn drain_tasks(
    tasks: &mut JoinSet<Result<(), RedisStreamsError>>,
    drain_timeout: Duration,
) -> Result<(), RedisStreamsError> {
    let drain = async {
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(result) => result?,
                Err(_) => return Err(RedisStreamsError::WorkerTask),
            }
        }
        Ok(())
    };
    if let Ok(result) = timeout(drain_timeout, drain).await {
        result
    } else {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        Err(RedisStreamsError::DrainTimeout)
    }
}
