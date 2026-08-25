use std::{fmt, future::Future, sync::Arc};

use rustee_jobs::{
    Job, JobDeliveryObserver, JobHandler, JobRegistry, NoopJobDeliveryObserver, RetryPolicy,
    WorkerConfig,
};
use rustee_redis::redis::{
    self, AsyncCommands, aio::ConnectionManager, streams::StreamInfoGroupsReply,
};

use crate::{
    RedisStreamsError, RedisStreamsWorkerConfig,
    delivery::{process_delivery, process_registry_delivery},
    operation::bounded,
};

mod receiver;
mod runner;
mod settlement;

#[cfg(test)]
pub(crate) use runner::{drain_tasks, validate_retry_policy};

/// A Redis Streams durable worker that requires a deployment-provisioned consumer group.
#[derive(Clone)]
pub struct RedisStreamsWorker {
    connection: ConnectionManager,
    config: RedisStreamsWorkerConfig,
    observer: Arc<dyn JobDeliveryObserver>,
}

impl RedisStreamsWorker {
    /// Creates a worker without provisioning Redis Streams infrastructure.
    #[must_use]
    pub fn new(connection: ConnectionManager, config: RedisStreamsWorkerConfig) -> Self {
        Self {
            connection,
            config,
            observer: Arc::new(NoopJobDeliveryObserver),
        }
    }

    /// Attaches a non-blocking observer for bounded delivery lifecycle telemetry.
    ///
    /// Observer panics are isolated from Redis acknowledgement, retry, and dead-letter behavior.
    #[must_use]
    pub fn with_delivery_observer(mut self, observer: Arc<dyn JobDeliveryObserver>) -> Self {
        self.observer = observer;
        self
    }

    /// Verifies the source and DLQ stream exist and the configured consumer group is present.
    ///
    /// # Errors
    ///
    /// Returns [`RedisStreamsError::ConsumerGroup`] when the configured group is absent, or
    /// [`RedisStreamsError::Readiness`] when Redis cannot inspect the required streams.
    pub async fn readiness(&self) -> Result<(), RedisStreamsError> {
        let mut connection = self.connection.clone();
        let groups: StreamInfoGroupsReply = bounded(
            self.config.operation_timeout(),
            connection.xinfo_groups(self.config.stream()),
        )
        .await
        .map_err(|()| RedisStreamsError::Readiness)?;
        if !groups
            .groups
            .iter()
            .any(|group| group.name == self.config.group())
        {
            return Err(RedisStreamsError::ConsumerGroup);
        }
        bounded(
            self.config.operation_timeout(),
            redis::cmd("XINFO")
                .arg("STREAM")
                .arg(self.config.dead_letter_stream())
                .query_async::<redis::Value>(&mut connection),
        )
        .await
        .map(|_| ())
        .map_err(|()| RedisStreamsError::Readiness)
    }

    /// Runs one typed handler until shutdown, then drains active handlers for the configured time.
    ///
    /// Successful handlers settle only their own pending delivery. Handler failure is moved to the
    /// durable retry schedule before the source entry is acknowledged; malformed envelopes route
    /// to the DLQ. Pending entries abandoned by a terminated worker are reclaimed after the
    /// configured idle period.
    ///
    /// # Errors
    ///
    /// Returns a sanitized Redis or lifecycle failure. Handler errors become retry/DLQ actions.
    pub async fn run_until<J, H, F>(
        &self,
        handler: H,
        worker_config: WorkerConfig,
        retry_policy: RetryPolicy,
        shutdown: F,
    ) -> Result<(), RedisStreamsError>
    where
        J: Job,
        H: JobHandler<J>,
        F: Future<Output = ()> + Send,
    {
        self.run_with(
            worker_config,
            retry_policy,
            shutdown,
            move |delivery, retry_policy| {
                let handler = handler.clone();
                Box::pin(
                    async move { process_delivery::<J, H>(delivery, handler, retry_policy).await },
                )
            },
        )
        .await
    }

    /// Runs a fixed typed job registry until shutdown.
    ///
    /// Unknown or malformed envelopes are poison messages and route to the DLQ without automatic
    /// retry. Registered handler failures use the supplied retry policy.
    ///
    /// # Errors
    ///
    /// Returns a sanitized Redis or lifecycle failure. Registered handler errors become retry/DLQ
    /// actions.
    pub async fn run_registry_until<F>(
        &self,
        registry: JobRegistry,
        worker_config: WorkerConfig,
        retry_policy: RetryPolicy,
        shutdown: F,
    ) -> Result<(), RedisStreamsError>
    where
        F: Future<Output = ()> + Send,
    {
        self.run_with(
            worker_config,
            retry_policy,
            shutdown,
            move |delivery, retry_policy| {
                let registry = registry.clone();
                Box::pin(async move {
                    process_registry_delivery(delivery, registry, retry_policy).await
                })
            },
        )
        .await
    }
}

impl fmt::Debug for RedisStreamsWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisStreamsWorker")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
