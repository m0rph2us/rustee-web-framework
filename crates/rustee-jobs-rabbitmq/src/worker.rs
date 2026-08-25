use std::{fmt, future::Future, num::NonZeroU16, sync::Arc, time::Duration};

use futures_util::{StreamExt, future::BoxFuture};
use lapin::{
    Channel, ExchangeKind,
    message::Delivery,
    options::{
        BasicCancelOptions, BasicConsumeOptions, BasicQosOptions, ExchangeDeclareOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
};
use rustee_jobs::{
    Job, JobDeliveryObservation, JobDeliveryObserver, JobDeliveryOutcome, JobHandler, JobRegistry,
    NoopJobDeliveryObserver, RetryPolicy, WorkerConfig,
};
use tokio::{task::JoinSet, time::timeout};

use crate::{
    RabbitMqConnection, RabbitMqError, RabbitMqWorkerConfig,
    delivery::{process_delivery, process_registry_delivery},
    publisher::open_confirm_channel,
};

const MAX_QUORUM_PREFETCH: usize = 2_000;

/// A `RabbitMQ` worker with explicit manual acknowledgement and confirmed settlement publishes.
#[derive(Clone)]
pub struct RabbitMqWorker {
    connection: RabbitMqConnection,
    config: RabbitMqWorkerConfig,
    observer: Arc<dyn JobDeliveryObserver>,
}

impl RabbitMqWorker {
    /// Creates a worker without declaring queues, exchanges, or bindings.
    #[must_use]
    pub fn new(connection: RabbitMqConnection, config: RabbitMqWorkerConfig) -> Self {
        Self {
            connection,
            config,
            observer: Arc::new(NoopJobDeliveryObserver),
        }
    }

    /// Attaches a non-blocking observer for bounded delivery lifecycle telemetry.
    ///
    /// Observer panics are isolated from `RabbitMQ` acknowledgement, retry, and dead-letter behavior.
    #[must_use]
    pub fn with_delivery_observer(mut self, observer: Arc<dyn JobDeliveryObserver>) -> Self {
        self.observer = observer;
        self
    }

    /// Verifies that the source queue and dead-letter exchange exist within a caller-supplied deadline.
    ///
    /// AMQP passive declarations can verify existence and access but cannot prove an existing queue
    /// is quorum/durable or has the configured native delayed-retry policy. Enforce those details
    /// through deployment provisioning tests.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqError::InvalidReadinessTimeout`] when the deadline is zero,
    /// [`RabbitMqError::ReadinessTimeout`] when it expires, or [`RabbitMqError::Readiness`] when
    /// a required topology component is unavailable.
    pub async fn readiness(&self, readiness_timeout: Duration) -> Result<(), RabbitMqError> {
        bounded_readiness(readiness_timeout, async {
            let channel = self
                .connection
                .inner
                .create_channel()
                .await
                .map_err(|_| RabbitMqError::Readiness)?;
            channel
                .queue_declare(
                    self.config.queue().into(),
                    QueueDeclareOptions {
                        passive: true,
                        ..QueueDeclareOptions::default()
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|_| RabbitMqError::Readiness)?;
            channel
                .exchange_declare(
                    self.config.dead_letter_exchange().into(),
                    ExchangeKind::Direct,
                    ExchangeDeclareOptions {
                        passive: true,
                        ..ExchangeDeclareOptions::default()
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|_| RabbitMqError::Readiness)
        })
        .await
    }

    /// Runs one typed handler until shutdown, then drains active handlers for the configured time.
    ///
    /// Handler success is acknowledged manually. A retry returns the original delivery to the
    /// deployment-owned `RabbitMQ` 4.3 native delayed-retry policy. Poison messages and exhausted
    /// retries use confirm-before-ack ordering on the explicit dead-letter exchange.
    ///
    /// # Errors
    ///
    /// Returns a sanitized broker or lifecycle failure. Typed handler failures use `retry_policy`.
    pub async fn run_until<J, H, F>(
        &self,
        handler: H,
        worker_config: WorkerConfig,
        retry_policy: RetryPolicy,
        shutdown: F,
    ) -> Result<(), RabbitMqError>
    where
        J: Job,
        H: JobHandler<J>,
        F: Future<Output = ()> + Send,
    {
        let config = self.config.clone();
        self.run_with(
            worker_config,
            retry_policy,
            shutdown,
            move |delivery, settlement_channel| {
                let handler = handler.clone();
                let config = config.clone();
                Box::pin(async move {
                    process_delivery::<J, H>(
                        settlement_channel,
                        config,
                        delivery,
                        handler,
                        retry_policy,
                    )
                    .await
                })
            },
        )
        .await
    }

    /// Runs a fixed typed job registry until shutdown.
    ///
    /// Unknown and malformed envelopes are poison messages and are dead-lettered without a retry.
    /// Registered handler failures preserve the configured retry policy.
    ///
    /// # Errors
    ///
    /// Returns a sanitized broker or lifecycle failure. Registered handler failures use
    /// `retry_policy` and are not exposed through this error.
    pub async fn run_registry_until<F>(
        &self,
        registry: JobRegistry,
        worker_config: WorkerConfig,
        retry_policy: RetryPolicy,
        shutdown: F,
    ) -> Result<(), RabbitMqError>
    where
        F: Future<Output = ()> + Send,
    {
        let config = self.config.clone();
        self.run_with(
            worker_config,
            retry_policy,
            shutdown,
            move |delivery, settlement_channel| {
                let config = config.clone();
                let registry = registry.clone();
                Box::pin(async move {
                    process_registry_delivery(
                        settlement_channel,
                        config,
                        delivery,
                        registry,
                        retry_policy,
                    )
                    .await
                })
            },
        )
        .await
    }

    async fn run_with<F, P>(
        &self,
        worker_config: WorkerConfig,
        retry_policy: RetryPolicy,
        shutdown: F,
        processor: P,
    ) -> Result<(), RabbitMqError>
    where
        F: Future<Output = ()> + Send,
        P: Fn(
                Delivery,
                Channel,
            ) -> BoxFuture<'static, Result<(u16, JobDeliveryOutcome), RabbitMqError>>
            + Clone
            + Send
            + Sync
            + 'static,
    {
        if !self.config.native_retry().matches(retry_policy) {
            return Err(RabbitMqError::RetryPolicyMismatch);
        }
        let prefetch = u16::try_from(worker_config.concurrency.get())
            .map_err(|_| RabbitMqError::WorkerConfiguration)?;
        if usize::from(prefetch) > MAX_QUORUM_PREFETCH {
            return Err(RabbitMqError::WorkerConfiguration);
        }
        let consumer_channel = self
            .connection
            .inner
            .create_channel()
            .await
            .map_err(|_| RabbitMqError::ConsumerChannel)?;
        consumer_channel
            .basic_qos(prefetch, BasicQosOptions::default())
            .await
            .map_err(|_| RabbitMqError::WorkerConfiguration)?;
        let mut consumer = consumer_channel
            .basic_consume(
                self.config.queue().into(),
                self.config.consumer_tag().into(),
                BasicConsumeOptions {
                    no_ack: false,
                    ..BasicConsumeOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|_| RabbitMqError::Receive)?;
        let consumer_tag = consumer.tag();
        let settlement_channel = open_confirm_channel(&self.connection).await?;
        let mut shutdown = Box::pin(shutdown);
        let mut tasks = JoinSet::new();

        let run_result = loop {
            tokio::select! {
                () = &mut shutdown => break Ok(()),
                Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                    match result {
                        Ok(Ok(())) => {},
                        Ok(Err(error)) => break Err(error),
                        Err(_) => break Err(RabbitMqError::WorkerTask),
                    }
                }
                delivery = consumer.next(), if tasks.len() < worker_config.concurrency.get() => {
                    let Some(delivery) = delivery else {
                        break Err(RabbitMqError::Receive);
                    };
                    let delivery = delivery.map_err(|_| RabbitMqError::Receive)?;
                    let processor = processor.clone();
                    let settlement_channel = settlement_channel.clone();
                    let observer = Arc::clone(&self.observer);
                    tasks.spawn(async move {
                        let observation = JobDeliveryObservation::start(observer, "rabbitmq");
                        match processor(delivery, settlement_channel).await {
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
        };

        let cancel_result = consumer_channel
            .basic_cancel(consumer_tag, BasicCancelOptions::default())
            .await
            .map_err(|_| RabbitMqError::ConsumerCancel);
        let drain_result = drain_tasks(&mut tasks, worker_config.drain_timeout).await;
        let close_result = consumer_channel
            .close(200, "Rustee worker shutdown".into())
            .await
            .map_err(|_| RabbitMqError::ConsumerChannel);
        run_result?;
        cancel_result?;
        drain_result?;
        close_result
    }
}

impl fmt::Debug for RabbitMqWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RabbitMqWorker")
            .field("connection", &self.connection)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

pub(crate) fn validate_readiness_timeout(timeout: Duration) -> Result<(), RabbitMqError> {
    if timeout.is_zero() {
        return Err(RabbitMqError::InvalidReadinessTimeout);
    }
    Ok(())
}

pub(crate) async fn bounded_readiness<T, F>(
    readiness_timeout: Duration,
    operation: F,
) -> Result<T, RabbitMqError>
where
    F: Future<Output = Result<T, RabbitMqError>>,
{
    validate_readiness_timeout(readiness_timeout)?;
    timeout(readiness_timeout, operation)
        .await
        .map_err(|_| RabbitMqError::ReadinessTimeout)?
}

pub(crate) async fn drain_tasks(
    tasks: &mut JoinSet<Result<(), RabbitMqError>>,
    drain_timeout: Duration,
) -> Result<(), RabbitMqError> {
    let drain = async {
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(result) => result?,
                Err(_) => return Err(RabbitMqError::WorkerTask),
            }
        }
        Ok(())
    };
    if let Ok(result) = timeout(drain_timeout, drain).await {
        result
    } else {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        Err(RabbitMqError::DrainTimeout)
    }
}
