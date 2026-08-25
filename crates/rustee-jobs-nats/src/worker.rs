use std::{fmt, future::Future, num::NonZeroU16, sync::Arc, time::Duration};

use async_nats::jetstream;
use futures_util::{StreamExt, future::BoxFuture};
use rustee_jobs::{
    Job, JobDeliveryObservation, JobDeliveryObserver, JobDeliveryOutcome, JobHandler, JobRegistry,
    NoopJobDeliveryObserver, RetryPolicy, WorkerConfig,
};
use tokio::{task::JoinSet, time::timeout};

use crate::{
    ConfigError, NatsError,
    config::validate_subject,
    delivery::{process_delivery, process_registry_delivery},
};

/// Durable pull-consumer worker for one typed `Rustee` job stream.
///
/// The caller supplies a pre-existing `JetStream` context and consumer. Stream, consumer, retry
/// limits, and dead-letter stream provisioning remain deployment-owned infrastructure. Its
/// `Debug` output keeps deployment-routing values redacted.
#[derive(Clone)]
pub struct JetStreamWorker {
    context: jetstream::Context,
    consumer: jetstream::consumer::PullConsumer,
    dead_letter_subject: String,
    pull_request_expires: Duration,
    observer: Arc<dyn JobDeliveryObserver>,
}

impl JetStreamWorker {
    /// Creates a worker for a pre-provisioned durable pull consumer with explicit acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when the dead-letter subject is invalid or the consumer cannot
    /// preserve at-least-once delivery semantics.
    pub fn new(
        context: jetstream::Context,
        consumer: jetstream::consumer::PullConsumer,
        dead_letter_subject: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        let dead_letter_subject = dead_letter_subject.into();
        validate_subject(&dead_letter_subject)?;
        let consumer_config = &consumer.cached_info().config;
        if consumer_config.durable_name.is_none() {
            return Err(ConfigError::EphemeralConsumer);
        }
        if consumer_config.ack_policy != jetstream::consumer::AckPolicy::Explicit {
            return Err(ConfigError::NonExplicitAcknowledgement);
        }
        Ok(Self {
            context,
            consumer,
            dead_letter_subject,
            pull_request_expires: Duration::from_secs(5),
            observer: Arc::new(NoopJobDeliveryObserver),
        })
    }

    /// Attaches a non-blocking observer for bounded delivery lifecycle telemetry.
    ///
    /// Observer panics are isolated from broker acknowledgement behavior. Use
    /// `rustee-jobs-observability::JobMetrics` for the built-in exporter-neutral collector.
    #[must_use]
    pub fn with_delivery_observer(mut self, observer: Arc<dyn JobDeliveryObserver>) -> Self {
        self.observer = observer;
        self
    }

    /// Sets the maximum time an idle pull request can remain on the `JetStream` server.
    ///
    /// A shorter value makes an idle worker react to infrastructure changes sooner; shutdown is
    /// selected locally and does not wait for this duration.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroPullRequestExpiry`] when `expires` is zero.
    pub fn with_pull_request_expires(mut self, expires: Duration) -> Result<Self, ConfigError> {
        if expires.is_zero() {
            return Err(ConfigError::ZeroPullRequestExpiry);
        }
        self.pull_request_expires = expires;
        Ok(self)
    }

    /// Runs until `shutdown` resolves, then drains active handlers for `WorkerConfig::drain_timeout`.
    ///
    /// A message is acknowledged only after its handler succeeds. A failed handler receives an
    /// explicit delayed negative acknowledgement while the retry policy has budget. Once the
    /// budget is exhausted, or an envelope is malformed, the worker waits for a durable
    /// dead-letter publish acknowledgement before acknowledging the source delivery.
    ///
    /// # Errors
    ///
    /// Returns a sanitized provider/lifecycle failure. Handler errors are converted into the
    /// configured retry or dead-letter action and are not exposed through this error.
    pub async fn run_until<J, H, F>(
        &self,
        handler: H,
        worker_config: WorkerConfig,
        retry_policy: RetryPolicy,
        shutdown: F,
    ) -> Result<(), NatsError>
    where
        J: Job,
        H: JobHandler<J>,
        F: Future<Output = ()> + Send,
    {
        let context = self.context.clone();
        let dead_letter_subject = self.dead_letter_subject.clone();
        self.run_with(worker_config, retry_policy, shutdown, move |message| {
            let context = context.clone();
            let dead_letter_subject = dead_letter_subject.clone();
            let handler = handler.clone();
            Box::pin(async move {
                process_delivery::<J, H>(
                    context,
                    dead_letter_subject,
                    message,
                    handler,
                    retry_policy,
                )
                .await
            })
        })
        .await
    }

    /// Runs registered typed job handlers until `shutdown` resolves.
    ///
    /// The registry is built before worker startup and selects handlers from the serialized job
    /// envelope name. Unknown or malformed envelopes are poison messages: they are durably
    /// published to the configured dead-letter subject before their source delivery is
    /// acknowledged. Registered handler failures keep the ordinary [`RetryPolicy`] behavior.
    ///
    /// # Errors
    ///
    /// Returns a sanitized provider/lifecycle failure. Registered handler failures are converted
    /// into the configured retry or dead-letter action and are not exposed through this error.
    pub async fn run_registry_until<F>(
        &self,
        registry: JobRegistry,
        worker_config: WorkerConfig,
        retry_policy: RetryPolicy,
        shutdown: F,
    ) -> Result<(), NatsError>
    where
        F: Future<Output = ()> + Send,
    {
        let context = self.context.clone();
        let dead_letter_subject = self.dead_letter_subject.clone();
        self.run_with(worker_config, retry_policy, shutdown, move |message| {
            let context = context.clone();
            let dead_letter_subject = dead_letter_subject.clone();
            let registry = registry.clone();
            Box::pin(async move {
                process_registry_delivery(
                    context,
                    dead_letter_subject,
                    message,
                    registry,
                    retry_policy,
                )
                .await
            })
        })
        .await
    }

    async fn run_with<F, Processor>(
        &self,
        worker_config: WorkerConfig,
        retry_policy: RetryPolicy,
        shutdown: F,
        processor: Processor,
    ) -> Result<(), NatsError>
    where
        F: Future<Output = ()> + Send,
        Processor: Fn(
                jetstream::Message,
            ) -> BoxFuture<'static, Result<(u16, JobDeliveryOutcome), NatsError>>
            + Clone
            + Send
            + Sync
            + 'static,
    {
        self.validate_runtime_config(worker_config, retry_policy)?;

        let mut messages = self
            .consumer
            .stream()
            .max_messages_per_batch(worker_config.concurrency.get())
            .expires(self.pull_request_expires)
            .messages()
            .await
            .map_err(|_| NatsError::Receive)?;
        let mut shutdown = Box::pin(shutdown);
        let mut tasks = JoinSet::new();

        let run_result = loop {
            tokio::select! {
                () = &mut shutdown => break Ok(()),
                Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                    match result {
                        Ok(Ok(())) => {},
                        Ok(Err(error)) => break Err(error),
                        Err(_) => break Err(NatsError::WorkerTask),
                    }
                }
                message = messages.next(), if tasks.len() < worker_config.concurrency.get() => {
                    let Some(message) = message else {
                        break Err(NatsError::Receive);
                    };
                    let Ok(message) = message else {
                        break Err(NatsError::Receive);
                    };
                    let processor = processor.clone();
                    let observer = Arc::clone(&self.observer);
                    tasks.spawn(async move {
                        let observation = JobDeliveryObservation::start(observer, "nats_jetstream");
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
        };

        let drain_result = drain_tasks(&mut tasks, worker_config.drain_timeout).await;
        run_result?;
        drain_result
    }

    fn validate_runtime_config(
        &self,
        worker_config: WorkerConfig,
        retry_policy: RetryPolicy,
    ) -> Result<(), NatsError> {
        validate_retry_policy(retry_policy)?;
        let consumer_config = &self.consumer.cached_info().config;
        let concurrency = i64::try_from(worker_config.concurrency.get())
            .map_err(|_| NatsError::ConsumerConfiguration)?;
        if consumer_config.max_ack_pending > 0 && consumer_config.max_ack_pending < concurrency {
            return Err(NatsError::ConsumerConfiguration);
        }
        let max_deliveries = i64::from(retry_policy.max_deliveries);
        if consumer_config.max_deliver > 0 && consumer_config.max_deliver < max_deliveries {
            return Err(NatsError::ConsumerConfiguration);
        }
        Ok(())
    }
}

pub(crate) fn validate_retry_policy(retry_policy: RetryPolicy) -> Result<(), NatsError> {
    if retry_policy.is_valid() {
        Ok(())
    } else {
        Err(NatsError::RetryPolicy)
    }
}

impl fmt::Debug for JetStreamWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JetStreamWorker")
            .field("consumer", &"[REDACTED]")
            .field(
                "consumer_name_length",
                &self.consumer.cached_info().name.len(),
            )
            .field("dead_letter_subject", &"[REDACTED]")
            .field(
                "dead_letter_subject_length",
                &self.dead_letter_subject.len(),
            )
            .field("pull_request_expires", &self.pull_request_expires)
            .finish_non_exhaustive()
    }
}

pub(crate) async fn drain_tasks(
    tasks: &mut JoinSet<Result<(), NatsError>>,
    drain_timeout: Duration,
) -> Result<(), NatsError> {
    let drain = async {
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(result) => result?,
                Err(_) => return Err(NatsError::WorkerTask),
            }
        }
        Ok(())
    };
    if let Ok(result) = timeout(drain_timeout, drain).await {
        result
    } else {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        Err(NatsError::DrainTimeout)
    }
}
