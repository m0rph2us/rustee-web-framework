//! NATS `JetStream` publishing and delivery acknowledgement helpers for `Rustee` jobs.
//!
//! Streams, durable consumers, and dead-letter subjects are deployment-owned infrastructure. This
//! crate never creates or mutates them during application or worker startup.

use std::{fmt, future::Future, num::NonZeroU16, sync::Arc, time::Duration};

use async_nats::{
    HeaderMap,
    jetstream::{self, AckKind, message::PublishMessage},
};
use bytes::Bytes;
use futures_util::{StreamExt, future::BoxFuture};
use rustee_jobs::{
    DeliveryAction, Job, JobDeliveryObservation, JobDeliveryObserver, JobDeliveryOutcome,
    JobEnvelope, JobHandler, JobId, JobMessage, JobPublisher, JobRegistry, JobRegistryError,
    NoopJobDeliveryObserver, RetryPolicy, WorkerConfig, dispatch,
};
use tokio::{task::JoinSet, time::timeout};

pub use async_nats;

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Connection and publishing settings for a `JetStream` job producer.
#[derive(Clone, Eq, PartialEq)]
pub struct NatsConfig {
    url: String,
    subject: String,
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl NatsConfig {
    /// Creates a producer configuration with a finite `JetStream` request timeout.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidSubject`] when `subject` is not a concrete publish subject.
    pub fn new(url: impl Into<String>, subject: impl Into<String>) -> Result<Self, ConfigError> {
        let subject = subject.into();
        validate_subject(&subject)?;
        Ok(Self {
            url: url.into(),
            subject,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: Duration::from_secs(5),
        })
    }

    /// Sets the bounded time allowed to establish the NATS connection.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroConnectTimeout`] when `connect_timeout` is zero.
    pub fn with_connect_timeout(mut self, connect_timeout: Duration) -> Result<Self, ConfigError> {
        if connect_timeout.is_zero() {
            return Err(ConfigError::ZeroConnectTimeout);
        }
        self.connect_timeout = connect_timeout;
        Ok(self)
    }

    /// Sets the timeout for `JetStream` API requests and publish acknowledgements.
    #[must_use]
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Returns the NATS connection establishment deadline.
    #[must_use]
    pub const fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Returns the concrete `JetStream` subject used for durable jobs.
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }
}

impl fmt::Debug for NatsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NatsConfig")
            .field("url", &"[REDACTED]")
            .field("subject", &self.subject)
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

/// Invalid NATS producer configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConfigError {
    /// The subject was empty, had an empty token, whitespace, or a subscription wildcard.
    #[error("NATS job publish subject must be a concrete dot-delimited subject")]
    InvalidSubject,
    /// The configured pull consumer is ephemeral and cannot survive worker recovery.
    #[error("NATS job worker requires a durable pull consumer")]
    EphemeralConsumer,
    /// The configured consumer cannot acknowledge individual completed jobs.
    #[error("NATS job worker requires explicit consumer acknowledgements")]
    NonExplicitAcknowledgement,
    /// The idle pull request timeout was zero.
    #[error("NATS job worker pull request expiry must be non-zero")]
    ZeroPullRequestExpiry,
    /// The initial NATS connection deadline was zero.
    #[error("NATS connection timeout must be non-zero")]
    ZeroConnectTimeout,
}

/// Acknowledged `JetStream` publisher for serialized `Rustee` jobs.
#[derive(Clone)]
pub struct JetStreamPublisher {
    context: jetstream::Context,
    subject: String,
}

impl JetStreamPublisher {
    /// Connects to NATS and creates a `JetStream` context without provisioning infrastructure.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::Connect`] when the NATS server cannot be reached.
    pub async fn connect(config: &NatsConfig) -> Result<Self, NatsError> {
        let client = timeout(
            config.connect_timeout,
            async_nats::connect(config.url.as_str()),
        )
        .await
        .map_err(|_| NatsError::Connect)?
        .map_err(|_| NatsError::Connect)?;
        let mut context = jetstream::new(client);
        context.set_timeout(config.request_timeout);
        Ok(Self {
            context,
            subject: config.subject.clone(),
        })
    }

    /// Wraps an already-configured `JetStream` context for dependency injection and testing.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidSubject`] when `subject` is not a concrete publish subject.
    pub fn new(
        context: jetstream::Context,
        subject: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        let subject = subject.into();
        validate_subject(&subject)?;
        Ok(Self { context, subject })
    }

    /// Verifies access to the NATS `JetStream` account without creating a stream or consumer.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::Readiness`] when the account cannot be queried.
    pub async fn readiness(&self) -> Result<(), NatsError> {
        self.context
            .query_account()
            .await
            .map(|_| ())
            .map_err(|_| NatsError::Readiness)
    }
}

impl fmt::Debug for JetStreamPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JetStreamPublisher")
            .field("subject", &self.subject)
            .finish_non_exhaustive()
    }
}

impl JobPublisher for JetStreamPublisher {
    type Error = NatsError;

    fn publish(&self, message: JobMessage) -> BoxFuture<'static, Result<(), Self::Error>> {
        let context = self.context.clone();
        let subject = self.subject.clone();
        let message_id = message.id().to_string();
        let payload = Bytes::from(message.into_payload());
        Box::pin(async move {
            let publish = PublishMessage::build()
                .payload(payload)
                .message_id(message_id);
            context
                .send_publish(subject, publish)
                .await
                .map_err(|_| NatsError::Publish)?
                .await
                .map_err(|_| NatsError::PublishAcknowledgement)?;
            Ok(())
        })
    }
}

/// Durable pull-consumer worker for one typed `Rustee` job stream.
///
/// The caller supplies a pre-existing `JetStream` context and consumer. Stream, consumer, retry
/// limits, and dead-letter stream provisioning remain deployment-owned infrastructure.
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
                    let context = self.context.clone();
                    let dead_letter_subject = self.dead_letter_subject.clone();
                    let handler = handler.clone();
                    let observer = Arc::clone(&self.observer);
                    tasks.spawn(async move {
                        let observation = JobDeliveryObservation::start(observer, "nats_jetstream");
                        match process_delivery::<J, H>(
                            context,
                            dead_letter_subject,
                            message,
                            handler,
                            retry_policy,
                        )
                        .await {
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
                    let context = self.context.clone();
                    let dead_letter_subject = self.dead_letter_subject.clone();
                    let registry = registry.clone();
                    let observer = Arc::clone(&self.observer);
                    tasks.spawn(async move {
                        let observation = JobDeliveryObservation::start(observer, "nats_jetstream");
                        match process_registry_delivery(
                            context,
                            dead_letter_subject,
                            message,
                            registry,
                            retry_policy,
                        )
                        .await {
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

impl fmt::Debug for JetStreamWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JetStreamWorker")
            .field("consumer", &self.consumer.cached_info().name)
            .field("dead_letter_subject", &self.dead_letter_subject)
            .field("pull_request_expires", &self.pull_request_expires)
            .finish_non_exhaustive()
    }
}

/// An owned NATS delivery whose acknowledgement stays explicit at the worker boundary.
#[derive(Debug)]
pub struct JetStreamDelivery {
    message: jetstream::Message,
}

impl JetStreamDelivery {
    /// Wraps one pull-consumer message after a provider has selected it for processing.
    #[must_use]
    pub fn new(message: jetstream::Message) -> Self {
        Self { message }
    }

    /// Returns the serialized `Rustee` job envelope without exposing acknowledgement internals.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.message.payload
    }

    /// Returns the one-based delivery attempt reported by `JetStream` metadata.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::DeliveryMetadata`] when the message does not carry usable `JetStream`
    /// acknowledgement metadata.
    pub fn delivery_attempt(&self) -> Result<u16, NatsError> {
        let delivered = self
            .message
            .info()
            .map_err(|_| NatsError::DeliveryMetadata)?
            .delivered;
        u16::try_from(delivered).map_err(|_| NatsError::DeliveryMetadata)
    }

    /// Acknowledges a successfully completed handler execution.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::Acknowledge`] when NATS cannot accept the acknowledgement.
    pub async fn acknowledge(&self) -> Result<(), NatsError> {
        self.message.ack().await.map_err(|_| NatsError::Acknowledge)
    }

    /// Requests delayed redelivery after a retryable handler failure.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::NegativeAcknowledge`] when NATS cannot accept the negative acknowledgement.
    pub async fn retry_after(&self, delay: Duration) -> Result<(), NatsError> {
        self.message
            .ack_with(AckKind::Nak(Some(delay)))
            .await
            .map_err(|_| NatsError::NegativeAcknowledge)
    }

    /// Returns NATS headers for provider-level correlation only; job payload data is not logged here.
    #[must_use]
    pub fn headers(&self) -> Option<&HeaderMap> {
        self.message.headers.as_ref()
    }
}

async fn process_delivery<J, H>(
    context: jetstream::Context,
    dead_letter_subject: String,
    message: jetstream::Message,
    handler: H,
    retry_policy: RetryPolicy,
) -> Result<(u16, JobDeliveryOutcome), NatsError>
where
    J: Job,
    H: JobHandler<J>,
{
    let delivery = JetStreamDelivery::new(message);
    let attempt = delivery.delivery_attempt()?;
    let envelope = match JobEnvelope::<J>::decode(delivery.payload()) {
        Ok(envelope) => envelope
            .with_attempt(attempt)
            .map_err(|_| NatsError::DeliveryMetadata)?,
        Err(_) => {
            return dead_letter_and_acknowledge(
                &context,
                &dead_letter_subject,
                &delivery,
                None,
                attempt,
            )
            .await
            .map(|()| (attempt, JobDeliveryOutcome::DeadLettered));
        }
    };
    let job_id = envelope.id();

    match dispatch(envelope, &handler).await {
        Ok(DeliveryAction::Acknowledge) => delivery
            .acknowledge()
            .await
            .map(|()| (attempt, JobDeliveryOutcome::Acknowledged)),
        Ok(DeliveryAction::Retry { delay, .. }) => delivery
            .retry_after(delay)
            .await
            .map(|()| (attempt, JobDeliveryOutcome::Retried)),
        Ok(DeliveryAction::DeadLetter) => dead_letter_and_acknowledge(
            &context,
            &dead_letter_subject,
            &delivery,
            Some(job_id),
            attempt,
        )
        .await
        .map(|()| (attempt, JobDeliveryOutcome::DeadLettered)),
        Err(_) => match retry_policy.after_failure(attempt) {
            DeliveryAction::Acknowledge => delivery
                .acknowledge()
                .await
                .map(|()| (attempt, JobDeliveryOutcome::Acknowledged)),
            DeliveryAction::Retry { delay, .. } => delivery
                .retry_after(delay)
                .await
                .map(|()| (attempt, JobDeliveryOutcome::Retried)),
            DeliveryAction::DeadLetter => dead_letter_and_acknowledge(
                &context,
                &dead_letter_subject,
                &delivery,
                Some(job_id),
                attempt,
            )
            .await
            .map(|()| (attempt, JobDeliveryOutcome::DeadLettered)),
        },
    }
}

async fn process_registry_delivery(
    context: jetstream::Context,
    dead_letter_subject: String,
    message: jetstream::Message,
    registry: JobRegistry,
    retry_policy: RetryPolicy,
) -> Result<(u16, JobDeliveryOutcome), NatsError> {
    let delivery = JetStreamDelivery::new(message);
    let attempt = delivery.delivery_attempt()?;
    match registry.dispatch(delivery.payload(), attempt).await {
        Ok(DeliveryAction::Acknowledge) => delivery
            .acknowledge()
            .await
            .map(|()| (attempt, JobDeliveryOutcome::Acknowledged)),
        Ok(DeliveryAction::Retry { delay, .. }) => delivery
            .retry_after(delay)
            .await
            .map(|()| (attempt, JobDeliveryOutcome::Retried)),
        Err(JobRegistryError::Handler { id, .. }) => match retry_policy.after_failure(attempt) {
            DeliveryAction::Acknowledge => delivery
                .acknowledge()
                .await
                .map(|()| (attempt, JobDeliveryOutcome::Acknowledged)),
            DeliveryAction::Retry { delay, .. } => delivery
                .retry_after(delay)
                .await
                .map(|()| (attempt, JobDeliveryOutcome::Retried)),
            DeliveryAction::DeadLetter => dead_letter_and_acknowledge(
                &context,
                &dead_letter_subject,
                &delivery,
                Some(id),
                attempt,
            )
            .await
            .map(|()| (attempt, JobDeliveryOutcome::DeadLettered)),
        },
        Ok(DeliveryAction::DeadLetter) | Err(_) => {
            dead_letter_and_acknowledge(&context, &dead_letter_subject, &delivery, None, attempt)
                .await
                .map(|()| (attempt, JobDeliveryOutcome::DeadLettered))
        }
    }
}

async fn dead_letter_and_acknowledge(
    context: &jetstream::Context,
    dead_letter_subject: &str,
    delivery: &JetStreamDelivery,
    job_id: Option<JobId>,
    attempt: u16,
) -> Result<(), NatsError> {
    let mut publish = PublishMessage::build()
        .payload(Bytes::copy_from_slice(delivery.payload()))
        .header("Rustee-Delivery-Attempt", attempt.to_string());
    if let Some(job_id) = job_id {
        publish = publish.message_id(job_id.to_string());
    }
    context
        .send_publish(dead_letter_subject.to_owned(), publish)
        .await
        .map_err(|_| NatsError::DeadLetterPublish)?
        .await
        .map_err(|_| NatsError::DeadLetterPublishAcknowledgement)?;
    delivery.acknowledge().await
}

async fn drain_tasks(
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
        Err(NatsError::DrainTimeout)
    }
}

/// Sanitized operational failures from the NATS adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum NatsError {
    /// NATS connection setup failed.
    #[error("NATS connection failed")]
    Connect,
    /// `JetStream` publish request failed.
    #[error("NATS JetStream publish failed")]
    Publish,
    /// `JetStream` did not acknowledge the publish request.
    #[error("NATS JetStream publish acknowledgement failed")]
    PublishAcknowledgement,
    /// `JetStream` account readiness query failed.
    #[error("NATS JetStream readiness check failed")]
    Readiness,
    /// NATS did not accept a successful-delivery acknowledgement.
    #[error("NATS JetStream acknowledgement failed")]
    Acknowledge,
    /// NATS did not accept a retry negative acknowledgement.
    #[error("NATS JetStream negative acknowledgement failed")]
    NegativeAcknowledge,
    /// A consumed message did not contain valid `JetStream` delivery metadata.
    #[error("NATS JetStream delivery metadata was invalid")]
    DeliveryMetadata,
    /// Receiving a pull-consumer message failed or the delivery stream ended unexpectedly.
    #[error("NATS JetStream job receive failed")]
    Receive,
    /// A dead-letter publish request failed before `JetStream` accepted it.
    #[error("NATS JetStream dead-letter publish failed")]
    DeadLetterPublish,
    /// `JetStream` did not acknowledge a dead-letter publish.
    #[error("NATS JetStream dead-letter publish acknowledgement failed")]
    DeadLetterPublishAcknowledgement,
    /// The supplied consumer limits cannot satisfy the Rustee worker configuration.
    #[error("NATS JetStream consumer configuration is incompatible with the Rustee worker")]
    ConsumerConfiguration,
    /// A worker task panicked or was cancelled before completing its acknowledgement decision.
    #[error("NATS JetStream worker task failed")]
    WorkerTask,
    /// Active handlers did not finish before the configured shutdown drain deadline.
    #[error("NATS JetStream worker drain timed out")]
    DrainTimeout,
}

fn validate_subject(subject: &str) -> Result<(), ConfigError> {
    if subject.trim().is_empty()
        || subject.chars().any(char::is_whitespace)
        || subject.contains('*')
        || subject.contains('>')
        || subject.split('.').any(str::is_empty)
    {
        return Err(ConfigError::InvalidSubject);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ConfigError, NatsConfig};
    use std::time::Duration;

    #[test]
    fn configuration_redacts_connection_secrets() {
        let config = NatsConfig::new("nats://user:password@localhost:4222", "jobs.email").unwrap();
        assert!(!format!("{config:?}").contains("password"));
    }

    #[test]
    fn configuration_rejects_subscription_wildcards() {
        let error = NatsConfig::new("nats://localhost:4222", "jobs.>").unwrap_err();
        assert_eq!(error, ConfigError::InvalidSubject);
    }

    #[test]
    fn configuration_requires_a_non_zero_connect_deadline() {
        let error = NatsConfig::new("nats://localhost:4222", "jobs.email")
            .unwrap()
            .with_connect_timeout(Duration::ZERO)
            .unwrap_err();
        assert_eq!(error, ConfigError::ZeroConnectTimeout);
    }
}
