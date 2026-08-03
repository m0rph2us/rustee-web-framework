#![allow(clippy::doc_markdown)]

//! RabbitMQ quorum-queue publishing and delivery for `Rustee` jobs.
//!
//! Quorum queues, direct exchanges, dead-letter exchanges, bindings, delivery limits, and native
//! delayed-retry policies are deployment-owned. The adapter uses passive checks at readiness and
//! never creates or mutates that topology. A retry rejects and requeues the original delivery so
//! RabbitMQ 4.3's quorum-queue delayed retry retains it durably. Poison messages and exhausted
//! retries are publisher-confirmed on the explicit dead-letter route before their source delivery
//! is acknowledged. Both paths intentionally retain at-least-once semantics.

use std::{fmt, future::Future, num::NonZeroU16, sync::Arc, time::Duration};

use futures_util::{StreamExt, future::BoxFuture};
use lapin::{
    BasicProperties, Channel, Confirmation, Connection, ConnectionProperties, ExchangeKind,
    message::Delivery,
    options::{
        BasicAckOptions, BasicCancelOptions, BasicConsumeOptions, BasicPublishOptions,
        BasicQosOptions, BasicRejectOptions, ConfirmSelectOptions, ExchangeDeclareOptions,
        QueueDeclareOptions,
    },
    types::{AMQPValue, FieldTable},
};
use rustee_jobs::{
    DeliveryAction, Job, JobDeliveryObservation, JobDeliveryObserver, JobDeliveryOutcome,
    JobEnvelope, JobHandler, JobMessage, JobPublisher, JobRegistry, JobRegistryError,
    NoopJobDeliveryObserver, RetryPolicy, WorkerConfig, dispatch,
};
use tokio::{task::JoinSet, time::timeout};

pub use lapin;

const CONTENT_TYPE: &str = "application/json";
const ACQUIRED_COUNT_HEADER: &str = "x-acquired-count";
const PERSISTENT_DELIVERY_MODE: u8 = 2;
const MAX_AMQP_SHORT_STRING_BYTES: usize = 255;
const MAX_QUORUM_PREFETCH: usize = 2_000;
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);

/// Redacted connection settings for one RabbitMQ broker endpoint.
#[derive(Clone, Eq, PartialEq)]
pub struct RabbitMqConnectionConfig {
    url: String,
    connect_timeout: Duration,
}

impl RabbitMqConnectionConfig {
    /// Creates a connection configuration from an AMQP(S) URL held in a secret source.
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
        }
    }

    /// Sets the bounded time allowed to establish the AMQP connection.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroDuration`] when `connect_timeout` is zero.
    pub fn with_connect_timeout(mut self, connect_timeout: Duration) -> Result<Self, ConfigError> {
        if connect_timeout.is_zero() {
            return Err(ConfigError::ZeroDuration);
        }
        self.connect_timeout = connect_timeout;
        Ok(self)
    }

    /// Returns the AMQP connection establishment deadline.
    #[must_use]
    pub const fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Opens one RabbitMQ connection without declaring application topology.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqError::Connect`] when the URL is invalid or the broker is unavailable.
    pub async fn connect(&self) -> Result<RabbitMqConnection, RabbitMqError> {
        timeout(
            self.connect_timeout,
            Connection::connect(&self.url, ConnectionProperties::default()),
        )
        .await
        .map_err(|_| RabbitMqError::Connect)?
        .map(RabbitMqConnection::new)
        .map_err(|_| RabbitMqError::Connect)
    }
}

impl fmt::Debug for RabbitMqConnectionConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RabbitMqConnectionConfig")
            .field("url", &"[REDACTED]")
            .field("connect_timeout", &self.connect_timeout)
            .finish()
    }
}

/// A shareable connected RabbitMQ session used to open isolated AMQP channels.
#[derive(Clone)]
pub struct RabbitMqConnection {
    inner: Arc<Connection>,
}

impl RabbitMqConnection {
    /// Wraps an already-connected `lapin` connection for dependency injection and testing.
    #[must_use]
    pub fn new(connection: Connection) -> Self {
        Self {
            inner: Arc::new(connection),
        }
    }

    /// Returns whether the underlying AMQP connection is presently connected.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.inner.status().connected()
    }
}

impl fmt::Debug for RabbitMqConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RabbitMqConnection")
            .field("connected", &self.is_connected())
            .finish()
    }
}

/// Settings for publishing jobs through a deployment-provisioned direct exchange.
#[derive(Clone, Eq, PartialEq)]
pub struct RabbitMqPublisherConfig {
    exchange: String,
    routing_key: String,
    publish_timeout: Duration,
}

impl RabbitMqPublisherConfig {
    /// Creates a direct-exchange route for durable job publishing.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidIdentifier`] for blank, whitespace-containing, or oversized
    /// AMQP names.
    pub fn new(
        exchange: impl Into<String>,
        routing_key: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        let exchange = exchange.into();
        let routing_key = routing_key.into();
        validate_identifier(&exchange)?;
        validate_identifier(&routing_key)?;
        Ok(Self {
            exchange,
            routing_key,
            publish_timeout: DEFAULT_PUBLISH_TIMEOUT,
        })
    }

    /// Sets the bounded time allowed for a broker publisher confirmation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroDuration`] when `publish_timeout` is zero.
    pub fn with_publish_timeout(mut self, publish_timeout: Duration) -> Result<Self, ConfigError> {
        if publish_timeout.is_zero() {
            return Err(ConfigError::ZeroDuration);
        }
        self.publish_timeout = publish_timeout;
        Ok(self)
    }

    /// Returns the deployment-provisioned direct exchange name.
    #[must_use]
    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    /// Returns the direct-exchange routing key.
    #[must_use]
    pub fn routing_key(&self) -> &str {
        &self.routing_key
    }
}

impl fmt::Debug for RabbitMqPublisherConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RabbitMqPublisherConfig")
            .field("exchange", &self.exchange)
            .field("routing_key", &self.routing_key)
            .field("publish_timeout", &self.publish_timeout)
            .finish()
    }
}

/// A publisher-confirming RabbitMQ job producer.
#[derive(Clone)]
pub struct RabbitMqPublisher {
    channel: Channel,
    config: RabbitMqPublisherConfig,
}

impl RabbitMqPublisher {
    /// Opens a dedicated publisher-confirm channel for one direct-exchange route.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqError::PublisherChannel`] when RabbitMQ cannot create the channel or
    /// enable publisher confirms.
    pub async fn new(
        connection: RabbitMqConnection,
        config: RabbitMqPublisherConfig,
    ) -> Result<Self, RabbitMqError> {
        let channel = open_confirm_channel(&connection).await?;
        Ok(Self { channel, config })
    }

    /// Verifies that the configured direct exchange exists and is accessible without creating it.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqError::Readiness`] when the exchange cannot be inspected.
    pub async fn readiness(&self) -> Result<(), RabbitMqError> {
        self.channel
            .exchange_declare(
                self.config.exchange.as_str().into(),
                ExchangeKind::Direct,
                ExchangeDeclareOptions {
                    passive: true,
                    ..ExchangeDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|_| RabbitMqError::Readiness)
    }

    /// Returns the configured direct-exchange route.
    #[must_use]
    pub fn config(&self) -> &RabbitMqPublisherConfig {
        &self.config
    }
}

impl fmt::Debug for RabbitMqPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RabbitMqPublisher")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl JobPublisher for RabbitMqPublisher {
    type Error = RabbitMqError;

    fn publish(&self, message: JobMessage) -> BoxFuture<'static, Result<(), Self::Error>> {
        let channel = self.channel.clone();
        let exchange = self.config.exchange.clone();
        let routing_key = self.config.routing_key.clone();
        let publish_timeout = self.config.publish_timeout;
        let message_id = message.id().to_string();
        let payload = message.into_payload();
        Box::pin(async move {
            publish_confirmed(
                &channel,
                &exchange,
                &routing_key,
                &payload,
                &message_id,
                publish_timeout,
                PublishKind::Job,
            )
            .await
        })
    }
}

/// The pre-provisioned RabbitMQ 4.3 quorum-queue delayed-retry policy.
///
/// RabbitMQ applies `min(minimum_delay * delivery_count, maximum_delay)` after a returned
/// delivery. Rustee therefore accepts this provider only for a compatible bounded
/// [`RetryPolicy`]: `minimum_delay == initial_backoff`, `maximum_delay == max_backoff`, and
/// `maximum_delay <= 3 * minimum_delay`. That range makes RabbitMQ's linear sequence equal to
/// the core policy's capped exponential sequence for every retry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RabbitMqNativeRetryConfig {
    minimum_delay: Duration,
    maximum_delay: Duration,
}

impl RabbitMqNativeRetryConfig {
    /// Describes the deployment-owned `delayed-retry-min` and `delayed-retry-max` policy values.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidRetryRange`] when the minimum is zero or exceeds the maximum.
    pub fn new(minimum_delay: Duration, maximum_delay: Duration) -> Result<Self, ConfigError> {
        if minimum_delay.is_zero() || minimum_delay > maximum_delay {
            return Err(ConfigError::InvalidRetryRange);
        }
        Ok(Self {
            minimum_delay,
            maximum_delay,
        })
    }

    /// Returns the policy's first-return delay.
    #[must_use]
    pub const fn minimum_delay(self) -> Duration {
        self.minimum_delay
    }

    /// Returns the policy's capped delay.
    #[must_use]
    pub const fn maximum_delay(self) -> Duration {
        self.maximum_delay
    }

    fn matches(self, retry_policy: RetryPolicy) -> bool {
        retry_policy.initial_backoff == self.minimum_delay
            && retry_policy.max_backoff == self.maximum_delay
            && self.maximum_delay <= self.minimum_delay.saturating_mul(3)
    }

    fn delay_for(self, next_attempt: u16) -> Duration {
        let retries_before_delivery = u32::from(next_attempt.saturating_sub(1));
        let delay = self.minimum_delay.saturating_mul(retries_before_delivery);
        delay.min(self.maximum_delay)
    }
}

/// Consumer, native delayed-retry, and dead-letter routes for one RabbitMQ job worker.
#[derive(Clone, Eq, PartialEq)]
pub struct RabbitMqWorkerConfig {
    queue: String,
    consumer_tag: String,
    native_retry: RabbitMqNativeRetryConfig,
    dead_letter_exchange: String,
    dead_letter_routing_key: String,
    publish_timeout: Duration,
}

impl RabbitMqWorkerConfig {
    /// Creates settings for a pre-provisioned RabbitMQ 4.3 quorum queue and dead-letter route.
    ///
    /// The queue must already have the matching native delayed-retry policy (`failed`,
    /// `delayed-retry-min`, and `delayed-retry-max`) and a broker-native DLX for delivery-limit
    /// failures. The adapter only uses its explicit direct exchange for poison messages and
    /// exhausted Rustee retries; it never creates queues, exchanges, bindings, or policies.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] for unsafe AMQP identifiers or an invalid native retry range.
    pub fn new(
        queue: impl Into<String>,
        consumer_tag: impl Into<String>,
        native_retry: RabbitMqNativeRetryConfig,
        dead_letter_exchange: impl Into<String>,
        dead_letter_routing_key: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        let queue = queue.into();
        let consumer_tag = consumer_tag.into();
        let dead_letter_exchange = dead_letter_exchange.into();
        let dead_letter_routing_key = dead_letter_routing_key.into();
        for identifier in [
            &queue,
            &consumer_tag,
            &dead_letter_exchange,
            &dead_letter_routing_key,
        ] {
            validate_identifier(identifier)?;
        }
        Ok(Self {
            queue,
            consumer_tag,
            native_retry,
            dead_letter_exchange,
            dead_letter_routing_key,
            publish_timeout: DEFAULT_PUBLISH_TIMEOUT,
        })
    }

    /// Sets the bounded time allowed for dead-letter publish confirmations.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroDuration`] when `publish_timeout` is zero.
    pub fn with_publish_timeout(mut self, publish_timeout: Duration) -> Result<Self, ConfigError> {
        if publish_timeout.is_zero() {
            return Err(ConfigError::ZeroDuration);
        }
        self.publish_timeout = publish_timeout;
        Ok(self)
    }

    /// Returns the deployment-provisioned source queue.
    #[must_use]
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// Returns the explicit consumer tag used by this worker.
    #[must_use]
    pub fn consumer_tag(&self) -> &str {
        &self.consumer_tag
    }

    /// Returns the expected deployment-owned native retry policy values.
    #[must_use]
    pub const fn native_retry(&self) -> RabbitMqNativeRetryConfig {
        self.native_retry
    }

    /// Returns the direct exchange used for poison messages and exhausted retries.
    #[must_use]
    pub fn dead_letter_exchange(&self) -> &str {
        &self.dead_letter_exchange
    }

    /// Returns the dead-letter direct-exchange routing key.
    #[must_use]
    pub fn dead_letter_routing_key(&self) -> &str {
        &self.dead_letter_routing_key
    }
}

impl fmt::Debug for RabbitMqWorkerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RabbitMqWorkerConfig")
            .field("queue", &self.queue)
            .field("consumer_tag", &self.consumer_tag)
            .field("native_retry", &self.native_retry)
            .field("dead_letter_exchange", &self.dead_letter_exchange)
            .field("dead_letter_routing_key", &self.dead_letter_routing_key)
            .field("publish_timeout", &self.publish_timeout)
            .finish()
    }
}

/// A RabbitMQ worker with explicit manual acknowledgement and confirmed settlement publishes.
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
    /// Observer panics are isolated from RabbitMQ acknowledgement, retry, and dead-letter behavior.
    #[must_use]
    pub fn with_delivery_observer(mut self, observer: Arc<dyn JobDeliveryObserver>) -> Self {
        self.observer = observer;
        self
    }

    /// Verifies that the source queue and dead-letter exchange exist.
    ///
    /// AMQP passive declarations can verify existence and access but cannot prove an existing queue
    /// is quorum/durable or has the configured native delayed-retry policy. Enforce those details
    /// through deployment provisioning tests.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqError::Readiness`] when a required topology component is unavailable.
    pub async fn readiness(&self) -> Result<(), RabbitMqError> {
        let channel = self
            .connection
            .inner
            .create_channel()
            .await
            .map_err(|_| RabbitMqError::Readiness)?;
        channel
            .queue_declare(
                self.config.queue.as_str().into(),
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
                self.config.dead_letter_exchange.as_str().into(),
                ExchangeKind::Direct,
                ExchangeDeclareOptions {
                    passive: true,
                    ..ExchangeDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|_| RabbitMqError::Readiness)
    }

    /// Runs one typed handler until shutdown, then drains active handlers for the configured time.
    ///
    /// Handler success is acknowledged manually. A retry returns the original delivery to the
    /// deployment-owned RabbitMQ 4.3 native delayed-retry policy. Poison messages and exhausted
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
        if !self.config.native_retry.matches(retry_policy) {
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
                self.config.queue.as_str().into(),
                self.config.consumer_tag.as_str().into(),
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

/// A received RabbitMQ delivery whose acknowledgement remains explicit.
#[derive(Debug)]
pub struct RabbitMqDelivery {
    message: Delivery,
}

impl RabbitMqDelivery {
    /// Wraps one manual-ack AMQP delivery after worker selection.
    #[must_use]
    pub fn new(message: Delivery) -> Self {
        Self { message }
    }

    /// Returns the serialized Rustee job envelope without acknowledgement internals.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.message.data
    }

    /// Returns RabbitMQ 4.3 quorum queue's one-based acquired delivery count, defaulting absent
    /// headers to the first attempt.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqError::DeliveryMetadata`] when the broker header is zero, exceeds the
    /// core attempt bound, or has another AMQP type.
    pub fn delivery_attempt(&self) -> Result<u16, RabbitMqError> {
        let Some(headers) = self.message.properties.headers().as_ref() else {
            return Ok(1);
        };
        let Some(value) = headers.inner().get(ACQUIRED_COUNT_HEADER) else {
            return Ok(1);
        };
        let attempt = match value {
            AMQPValue::ShortShortUInt(value) => u16::from(*value),
            AMQPValue::ShortUInt(value) => *value,
            AMQPValue::LongUInt(value) => {
                u16::try_from(*value).map_err(|_| RabbitMqError::DeliveryMetadata)?
            }
            AMQPValue::LongInt(value) => {
                u16::try_from(*value).map_err(|_| RabbitMqError::DeliveryMetadata)?
            }
            AMQPValue::LongLongInt(value) => {
                u16::try_from(*value).map_err(|_| RabbitMqError::DeliveryMetadata)?
            }
            _ => return Err(RabbitMqError::DeliveryMetadata),
        };
        if attempt == 0 {
            return Err(RabbitMqError::DeliveryMetadata);
        }
        Ok(attempt)
    }

    /// Acknowledges a completed delivery on its original consumer channel.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqError::Acknowledge`] when RabbitMQ rejects or cannot receive the ack.
    pub async fn acknowledge(&self) -> Result<(), RabbitMqError> {
        self.message
            .ack(BasicAckOptions::default())
            .await
            .map_err(|_| RabbitMqError::Acknowledge)?
            .then_some(())
            .ok_or(RabbitMqError::Acknowledge)
    }

    /// Returns this message to the source quorum queue so its native delayed-retry policy applies.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqError::RetryReturn`] when RabbitMQ rejects or cannot receive the return.
    pub async fn return_for_retry(&self) -> Result<(), RabbitMqError> {
        self.message
            .reject(BasicRejectOptions { requeue: true })
            .await
            .map_err(|_| RabbitMqError::RetryReturn)?
            .then_some(())
            .ok_or(RabbitMqError::RetryReturn)
    }

    fn message_id(&self) -> String {
        self.message
            .properties
            .message_id()
            .as_ref()
            .map_or_else(|| "rustee-job".to_owned(), ToString::to_string)
    }
}

async fn process_delivery<J, H>(
    settlement_channel: Channel,
    config: RabbitMqWorkerConfig,
    message: Delivery,
    handler: H,
    retry_policy: RetryPolicy,
) -> Result<(u16, JobDeliveryOutcome), RabbitMqError>
where
    J: Job,
    H: JobHandler<J>,
{
    let delivery = RabbitMqDelivery::new(message);
    let Ok(attempt) = delivery.delivery_attempt() else {
        return dead_letter_and_acknowledge(&settlement_channel, &config, &delivery, 1)
            .await
            .map(|()| (1, JobDeliveryOutcome::DeadLettered));
    };
    let envelope = match JobEnvelope::<J>::decode(delivery.payload()) {
        Ok(envelope) => envelope
            .with_attempt(attempt)
            .map_err(|_| RabbitMqError::DeliveryMetadata)?,
        Err(_) => {
            return dead_letter_and_acknowledge(&settlement_channel, &config, &delivery, attempt)
                .await
                .map(|()| (attempt, JobDeliveryOutcome::DeadLettered));
        }
    };

    match dispatch(envelope, &handler).await {
        Ok(DeliveryAction::Acknowledge) => delivery
            .acknowledge()
            .await
            .map(|()| (attempt, JobDeliveryOutcome::Acknowledged)),
        Ok(DeliveryAction::Retry {
            next_attempt,
            delay,
        }) => retry_and_return(&config, &delivery, next_attempt, delay)
            .await
            .map(|()| (attempt, JobDeliveryOutcome::Retried)),
        Ok(DeliveryAction::DeadLetter) => {
            dead_letter_and_acknowledge(&settlement_channel, &config, &delivery, attempt)
                .await
                .map(|()| (attempt, JobDeliveryOutcome::DeadLettered))
        }
        Err(_) => settle_handler_failure(
            &settlement_channel,
            &config,
            &delivery,
            retry_policy.after_failure(attempt),
            attempt,
        )
        .await
        .map(|outcome| (attempt, outcome)),
    }
}

async fn process_registry_delivery(
    settlement_channel: Channel,
    config: RabbitMqWorkerConfig,
    message: Delivery,
    registry: JobRegistry,
    retry_policy: RetryPolicy,
) -> Result<(u16, JobDeliveryOutcome), RabbitMqError> {
    let delivery = RabbitMqDelivery::new(message);
    let Ok(attempt) = delivery.delivery_attempt() else {
        return dead_letter_and_acknowledge(&settlement_channel, &config, &delivery, 1)
            .await
            .map(|()| (1, JobDeliveryOutcome::DeadLettered));
    };
    match registry.dispatch(delivery.payload(), attempt).await {
        Ok(DeliveryAction::Acknowledge) => delivery
            .acknowledge()
            .await
            .map(|()| (attempt, JobDeliveryOutcome::Acknowledged)),
        Ok(DeliveryAction::Retry {
            next_attempt,
            delay,
        }) => retry_and_return(&config, &delivery, next_attempt, delay)
            .await
            .map(|()| (attempt, JobDeliveryOutcome::Retried)),
        Err(JobRegistryError::Handler { .. }) => settle_handler_failure(
            &settlement_channel,
            &config,
            &delivery,
            retry_policy.after_failure(attempt),
            attempt,
        )
        .await
        .map(|outcome| (attempt, outcome)),
        Ok(DeliveryAction::DeadLetter) | Err(_) => {
            dead_letter_and_acknowledge(&settlement_channel, &config, &delivery, attempt)
                .await
                .map(|()| (attempt, JobDeliveryOutcome::DeadLettered))
        }
    }
}

async fn settle_handler_failure(
    settlement_channel: &Channel,
    config: &RabbitMqWorkerConfig,
    delivery: &RabbitMqDelivery,
    action: DeliveryAction,
    attempt: u16,
) -> Result<JobDeliveryOutcome, RabbitMqError> {
    match action {
        DeliveryAction::Acknowledge => delivery
            .acknowledge()
            .await
            .map(|()| JobDeliveryOutcome::Acknowledged),
        DeliveryAction::Retry {
            next_attempt,
            delay,
        } => retry_and_return(config, delivery, next_attempt, delay)
            .await
            .map(|()| JobDeliveryOutcome::Retried),
        DeliveryAction::DeadLetter => {
            dead_letter_and_acknowledge(settlement_channel, config, delivery, attempt)
                .await
                .map(|()| JobDeliveryOutcome::DeadLettered)
        }
    }
}

async fn retry_and_return(
    config: &RabbitMqWorkerConfig,
    delivery: &RabbitMqDelivery,
    next_attempt: u16,
    delay: Duration,
) -> Result<(), RabbitMqError> {
    if next_attempt < 2 {
        return Err(RabbitMqError::RetryPolicyMismatch);
    }
    let expected_delay = config.native_retry.delay_for(next_attempt);
    if delay != expected_delay {
        return Err(RabbitMqError::RetryPolicyMismatch);
    }
    delivery.return_for_retry().await
}

async fn dead_letter_and_acknowledge(
    settlement_channel: &Channel,
    config: &RabbitMqWorkerConfig,
    delivery: &RabbitMqDelivery,
    _attempt: u16,
) -> Result<(), RabbitMqError> {
    publish_confirmed(
        settlement_channel,
        &config.dead_letter_exchange,
        &config.dead_letter_routing_key,
        delivery.payload(),
        &delivery.message_id(),
        config.publish_timeout,
        PublishKind::DeadLetter,
    )
    .await?;
    delivery.acknowledge().await
}

async fn open_confirm_channel(connection: &RabbitMqConnection) -> Result<Channel, RabbitMqError> {
    let channel = connection
        .inner
        .create_channel()
        .await
        .map_err(|_| RabbitMqError::PublisherChannel)?;
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .map_err(|_| RabbitMqError::PublisherChannel)?;
    Ok(channel)
}

async fn publish_confirmed(
    channel: &Channel,
    exchange: &str,
    routing_key: &str,
    payload: &[u8],
    message_id: &str,
    publish_timeout: Duration,
    kind: PublishKind,
) -> Result<(), RabbitMqError> {
    let properties = persistent_properties(message_id);
    let publish = async {
        let confirmation = channel
            .basic_publish(
                exchange.into(),
                routing_key.into(),
                BasicPublishOptions {
                    mandatory: true,
                    ..BasicPublishOptions::default()
                },
                payload,
                properties,
            )
            .await
            .map_err(|_| kind.publish_error())?
            .await
            .map_err(|_| kind.confirm_error())?;
        match confirmation {
            Confirmation::Ack(None) => Ok(()),
            Confirmation::Ack(Some(_)) | Confirmation::Nack(Some(_)) => {
                Err(kind.unroutable_error())
            }
            Confirmation::Nack(None) => Err(kind.nack_error()),
            Confirmation::NotRequested => Err(kind.confirm_error()),
        }
    };
    match timeout(publish_timeout, publish).await {
        Ok(result) => result,
        Err(_) => Err(kind.timeout_error()),
    }
}

fn persistent_properties(message_id: &str) -> BasicProperties {
    BasicProperties::default()
        .with_content_type(CONTENT_TYPE.into())
        .with_delivery_mode(PERSISTENT_DELIVERY_MODE)
        .with_message_id(message_id.into())
}

async fn drain_tasks(
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
        Err(RabbitMqError::DrainTimeout)
    }
}

#[derive(Clone, Copy)]
enum PublishKind {
    Job,
    DeadLetter,
}

impl PublishKind {
    const fn publish_error(self) -> RabbitMqError {
        match self {
            Self::Job => RabbitMqError::Publish,
            Self::DeadLetter => RabbitMqError::DeadLetterPublish,
        }
    }

    const fn confirm_error(self) -> RabbitMqError {
        match self {
            Self::Job => RabbitMqError::PublishConfirmation,
            Self::DeadLetter => RabbitMqError::DeadLetterConfirmation,
        }
    }

    const fn nack_error(self) -> RabbitMqError {
        match self {
            Self::Job => RabbitMqError::PublishNack,
            Self::DeadLetter => RabbitMqError::DeadLetterNack,
        }
    }

    const fn unroutable_error(self) -> RabbitMqError {
        match self {
            Self::Job => RabbitMqError::PublishUnroutable,
            Self::DeadLetter => RabbitMqError::DeadLetterUnroutable,
        }
    }

    const fn timeout_error(self) -> RabbitMqError {
        match self {
            Self::Job => RabbitMqError::PublishTimeout,
            Self::DeadLetter => RabbitMqError::DeadLetterTimeout,
        }
    }
}

/// Invalid RabbitMQ job-provider configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConfigError {
    /// An AMQP short string was blank, had whitespace, or exceeded the protocol bound.
    #[error("RabbitMQ job identifier must be a non-empty AMQP short string without whitespace")]
    InvalidIdentifier,
    /// A connection or broker confirmation timeout was zero.
    #[error("RabbitMQ timeout must be non-zero")]
    ZeroDuration,
    /// A native delayed-retry policy must have a non-zero minimum no greater than its maximum.
    #[error("RabbitMQ native retry minimum must be non-zero and not exceed its maximum")]
    InvalidRetryRange,
}

/// Sanitized operational failures from the RabbitMQ adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RabbitMqError {
    /// AMQP connection setup failed.
    #[error("RabbitMQ connection failed")]
    Connect,
    /// A pre-provisioned queue or exchange could not be inspected.
    #[error("RabbitMQ job topology readiness check failed")]
    Readiness,
    /// A dedicated publisher-confirm channel could not be opened.
    #[error("RabbitMQ publisher channel setup failed")]
    PublisherChannel,
    /// A consumer channel could not be opened or closed.
    #[error("RabbitMQ consumer channel operation failed")]
    ConsumerChannel,
    /// The worker concurrency cannot be represented by the configured quorum-queue prefetch limit.
    #[error("RabbitMQ worker concurrency is incompatible with quorum-queue prefetch")]
    WorkerConfiguration,
    /// The broker did not establish or continue a manual-ack consumer stream.
    #[error("RabbitMQ job receive failed")]
    Receive,
    /// The broker did not accept cancellation of the consumer during shutdown.
    #[error("RabbitMQ consumer cancellation failed")]
    ConsumerCancel,
    /// The source job publish request could not be sent.
    #[error("RabbitMQ job publish failed")]
    Publish,
    /// The source job publish was not publisher-confirmed.
    #[error("RabbitMQ job publish confirmation failed")]
    PublishConfirmation,
    /// The source job publish received a broker negative acknowledgement.
    #[error("RabbitMQ job publish was negatively acknowledged")]
    PublishNack,
    /// The source job publish was mandatory but had no matching route.
    #[error("RabbitMQ job publish was unroutable")]
    PublishUnroutable,
    /// The source job publish confirmation exceeded its configured timeout.
    #[error("RabbitMQ job publish confirmation timed out")]
    PublishTimeout,
    /// The dead-letter publish request could not be sent.
    #[error("RabbitMQ dead-letter publish failed")]
    DeadLetterPublish,
    /// The dead-letter publish was not publisher-confirmed.
    #[error("RabbitMQ dead-letter confirmation failed")]
    DeadLetterConfirmation,
    /// The dead-letter publish received a broker negative acknowledgement.
    #[error("RabbitMQ dead-letter publish was negatively acknowledged")]
    DeadLetterNack,
    /// The dead-letter publish was mandatory but had no matching route.
    #[error("RabbitMQ dead-letter publish was unroutable")]
    DeadLetterUnroutable,
    /// The dead-letter confirmation exceeded its configured timeout.
    #[error("RabbitMQ dead-letter confirmation timed out")]
    DeadLetterTimeout,
    /// RabbitMQ could not accept an acknowledgement for the original delivery.
    #[error("RabbitMQ delivery acknowledgement failed")]
    Acknowledge,
    /// RabbitMQ could not return the source delivery to the native delayed-retry queue state.
    #[error("RabbitMQ delayed retry return failed")]
    RetryReturn,
    /// The native broker retry policy does not exactly match the requested Rustee retry policy.
    #[error("RabbitMQ native delayed retry policy is incompatible with the Rustee retry policy")]
    RetryPolicyMismatch,
    /// The broker acquired-delivery header was zero or used an unsupported AMQP value type.
    #[error("RabbitMQ delivery attempt metadata was invalid")]
    DeliveryMetadata,
    /// A worker task panicked or was cancelled before choosing an acknowledgement action.
    #[error("RabbitMQ worker task failed")]
    WorkerTask,
    /// Active handlers did not finish before the worker drain deadline.
    #[error("RabbitMQ worker drain timed out")]
    DrainTimeout,
}

fn validate_identifier(value: &str) -> Result<(), ConfigError> {
    if value.is_empty()
        || value.len() > MAX_AMQP_SHORT_STRING_BYTES
        || value.chars().any(char::is_whitespace)
    {
        return Err(ConfigError::InvalidIdentifier);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use lapin::{
        BasicProperties,
        message::Delivery,
        types::{AMQPValue, FieldTable},
    };
    use rustee_jobs::RetryPolicy;

    use super::{
        ACQUIRED_COUNT_HEADER, ConfigError, RabbitMqConnectionConfig, RabbitMqDelivery,
        RabbitMqNativeRetryConfig, persistent_properties,
    };

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

    #[test]
    fn native_retry_configuration_rejects_an_invalid_range() {
        let error =
            RabbitMqNativeRetryConfig::new(Duration::ZERO, Duration::from_secs(1)).unwrap_err();
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
    }
}
