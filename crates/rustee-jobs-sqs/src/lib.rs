#![allow(clippy::doc_markdown)]

//! Amazon SQS publishing and visibility-lease delivery for `Rustee` jobs.
//!
//! Queue creation, queue type, redrive policy, IAM, encryption, and retention are deployment
//! owned. This adapter verifies those settings at readiness and never mutates them. It keeps the
//! SQS acknowledgement model explicit: a successful handler deletes its receipt, a retry changes
//! visibility, and a poison or exhausted delivery is sent to the configured DLQ before the source
//! receipt is deleted. All three paths retain at-least-once semantics.

use std::{fmt, future::Future, num::NonZeroU16, sync::Arc, time::Duration};

use aws_sdk_sqs::{
    Client,
    types::{Message, MessageSystemAttributeName, QueueAttributeName},
};
use futures_util::future::BoxFuture;
use rustee_jobs::{
    DeliveryAction, Job, JobDeliveryObservation, JobDeliveryObserver, JobDeliveryOutcome,
    JobEnvelope, JobHandler, JobMessage, JobPublisher, JobRegistry, JobRegistryError,
    NoopJobDeliveryObserver, RetryPolicy, WorkerConfig, dispatch,
};
use serde::Deserialize;
use tokio::{
    sync::watch,
    task::JoinSet,
    time::{Instant, MissedTickBehavior, interval_at, timeout},
};
use url::Url;

pub use aws_sdk_sqs;

const MAX_QUEUE_URL_BYTES: usize = 1_024;
const MAX_FIFO_IDENTIFIER_BYTES: usize = 128;
const MAX_MESSAGE_BYTES: usize = 1_048_576;
const MAX_LONG_POLL_SECONDS: u64 = 20;
const MAX_VISIBILITY_SECONDS: u64 = 43_200;
const MAX_REDRIVE_RECEIVE_COUNT: u16 = 1_000;
const DEFAULT_LONG_POLL: Duration = Duration::from_secs(20);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(25);
const DEFAULT_VISIBILITY_TIMEOUT: Duration = Duration::from_mins(2);
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_HANDLER_TIMEOUT: Duration = Duration::from_mins(30);

/// The deployment-provisioned SQS queue mode.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqsQueueKind {
    /// A Standard queue with at-least-once delivery and no ordering contract.
    Standard,
    /// A FIFO queue with an application-chosen, stable message group.
    Fifo {
        /// The group that SQS uses to preserve order within this publisher route.
        message_group_id: String,
    },
}

impl SqsQueueKind {
    /// Creates a FIFO queue mode with one bounded SQS message-group identifier.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidFifoMessageGroup`] when the identifier is empty, oversized,
    /// or contains characters outside the SQS message-group character set.
    pub fn fifo(message_group_id: impl Into<String>) -> Result<Self, ConfigError> {
        let message_group_id = message_group_id.into();
        validate_fifo_identifier(&message_group_id)?;
        Ok(Self::Fifo { message_group_id })
    }

    /// Returns whether this target must be a FIFO queue.
    #[must_use]
    pub const fn is_fifo(&self) -> bool {
        matches!(self, Self::Fifo { .. })
    }

    fn message_group_id(&self) -> Option<&str> {
        match self {
            Self::Standard => None,
            Self::Fifo { message_group_id } => Some(message_group_id),
        }
    }
}

/// One deployment-provisioned SQS queue used as a source or destination.
#[derive(Clone, Eq, PartialEq)]
pub struct SqsQueueTarget {
    queue_url: String,
    kind: SqsQueueKind,
}

impl SqsQueueTarget {
    /// Creates a validated HTTP(S) queue URL and its expected deployment queue mode.
    ///
    /// HTTP is accepted for LocalStack and other explicit local test endpoints. Production AWS
    /// queue URLs use HTTPS and credentials remain entirely in the injected AWS SDK client.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidQueueUrl`] when the URL is not an absolute HTTP(S) URL, has
    /// embedded credentials, or exceeds the SQS queue URL bound.
    pub fn new(queue_url: impl Into<String>, kind: SqsQueueKind) -> Result<Self, ConfigError> {
        let queue_url = queue_url.into();
        validate_queue_url(&queue_url)?;
        Ok(Self { queue_url, kind })
    }

    /// Returns the configured SQS queue URL.
    #[must_use]
    pub fn queue_url(&self) -> &str {
        &self.queue_url
    }

    /// Returns the expected deployment queue mode.
    #[must_use]
    pub fn kind(&self) -> &SqsQueueKind {
        &self.kind
    }
}

impl fmt::Debug for SqsQueueTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqsQueueTarget")
            .field("queue_url", &self.queue_url)
            .field("kind", &self.kind)
            .finish()
    }
}

/// A response-acknowledged SQS job publisher.
#[derive(Clone)]
pub struct SqsPublisher {
    client: Client,
    target: SqsQueueTarget,
    request_timeout: Duration,
}

impl SqsPublisher {
    /// Wraps an explicitly configured AWS SDK client and one deployment-provisioned target queue.
    #[must_use]
    pub fn new(client: Client, target: SqsQueueTarget) -> Self {
        Self {
            client,
            target,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }

    /// Sets the maximum time one SQS readiness or publish request may occupy this adapter.
    ///
    /// The injected AWS SDK client's retry policy remains application-owned, but Rustee returns a
    /// sanitized provider error once this deadline expires.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroRequestTimeout`] when the deadline is zero.
    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Result<Self, ConfigError> {
        validate_request_timeout(request_timeout)?;
        self.request_timeout = request_timeout;
        Ok(self)
    }

    /// Verifies queue access and the configured Standard/FIFO mode without changing queue state.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError::Readiness`] when the queue is unavailable, inaccessible, or has a
    /// different type than this publisher configuration.
    pub async fn readiness(&self) -> Result<(), SqsError> {
        verify_queue_kind(&self.client, &self.target, self.request_timeout)
            .await
            .map_err(|_| SqsError::Readiness)
    }

    /// Returns the deployment-provisioned destination target.
    #[must_use]
    pub fn target(&self) -> &SqsQueueTarget {
        &self.target
    }

    /// Returns the maximum time one SQS readiness or publish request may use.
    #[must_use]
    pub const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }
}

impl fmt::Debug for SqsPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqsPublisher")
            .field("target", &self.target)
            .field("request_timeout", &self.request_timeout)
            .finish_non_exhaustive()
    }
}

impl JobPublisher for SqsPublisher {
    type Error = SqsError;

    fn publish(&self, message: JobMessage) -> BoxFuture<'static, Result<(), Self::Error>> {
        let client = self.client.clone();
        let target = self.target.clone();
        let request_timeout = self.request_timeout;
        let deduplication_id = message.id().to_string();
        let payload = message.into_payload();
        Box::pin(async move {
            let payload = String::from_utf8(payload).map_err(|_| SqsError::InvalidMessageBody)?;
            send_payload(
                &client,
                &target,
                payload,
                &deduplication_id,
                request_timeout,
            )
            .await
            .map_err(|()| SqsError::Publish)
        })
    }
}

/// Deployment and lease settings for one SQS worker.
#[derive(Clone, Eq, PartialEq)]
pub struct SqsWorkerConfig {
    source: SqsQueueTarget,
    dead_letter: SqsQueueTarget,
    expected_redrive_max_receive_count: u16,
    long_poll: Duration,
    request_timeout: Duration,
    visibility_timeout: Duration,
    heartbeat_interval: Duration,
    handler_timeout: Duration,
}

impl SqsWorkerConfig {
    /// Creates a worker configuration for pre-provisioned source and dead-letter queues.
    ///
    /// The source queue must use an SQS redrive policy pointing to `dead_letter` with exactly
    /// `expected_redrive_max_receive_count`. Rustee also sends malformed, unknown, and exhausted
    /// deliveries directly to that target before deleting the source receipt. The broker redrive
    /// policy remains the recovery path for a process that loses a receipt before settlement.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when source and DLQ targets differ in queue mode, point to the same
    /// queue, or the expected redrive receive count is outside the SQS range.
    pub fn new(
        source: SqsQueueTarget,
        dead_letter: SqsQueueTarget,
        expected_redrive_max_receive_count: u16,
    ) -> Result<Self, ConfigError> {
        if source.queue_url == dead_letter.queue_url {
            return Err(ConfigError::DeadLetterMatchesSource);
        }
        if source.kind.is_fifo() != dead_letter.kind.is_fifo() {
            return Err(ConfigError::QueueKindMismatch);
        }
        if !(1..=MAX_REDRIVE_RECEIVE_COUNT).contains(&expected_redrive_max_receive_count) {
            return Err(ConfigError::InvalidRedriveReceiveCount);
        }
        let config = Self {
            source,
            dead_letter,
            expected_redrive_max_receive_count,
            long_poll: DEFAULT_LONG_POLL,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            visibility_timeout: DEFAULT_VISIBILITY_TIMEOUT,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            handler_timeout: DEFAULT_HANDLER_TIMEOUT,
        };
        config.validate_lease_settings()?;
        Ok(config)
    }

    /// Sets the SQS long-poll duration used for an idle worker receive request.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidLongPoll`] unless the duration is a whole 1 through 20
    /// seconds, the SQS receive request range.
    pub fn with_long_poll(mut self, long_poll: Duration) -> Result<Self, ConfigError> {
        validate_whole_seconds(long_poll, 1, MAX_LONG_POLL_SECONDS)
            .map_err(|()| ConfigError::InvalidLongPoll)?;
        self.long_poll = long_poll;
        self.validate_lease_settings()?;
        Ok(self)
    }

    /// Sets the maximum time any one SQS request may occupy this worker.
    ///
    /// It must exceed the configured long-poll receive duration so an idle receive is never
    /// cancelled before SQS can complete it. The injected AWS SDK client's retry policy remains
    /// application-owned inside this outer deadline.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroRequestTimeout`] for a zero duration and
    /// [`ConfigError::RequestTimeoutNotLongerThanLongPoll`] when it cannot contain the current
    /// SQS long poll.
    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Result<Self, ConfigError> {
        validate_request_timeout(request_timeout)?;
        self.request_timeout = request_timeout;
        self.validate_lease_settings()?;
        Ok(self)
    }

    /// Sets the visibility lease applied when SQS returns a source message.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidVisibilityTimeout`] outside SQS's whole-second 1 through
    /// 43,200 second range, or when the existing heartbeat/handler bounds cannot honor it.
    pub fn with_visibility_timeout(
        mut self,
        visibility_timeout: Duration,
    ) -> Result<Self, ConfigError> {
        validate_whole_seconds(visibility_timeout, 1, MAX_VISIBILITY_SECONDS)
            .map_err(|()| ConfigError::InvalidVisibilityTimeout)?;
        self.visibility_timeout = visibility_timeout;
        self.validate_lease_settings()?;
        Ok(self)
    }

    /// Sets how often an active handler extends its SQS visibility lease.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidHeartbeatInterval`] when the interval is zero or is not
    /// strictly shorter than the configured visibility timeout.
    pub fn with_heartbeat_interval(
        mut self,
        heartbeat_interval: Duration,
    ) -> Result<Self, ConfigError> {
        self.heartbeat_interval = heartbeat_interval;
        self.validate_lease_settings()?;
        Ok(self)
    }

    /// Sets the maximum one-delivery handler time managed by this worker.
    ///
    /// The value stays below SQS's 12-hour visibility ceiling, leaving one full visibility period
    /// for the final renewal. A timed-out handler is dropped and follows the ordinary retry path.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidHandlerTimeout`] when the timeout is zero or cannot fit
    /// within the current SQS visibility window.
    pub fn with_handler_timeout(mut self, handler_timeout: Duration) -> Result<Self, ConfigError> {
        self.handler_timeout = handler_timeout;
        self.validate_lease_settings()?;
        Ok(self)
    }

    /// Returns the deployment-provisioned source queue.
    #[must_use]
    pub fn source(&self) -> &SqsQueueTarget {
        &self.source
    }

    /// Returns the deployment-provisioned direct dead-letter queue.
    #[must_use]
    pub fn dead_letter(&self) -> &SqsQueueTarget {
        &self.dead_letter
    }

    /// Returns the exact deployment redrive `maxReceiveCount` expected at readiness.
    #[must_use]
    pub const fn expected_redrive_max_receive_count(&self) -> u16 {
        self.expected_redrive_max_receive_count
    }

    /// Returns the maximum time any one SQS request may use.
    #[must_use]
    pub const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    fn validate_lease_settings(&self) -> Result<(), ConfigError> {
        validate_request_timeout(self.request_timeout)?;
        if self.request_timeout <= self.long_poll {
            return Err(ConfigError::RequestTimeoutNotLongerThanLongPoll);
        }
        if self.heartbeat_interval.is_zero() || self.heartbeat_interval >= self.visibility_timeout {
            return Err(ConfigError::InvalidHeartbeatInterval);
        }
        let max_handler_timeout = Duration::from_secs(MAX_VISIBILITY_SECONDS)
            .checked_sub(self.visibility_timeout)
            .unwrap_or_default();
        if self.handler_timeout.is_zero() || self.handler_timeout > max_handler_timeout {
            return Err(ConfigError::InvalidHandlerTimeout);
        }
        Ok(())
    }
}

impl fmt::Debug for SqsWorkerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqsWorkerConfig")
            .field("source", &self.source)
            .field("dead_letter", &self.dead_letter)
            .field(
                "expected_redrive_max_receive_count",
                &self.expected_redrive_max_receive_count,
            )
            .field("long_poll", &self.long_poll)
            .field("request_timeout", &self.request_timeout)
            .field("visibility_timeout", &self.visibility_timeout)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("handler_timeout", &self.handler_timeout)
            .finish()
    }
}

/// An SQS worker that preserves visibility/delete/redrive semantics for one Rustee job queue.
#[derive(Clone)]
pub struct SqsWorker {
    client: Client,
    config: SqsWorkerConfig,
    observer: Arc<dyn JobDeliveryObserver>,
}

impl SqsWorker {
    /// Wraps an explicitly configured AWS SDK client and a deployment-owned SQS worker route.
    #[must_use]
    pub fn new(client: Client, config: SqsWorkerConfig) -> Self {
        Self {
            client,
            config,
            observer: Arc::new(NoopJobDeliveryObserver),
        }
    }

    /// Attaches a non-blocking observer for bounded delivery lifecycle telemetry.
    ///
    /// Observer panics are isolated from SQS visibility, direct-DLQ, and delete behavior.
    #[must_use]
    pub fn with_delivery_observer(mut self, observer: Arc<dyn JobDeliveryObserver>) -> Self {
        self.observer = observer;
        self
    }

    /// Verifies source/DLQ access, queue kinds, and the source redrive policy without mutation.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError::Readiness`] for a service or IAM failure, [`SqsError::QueueType`] when
    /// a configured Standard/FIFO mode does not match, or [`SqsError::RedrivePolicy`] when the
    /// deployment's DLQ ARN or max receive count differs from this worker configuration.
    pub async fn readiness(&self) -> Result<(), SqsError> {
        let source = queue_attributes(
            &self.client,
            &self.config.source,
            self.config.request_timeout,
        )
        .await
        .map_err(|()| SqsError::Readiness)?;
        let dead_letter = queue_attributes(
            &self.client,
            &self.config.dead_letter,
            self.config.request_timeout,
        )
        .await
        .map_err(|()| SqsError::Readiness)?;
        verify_queue_kind_attributes(&source, &self.config.source)?;
        verify_queue_kind_attributes(&dead_letter, &self.config.dead_letter)?;

        let dead_letter_arn = dead_letter
            .get(&QueueAttributeName::QueueArn)
            .ok_or(SqsError::RedrivePolicy)?;
        let redrive = source
            .get(&QueueAttributeName::RedrivePolicy)
            .ok_or(SqsError::RedrivePolicy)?;
        let redrive: RedrivePolicy =
            serde_json::from_str(redrive).map_err(|_| SqsError::RedrivePolicy)?;
        let max_receive_count = redrive
            .max_receive_count
            .parse::<u16>()
            .map_err(|_| SqsError::RedrivePolicy)?;
        if redrive.dead_letter_target_arn != *dead_letter_arn
            || max_receive_count != self.config.expected_redrive_max_receive_count
        {
            return Err(SqsError::RedrivePolicy);
        }
        Ok(())
    }

    /// Runs one typed handler until `shutdown` resolves, preserving the SQS visibility lease.
    ///
    /// # Errors
    ///
    /// Returns a sanitized [`SqsError`] when a receive, heartbeat, visibility, direct-DLQ, or
    /// delete operation fails. The source receipt is left unacknowledged on a settlement failure.
    pub async fn run_until<J, H, Shutdown>(
        &self,
        handler: H,
        retry_policy: RetryPolicy,
        worker_config: WorkerConfig,
        shutdown: Shutdown,
    ) -> Result<(), SqsError>
    where
        J: Job,
        H: JobHandler<J>,
        Shutdown: Future<Output = ()> + Send,
    {
        validate_retry_policy(retry_policy, self.config.expected_redrive_max_receive_count)?;
        let client = self.client.clone();
        let config = self.config.clone();
        let processor = move |message: Message| {
            let client = client.clone();
            let config = config.clone();
            let handler = handler.clone();
            Box::pin(async move {
                let delivery = SqsDelivery::from_message(&message)?;
                let payload = delivery.payload.clone();
                let attempt = delivery.attempt();
                let timeout_action = retry_policy.after_failure(attempt);
                let action = run_with_lease(
                    &client,
                    &config,
                    &delivery,
                    async move {
                        match JobEnvelope::<J>::decode(&payload) {
                            Ok(envelope) => match envelope.with_attempt(attempt) {
                                Ok(envelope) => match dispatch(envelope, &handler).await {
                                    Ok(action) => Ok(action),
                                    Err(_) => Ok(retry_policy.after_failure(attempt)),
                                },
                                Err(_) => Ok(DeliveryAction::DeadLetter),
                            },
                            Err(_) => Ok(DeliveryAction::DeadLetter),
                        }
                    },
                    timeout_action,
                )
                .await?;
                settle_delivery(&client, &config, &delivery, action).await?;
                Ok((attempt, outcome_for_action(action)))
            }) as BoxFuture<'static, Result<(u16, JobDeliveryOutcome), SqsError>>
        };
        self.run_with_processor(worker_config, shutdown, processor)
            .await
    }

    /// Runs an immutable typed registry until `shutdown` resolves.
    ///
    /// Unknown and malformed envelopes are sent straight to the configured direct DLQ; only a
    /// registered handler failure follows `retry_policy`.
    ///
    /// # Errors
    ///
    /// Returns a sanitized [`SqsError`] when delivery, lease, or settlement fails.
    pub async fn run_registry_until<Shutdown>(
        &self,
        registry: JobRegistry,
        retry_policy: RetryPolicy,
        worker_config: WorkerConfig,
        shutdown: Shutdown,
    ) -> Result<(), SqsError>
    where
        Shutdown: Future<Output = ()> + Send,
    {
        validate_retry_policy(retry_policy, self.config.expected_redrive_max_receive_count)?;
        let client = self.client.clone();
        let config = self.config.clone();
        let processor = move |message: Message| {
            let client = client.clone();
            let config = config.clone();
            let registry = registry.clone();
            Box::pin(async move {
                let delivery = SqsDelivery::from_message(&message)?;
                let payload = delivery.payload.clone();
                let attempt = delivery.attempt();
                let timeout_action = retry_policy.after_failure(attempt);
                let action = run_with_lease(
                    &client,
                    &config,
                    &delivery,
                    async move {
                        match registry.dispatch(&payload, attempt).await {
                            Ok(action) => Ok(action),
                            Err(JobRegistryError::Handler { .. }) => {
                                Ok(retry_policy.after_failure(attempt))
                            }
                            Err(_) => Ok(DeliveryAction::DeadLetter),
                        }
                    },
                    timeout_action,
                )
                .await?;
                settle_delivery(&client, &config, &delivery, action).await?;
                Ok((attempt, outcome_for_action(action)))
            }) as BoxFuture<'static, Result<(u16, JobDeliveryOutcome), SqsError>>
        };
        self.run_with_processor(worker_config, shutdown, processor)
            .await
    }

    async fn run_with_processor<Shutdown, Processor>(
        &self,
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
                match tasks.join_next().await {
                    Some(Ok(Err(error))) => break Err(error),
                    Some(Err(_)) => break Err(SqsError::WorkerTask),
                    Some(Ok(Ok(()))) | None => continue,
                }
            }
            let max_number_of_messages = i32::try_from(available.min(10)).expect("bounded at 10");
            let receive = self
                .client
                .receive_message()
                .queue_url(self.config.source.queue_url())
                .max_number_of_messages(max_number_of_messages)
                .wait_time_seconds(duration_seconds(self.config.long_poll).expect("validated"))
                .visibility_timeout(
                    duration_seconds(self.config.visibility_timeout).expect("validated"),
                )
                .message_system_attribute_names(MessageSystemAttributeName::ApproximateReceiveCount)
                .send();
            tokio::select! {
                () = &mut shutdown => break Ok(()),
                Some(result) = tasks.join_next(), if !tasks.is_empty() => match result {
                    Ok(Ok(())) => {},
                    Ok(Err(error)) => break Err(error),
                    Err(_) => break Err(SqsError::WorkerTask),
                },
                received = timeout(self.config.request_timeout, receive) => {
                    let received = received
                        .map_err(|_| SqsError::Receive)?
                        .map_err(|_| SqsError::Receive)?;
                    for message in received.messages() {
                        let processor = processor.clone();
                        let observer = Arc::clone(&self.observer);
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

    /// Returns the worker's deployment-provisioned configuration.
    #[must_use]
    pub fn config(&self) -> &SqsWorkerConfig {
        &self.config
    }
}

impl fmt::Debug for SqsWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqsWorker")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// A received SQS delivery without exposing its secret receipt handle.
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

    /// Returns SQS's one-based approximate receive count used as the provider attempt.
    #[must_use]
    pub const fn attempt(&self) -> u16 {
        self.attempt
    }

    fn from_message(message: &Message) -> Result<Self, SqsError> {
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
            .field("message_id", &self.message_id)
            .field("attempt", &self.attempt)
            .finish()
    }
}

/// SQS worker configuration validation errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConfigError {
    /// A queue URL was blank, oversized, not HTTP(S), had no host, or embedded credentials.
    #[error("SQS queue URL must be a bounded absolute HTTP(S) URL without credentials")]
    InvalidQueueUrl,
    /// A FIFO message group was blank, oversized, or used unsupported characters.
    #[error("SQS FIFO message group must use the bounded SQS identifier character set")]
    InvalidFifoMessageGroup,
    /// Source and direct-DLQ queue URLs are the same.
    #[error("SQS source and direct DLQ must differ")]
    DeadLetterMatchesSource,
    /// Source and direct-DLQ queue modes differ.
    #[error("SQS source and direct DLQ must both be Standard or both be FIFO")]
    QueueKindMismatch,
    /// The expected SQS redrive max receive count is outside the SQS range.
    #[error("SQS redrive max receive count must be in 1..=1000")]
    InvalidRedriveReceiveCount,
    /// The long poll is not a whole 1 through 20 seconds.
    #[error("SQS long poll must be a whole 1 through 20 seconds")]
    InvalidLongPoll,
    /// An SQS request deadline was zero.
    #[error("SQS request timeout must be non-zero")]
    ZeroRequestTimeout,
    /// A worker request deadline cannot contain its configured SQS long poll.
    #[error("SQS worker request timeout must be longer than the configured long poll")]
    RequestTimeoutNotLongerThanLongPoll,
    /// The visibility timeout is not a whole 1 through 43,200 seconds.
    #[error("SQS visibility timeout must be a whole 1 through 43,200 seconds")]
    InvalidVisibilityTimeout,
    /// The heartbeat cannot renew the configured visibility lease safely.
    #[error("SQS heartbeat must be non-zero and shorter than visibility timeout")]
    InvalidHeartbeatInterval,
    /// The handler timeout cannot fit into the SQS visibility renewal window.
    #[error("SQS handler timeout must fit before the twelve-hour visibility limit")]
    InvalidHandlerTimeout,
}

/// Sanitized Amazon SQS provider failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SqsError {
    /// The publisher could not obtain a successful SQS send response.
    #[error("SQS job publish failed")]
    Publish,
    /// A `JobMessage` body was not valid UTF-8 for SQS text transport.
    #[error("SQS job message body was not valid UTF-8")]
    InvalidMessageBody,
    /// A read-only queue inspection could not complete.
    #[error("SQS readiness check failed")]
    Readiness,
    /// A configured Standard/FIFO mode differed from the actual queue attribute.
    #[error("SQS configured queue type did not match deployment")]
    QueueType,
    /// The source queue redrive policy did not match the configured direct DLQ route.
    #[error("SQS redrive policy did not match worker configuration")]
    RedrivePolicy,
    /// A long-poll receive request failed.
    #[error("SQS receive failed")]
    Receive,
    /// An SQS delivery omitted or malformed body, receipt, message ID, or receive-count metadata.
    #[error("SQS delivery metadata was invalid")]
    DeliveryMetadata,
    /// A visibility heartbeat request failed; the receipt is deliberately left unsettled.
    #[error("SQS visibility lease renewal failed")]
    VisibilityLease,
    /// Retry visibility could not be changed; the receipt is deliberately left unsettled.
    #[error("SQS retry visibility update failed")]
    RetryVisibility,
    /// Direct DLQ send failed; the source receipt is deliberately left unsettled.
    #[error("SQS direct dead-letter publish failed")]
    DeadLetterPublish,
    /// A completed source receipt could not be deleted.
    #[error("SQS source receipt delete failed")]
    Delete,
    /// The core retry policy cannot be represented as bounded whole-second SQS visibility values.
    #[error("SQS retry policy is incompatible with visibility timeout semantics")]
    RetryPolicyMismatch,
    /// An internal worker task panicked or was cancelled unexpectedly.
    #[error("SQS worker task ended unexpectedly")]
    WorkerTask,
    /// Active tasks did not settle during graceful shutdown and were aborted without deletion.
    #[error("SQS worker drain timed out")]
    DrainTimeout,
}

#[derive(Deserialize)]
struct RedrivePolicy {
    #[serde(rename = "deadLetterTargetArn")]
    dead_letter_target_arn: String,
    #[serde(rename = "maxReceiveCount")]
    max_receive_count: String,
}

async fn run_with_lease<F>(
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
    let heartbeat = tokio::spawn(async move {
        renew_visibility(
            &heartbeat_client,
            &heartbeat_config,
            receipt_handle,
            stop_rx,
        )
        .await
    });
    let handler_result = timeout(config.handler_timeout, handler).await;
    let _ = stop_tx.send(true);
    let heartbeat_result = heartbeat.await.map_err(|_| SqsError::WorkerTask)?;
    heartbeat_result?;
    match handler_result {
        Ok(action) => action,
        Err(_) => Ok(timeout_action),
    }
}

async fn renew_visibility(
    client: &Client,
    config: &SqsWorkerConfig,
    receipt_handle: String,
    mut stop: watch::Receiver<bool>,
) -> Result<(), SqsError> {
    let mut heartbeat = interval_at(
        Instant::now() + config.heartbeat_interval,
        config.heartbeat_interval,
    );
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    return Ok(());
                }
            }
            _ = heartbeat.tick() => {
                timeout(
                    config.request_timeout,
                    client
                        .change_message_visibility()
                        .queue_url(config.source.queue_url())
                        .receipt_handle(&receipt_handle)
                        .visibility_timeout(duration_seconds(config.visibility_timeout).expect("validated"))
                        .send(),
                )
                .await
                .map_err(|_| SqsError::VisibilityLease)?
                .map_err(|_| SqsError::VisibilityLease)?;
            }
        }
    }
}

async fn settle_delivery(
    client: &Client,
    config: &SqsWorkerConfig,
    delivery: &SqsDelivery,
    action: DeliveryAction,
) -> Result<(), SqsError> {
    match action {
        DeliveryAction::Acknowledge => {
            delete_delivery(
                client,
                config.source.queue_url(),
                &delivery.receipt_handle,
                config.request_timeout,
            )
            .await
        }
        DeliveryAction::Retry { delay, .. } => timeout(
            config.request_timeout,
            client
                .change_message_visibility()
                .queue_url(config.source.queue_url())
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
                &config.dead_letter,
                payload,
                &delivery.message_id,
                config.request_timeout,
            )
            .await
            .map_err(|()| SqsError::DeadLetterPublish)?;
            delete_delivery(
                client,
                config.source.queue_url(),
                &delivery.receipt_handle,
                config.request_timeout,
            )
            .await
        }
    }
}

const fn outcome_for_action(action: DeliveryAction) -> JobDeliveryOutcome {
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

async fn send_payload(
    client: &Client,
    target: &SqsQueueTarget,
    payload: String,
    deduplication_id: &str,
    request_timeout: Duration,
) -> Result<(), ()> {
    if payload.is_empty() || payload.len() > MAX_MESSAGE_BYTES {
        return Err(());
    }
    let request = client
        .send_message()
        .queue_url(target.queue_url())
        .message_body(payload);
    let response = match target.kind.message_group_id() {
        Some(message_group_id) => request
            .message_group_id(message_group_id)
            .message_deduplication_id(deduplication_id)
            .send(),
        None => request.send(),
    };
    timeout(request_timeout, response)
        .await
        .map_err(|_| ())?
        .map(|_| ())
        .map_err(|_| ())
}

async fn queue_attributes(
    client: &Client,
    target: &SqsQueueTarget,
    request_timeout: Duration,
) -> Result<std::collections::HashMap<QueueAttributeName, String>, ()> {
    timeout(
        request_timeout,
        client
            .get_queue_attributes()
            .queue_url(target.queue_url())
            .attribute_names(QueueAttributeName::FifoQueue)
            .attribute_names(QueueAttributeName::QueueArn)
            .attribute_names(QueueAttributeName::RedrivePolicy)
            .send(),
    )
    .await
    .map_err(|_| ())?
    .map_err(|_| ())?
    .attributes
    .ok_or(())
}

async fn verify_queue_kind(
    client: &Client,
    target: &SqsQueueTarget,
    request_timeout: Duration,
) -> Result<(), SqsError> {
    let attributes = queue_attributes(client, target, request_timeout)
        .await
        .map_err(|()| SqsError::Readiness)?;
    verify_queue_kind_attributes(&attributes, target)
}

fn verify_queue_kind_attributes(
    attributes: &std::collections::HashMap<QueueAttributeName, String>,
    target: &SqsQueueTarget,
) -> Result<(), SqsError> {
    let actual_fifo = attributes
        .get(&QueueAttributeName::FifoQueue)
        .is_some_and(|value| value == "true");
    if actual_fifo == target.kind.is_fifo() {
        Ok(())
    } else {
        Err(SqsError::QueueType)
    }
}

fn validate_retry_policy(
    retry_policy: RetryPolicy,
    expected_redrive_max_receive_count: u16,
) -> Result<(), SqsError> {
    if retry_policy.max_deliveries == 0
        || retry_policy.max_deliveries > expected_redrive_max_receive_count
        || validate_whole_seconds(retry_policy.initial_backoff, 1, MAX_VISIBILITY_SECONDS).is_err()
        || validate_whole_seconds(retry_policy.max_backoff, 1, MAX_VISIBILITY_SECONDS).is_err()
    {
        return Err(SqsError::RetryPolicyMismatch);
    }
    Ok(())
}

fn validate_queue_url(queue_url: &str) -> Result<(), ConfigError> {
    if queue_url.is_empty()
        || queue_url.len() > MAX_QUEUE_URL_BYTES
        || queue_url.chars().any(char::is_whitespace)
    {
        return Err(ConfigError::InvalidQueueUrl);
    }
    let parsed = Url::parse(queue_url).map_err(|_| ConfigError::InvalidQueueUrl)?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
    {
        return Err(ConfigError::InvalidQueueUrl);
    }
    Ok(())
}

fn validate_request_timeout(request_timeout: Duration) -> Result<(), ConfigError> {
    if request_timeout.is_zero() {
        Err(ConfigError::ZeroRequestTimeout)
    } else {
        Ok(())
    }
}

fn validate_fifo_identifier(value: &str) -> Result<(), ConfigError> {
    if value.is_empty()
        || value.len() > MAX_FIFO_IDENTIFIER_BYTES
        || !value.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~".contains(character)
        })
    {
        return Err(ConfigError::InvalidFifoMessageGroup);
    }
    Ok(())
}

fn validate_whole_seconds(
    value: Duration,
    minimum_seconds: u64,
    maximum_seconds: u64,
) -> Result<u32, ()> {
    if value.subsec_nanos() != 0
        || value.as_secs() < minimum_seconds
        || value.as_secs() > maximum_seconds
    {
        return Err(());
    }
    u32::try_from(value.as_secs()).map_err(|_| ())
}

fn duration_seconds(value: Duration) -> Result<i32, ()> {
    let seconds = validate_whole_seconds(value, 0, MAX_VISIBILITY_SECONDS)?;
    i32::try_from(seconds).map_err(|_| ())
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use aws_sdk_sqs::{
        Client,
        types::{Message, MessageSystemAttributeName},
    };
    use aws_smithy_http_client::Builder as HttpClientBuilder;
    use rustee_jobs::RetryPolicy;

    use super::{
        ConfigError, SqsDelivery, SqsError, SqsPublisher, SqsQueueKind, SqsQueueTarget,
        SqsWorkerConfig, validate_retry_policy,
    };

    #[test]
    fn queue_target_rejects_embedded_credentials_and_invalid_fifo_group() {
        assert_eq!(
            SqsQueueTarget::new(
                "https://key:secret@sqs.us-east-1.amazonaws.com/123/jobs",
                SqsQueueKind::Standard,
            )
            .unwrap_err(),
            ConfigError::InvalidQueueUrl
        );
        assert_eq!(
            SqsQueueKind::fifo("not allowed space").unwrap_err(),
            ConfigError::InvalidFifoMessageGroup
        );
    }

    #[test]
    fn worker_requires_matching_source_and_dead_letter_queue_kinds() {
        let source =
            SqsQueueTarget::new("http://localhost:4566/000/jobs", SqsQueueKind::Standard).unwrap();
        let dead_letter = SqsQueueTarget::new(
            "http://localhost:4566/000/jobs-dlq.fifo",
            SqsQueueKind::fifo("jobs").unwrap(),
        )
        .unwrap();
        assert_eq!(
            SqsWorkerConfig::new(source, dead_letter, 5).unwrap_err(),
            ConfigError::QueueKindMismatch
        );
    }

    #[test]
    fn worker_rejects_unsafe_lease_configuration() {
        let source =
            SqsQueueTarget::new("http://localhost:4566/000/jobs", SqsQueueKind::Standard).unwrap();
        let dead_letter =
            SqsQueueTarget::new("http://localhost:4566/000/jobs-dlq", SqsQueueKind::Standard)
                .unwrap();
        let config = SqsWorkerConfig::new(source, dead_letter, 5).unwrap();
        assert_eq!(
            config
                .clone()
                .with_heartbeat_interval(Duration::from_mins(2))
                .unwrap_err(),
            ConfigError::InvalidHeartbeatInterval
        );
        assert_eq!(
            config
                .clone()
                .with_request_timeout(Duration::ZERO)
                .unwrap_err(),
            ConfigError::ZeroRequestTimeout
        );
        assert_eq!(
            config
                .clone()
                .with_request_timeout(Duration::from_secs(20))
                .unwrap_err(),
            ConfigError::RequestTimeoutNotLongerThanLongPoll
        );
        assert_eq!(
            config
                .with_long_poll(Duration::from_millis(500))
                .unwrap_err(),
            ConfigError::InvalidLongPoll
        );
    }

    #[test]
    fn publisher_request_timeout_cannot_be_zero() {
        let client = Client::from_conf(
            aws_sdk_sqs::Config::builder()
                .http_client(HttpClientBuilder::new().build_http())
                .build(),
        );
        let target =
            SqsQueueTarget::new("http://localhost:4566/000/jobs", SqsQueueKind::Standard).unwrap();
        assert_eq!(
            SqsPublisher::new(client, target)
                .with_request_timeout(Duration::ZERO)
                .unwrap_err(),
            ConfigError::ZeroRequestTimeout
        );
    }

    #[test]
    fn delivery_uses_approximate_receive_count_and_redacts_receipt() {
        let message = Message::builder()
            .body("{\"name\":\"email.welcome\"}")
            .message_id("message-1")
            .receipt_handle("secret-receipt")
            .attributes(MessageSystemAttributeName::ApproximateReceiveCount, "3")
            .build();
        let delivery = SqsDelivery::from_message(&message).unwrap();
        assert_eq!(delivery.attempt(), 3);
        assert_eq!(delivery.payload(), br#"{"name":"email.welcome"}"#);
        let debug = format!("{delivery:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("secret-receipt"));
    }

    #[test]
    fn retry_policy_requires_whole_seconds_within_redrive_budget() {
        let valid = RetryPolicy::default();
        assert_eq!(validate_retry_policy(valid, 5), Ok(()));
        assert_eq!(
            validate_retry_policy(
                RetryPolicy {
                    initial_backoff: Duration::from_millis(1_500),
                    ..valid
                },
                5,
            ),
            Err(SqsError::RetryPolicyMismatch)
        );
        assert_eq!(
            validate_retry_policy(
                RetryPolicy {
                    max_deliveries: 6,
                    ..valid
                },
                5,
            ),
            Err(SqsError::RetryPolicyMismatch)
        );
    }
}
