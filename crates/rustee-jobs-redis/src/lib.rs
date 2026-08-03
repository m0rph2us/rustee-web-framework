//! Redis Streams publishing and consumer-group delivery for `Rustee` jobs.
//!
//! Streams, consumer groups, retention, ACLs, and dead-letter streams are deployment-owned. A
//! worker verifies that its configured consumer group already exists; it never provisions it.
//! Retry records use a provider-private sorted set and hashes so the requested retry delay survives
//! worker restart. The configured reclaim idle time applies only to deliveries abandoned by a
//! worker before it could settle them.

use std::{fmt, future::Future, num::NonZeroU16, sync::Arc, time::Duration};

use futures_util::future::BoxFuture;
use rustee_jobs::{
    DeliveryAction, Job, JobDeliveryObservation, JobDeliveryObserver, JobDeliveryOutcome,
    JobEnvelope, JobHandler, JobMessage, JobPublisher, JobRegistry, JobRegistryError,
    NoopJobDeliveryObserver, RetryPolicy, WorkerConfig, dispatch,
};
use rustee_redis::redis::{
    AsyncCommands, Script,
    aio::ConnectionManager,
    streams::{
        StreamAutoClaimOptions, StreamId, StreamInfoGroupsReply, StreamPendingCountReply,
        StreamReadOptions, StreamReadReply,
    },
};
use tokio::{
    task::JoinSet,
    time::{MissedTickBehavior, interval, timeout},
};

pub use rustee_redis::redis;

const PAYLOAD_FIELD: &str = "payload";
const ATTEMPT_FIELD: &str = "attempt";
const DEFAULT_BLOCK_TIMEOUT: Duration = Duration::from_millis(250);
const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_RECLAIM_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_RECLAIM_IDLE: Duration = Duration::from_secs(30);
const DEFAULT_BATCH_SIZE: usize = 64;
const MAX_BATCH_SIZE: usize = 1_000;

const ACK_IF_OWNED_SCRIPT: &str = r"
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1, ARGV[3])
if #pending == 0 then
  return 0
end
return redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
";

const SCHEDULE_RETRY_SCRIPT: &str = r"
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1, ARGV[3])
if #pending == 0 then
  return 0
end
local now = redis.call('TIME')
local due = now[1] * 1000 + math.floor(now[2] / 1000) + tonumber(ARGV[5])
redis.call('HSET', KEYS[3], ARGV[4], ARGV[6])
redis.call('HSET', KEYS[4], ARGV[4], ARGV[7])
redis.call('ZADD', KEYS[2], due, ARGV[4])
return redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
";

const DEAD_LETTER_IF_OWNED_SCRIPT: &str = r"
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1, ARGV[3])
if #pending == 0 then
  return 0
end
redis.call('XADD', KEYS[2], '*', 'payload', ARGV[4], 'attempt', ARGV[5], 'source_entry_id', ARGV[2])
return redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
";

const PROMOTE_DUE_RETRIES_SCRIPT: &str = r"
local now = redis.call('TIME')
local now_ms = now[1] * 1000 + math.floor(now[2] / 1000)
local ids = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now_ms, 'LIMIT', 0, tonumber(ARGV[1]))
for _, id in ipairs(ids) do
  if not redis.call('HEXISTS', KEYS[3], id) or not redis.call('HEXISTS', KEYS[4], id) then
    return redis.error_reply('rustee retry record is incomplete')
  end
end
for _, id in ipairs(ids) do
  local payload = redis.call('HGET', KEYS[3], id)
  local attempt = redis.call('HGET', KEYS[4], id)
  redis.call('XADD', KEYS[1], '*', 'payload', payload, 'attempt', attempt)
  redis.call('HDEL', KEYS[3], id)
  redis.call('HDEL', KEYS[4], id)
  redis.call('ZREM', KEYS[2], id)
end
return #ids
";

/// Acknowledged Redis Streams publisher for serialized `Rustee` jobs.
#[derive(Clone)]
pub struct RedisStreamsPublisher {
    connection: ConnectionManager,
    stream: String,
    operation_timeout: Duration,
}

impl RedisStreamsPublisher {
    /// Wraps a reconnecting Redis connection and one deployment-provisioned stream.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidKey`] when `stream` is blank, contains whitespace, or is
    /// outside the provider's bounded key length.
    pub fn new(
        connection: ConnectionManager,
        stream: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        let stream = stream.into();
        validate_key(&stream)?;
        Ok(Self {
            connection,
            stream,
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
        })
    }

    /// Sets the maximum time one Redis Streams readiness or publish operation may use.
    ///
    /// The injected connection manager owns reconnect policy. Rustee cancels the current command
    /// and returns a sanitized provider error once this deadline expires.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroDuration`] when `operation_timeout` is zero.
    pub fn with_operation_timeout(
        mut self,
        operation_timeout: Duration,
    ) -> Result<Self, ConfigError> {
        validate_operation_timeout(operation_timeout)?;
        self.operation_timeout = operation_timeout;
        Ok(self)
    }

    /// Verifies that the configured stream can be inspected without mutating Redis state.
    ///
    /// # Errors
    ///
    /// Returns [`RedisStreamsError::Readiness`] when the stream is absent or Redis cannot answer.
    pub async fn readiness(&self) -> Result<(), RedisStreamsError> {
        let mut connection = self.connection.clone();
        bounded(
            self.operation_timeout,
            redis::cmd("XINFO")
                .arg("STREAM")
                .arg(&self.stream)
                .query_async::<redis::Value>(&mut connection),
        )
        .await
        .map(|_| ())
        .map_err(|()| RedisStreamsError::Readiness)
    }

    /// Returns the configured destination stream.
    #[must_use]
    pub fn stream(&self) -> &str {
        &self.stream
    }

    /// Returns the maximum time one Redis Streams readiness or publish operation may use.
    #[must_use]
    pub const fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }
}

impl fmt::Debug for RedisStreamsPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisStreamsPublisher")
            .field("stream", &self.stream)
            .field("operation_timeout", &self.operation_timeout)
            .finish_non_exhaustive()
    }
}

impl JobPublisher for RedisStreamsPublisher {
    type Error = RedisStreamsError;

    fn publish(&self, message: JobMessage) -> BoxFuture<'static, Result<(), Self::Error>> {
        let mut connection = self.connection.clone();
        let stream = self.stream.clone();
        let operation_timeout = self.operation_timeout;
        let attempt = message.attempt();
        let payload = message.into_payload();
        Box::pin(async move {
            bounded(
                operation_timeout,
                redis::cmd("XADD")
                    .arg(stream)
                    .arg("*")
                    .arg(PAYLOAD_FIELD)
                    .arg(payload)
                    .arg(ATTEMPT_FIELD)
                    .arg(attempt)
                    .query_async::<String>(&mut connection),
            )
            .await
            .map(|_| ())
            .map_err(|()| RedisStreamsError::Publish)
        })
    }
}

/// Consumer-group and retry settings for one Redis Streams job worker.
#[derive(Clone, Eq, PartialEq)]
pub struct RedisStreamsWorkerConfig {
    stream: String,
    group: String,
    consumer: String,
    dead_letter_stream: String,
    retry_schedule_key: String,
    retry_payload_key: String,
    retry_attempt_key: String,
    block_timeout_ms: usize,
    operation_timeout: Duration,
    reclaim_interval: Duration,
    reclaim_idle_ms: usize,
    batch_size: usize,
}

impl RedisStreamsWorkerConfig {
    /// Creates a worker configuration for a pre-existing stream, consumer group, and DLQ stream.
    ///
    /// The consumer name must be unique per concurrently running worker process. Internal retry
    /// keys are deterministically scoped to this source stream and consumer group.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when an identifier is unsafe, a DLQ equals the source stream, or
    /// a bounded duration cannot be represented by Redis milliseconds.
    pub fn new(
        stream: impl Into<String>,
        group: impl Into<String>,
        consumer: impl Into<String>,
        dead_letter_stream: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        let stream = stream.into();
        let group = group.into();
        let consumer = consumer.into();
        let dead_letter_stream = dead_letter_stream.into();
        validate_key(&stream)?;
        validate_group_or_consumer(&group)?;
        validate_group_or_consumer(&consumer)?;
        validate_key(&dead_letter_stream)?;
        if stream == dead_letter_stream {
            return Err(ConfigError::DeadLetterMatchesSource);
        }
        Ok(Self {
            retry_schedule_key: format!("{stream}:rustee:jobs:{group}:retry:schedule"),
            retry_payload_key: format!("{stream}:rustee:jobs:{group}:retry:payload"),
            retry_attempt_key: format!("{stream}:rustee:jobs:{group}:retry:attempt"),
            stream,
            group,
            consumer,
            dead_letter_stream,
            block_timeout_ms: nonzero_duration_to_millis(DEFAULT_BLOCK_TIMEOUT)?,
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
            reclaim_interval: DEFAULT_RECLAIM_INTERVAL,
            reclaim_idle_ms: nonzero_duration_to_millis(DEFAULT_RECLAIM_IDLE)?,
            batch_size: DEFAULT_BATCH_SIZE,
        })
    }

    /// Sets the bounded duration of an idle `XREADGROUP` call.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroDuration`] or [`ConfigError::DurationOutOfRange`] for an
    /// unsupported Redis millisecond value.
    pub fn with_block_timeout(mut self, timeout: Duration) -> Result<Self, ConfigError> {
        self.block_timeout_ms = nonzero_duration_to_millis(timeout)?;
        self.validate_operation_timeout()?;
        Ok(self)
    }

    /// Sets the outer deadline for one Redis Streams command.
    ///
    /// This deadline must be longer than the configured blocking read so an idle
    /// `XREADGROUP` request can complete normally. The connection manager's reconnect
    /// policy remains application-owned inside this adapter boundary.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroDuration`] for zero and
    /// [`ConfigError::OperationTimeoutNotLongerThanBlock`] when it cannot contain the current
    /// blocking read duration.
    pub fn with_operation_timeout(
        mut self,
        operation_timeout: Duration,
    ) -> Result<Self, ConfigError> {
        validate_operation_timeout(operation_timeout)?;
        self.operation_timeout = operation_timeout;
        self.validate_operation_timeout()?;
        Ok(self)
    }

    /// Sets how often this worker promotes due retries and looks for abandoned pending entries.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroDuration`] when `interval` is zero.
    pub fn with_reclaim_interval(
        mut self,
        reclaim_interval: Duration,
    ) -> Result<Self, ConfigError> {
        if reclaim_interval.is_zero() {
            return Err(ConfigError::ZeroDuration);
        }
        self.reclaim_interval = reclaim_interval;
        Ok(self)
    }

    /// Sets the minimum pending-entry idle time before another consumer can reclaim it.
    ///
    /// This must exceed the longest un-heartbeated handler execution that the deployment permits.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroDuration`] or [`ConfigError::DurationOutOfRange`] for an
    /// unsupported Redis millisecond value.
    pub fn with_reclaim_idle(mut self, reclaim_idle: Duration) -> Result<Self, ConfigError> {
        self.reclaim_idle_ms = nonzero_duration_to_millis(reclaim_idle)?;
        Ok(self)
    }

    /// Sets the maximum records fetched or reclaimed in one provider operation.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidBatchSize`] outside `1..={MAX_BATCH_SIZE}`.
    pub fn with_batch_size(mut self, batch_size: usize) -> Result<Self, ConfigError> {
        if !(1..=MAX_BATCH_SIZE).contains(&batch_size) {
            return Err(ConfigError::InvalidBatchSize);
        }
        self.batch_size = batch_size;
        Ok(self)
    }

    /// Returns the deployment-provisioned source stream.
    #[must_use]
    pub fn stream(&self) -> &str {
        &self.stream
    }

    /// Returns the deployment-provisioned consumer group.
    #[must_use]
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Returns the unique worker consumer name.
    #[must_use]
    pub fn consumer(&self) -> &str {
        &self.consumer
    }

    /// Returns the deployment-provisioned dead-letter stream.
    #[must_use]
    pub fn dead_letter_stream(&self) -> &str {
        &self.dead_letter_stream
    }

    /// Returns the provider-private retry schedule key.
    #[must_use]
    pub fn retry_schedule_key(&self) -> &str {
        &self.retry_schedule_key
    }

    /// Returns the provider-private retry payload hash key.
    #[must_use]
    pub fn retry_payload_key(&self) -> &str {
        &self.retry_payload_key
    }

    /// Returns the provider-private retry attempt hash key.
    #[must_use]
    pub fn retry_attempt_key(&self) -> &str {
        &self.retry_attempt_key
    }

    /// Returns the outer deadline for one Redis Streams command.
    #[must_use]
    pub const fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }

    fn validate_operation_timeout(&self) -> Result<(), ConfigError> {
        validate_operation_timeout(self.operation_timeout)?;
        let block_timeout_ms =
            u64::try_from(self.block_timeout_ms).map_err(|_| ConfigError::DurationOutOfRange)?;
        if self.operation_timeout <= Duration::from_millis(block_timeout_ms) {
            return Err(ConfigError::OperationTimeoutNotLongerThanBlock);
        }
        Ok(())
    }
}

impl fmt::Debug for RedisStreamsWorkerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisStreamsWorkerConfig")
            .field("stream", &self.stream)
            .field("group", &self.group)
            .field("consumer", &self.consumer)
            .field("dead_letter_stream", &self.dead_letter_stream)
            .field("retry_schedule_key", &self.retry_schedule_key)
            .field("retry_payload_key", &self.retry_payload_key)
            .field("retry_attempt_key", &self.retry_attempt_key)
            .field("block_timeout_ms", &self.block_timeout_ms)
            .field("operation_timeout", &self.operation_timeout)
            .field("reclaim_interval", &self.reclaim_interval)
            .field("reclaim_idle_ms", &self.reclaim_idle_ms)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

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
            self.config.operation_timeout,
            connection.xinfo_groups(&self.config.stream),
        )
        .await
        .map_err(|()| RedisStreamsError::Readiness)?;
        if !groups
            .groups
            .iter()
            .any(|group| group.name == self.config.group)
        {
            return Err(RedisStreamsError::ConsumerGroup);
        }
        bounded(
            self.config.operation_timeout,
            redis::cmd("XINFO")
                .arg("STREAM")
                .arg(&self.config.dead_letter_stream)
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

    async fn run_with<F, P>(
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
        self.readiness().await?;
        let mut shutdown = Box::pin(shutdown);
        let mut maintenance = interval(self.config.reclaim_interval);
        maintenance.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut tasks = JoinSet::new();

        let run_result = loop {
            let available = worker_config.concurrency.get() - tasks.len();
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
                    self.promote_due_retries(available.min(self.config.batch_size)).await?;
                    let deliveries = self.reclaim_pending(available.min(self.config.batch_size)).await?;
                    for delivery in deliveries {
                        let process = process.clone();
                        let observer = Arc::clone(&self.observer);
                        tasks.spawn(async move {
                            let observation =
                                JobDeliveryObservation::start(observer, "redis_streams");
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
                }
                deliveries = self.read_new(available), if available > 0 => {
                    for delivery in deliveries? {
                        let process = process.clone();
                        let observer = Arc::clone(&self.observer);
                        tasks.spawn(async move {
                            let observation =
                                JobDeliveryObservation::start(observer, "redis_streams");
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
                }
            }
        };

        let drain_result = drain_tasks(&mut tasks, worker_config.drain_timeout).await;
        run_result?;
        drain_result
    }

    async fn read_new(
        &self,
        capacity: usize,
    ) -> Result<Vec<RedisStreamsDelivery>, RedisStreamsError> {
        let count = capacity.min(self.config.batch_size);
        let options = StreamReadOptions::default()
            .group(&self.config.group, &self.config.consumer)
            .count(count)
            .block(self.config.block_timeout_ms);
        let mut connection = self.connection.clone();
        let reply: StreamReadReply = bounded(
            self.config.operation_timeout,
            connection.xread_options(&[self.config.stream.as_str()], &[">"], &options),
        )
        .await
        .map_err(|()| RedisStreamsError::Receive)?;
        Ok(reply
            .keys
            .into_iter()
            .flat_map(|key| key.ids)
            .map(|entry| self.delivery_from_entry(entry, None))
            .collect())
    }

    async fn reclaim_pending(
        &self,
        count: usize,
    ) -> Result<Vec<RedisStreamsDelivery>, RedisStreamsError> {
        let mut connection = self.connection.clone();
        let response: redis::streams::StreamAutoClaimReply = bounded(
            self.config.operation_timeout,
            connection.xautoclaim_options(
                &self.config.stream,
                &self.config.group,
                &self.config.consumer,
                self.config.reclaim_idle_ms,
                "0-0",
                StreamAutoClaimOptions::default().count(count),
            ),
        )
        .await
        .map_err(|()| RedisStreamsError::Reclaim)?;
        if !response.deleted_ids.is_empty() || response.invalid_entries {
            return Err(RedisStreamsError::ClaimedEntryMissing);
        }
        let mut deliveries = Vec::with_capacity(response.claimed.len());
        for entry in response.claimed {
            let delivery_count = self.pending_delivery_count(&entry.id).await?;
            deliveries.push(self.delivery_from_entry(entry, Some(delivery_count)));
        }
        Ok(deliveries)
    }

    async fn pending_delivery_count(&self, entry_id: &str) -> Result<usize, RedisStreamsError> {
        let mut connection = self.connection.clone();
        let pending: StreamPendingCountReply = bounded(
            self.config.operation_timeout,
            connection.xpending_count(
                &self.config.stream,
                &self.config.group,
                entry_id,
                entry_id,
                1,
            ),
        )
        .await
        .map_err(|()| RedisStreamsError::Reclaim)?;
        pending
            .ids
            .first()
            .map(|entry| entry.times_delivered)
            .ok_or(RedisStreamsError::DeliveryMetadata)
    }

    fn delivery_from_entry(
        &self,
        entry: StreamId,
        pending_deliveries: Option<usize>,
    ) -> RedisStreamsDelivery {
        let payload = entry.get::<Vec<u8>>(PAYLOAD_FIELD).unwrap_or_default();
        let attempt = entry
            .get::<u16>(ATTEMPT_FIELD)
            .and_then(|base_attempt| match pending_deliveries {
                Some(deliveries) => deliveries
                    .checked_sub(1)
                    .and_then(|redeliveries| u16::try_from(redeliveries).ok())
                    .and_then(|redeliveries| base_attempt.checked_add(redeliveries)),
                None => Some(base_attempt),
            })
            .filter(|attempt| *attempt > 0);
        RedisStreamsDelivery {
            worker: self.clone(),
            entry_id: entry.id,
            payload,
            attempt,
        }
    }

    async fn acknowledge(&self, entry_id: &str) -> Result<(), RedisStreamsError> {
        let mut connection = self.connection.clone();
        let script = Script::new(ACK_IF_OWNED_SCRIPT);
        let mut invoke = script.prepare_invoke();
        invoke
            .key(&self.config.stream)
            .arg(&self.config.group)
            .arg(entry_id)
            .arg(&self.config.consumer);
        let settled: usize = bounded(
            self.config.operation_timeout,
            invoke.invoke_async(&mut connection),
        )
        .await
        .map_err(|()| RedisStreamsError::Acknowledge)?;
        (settled == 1)
            .then_some(())
            .ok_or(RedisStreamsError::DeliveryOwnershipLost)
    }

    async fn schedule_retry(
        &self,
        entry_id: &str,
        payload: &[u8],
        next_attempt: u16,
        delay: Duration,
    ) -> Result<(), RedisStreamsError> {
        let delay_ms = duration_to_millis(delay).map_err(|_| RedisStreamsError::RetrySchedule)?;
        let mut connection = self.connection.clone();
        let script = Script::new(SCHEDULE_RETRY_SCRIPT);
        let mut invoke = script.prepare_invoke();
        invoke
            .key(&self.config.stream)
            .key(&self.config.retry_schedule_key)
            .key(&self.config.retry_payload_key)
            .key(&self.config.retry_attempt_key)
            .arg(&self.config.group)
            .arg(entry_id)
            .arg(&self.config.consumer)
            .arg(entry_id)
            .arg(delay_ms)
            .arg(payload)
            .arg(next_attempt);
        let settled: usize = bounded(
            self.config.operation_timeout,
            invoke.invoke_async(&mut connection),
        )
        .await
        .map_err(|()| RedisStreamsError::RetrySchedule)?;
        (settled == 1)
            .then_some(())
            .ok_or(RedisStreamsError::DeliveryOwnershipLost)
    }

    async fn dead_letter(
        &self,
        entry_id: &str,
        payload: &[u8],
        attempt: u16,
    ) -> Result<(), RedisStreamsError> {
        let mut connection = self.connection.clone();
        let script = Script::new(DEAD_LETTER_IF_OWNED_SCRIPT);
        let mut invoke = script.prepare_invoke();
        invoke
            .key(&self.config.stream)
            .key(&self.config.dead_letter_stream)
            .arg(&self.config.group)
            .arg(entry_id)
            .arg(&self.config.consumer)
            .arg(payload)
            .arg(attempt);
        let settled: usize = bounded(
            self.config.operation_timeout,
            invoke.invoke_async(&mut connection),
        )
        .await
        .map_err(|()| RedisStreamsError::DeadLetter)?;
        (settled == 1)
            .then_some(())
            .ok_or(RedisStreamsError::DeliveryOwnershipLost)
    }

    async fn promote_due_retries(&self, count: usize) -> Result<(), RedisStreamsError> {
        let mut connection = self.connection.clone();
        let script = Script::new(PROMOTE_DUE_RETRIES_SCRIPT);
        let mut invoke = script.prepare_invoke();
        invoke
            .key(&self.config.stream)
            .key(&self.config.retry_schedule_key)
            .key(&self.config.retry_payload_key)
            .key(&self.config.retry_attempt_key)
            .arg(count);
        bounded(
            self.config.operation_timeout,
            invoke.invoke_async::<usize>(&mut connection),
        )
        .await
        .map(|_| ())
        .map_err(|()| RedisStreamsError::RetryPromotion)
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

/// One Redis pending delivery with settlement kept private to the provider.
#[derive(Clone, Debug)]
pub struct RedisStreamsDelivery {
    worker: RedisStreamsWorker,
    entry_id: String,
    payload: Vec<u8>,
    attempt: Option<u16>,
}

impl RedisStreamsDelivery {
    /// Returns the serialized job envelope bytes. A malformed external stream entry may be empty.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Returns the end-to-end one-based attempt, including reclaimed pending deliveries.
    ///
    /// # Errors
    ///
    /// Returns [`RedisStreamsError::DeliveryMetadata`] when a producer omitted a valid base
    /// attempt or reclaim count could not be represented safely.
    pub fn delivery_attempt(&self) -> Result<u16, RedisStreamsError> {
        self.attempt.ok_or(RedisStreamsError::DeliveryMetadata)
    }

    /// Settles this entry only when this configured consumer remains its current PEL owner.
    ///
    /// # Errors
    ///
    /// Returns [`RedisStreamsError::DeliveryOwnershipLost`] when another consumer reclaimed the
    /// entry, or [`RedisStreamsError::Acknowledge`] when Redis cannot execute the settlement.
    pub async fn acknowledge(&self) -> Result<(), RedisStreamsError> {
        self.worker.acknowledge(&self.entry_id).await
    }

    /// Stores one durable delayed retry before acknowledging this source entry.
    ///
    /// # Errors
    ///
    /// Returns [`RedisStreamsError::DeliveryOwnershipLost`] when another consumer reclaimed the
    /// entry, or [`RedisStreamsError::RetrySchedule`] when Redis cannot atomically store retry
    /// state and acknowledge this source delivery.
    pub async fn retry_after(
        &self,
        next_attempt: u16,
        delay: Duration,
    ) -> Result<(), RedisStreamsError> {
        self.worker
            .schedule_retry(&self.entry_id, &self.payload, next_attempt, delay)
            .await
    }

    /// Publishes this payload to the configured DLQ before acknowledging its source entry.
    ///
    /// # Errors
    ///
    /// Returns [`RedisStreamsError::DeliveryOwnershipLost`] when another consumer reclaimed the
    /// entry, or [`RedisStreamsError::DeadLetter`] when Redis cannot atomically write the DLQ
    /// record and acknowledge this source delivery.
    pub async fn dead_letter(&self) -> Result<(), RedisStreamsError> {
        self.worker
            .dead_letter(&self.entry_id, &self.payload, self.attempt.unwrap_or(1))
            .await
    }
}

async fn process_delivery<J, H>(
    delivery: RedisStreamsDelivery,
    handler: H,
    retry_policy: RetryPolicy,
) -> Result<(u16, JobDeliveryOutcome), RedisStreamsError>
where
    J: Job,
    H: JobHandler<J>,
{
    let Ok(attempt) = delivery.delivery_attempt() else {
        return delivery
            .dead_letter()
            .await
            .map(|()| (1, JobDeliveryOutcome::DeadLettered));
    };
    let envelope = match JobEnvelope::<J>::decode(delivery.payload()) {
        Ok(envelope) => envelope
            .with_attempt(attempt)
            .map_err(|_| RedisStreamsError::DeliveryMetadata)?,
        Err(_) => {
            return delivery
                .dead_letter()
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
        }) => delivery
            .retry_after(next_attempt, delay)
            .await
            .map(|()| (attempt, JobDeliveryOutcome::Retried)),
        Ok(DeliveryAction::DeadLetter) => delivery
            .dead_letter()
            .await
            .map(|()| (attempt, JobDeliveryOutcome::DeadLettered)),
        Err(_) => apply_retry_policy(delivery, attempt, retry_policy)
            .await
            .map(|outcome| (attempt, outcome)),
    }
}

async fn process_registry_delivery(
    delivery: RedisStreamsDelivery,
    registry: JobRegistry,
    retry_policy: RetryPolicy,
) -> Result<(u16, JobDeliveryOutcome), RedisStreamsError> {
    let Ok(attempt) = delivery.delivery_attempt() else {
        return delivery
            .dead_letter()
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
        }) => delivery
            .retry_after(next_attempt, delay)
            .await
            .map(|()| (attempt, JobDeliveryOutcome::Retried)),
        Err(JobRegistryError::Handler { .. }) => {
            apply_retry_policy(delivery, attempt, retry_policy)
                .await
                .map(|outcome| (attempt, outcome))
        }
        Ok(DeliveryAction::DeadLetter) | Err(_) => delivery
            .dead_letter()
            .await
            .map(|()| (attempt, JobDeliveryOutcome::DeadLettered)),
    }
}

async fn apply_retry_policy(
    delivery: RedisStreamsDelivery,
    attempt: u16,
    retry_policy: RetryPolicy,
) -> Result<JobDeliveryOutcome, RedisStreamsError> {
    match retry_policy.after_failure(attempt) {
        DeliveryAction::Acknowledge => delivery
            .acknowledge()
            .await
            .map(|()| JobDeliveryOutcome::Acknowledged),
        DeliveryAction::Retry {
            next_attempt,
            delay,
        } => delivery
            .retry_after(next_attempt, delay)
            .await
            .map(|()| JobDeliveryOutcome::Retried),
        DeliveryAction::DeadLetter => delivery
            .dead_letter()
            .await
            .map(|()| JobDeliveryOutcome::DeadLettered),
    }
}

async fn drain_tasks(
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
        Err(RedisStreamsError::DrainTimeout)
    }
}

fn validate_key(value: &str) -> Result<(), ConfigError> {
    if value.trim().is_empty() || value.len() > 512 || value.chars().any(char::is_whitespace) {
        return Err(ConfigError::InvalidKey);
    }
    Ok(())
}

fn validate_group_or_consumer(value: &str) -> Result<(), ConfigError> {
    if value.trim().is_empty() || value.len() > 255 || value.chars().any(char::is_whitespace) {
        return Err(ConfigError::InvalidGroupOrConsumer);
    }
    Ok(())
}

fn duration_to_millis(duration: Duration) -> Result<usize, ConfigError> {
    usize::try_from(duration.as_millis()).map_err(|_| ConfigError::DurationOutOfRange)
}

fn nonzero_duration_to_millis(duration: Duration) -> Result<usize, ConfigError> {
    if duration.is_zero() {
        return Err(ConfigError::ZeroDuration);
    }
    duration_to_millis(duration)
}

fn validate_operation_timeout(operation_timeout: Duration) -> Result<(), ConfigError> {
    if operation_timeout.is_zero() {
        Err(ConfigError::ZeroDuration)
    } else {
        Ok(())
    }
}

async fn bounded<T, E, F>(operation_timeout: Duration, operation: F) -> Result<T, ()>
where
    F: Future<Output = Result<T, E>>,
{
    timeout(operation_timeout, operation)
        .await
        .map_err(|_| ())?
        .map_err(|_| ())
}

/// Invalid Redis Streams job provider configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConfigError {
    /// A Redis key was blank, whitespace-containing, or too long for this provider boundary.
    #[error("Redis Streams job key must be non-blank, whitespace-free, and bounded")]
    InvalidKey,
    /// A Redis consumer group or consumer name was blank, whitespace-containing, or too long.
    #[error(
        "Redis Streams job group and consumer names must be non-blank, whitespace-free, and bounded"
    )]
    InvalidGroupOrConsumer,
    /// The dead-letter stream must be distinct from the source stream.
    #[error("Redis Streams dead-letter stream must differ from the source stream")]
    DeadLetterMatchesSource,
    /// A time setting must use a positive duration.
    #[error("Redis Streams job duration must be greater than zero")]
    ZeroDuration,
    /// A duration cannot be represented as Redis milliseconds on this target.
    #[error("Redis Streams job duration cannot be represented as Redis milliseconds")]
    DurationOutOfRange,
    /// An operation deadline cannot contain the configured blocking read.
    #[error("Redis Streams operation timeout must be longer than the blocking read timeout")]
    OperationTimeoutNotLongerThanBlock,
    /// A fetch or reclaim batch was outside the bounded provider range.
    #[error("Redis Streams job batch size must be between 1 and 1000")]
    InvalidBatchSize,
}

/// Sanitized operational failures from the Redis Streams provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RedisStreamsError {
    /// Redis did not accept a durable stream append.
    #[error("Redis Streams job publish failed")]
    Publish,
    /// Redis could not inspect a configured source or dead-letter stream.
    #[error("Redis Streams job readiness check failed")]
    Readiness,
    /// The deployment did not pre-provision the configured consumer group.
    #[error("Redis Streams job consumer group is not configured")]
    ConsumerGroup,
    /// A consumer-group read failed.
    #[error("Redis Streams job receive failed")]
    Receive,
    /// Pending recovery or its delivery-count inspection failed.
    #[error("Redis Streams pending job recovery failed")]
    Reclaim,
    /// Redis reported a pending record whose stream entry had been removed by retention or trim.
    #[error("Redis Streams claimed job entry was missing")]
    ClaimedEntryMissing,
    /// A message omitted required provider metadata or had an unrepresentable cumulative attempt.
    #[error("Redis Streams job delivery metadata was invalid")]
    DeliveryMetadata,
    /// A consumer lost PEL ownership before it could settle its selected delivery.
    #[error("Redis Streams job delivery ownership was lost")]
    DeliveryOwnershipLost,
    /// Redis could not atomically acknowledge one successful delivery.
    #[error("Redis Streams job acknowledgement failed")]
    Acknowledge,
    /// Redis could not atomically persist a delayed retry and settle its source delivery.
    #[error("Redis Streams job retry scheduling failed")]
    RetrySchedule,
    /// Redis could not atomically promote due retries to the source stream.
    #[error("Redis Streams job retry promotion failed")]
    RetryPromotion,
    /// Redis could not atomically write a dead-letter entry and settle its source delivery.
    #[error("Redis Streams job dead-letter publish failed")]
    DeadLetter,
    /// A worker task panicked or was cancelled before settling its delivery.
    #[error("Redis Streams job worker task failed")]
    WorkerTask,
    /// Active handlers did not finish before the configured shutdown drain deadline.
    #[error("Redis Streams job worker drain timed out")]
    DrainTimeout,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{ConfigError, RedisStreamsWorkerConfig};

    #[test]
    fn worker_configuration_scopes_retry_storage_and_rejects_unsafe_values() {
        let config =
            RedisStreamsWorkerConfig::new("jobs", "email", "worker-a", "jobs.dlq").unwrap();
        assert_eq!(
            config.retry_schedule_key(),
            "jobs:rustee:jobs:email:retry:schedule"
        );
        assert_eq!(
            config.retry_payload_key(),
            "jobs:rustee:jobs:email:retry:payload"
        );
        assert_eq!(
            config.retry_attempt_key(),
            "jobs:rustee:jobs:email:retry:attempt"
        );
        assert_eq!(
            RedisStreamsWorkerConfig::new("jobs", "workers", "worker-a", "jobs").unwrap_err(),
            ConfigError::DeadLetterMatchesSource
        );
        assert_eq!(
            config
                .clone()
                .with_reclaim_idle(Duration::ZERO)
                .unwrap_err(),
            ConfigError::ZeroDuration
        );
        assert_eq!(
            config
                .clone()
                .with_operation_timeout(Duration::ZERO)
                .unwrap_err(),
            ConfigError::ZeroDuration
        );
        assert_eq!(
            config
                .with_operation_timeout(Duration::from_millis(250))
                .unwrap_err(),
            ConfigError::OperationTimeoutNotLongerThanBlock
        );
    }
}
