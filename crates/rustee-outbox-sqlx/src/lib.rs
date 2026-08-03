//! `PostgreSQL` transactional outbox storage and inbox deduplication for `Rustee` events and jobs.
//!
//! Applications write their business rows and one [`OutboxMessage`] through the same `PostgreSQL`
//! transaction. Relays lease rows with `FOR UPDATE SKIP LOCKED`, publish through the existing
//! event or job publisher contract, then confirm the lease. A process crash or lost confirmation
//! after a broker acknowledgement can publish a message again, so consumers must remain
//! idempotent. This crate intentionally does not claim a cross-store exactly-once guarantee.
//!
//! The SQL migrations are deployment-owned. Add [`OUTBOX_MIGRATION_SQL`] and
//! [`INBOX_MIGRATION_SQL`] to an application's normal migration set; do not apply them from HTTP
//! application startup.

use std::{
    error::Error as StdError,
    fmt,
    future::Future,
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    time::{Duration, Instant},
};

use rustee_events::{
    EnvelopeError as EventEnvelopeError, Event, EventEnvelope, EventId, EventMessage,
    EventPublisher,
};
use rustee_jobs::{
    EnvelopeError as JobEnvelopeError, Job, JobEnvelope, JobId, JobMessage, JobPublisher,
};
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

const EVENT_KIND: &str = "event";
const JOB_KIND: &str = "job";
const MAX_DESTINATION_BYTES: usize = 255;
const MAX_MESSAGE_ID_BYTES: usize = 255;
const MAX_MESSAGE_TYPE_BYTES: usize = 255;
const MAX_ORDERING_KEY_BYTES: usize = 512;
const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
const MAX_BATCH_SIZE: usize = 1_000;
const MAX_LEASE_DURATION: Duration = Duration::from_hours(1);
const MAX_RELAY_IDLE_DELAY: Duration = Duration::from_hours(1);
const MAX_JOB_SCHEDULE_DELAY: Duration = Duration::from_hours(8_784);

/// The deployment migration for the shared `PostgreSQL` outbox table.
pub const OUTBOX_MIGRATION_SQL: &str = include_str!("../migrations/0001_rustee_outbox.sql");

/// The forward-only deployment migration that adds durable local relay priority to the outbox.
///
/// Apply this after [`OUTBOX_MIGRATION_SQL`]. Existing deployments must retain their original
/// `0001` migration checksum and add this file as a new migration rather than editing history.
pub const OUTBOX_PRIORITY_MIGRATION_SQL: &str =
    include_str!("../migrations/0003_rustee_outbox_priority.sql");

/// The deployment migration for the transactional consumer inbox table.
pub const INBOX_MIGRATION_SQL: &str = include_str!("../migrations/0002_rustee_inbox.sql");

/// A validated logical broker destination used to isolate a relay's leased records.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct OutboxDestination(String);

impl OutboxDestination {
    /// Creates a non-empty, bounded destination label.
    ///
    /// The adapter stores this label and uses it to select rows for a relay. Provider-specific
    /// topic or subject validation remains with the configured publisher.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxMessageError::InvalidDestination`] when the label is blank, contains a
    /// NUL byte, or exceeds the storage bound.
    pub fn new(destination: impl Into<String>) -> Result<Self, OutboxMessageError> {
        let destination = destination.into();
        validate_text(
            &destination,
            MAX_DESTINATION_BYTES,
            OutboxMessageError::InvalidDestination,
        )?;
        Ok(Self(destination))
    }

    /// Returns the stored logical destination label.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for OutboxDestination {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A bounded consumer identity that scopes one durable idempotency ledger.
///
/// Use a stable name per projection, integration, or job side effect. Independent consumer groups
/// must not accidentally share a receipt merely because they receive the same event ID.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct InboxConsumer(String);

impl InboxConsumer {
    /// Creates a non-empty, bounded consumer identity.
    ///
    /// # Errors
    ///
    /// Returns [`InboxError::InvalidConsumer`] when the value is blank, contains a NUL byte, or
    /// exceeds the storage bound.
    pub fn new(consumer: impl Into<String>) -> Result<Self, InboxError> {
        let consumer = consumer.into();
        validate_inbox_text(&consumer, InboxError::InvalidConsumer)?;
        Ok(Self(consumer))
    }

    /// Returns the stable consumer identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for InboxConsumer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A bounded source message identifier used as an inbox receipt key.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct InboxMessageId(String);

impl InboxMessageId {
    /// Creates a receipt key from a stable source message identifier.
    ///
    /// # Errors
    ///
    /// Returns [`InboxError::InvalidMessageId`] when the value is blank, contains a NUL byte, or
    /// exceeds the storage bound.
    pub fn new(message_id: impl Into<String>) -> Result<Self, InboxError> {
        let message_id = message_id.into();
        validate_inbox_text(&message_id, InboxError::InvalidMessageId)?;
        Ok(Self(message_id))
    }

    /// Creates a receipt key from one Rustee event ID.
    #[must_use]
    pub fn event(id: EventId) -> Self {
        Self(id.to_string())
    }

    /// Creates a receipt key from one Rustee durable job ID.
    #[must_use]
    pub fn job(id: JobId) -> Self {
        Self(id.to_string())
    }

    /// Returns the stable source message identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for InboxMessageId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Result of registering a durable receipt inside the caller's side-effect transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InboxDecision {
    /// This transaction owns the first successful business effect for this consumer and source ID.
    FirstDelivery,
    /// A previously committed transaction already completed this consumer and source ID.
    Duplicate,
}

/// `PostgreSQL` inbox receipt operations for idempotent database-backed consumers.
///
/// Call [`Self::register`] in the same transaction as the database side effect. A rollback removes
/// a first-delivery receipt, while a committed receipt makes the next delivery return
/// [`InboxDecision::Duplicate`]. This is intentionally not a generic handler wrapper: external
/// side effects such as email or HTTP calls require their own idempotency capability.
#[derive(Clone, Copy, Debug, Default)]
pub struct PostgresInbox;

impl PostgresInbox {
    /// Registers a source message receipt in the caller's open business-data transaction.
    ///
    /// A caller that receives [`InboxDecision::FirstDelivery`] performs only database side effects
    /// in this transaction and commits it. A caller that receives [`InboxDecision::Duplicate`]
    /// skips those side effects and can return handler success so its broker position is settled.
    ///
    /// # Errors
    ///
    /// Returns the database error when the inbox migration is absent or the transaction fails.
    pub async fn register(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        consumer: &InboxConsumer,
        message_id: &InboxMessageId,
    ) -> Result<InboxDecision, sqlx::Error> {
        let result = sqlx::query(
            "INSERT INTO rustee_inbox (consumer, message_id) VALUES ($1, $2) \
             ON CONFLICT (consumer, message_id) DO NOTHING",
        )
        .bind(consumer.as_str())
        .bind(message_id.as_str())
        .execute(&mut **transaction)
        .await?;
        if result.rows_affected() == 1 {
            Ok(InboxDecision::FirstDelivery)
        } else {
            Ok(InboxDecision::Duplicate)
        }
    }
}

/// Invalid inbox receipt metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InboxError {
    /// The stable consumer identity was not safe to store.
    #[error("inbox consumer must be non-blank, NUL-free, and bounded")]
    InvalidConsumer,
    /// The source message identifier was not safe to store.
    #[error("inbox message ID must be non-blank, NUL-free, and bounded")]
    InvalidMessageId,
}

/// Unique identifier for one durable outbox row.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct OutboxId(Uuid);

impl OutboxId {
    /// Creates an identifier for one newly staged outbox record.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }
}

impl Default for OutboxId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for OutboxId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// The durable envelope category stored in an outbox row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxKind {
    /// A versioned append-only event stream record.
    Event,
    /// A versioned durable background job.
    Job,
}

/// A bounded relay preference for one staged outbox message.
///
/// Higher values are claimed before lower values for the same outbox kind and destination. This
/// is a local relay ordering hint, not a broker priority, fairness guarantee, or global rate
/// limit. Equal priorities retain the durable `created_at`, then row-ID order.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct OutboxPriority(u8);

impl OutboxPriority {
    /// The default priority that preserves the existing FIFO claim order among ordinary rows.
    pub const NORMAL: Self = Self(0);

    /// Creates a priority from its bounded durable representation.
    #[must_use]
    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    /// Returns the priority value stored with the staged row.
    #[must_use]
    pub const fn value(self) -> u8 {
        self.0
    }
}

impl OutboxKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Event => EVENT_KIND,
            Self::Job => JOB_KIND,
        }
    }
}

/// A validated, serialized event or job awaiting a `PostgreSQL` transaction commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxMessage {
    id: OutboxId,
    kind: OutboxKind,
    destination: OutboxDestination,
    message_id: String,
    message_type: String,
    schema_version: u16,
    ordering_key: String,
    delivery_attempt: u16,
    priority: OutboxPriority,
    payload: Vec<u8>,
}

impl OutboxMessage {
    /// Serializes one event envelope for an outbox transaction.
    ///
    /// # Errors
    ///
    /// Returns an event envelope encoding error or [`OutboxMessageError`] when its provider
    /// metadata cannot fit within the durable outbox contract.
    pub fn event<E>(
        destination: OutboxDestination,
        envelope: &EventEnvelope<E>,
    ) -> Result<Self, StageEventError>
    where
        E: Event,
    {
        let message = envelope.message().map_err(StageEventError::Envelope)?;
        Self::from_event_message(destination, message).map_err(StageEventError::Outbox)
    }

    /// Builds an outbox record from an already serialized event message.
    ///
    /// This is useful when an application has intentionally separated envelope construction from
    /// its database transaction. The message must still be staged before that transaction commits.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxMessageError`] when stored metadata exceeds its bounded contract.
    pub fn from_event_message(
        destination: OutboxDestination,
        message: EventMessage,
    ) -> Result<Self, OutboxMessageError> {
        let id = message.id().to_string();
        let message_type = message.event_type().to_owned();
        let schema_version = message.version();
        let ordering_key = message.key().to_owned();
        let payload = message.into_payload();
        Self::new(
            OutboxKind::Event,
            destination,
            id,
            message_type,
            schema_version,
            ordering_key,
            1,
            payload,
        )
    }

    /// Serializes one durable job envelope for an outbox transaction.
    ///
    /// The job's stable ID becomes the ordering key because job providers do not share an event
    /// partition-key contract.
    ///
    /// # Errors
    ///
    /// Returns a job envelope encoding error or [`OutboxMessageError`] when metadata cannot fit
    /// within the durable outbox contract.
    pub fn job<J>(
        destination: OutboxDestination,
        envelope: &JobEnvelope<J>,
    ) -> Result<Self, StageJobError>
    where
        J: Job,
    {
        let message = envelope.message().map_err(StageJobError::Envelope)?;
        Self::from_job_message(destination, message).map_err(StageJobError::Outbox)
    }

    /// Builds an outbox record from an already serialized durable job message.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxMessageError`] when stored metadata exceeds its bounded contract.
    pub fn from_job_message(
        destination: OutboxDestination,
        message: JobMessage,
    ) -> Result<Self, OutboxMessageError> {
        let id = message.id().to_string();
        let message_type = message.name().to_owned();
        let schema_version = message.version();
        let delivery_attempt = message.attempt();
        let ordering_key = id.clone();
        let payload = message.into_payload();
        Self::new(
            OutboxKind::Job,
            destination,
            id,
            message_type,
            schema_version,
            ordering_key,
            delivery_attempt,
            payload,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        kind: OutboxKind,
        destination: OutboxDestination,
        message_id: String,
        message_type: String,
        schema_version: u16,
        ordering_key: String,
        delivery_attempt: u16,
        payload: Vec<u8>,
    ) -> Result<Self, OutboxMessageError> {
        validate_text(
            &message_id,
            MAX_MESSAGE_ID_BYTES,
            OutboxMessageError::InvalidMessageId,
        )?;
        validate_text(
            &message_type,
            MAX_MESSAGE_TYPE_BYTES,
            OutboxMessageError::InvalidMessageType,
        )?;
        validate_text(
            &ordering_key,
            MAX_ORDERING_KEY_BYTES,
            OutboxMessageError::InvalidOrderingKey,
        )?;
        if delivery_attempt == 0 {
            return Err(OutboxMessageError::InvalidDeliveryAttempt);
        }
        if payload.is_empty() || payload.len() > MAX_PAYLOAD_BYTES {
            return Err(OutboxMessageError::InvalidPayload);
        }
        Ok(Self {
            id: OutboxId::new(),
            kind,
            destination,
            message_id,
            message_type,
            schema_version,
            ordering_key,
            delivery_attempt,
            priority: OutboxPriority::NORMAL,
            payload,
        })
    }

    /// Returns the row identifier assigned when this message was staged.
    #[must_use]
    pub const fn id(&self) -> OutboxId {
        self.id
    }

    /// Returns whether this message carries an event or a job envelope.
    #[must_use]
    pub const fn kind(&self) -> OutboxKind {
        self.kind
    }

    /// Returns the logical destination label that selects the relay.
    #[must_use]
    pub fn destination(&self) -> &OutboxDestination {
        &self.destination
    }

    /// Overrides this row's relay preference before it is staged.
    ///
    /// Priority changes only the order in which one destination's eligible outbox rows are
    /// claimed. It neither changes the durable source-message deduplication key nor guarantees
    /// ordering after a broker accepts the message.
    #[must_use]
    pub fn with_priority(mut self, priority: OutboxPriority) -> Self {
        self.priority = priority;
        self
    }

    /// Returns this row's local relay preference.
    #[must_use]
    pub const fn priority(&self) -> OutboxPriority {
        self.priority
    }
}

/// Failed serialization or validation while staging an event.
#[derive(Debug, thiserror::Error)]
pub enum StageEventError {
    /// The event envelope could not be encoded.
    #[error(transparent)]
    Envelope(EventEnvelopeError),
    /// The serialized event metadata could not be stored safely.
    #[error(transparent)]
    Outbox(OutboxMessageError),
}

/// Failed serialization or validation while staging a durable job.
#[derive(Debug, thiserror::Error)]
pub enum StageJobError {
    /// The job envelope could not be encoded.
    #[error(transparent)]
    Envelope(JobEnvelopeError),
    /// The serialized job metadata could not be stored safely.
    #[error(transparent)]
    Outbox(OutboxMessageError),
}

/// Invalid metadata for a message entering the durable outbox.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OutboxMessageError {
    /// The logical destination label was not safe to store.
    #[error("outbox destination must be non-blank, NUL-free, and bounded")]
    InvalidDestination,
    /// The source event or job ID was not safe to store.
    #[error("outbox message ID must be non-blank, NUL-free, and bounded")]
    InvalidMessageId,
    /// The stable event type or job name was not safe to store.
    #[error("outbox message type must be non-blank, NUL-free, and bounded")]
    InvalidMessageType,
    /// The event partition key or job ordering key was not safe to store.
    #[error("outbox ordering key must be non-blank, NUL-free, and bounded")]
    InvalidOrderingKey,
    /// The durable job delivery attempt was zero.
    #[error("outbox job delivery attempt must be at least one")]
    InvalidDeliveryAttempt,
    /// The serialized envelope was empty or exceeded the outbox row payload limit.
    #[error("outbox payload must be non-empty and at most one MiB")]
    InvalidPayload,
}

/// A validated relative delay for one durable job staged through the `PostgreSQL` outbox.
///
/// The delay is evaluated by `PostgreSQL`'s clock when the job is staged, not by an application
/// process clock. The existing [`JobOutboxRelay`] claims the row only after it becomes available;
/// applications still own the relay loop, readiness, metrics, and shutdown lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobSchedule {
    delay: Duration,
}

impl JobSchedule {
    /// Creates one delayed-job schedule relative to staging time.
    ///
    /// Use [`PostgresOutbox::stage`] for immediately eligible messages. One-time job schedules
    /// are bounded to 366 days; cron and recurring schedules remain deployment-owned workflows.
    ///
    /// # Errors
    ///
    /// Returns [`JobScheduleError::ZeroDelay`] for an immediate delay or
    /// [`JobScheduleError::DelayTooLong`] when the delay exceeds the durable scheduling bound.
    pub fn after(delay: Duration) -> Result<Self, JobScheduleError> {
        if delay.is_zero() {
            return Err(JobScheduleError::ZeroDelay);
        }
        if delay > MAX_JOB_SCHEDULE_DELAY {
            return Err(JobScheduleError::DelayTooLong);
        }
        Ok(Self { delay })
    }

    /// Returns the delay evaluated by the `PostgreSQL` staging operation.
    #[must_use]
    pub const fn delay(&self) -> Duration {
        self.delay
    }

    fn delay_millis(&self) -> i64 {
        i64::try_from(self.delay.as_millis())
            .expect("validated job schedule delay must fit PostgreSQL milliseconds")
    }
}

/// Invalid one-time durable job schedule configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum JobScheduleError {
    /// Immediate jobs use the ordinary outbox staging operation.
    #[error("delayed job schedule must be greater than zero")]
    ZeroDelay,
    /// Recurring or longer-lived workflows must be handled by a deployment-owned scheduler.
    #[error("delayed job schedule must be at most 366 days")]
    DelayTooLong,
}

/// A validated relative delay for one append-only event staged through the `PostgreSQL` outbox.
///
/// The delay is evaluated by `PostgreSQL`'s clock. The existing [`EventOutboxRelay`] claims the
/// row only after it is due; callers still own relay supervision, broker provisioning, and any
/// retry-attempt metadata required by a particular event provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventSchedule {
    delay: Duration,
}

impl EventSchedule {
    /// Creates one delayed-event schedule relative to staging time.
    ///
    /// Use [`PostgresOutbox::stage`] for immediately eligible messages. Delayed events are
    /// bounded to 366 days; recurring calendars and provider-specific retry routing remain
    /// explicit integrations.
    ///
    /// # Errors
    ///
    /// Returns [`EventScheduleError::ZeroDelay`] for an immediate delay or
    /// [`EventScheduleError::DelayTooLong`] when the delay exceeds the durable scheduling bound.
    pub fn after(delay: Duration) -> Result<Self, EventScheduleError> {
        if delay.is_zero() {
            return Err(EventScheduleError::ZeroDelay);
        }
        if delay > MAX_JOB_SCHEDULE_DELAY {
            return Err(EventScheduleError::DelayTooLong);
        }
        Ok(Self { delay })
    }

    /// Returns the PostgreSQL-clock-relative delay.
    #[must_use]
    pub const fn delay(&self) -> Duration {
        self.delay
    }

    fn delay_millis(&self) -> i64 {
        i64::try_from(self.delay.as_millis())
            .expect("validated event schedule delay must fit PostgreSQL milliseconds")
    }
}

/// Invalid one-time durable event schedule configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EventScheduleError {
    /// Immediate events use the ordinary outbox staging operation.
    #[error("delayed event schedule must be greater than zero")]
    ZeroDelay,
    /// Recurring or longer-lived workflows must be handled by a dedicated calendar integration.
    #[error("delayed event schedule must be at most 366 days")]
    DelayTooLong,
}

/// Failure while staging a delayed durable job.
#[derive(Debug, thiserror::Error)]
pub enum ScheduleJobError {
    /// Only durable jobs, rather than append-only events, can use the delayed-job API.
    #[error("only durable job messages can be staged with a job schedule")]
    NotAJob,
    /// `PostgreSQL` rejected the scheduling insert or the outbox migration is unavailable.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Failure while staging a delayed append-only event.
#[derive(Debug, thiserror::Error)]
pub enum ScheduleEventError {
    /// Only append-only events, rather than durable jobs, can use the delayed-event API.
    #[error("only event messages can be staged with an event schedule")]
    NotAnEvent,
    /// `PostgreSQL` rejected the scheduling insert or the outbox migration is unavailable.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Configuration for one bounded `SKIP LOCKED` claim operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeaseConfig {
    batch_size: NonZeroUsize,
    lease_duration: Duration,
}

impl LeaseConfig {
    /// Creates a bounded lease configuration.
    ///
    /// # Errors
    ///
    /// Returns [`LeaseConfigError`] when the batch is too large, or a lease duration is zero or
    /// longer than one hour.
    pub fn new(
        batch_size: NonZeroUsize,
        lease_duration: Duration,
    ) -> Result<Self, LeaseConfigError> {
        if batch_size.get() > MAX_BATCH_SIZE {
            return Err(LeaseConfigError::BatchTooLarge);
        }
        if lease_duration.is_zero() || lease_duration > MAX_LEASE_DURATION {
            return Err(LeaseConfigError::InvalidLeaseDuration);
        }
        Ok(Self {
            batch_size,
            lease_duration,
        })
    }

    /// Returns the maximum number of rows one relay process can claim at a time.
    #[must_use]
    pub const fn batch_size(&self) -> NonZeroUsize {
        self.batch_size
    }

    /// Returns the bounded exclusive-lease duration.
    #[must_use]
    pub const fn lease_duration(&self) -> Duration {
        self.lease_duration
    }
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self::new(
            NonZeroUsize::new(100).expect("100 is non-zero"),
            Duration::from_secs(30),
        )
        .expect("default outbox lease configuration is valid")
    }
}

/// Invalid relay lease configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum LeaseConfigError {
    /// One relay attempted to claim more than the fixed bound.
    #[error("outbox lease batch size must be at most 1000")]
    BatchTooLarge,
    /// The lease duration could not protect one bounded broker publish attempt.
    #[error("outbox lease duration must be greater than zero and at most one hour")]
    InvalidLeaseDuration,
}

/// Outcome of an insert that is deduplicated by kind, destination, and source message ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StageOutcome {
    /// The event or job was inserted into the still-open transaction.
    Inserted(OutboxId),
    /// An earlier transaction already staged the same source message for this destination.
    AlreadyPresent,
}

/// Outcome of confirming or releasing a row with a lease token.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseOutcome {
    /// This relay still owned the row and the state transition was persisted.
    Applied,
    /// The row was already confirmed or its lease was replaced by another relay.
    Lost,
}

/// Shared `PostgreSQL` outbox storage operations.
///
/// This type has no background task. Applications control migration deployment, relay scheduling,
/// readiness, logging, metrics, and graceful shutdown explicitly.
#[derive(Clone, Copy, Debug, Default)]
pub struct PostgresOutbox;

impl PostgresOutbox {
    /// Inserts one message using the caller's existing business-data transaction.
    ///
    /// A rollback removes both the business mutation and this staged message. A successful commit
    /// makes it visible to a relay. The unique constraint also suppresses a repeated attempt to
    /// stage the same source message to the same destination.
    ///
    /// # Errors
    ///
    /// Returns the database error when the outbox migration is absent or the transaction fails.
    pub async fn stage(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        message: &OutboxMessage,
    ) -> Result<StageOutcome, sqlx::Error> {
        let result = sqlx::query(
            "INSERT INTO rustee_outbox \
             (id, kind, destination, message_id, message_type, schema_version, ordering_key, \
              delivery_attempt, priority, payload) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
             ON CONFLICT (kind, destination, message_id) DO NOTHING",
        )
        .bind(message.id.0)
        .bind(message.kind.as_str())
        .bind(message.destination.as_str())
        .bind(&message.message_id)
        .bind(&message.message_type)
        .bind(i32::from(message.schema_version))
        .bind(&message.ordering_key)
        .bind(i32::from(message.delivery_attempt))
        .bind(i16::from(message.priority.value()))
        .bind(&message.payload)
        .execute(&mut **transaction)
        .await?;
        if result.rows_affected() == 1 {
            Ok(StageOutcome::Inserted(message.id))
        } else {
            Ok(StageOutcome::AlreadyPresent)
        }
    }

    /// Inserts one durable job so it becomes eligible for relay after a PostgreSQL-clock delay.
    ///
    /// This stays inside the caller's business-data transaction, so rollback removes both the
    /// business mutation and the scheduled job. The unique source-message constraint still wins:
    /// a duplicate stage returns [`StageOutcome::AlreadyPresent`] and never shifts a job that was
    /// already scheduled. The relay must continue calling [`Self::lease_jobs`] to release due
    /// rows; this type deliberately owns no background task.
    ///
    /// # Errors
    ///
    /// Returns [`ScheduleJobError::NotAJob`] when an event message is supplied, or
    /// [`ScheduleJobError::Database`] when the outbox migration is absent or the transaction
    /// fails.
    pub async fn stage_job_after(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        message: &OutboxMessage,
        schedule: JobSchedule,
    ) -> Result<StageOutcome, ScheduleJobError> {
        if message.kind != OutboxKind::Job {
            return Err(ScheduleJobError::NotAJob);
        }
        let result = sqlx::query(
            "INSERT INTO rustee_outbox \
             (id, kind, destination, message_id, message_type, schema_version, ordering_key, \
              delivery_attempt, priority, payload, available_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, \
                     clock_timestamp() + ($11::bigint * INTERVAL '1 millisecond')) \
             ON CONFLICT (kind, destination, message_id) DO NOTHING",
        )
        .bind(message.id.0)
        .bind(message.kind.as_str())
        .bind(message.destination.as_str())
        .bind(&message.message_id)
        .bind(&message.message_type)
        .bind(i32::from(message.schema_version))
        .bind(&message.ordering_key)
        .bind(i32::from(message.delivery_attempt))
        .bind(i16::from(message.priority.value()))
        .bind(&message.payload)
        .bind(schedule.delay_millis())
        .execute(&mut **transaction)
        .await?;
        if result.rows_affected() == 1 {
            Ok(StageOutcome::Inserted(message.id))
        } else {
            Ok(StageOutcome::AlreadyPresent)
        }
    }

    /// Inserts one append-only event so it becomes eligible for relay after a PostgreSQL-clock
    /// delay.
    ///
    /// A duplicate stage preserves the first durable availability timestamp. This primitive does
    /// not create a Kafka retry header, commit a broker offset, or decide a retry policy; those
    /// cross-store semantics belong to a provider-specific failure router.
    ///
    /// # Errors
    ///
    /// Returns [`ScheduleEventError::NotAnEvent`] when a job message is supplied, or
    /// [`ScheduleEventError::Database`] when the outbox migration is absent or the transaction
    /// fails.
    pub async fn stage_event_after(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        message: &OutboxMessage,
        schedule: EventSchedule,
    ) -> Result<StageOutcome, ScheduleEventError> {
        if message.kind != OutboxKind::Event {
            return Err(ScheduleEventError::NotAnEvent);
        }
        let result = sqlx::query(
            "INSERT INTO rustee_outbox \
             (id, kind, destination, message_id, message_type, schema_version, ordering_key, \
              delivery_attempt, priority, payload, available_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, \
                     clock_timestamp() + ($11::bigint * INTERVAL '1 millisecond')) \
             ON CONFLICT (kind, destination, message_id) DO NOTHING",
        )
        .bind(message.id.0)
        .bind(message.kind.as_str())
        .bind(message.destination.as_str())
        .bind(&message.message_id)
        .bind(&message.message_type)
        .bind(i32::from(message.schema_version))
        .bind(&message.ordering_key)
        .bind(i32::from(message.delivery_attempt))
        .bind(i16::from(message.priority.value()))
        .bind(&message.payload)
        .bind(schedule.delay_millis())
        .execute(&mut **transaction)
        .await?;
        if result.rows_affected() == 1 {
            Ok(StageOutcome::Inserted(message.id))
        } else {
            Ok(StageOutcome::AlreadyPresent)
        }
    }

    /// Claims available event rows for exactly one logical destination.
    ///
    /// Expired leases become eligible again. A relay must publish and then call
    /// [`Self::acknowledge_event`], or call [`Self::retry_event`] after a failed publish.
    ///
    /// # Errors
    ///
    /// Returns a database error or [`OutboxError::StoredEvent`] when rows violate the migration
    /// contract and cannot safely reconstruct an event provider message.
    pub async fn lease_events(
        &self,
        pool: &PgPool,
        destination: &OutboxDestination,
        config: LeaseConfig,
    ) -> Result<Vec<LeasedEvent>, OutboxError> {
        let records = self
            .lease(pool, OutboxKind::Event, destination, config)
            .await?;
        records
            .into_iter()
            .map(LeasedEvent::try_from_record)
            .collect()
    }

    /// Claims available durable-job rows for exactly one logical destination.
    ///
    /// # Errors
    ///
    /// Returns a database error or [`OutboxError::StoredJob`] when rows violate the migration
    /// contract and cannot safely reconstruct a job provider message.
    pub async fn lease_jobs(
        &self,
        pool: &PgPool,
        destination: &OutboxDestination,
        config: LeaseConfig,
    ) -> Result<Vec<LeasedJob>, OutboxError> {
        let records = self
            .lease(pool, OutboxKind::Job, destination, config)
            .await?;
        records
            .into_iter()
            .map(LeasedJob::try_from_record)
            .collect()
    }

    /// Confirms an event after the event publisher reports broker acknowledgement.
    ///
    /// A lost lease must be treated as possible duplicate delivery, not as proof that the broker
    /// append failed.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Database`] when the confirmation cannot be persisted.
    pub async fn acknowledge_event(
        &self,
        pool: &PgPool,
        lease: &LeasedEvent,
    ) -> Result<LeaseOutcome, OutboxError> {
        self.acknowledge(pool, &lease.lease).await
    }

    /// Confirms a durable job after its provider reports durable publication.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Database`] when the confirmation cannot be persisted.
    pub async fn acknowledge_job(
        &self,
        pool: &PgPool,
        lease: &LeasedJob,
    ) -> Result<LeaseOutcome, OutboxError> {
        self.acknowledge(pool, &lease.lease).await
    }

    /// Releases an event lease and makes the record eligible after a bounded delay.
    ///
    /// The persisted reason is the constant category `publish_failed`; raw provider error strings
    /// are deliberately not written to the database.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::InvalidDuration`] for an excessive delay or
    /// [`OutboxError::Database`] when the release cannot be persisted.
    pub async fn retry_event(
        &self,
        pool: &PgPool,
        lease: &LeasedEvent,
        delay: Duration,
    ) -> Result<LeaseOutcome, OutboxError> {
        self.retry(pool, &lease.lease, delay).await
    }

    /// Releases a durable-job lease and makes the record eligible after a bounded delay.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::InvalidDuration`] for an excessive delay or
    /// [`OutboxError::Database`] when the release cannot be persisted.
    pub async fn retry_job(
        &self,
        pool: &PgPool,
        lease: &LeasedJob,
        delay: Duration,
    ) -> Result<LeaseOutcome, OutboxError> {
        self.retry(pool, &lease.lease, delay).await
    }

    async fn lease(
        &self,
        pool: &PgPool,
        kind: OutboxKind,
        destination: &OutboxDestination,
        config: LeaseConfig,
    ) -> Result<Vec<StoredLease>, OutboxError> {
        let batch_size = i64::try_from(config.batch_size.get())
            .expect("lease batch size is bounded below i64::MAX");
        let lease_millis = duration_millis(config.lease_duration)?;
        let token = Uuid::new_v4();
        let rows = sqlx::query(
            "WITH candidates AS ( \
               SELECT id \
               FROM rustee_outbox \
               WHERE published_at IS NULL \
                 AND kind = $1 \
                 AND destination = $2 \
                 AND available_at <= clock_timestamp() \
                 AND (leased_until IS NULL OR leased_until <= clock_timestamp()) \
               ORDER BY priority DESC, created_at, id \
               FOR UPDATE SKIP LOCKED \
               LIMIT $3 \
             ), claimed AS ( \
               UPDATE rustee_outbox AS outbox \
               SET lease_token = $4, \
                   leased_until = clock_timestamp() + ($5::bigint * INTERVAL '1 millisecond'), \
                   relay_attempt = outbox.relay_attempt + 1 \
               FROM candidates \
               WHERE outbox.id = candidates.id \
               RETURNING outbox.id, outbox.message_id, outbox.message_type, \
                         outbox.schema_version, outbox.ordering_key, outbox.delivery_attempt, \
                         outbox.payload, outbox.relay_attempt, outbox.priority, outbox.created_at \
             ) \
             SELECT id, message_id, message_type, schema_version, ordering_key, delivery_attempt, \
                    payload, relay_attempt \
             FROM claimed \
             ORDER BY priority DESC, created_at, id",
        )
        .bind(kind.as_str())
        .bind(destination.as_str())
        .bind(batch_size)
        .bind(token)
        .bind(lease_millis)
        .fetch_all(pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let id = row.try_get::<Uuid, _>("id")?;
                let stored_error = || match kind {
                    OutboxKind::Event => OutboxError::StoredEvent,
                    OutboxKind::Job => OutboxError::StoredJob,
                };
                let schema_version = u16::try_from(row.try_get::<i32, _>("schema_version")?)
                    .map_err(|_| stored_error())?;
                let delivery_attempt = u16::try_from(row.try_get::<i32, _>("delivery_attempt")?)
                    .map_err(|_| stored_error())?;
                let relay_attempt = u32::try_from(row.try_get::<i32, _>("relay_attempt")?)
                    .map_err(|_| stored_error())?;
                Ok(StoredLease {
                    lease: Lease {
                        id: OutboxId::from_uuid(id),
                        token,
                        relay_attempt,
                    },
                    destination: destination.clone(),
                    message_id: row.try_get("message_id")?,
                    message_type: row.try_get("message_type")?,
                    schema_version,
                    ordering_key: row.try_get("ordering_key")?,
                    delivery_attempt,
                    payload: row.try_get("payload")?,
                })
            })
            .collect()
    }

    async fn acknowledge(&self, pool: &PgPool, lease: &Lease) -> Result<LeaseOutcome, OutboxError> {
        let result = sqlx::query(
            "UPDATE rustee_outbox \
             SET published_at = clock_timestamp(), lease_token = NULL, leased_until = NULL, \
                 last_failure_kind = NULL \
             WHERE id = $1 AND lease_token = $2 AND published_at IS NULL",
        )
        .bind(lease.id.0)
        .bind(lease.token)
        .execute(pool)
        .await?;
        Ok(outcome(result.rows_affected()))
    }

    async fn retry(
        &self,
        pool: &PgPool,
        lease: &Lease,
        delay: Duration,
    ) -> Result<LeaseOutcome, OutboxError> {
        let delay_millis = duration_millis(delay)?;
        let result = sqlx::query(
            "UPDATE rustee_outbox \
             SET available_at = clock_timestamp() + ($3::bigint * INTERVAL '1 millisecond'), \
                 lease_token = NULL, leased_until = NULL, last_failure_kind = 'publish_failed' \
             WHERE id = $1 AND lease_token = $2 AND published_at IS NULL",
        )
        .bind(lease.id.0)
        .bind(lease.token)
        .bind(delay_millis)
        .execute(pool)
        .await?;
        Ok(outcome(result.rows_affected()))
    }
}

/// One leased event plus the opaque token required to settle its outbox state.
#[derive(Clone, Debug)]
pub struct LeasedEvent {
    lease: Lease,
    destination: OutboxDestination,
    message: EventMessage,
}

impl LeasedEvent {
    fn try_from_record(record: StoredLease) -> Result<Self, OutboxError> {
        let message_id =
            Uuid::parse_str(&record.message_id).map_err(|_| OutboxError::StoredEvent)?;
        let message = EventMessage::from_parts(
            EventId::from_uuid(message_id),
            record.message_type,
            record.schema_version,
            record.ordering_key,
            record.payload,
        )
        .map_err(|_| OutboxError::StoredEvent)?;
        Ok(Self {
            lease: record.lease,
            destination: record.destination,
            message,
        })
    }

    /// Returns the durable outbox row identifier.
    #[must_use]
    pub const fn id(&self) -> OutboxId {
        self.lease.id
    }

    /// Returns how many relay publish attempts have claimed this row, starting at one.
    #[must_use]
    pub const fn relay_attempt(&self) -> u32 {
        self.lease.relay_attempt
    }

    /// Returns the destination label selected for this relay.
    #[must_use]
    pub fn destination(&self) -> &OutboxDestination {
        &self.destination
    }

    /// Returns the reconstructed event provider message.
    #[must_use]
    pub fn message(&self) -> &EventMessage {
        &self.message
    }
}

/// One leased durable job plus the opaque token required to settle its outbox state.
#[derive(Clone, Debug)]
pub struct LeasedJob {
    lease: Lease,
    destination: OutboxDestination,
    message: JobMessage,
}

impl LeasedJob {
    fn try_from_record(record: StoredLease) -> Result<Self, OutboxError> {
        let message_id = Uuid::parse_str(&record.message_id).map_err(|_| OutboxError::StoredJob)?;
        let message = JobMessage::from_parts(
            JobId::from_uuid(message_id),
            record.message_type,
            record.schema_version,
            record.delivery_attempt,
            record.payload,
        )
        .map_err(|_| OutboxError::StoredJob)?;
        Ok(Self {
            lease: record.lease,
            destination: record.destination,
            message,
        })
    }

    /// Returns the durable outbox row identifier.
    #[must_use]
    pub const fn id(&self) -> OutboxId {
        self.lease.id
    }

    /// Returns how many relay publish attempts have claimed this row, starting at one.
    #[must_use]
    pub const fn relay_attempt(&self) -> u32 {
        self.lease.relay_attempt
    }

    /// Returns the destination label selected for this relay.
    #[must_use]
    pub fn destination(&self) -> &OutboxDestination {
        &self.destination
    }

    /// Returns the reconstructed durable job provider message.
    #[must_use]
    pub fn message(&self) -> &JobMessage {
        &self.message
    }
}

#[derive(Clone, Debug)]
struct Lease {
    id: OutboxId,
    token: Uuid,
    relay_attempt: u32,
}

#[derive(Clone, Debug)]
struct StoredLease {
    lease: Lease,
    destination: OutboxDestination,
    message_id: String,
    message_type: String,
    schema_version: u16,
    ordering_key: String,
    delivery_attempt: u16,
    payload: Vec<u8>,
}

/// Event relay settings, including its claim lease and retry delay after publish failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelayConfig {
    lease: LeaseConfig,
    retry_delay: Duration,
}

impl RelayConfig {
    /// Creates relay settings with a finite retry delay.
    ///
    /// # Errors
    ///
    /// Returns [`RelayConfigError::InvalidRetryDelay`] when the delay is longer than one hour.
    pub fn new(lease: LeaseConfig, retry_delay: Duration) -> Result<Self, RelayConfigError> {
        if retry_delay > MAX_LEASE_DURATION {
            return Err(RelayConfigError::InvalidRetryDelay);
        }
        Ok(Self { lease, retry_delay })
    }

    /// Returns the bounded row claim configuration.
    #[must_use]
    pub const fn lease(&self) -> LeaseConfig {
        self.lease
    }

    /// Returns the delay used after the publisher reports failure.
    #[must_use]
    pub const fn retry_delay(&self) -> Duration {
        self.retry_delay
    }
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self::new(LeaseConfig::default(), Duration::from_secs(1))
            .expect("default outbox relay configuration is valid")
    }
}

/// Invalid outbox relay configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RelayConfigError {
    /// A delayed retry exceeded the bounded storage interval.
    #[error("outbox retry delay must be at most one hour")]
    InvalidRetryDelay,
}

/// Explicit polling settings for [`EventOutboxRelay::run_until`] and
/// [`JobOutboxRelay::run_until`].
///
/// This config does not start a background task. The application chooses where to await the
/// relay, supplies its shutdown future, and owns readiness, supervision, and metric export.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelayLoopConfig {
    idle_delay: Duration,
}

impl RelayLoopConfig {
    /// Creates relay loop settings with a bounded delay after an empty pass.
    ///
    /// # Errors
    ///
    /// Returns [`RelayLoopConfigError`] when the delay is zero or longer than one hour.
    pub fn new(idle_delay: Duration) -> Result<Self, RelayLoopConfigError> {
        if idle_delay.is_zero() {
            return Err(RelayLoopConfigError::ZeroIdleDelay);
        }
        if idle_delay > MAX_RELAY_IDLE_DELAY {
            return Err(RelayLoopConfigError::IdleDelayTooLong);
        }
        Ok(Self { idle_delay })
    }

    /// Returns the delay inserted only after a relay pass claims no rows.
    #[must_use]
    pub const fn idle_delay(&self) -> Duration {
        self.idle_delay
    }
}

impl Default for RelayLoopConfig {
    fn default() -> Self {
        Self::new(Duration::from_secs(1)).expect("default outbox relay loop configuration is valid")
    }
}

/// Invalid explicit outbox relay loop settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RelayLoopConfigError {
    /// An empty-pass delay of zero would create a database polling loop.
    #[error("outbox relay idle delay must be greater than zero")]
    ZeroIdleDelay,
    /// The bounded polling delay exceeded the supported operational interval.
    #[error("outbox relay idle delay must be at most one hour")]
    IdleDelayTooLong,
}

/// Per-pass relay counts. Provider failure ends the pass after scheduling its retry.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RelayReport {
    /// Number of rows leased at the start of the pass.
    pub claimed: usize,
    /// Number of rows confirmed after broker acknowledgement.
    pub published: usize,
    /// Number of failed rows successfully released for a later retry.
    pub retry_scheduled: usize,
    /// Number of confirmation or retry operations that lost their lease ownership.
    pub lease_lost: usize,
}

/// Aggregate counts collected while an explicit relay loop was running.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RelayLoopReport {
    /// Number of bounded relay passes completed before shutdown.
    pub passes: usize,
    /// Total rows claimed across completed passes.
    pub claimed: usize,
    /// Total rows confirmed after broker acknowledgement across completed passes.
    pub published: usize,
    /// Total failed rows successfully released for a later retry across completed passes.
    pub retry_scheduled: usize,
    /// Total confirmation or retry operations that lost lease ownership across completed passes.
    pub lease_lost: usize,
}

/// Fixed relay category used by bounded observability labels.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RelayPassKind {
    /// The relay is publishing append-only events.
    Event,
    /// The relay is publishing durable jobs.
    Job,
}

impl RelayPassKind {
    /// Returns the exporter-safe relay category label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Event => "event",
            Self::Job => "job",
        }
    }
}

/// Terminal outcome of one relay pass.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RelayPassOutcome {
    /// The pass completed its bounded claim/publish/confirm sequence.
    Succeeded,
    /// A database or publisher failure ended the pass.
    Failed,
    /// The async task was cancelled before the pass returned a result.
    Abandoned,
}

impl RelayPassOutcome {
    /// Returns the exporter-safe pass outcome label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Abandoned => "abandoned",
        }
    }
}

/// Metadata emitted when an event or job relay pass starts.
///
/// Destination, message identifiers, payloads, broker endpoint, and configuration are excluded
/// so an observer can safely use this as bounded operational telemetry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelayPassStarted {
    kind: RelayPassKind,
}

impl RelayPassStarted {
    /// Returns whether this pass relays events or durable jobs.
    #[must_use]
    pub const fn kind(self) -> RelayPassKind {
        self.kind
    }
}

/// Metadata emitted when an event or job relay pass finishes or is abandoned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelayPassFinished {
    kind: RelayPassKind,
    outcome: RelayPassOutcome,
    report: Option<RelayReport>,
    duration: Duration,
}

impl RelayPassFinished {
    /// Returns whether this pass relayed events or durable jobs.
    #[must_use]
    pub const fn kind(self) -> RelayPassKind {
        self.kind
    }

    /// Returns the terminal status without exposing provider error detail.
    #[must_use]
    pub const fn outcome(self) -> RelayPassOutcome {
        self.outcome
    }

    /// Returns bounded pass counts when the pass reached a reportable terminal state.
    ///
    /// Database failures before a stable report and externally cancelled passes return `None`.
    #[must_use]
    pub const fn report(self) -> Option<RelayReport> {
        self.report
    }

    /// Returns elapsed pass time, including broker publish and durable confirmation work.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.duration
    }
}

/// Synchronous, non-blocking observer for one transactional-outbox relay pass.
///
/// Implementations should aggregate locally or hand work to a bounded exporter queue. Observer
/// panics are caught so observability cannot alter durable publish or retry semantics.
pub trait OutboxRelayObserver: Send + Sync + 'static {
    /// Records the beginning of one bounded relay pass.
    fn on_relay_pass_started(&self, pass: RelayPassStarted);

    /// Records one completed, failed, or externally abandoned relay pass.
    fn on_relay_pass_finished(&self, pass: RelayPassFinished);
}

/// No-op observer used unless a relay opts into observability.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopOutboxRelayObserver;

impl OutboxRelayObserver for NoopOutboxRelayObserver {
    fn on_relay_pass_started(&self, _pass: RelayPassStarted) {}

    fn on_relay_pass_finished(&self, _pass: RelayPassFinished) {}
}

/// In-progress observability value owned by a single relay pass future.
///
/// Dropping this value without [`Self::finish`] records an `abandoned` pass, including a task
/// cancellation while it held or was about to obtain database leases.
pub struct RelayPassObservation {
    observer: Arc<dyn OutboxRelayObserver>,
    kind: RelayPassKind,
    started_at: Instant,
    finished: bool,
}

impl RelayPassObservation {
    /// Starts observing one bounded event or durable-job relay pass.
    #[must_use]
    pub fn start(observer: Arc<dyn OutboxRelayObserver>, kind: RelayPassKind) -> Self {
        notify_relay_started(&observer, RelayPassStarted { kind });
        Self {
            observer,
            kind,
            started_at: Instant::now(),
            finished: false,
        }
    }

    /// Emits a terminal pass outcome after the relay future has returned.
    pub fn finish(mut self, outcome: RelayPassOutcome, report: Option<RelayReport>) {
        self.finished = true;
        notify_relay_finished(
            &self.observer,
            RelayPassFinished {
                kind: self.kind,
                outcome,
                report,
                duration: self.started_at.elapsed(),
            },
        );
    }
}

impl Drop for RelayPassObservation {
    fn drop(&mut self) {
        if !self.finished {
            notify_relay_finished(
                &self.observer,
                RelayPassFinished {
                    kind: self.kind,
                    outcome: RelayPassOutcome::Abandoned,
                    report: None,
                    duration: self.started_at.elapsed(),
                },
            );
        }
    }
}

impl fmt::Debug for RelayPassObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelayPassObservation")
            .field("kind", &self.kind)
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

fn notify_relay_started(observer: &Arc<dyn OutboxRelayObserver>, pass: RelayPassStarted) {
    let _ = catch_unwind(AssertUnwindSafe(|| observer.on_relay_pass_started(pass)));
}

fn notify_relay_finished(observer: &Arc<dyn OutboxRelayObserver>, pass: RelayPassFinished) {
    let _ = catch_unwind(AssertUnwindSafe(|| observer.on_relay_pass_finished(pass)));
}

impl RelayLoopReport {
    fn record(&mut self, report: RelayReport) {
        self.passes = self.passes.saturating_add(1);
        self.claimed = self.claimed.saturating_add(report.claimed);
        self.published = self.published.saturating_add(report.published);
        self.retry_scheduled = self.retry_scheduled.saturating_add(report.retry_scheduled);
        self.lease_lost = self.lease_lost.saturating_add(report.lease_lost);
    }
}

/// A single-pass event relay wired to an existing [`EventPublisher`].
#[derive(Clone)]
pub struct EventOutboxRelay<P> {
    pool: PgPool,
    outbox: PostgresOutbox,
    publisher: P,
    destination: OutboxDestination,
    config: RelayConfig,
    observer: Arc<dyn OutboxRelayObserver>,
}

impl<P> fmt::Debug for EventOutboxRelay<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventOutboxRelay")
            .field("destination", &self.destination)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl<P> EventOutboxRelay<P>
where
    P: EventPublisher,
{
    /// Creates a relay for exactly one event destination and publisher configuration.
    #[must_use]
    pub fn new(
        pool: PgPool,
        publisher: P,
        destination: OutboxDestination,
        config: RelayConfig,
    ) -> Self {
        Self {
            pool,
            outbox: PostgresOutbox,
            publisher,
            destination,
            config,
            observer: Arc::new(NoopOutboxRelayObserver),
        }
    }

    /// Attaches one exporter-neutral relay pass observer.
    #[must_use]
    pub fn with_relay_observer(mut self, observer: Arc<dyn OutboxRelayObserver>) -> Self {
        self.observer = observer;
        self
    }

    /// Publishes one bounded batch and settles each successful lease.
    ///
    /// A publisher error schedules the failed row for retry using a constant sanitized failure
    /// category, then ends this pass with the original provider error. A caller owns loop timing,
    /// readiness, metrics, and graceful shutdown.
    ///
    /// # Errors
    ///
    /// Returns [`RelayError::Outbox`] when a lease transition cannot be persisted, or
    /// [`RelayError::Publisher`] after a failed broker append was rescheduled.
    pub async fn relay_once(&self) -> Result<RelayReport, RelayError<P::Error>> {
        let observation =
            RelayPassObservation::start(Arc::clone(&self.observer), RelayPassKind::Event);
        match self.relay_once_inner().await {
            Ok(report) => {
                observation.finish(RelayPassOutcome::Succeeded, Some(report));
                Ok(report)
            }
            Err(error) => {
                let report = match &error {
                    RelayError::Outbox(_) => None,
                    RelayError::Publisher { report, .. } => Some(*report),
                };
                observation.finish(RelayPassOutcome::Failed, report);
                Err(error)
            }
        }
    }

    async fn relay_once_inner(&self) -> Result<RelayReport, RelayError<P::Error>> {
        let leases = self
            .outbox
            .lease_events(&self.pool, &self.destination, self.config.lease)
            .await?;
        let mut report = RelayReport {
            claimed: leases.len(),
            ..RelayReport::default()
        };
        for lease in leases {
            match self.publisher.publish(lease.message.clone()).await {
                Ok(()) => match self.outbox.acknowledge_event(&self.pool, &lease).await? {
                    LeaseOutcome::Applied => report.published += 1,
                    LeaseOutcome::Lost => report.lease_lost += 1,
                },
                Err(error) => {
                    match self
                        .outbox
                        .retry_event(&self.pool, &lease, self.config.retry_delay)
                        .await?
                    {
                        LeaseOutcome::Applied => report.retry_scheduled += 1,
                        LeaseOutcome::Lost => report.lease_lost += 1,
                    }
                    return Err(RelayError::Publisher {
                        source: error,
                        report,
                    });
                }
            }
        }
        Ok(report)
    }

    /// Repeatedly executes bounded passes until the supplied shutdown future resolves.
    ///
    /// A shutdown signal is observed before each new pass and while waiting after an empty pass.
    /// A pass already holding leases finishes before shutdown is returned, so the loop never drops
    /// an in-progress pass merely to stop quickly. Publisher and database errors retain
    /// [`Self::relay_once`]'s behavior and end the loop for the application supervisor to handle.
    ///
    /// # Errors
    ///
    /// Returns the first [`RelayError`] produced by one bounded pass.
    pub async fn run_until<Shutdown>(
        &self,
        loop_config: RelayLoopConfig,
        shutdown: Shutdown,
    ) -> Result<RelayLoopReport, RelayError<P::Error>>
    where
        Shutdown: Future<Output = ()> + Send,
    {
        tokio::pin!(shutdown);
        let mut total = RelayLoopReport::default();
        loop {
            tokio::select! {
                biased;
                () = &mut shutdown => return Ok(total),
                () = tokio::task::yield_now() => {}
            }

            let report = self.relay_once().await?;
            total.record(report);
            if report.claimed == 0 {
                tokio::select! {
                    biased;
                    () = &mut shutdown => return Ok(total),
                    () = tokio::time::sleep(loop_config.idle_delay) => {}
                }
            }
        }
    }
}

/// A single-pass durable-job relay wired to an existing [`JobPublisher`].
#[derive(Clone)]
pub struct JobOutboxRelay<P> {
    pool: PgPool,
    outbox: PostgresOutbox,
    publisher: P,
    destination: OutboxDestination,
    config: RelayConfig,
    observer: Arc<dyn OutboxRelayObserver>,
}

impl<P> fmt::Debug for JobOutboxRelay<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobOutboxRelay")
            .field("destination", &self.destination)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl<P> JobOutboxRelay<P>
where
    P: JobPublisher,
{
    /// Creates a relay for exactly one durable-job destination and publisher configuration.
    #[must_use]
    pub fn new(
        pool: PgPool,
        publisher: P,
        destination: OutboxDestination,
        config: RelayConfig,
    ) -> Self {
        Self {
            pool,
            outbox: PostgresOutbox,
            publisher,
            destination,
            config,
            observer: Arc::new(NoopOutboxRelayObserver),
        }
    }

    /// Attaches one exporter-neutral relay pass observer.
    #[must_use]
    pub fn with_relay_observer(mut self, observer: Arc<dyn OutboxRelayObserver>) -> Self {
        self.observer = observer;
        self
    }

    /// Publishes one bounded batch and settles each successful lease.
    ///
    /// The retry and ownership behavior matches [`EventOutboxRelay::relay_once`].
    ///
    /// # Errors
    ///
    /// Returns [`RelayError::Outbox`] when a lease transition cannot be persisted, or
    /// [`RelayError::Publisher`] after a failed broker publish was rescheduled.
    pub async fn relay_once(&self) -> Result<RelayReport, RelayError<P::Error>> {
        let observation =
            RelayPassObservation::start(Arc::clone(&self.observer), RelayPassKind::Job);
        match self.relay_once_inner().await {
            Ok(report) => {
                observation.finish(RelayPassOutcome::Succeeded, Some(report));
                Ok(report)
            }
            Err(error) => {
                let report = match &error {
                    RelayError::Outbox(_) => None,
                    RelayError::Publisher { report, .. } => Some(*report),
                };
                observation.finish(RelayPassOutcome::Failed, report);
                Err(error)
            }
        }
    }

    async fn relay_once_inner(&self) -> Result<RelayReport, RelayError<P::Error>> {
        let leases = self
            .outbox
            .lease_jobs(&self.pool, &self.destination, self.config.lease)
            .await?;
        let mut report = RelayReport {
            claimed: leases.len(),
            ..RelayReport::default()
        };
        for lease in leases {
            match self.publisher.publish(lease.message.clone()).await {
                Ok(()) => match self.outbox.acknowledge_job(&self.pool, &lease).await? {
                    LeaseOutcome::Applied => report.published += 1,
                    LeaseOutcome::Lost => report.lease_lost += 1,
                },
                Err(error) => {
                    match self
                        .outbox
                        .retry_job(&self.pool, &lease, self.config.retry_delay)
                        .await?
                    {
                        LeaseOutcome::Applied => report.retry_scheduled += 1,
                        LeaseOutcome::Lost => report.lease_lost += 1,
                    }
                    return Err(RelayError::Publisher {
                        source: error,
                        report,
                    });
                }
            }
        }
        Ok(report)
    }

    /// Repeatedly executes bounded passes until the supplied shutdown future resolves.
    ///
    /// The shutdown and failure behavior matches [`EventOutboxRelay::run_until`]. This remains an
    /// explicit caller-owned future rather than a background scheduler.
    ///
    /// # Errors
    ///
    /// Returns the first [`RelayError`] produced by one bounded pass.
    pub async fn run_until<Shutdown>(
        &self,
        loop_config: RelayLoopConfig,
        shutdown: Shutdown,
    ) -> Result<RelayLoopReport, RelayError<P::Error>>
    where
        Shutdown: Future<Output = ()> + Send,
    {
        tokio::pin!(shutdown);
        let mut total = RelayLoopReport::default();
        loop {
            tokio::select! {
                biased;
                () = &mut shutdown => return Ok(total),
                () = tokio::task::yield_now() => {}
            }

            let report = self.relay_once().await?;
            total.record(report);
            if report.claimed == 0 {
                tokio::select! {
                    biased;
                    () = &mut shutdown => return Ok(total),
                    () = tokio::time::sleep(loop_config.idle_delay) => {}
                }
            }
        }
    }
}

/// Database or provider failure while executing a relay pass.
#[derive(Debug, thiserror::Error)]
pub enum RelayError<E>
where
    E: StdError + Send + Sync + 'static,
{
    /// A durable outbox operation failed.
    #[error(transparent)]
    Outbox(#[from] OutboxError),
    /// The broker publisher failed after the row had been leased and rescheduled.
    #[error("outbox publisher failed: {source}")]
    Publisher {
        /// The publisher error returned after the row was rescheduled.
        #[source]
        source: E,
        /// Counts collected before the failed pass ended.
        report: RelayReport,
    },
}

/// Database or trusted-record failure while operating the `PostgreSQL` outbox.
#[derive(Debug, thiserror::Error)]
pub enum OutboxError {
    /// The `PostgreSQL` query failed.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    /// A duration could not be represented as the bounded `PostgreSQL` interval used by this crate.
    #[error("outbox duration must be at most one hour")]
    InvalidDuration,
    /// A stored row did not contain a valid event message.
    #[error("stored outbox event record is invalid")]
    StoredEvent,
    /// A stored row did not contain a valid durable-job message.
    #[error("stored outbox job record is invalid")]
    StoredJob,
}

fn validate_text(
    value: &str,
    max_bytes: usize,
    error: OutboxMessageError,
) -> Result<(), OutboxMessageError> {
    if value.trim().is_empty() || value.contains('\0') || value.len() > max_bytes {
        return Err(error);
    }
    Ok(())
}

fn validate_inbox_text(value: &str, error: InboxError) -> Result<(), InboxError> {
    if value.trim().is_empty() || value.contains('\0') || value.len() > MAX_MESSAGE_ID_BYTES {
        return Err(error);
    }
    Ok(())
}

fn duration_millis(duration: Duration) -> Result<i64, OutboxError> {
    if duration > MAX_LEASE_DURATION {
        return Err(OutboxError::InvalidDuration);
    }
    i64::try_from(duration.as_millis()).map_err(|_| OutboxError::InvalidDuration)
}

fn outcome(rows_affected: u64) -> LeaseOutcome {
    if rows_affected == 1 {
        LeaseOutcome::Applied
    } else {
        LeaseOutcome::Lost
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use rustee_events::{Event, EventEnvelope};
    use rustee_jobs::{Job, JobEnvelope};
    use serde::{Deserialize, Serialize};

    use super::{
        EventSchedule, EventScheduleError, InboxConsumer, InboxMessageId, JobSchedule,
        JobScheduleError, LeaseConfig, OutboxDestination, OutboxMessage, OutboxPriority,
        RelayConfig, RelayLoopConfig, RelayLoopConfigError,
    };

    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    struct OrderPaid;

    impl Event for OrderPaid {
        const TYPE: &'static str = "orders.paid";
        const VERSION: u16 = 1;
    }

    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    struct SendReceipt;

    impl Job for SendReceipt {
        const NAME: &'static str = "receipts.send";
        const VERSION: u16 = 1;
    }

    #[test]
    fn event_and_job_messages_preserve_their_provider_metadata() {
        let event = EventEnvelope::with_metadata(
            rustee_events::EventId::new(),
            OrderPaid,
            "account-7",
            123,
        )
        .unwrap();
        let job = JobEnvelope::with_metadata(rustee_jobs::JobId::new(), SendReceipt, 456);
        let event_message =
            OutboxMessage::event(OutboxDestination::new("orders.events").unwrap(), &event).unwrap();
        let job_message =
            OutboxMessage::job(OutboxDestination::new("jobs.receipts").unwrap(), &job).unwrap();

        assert_eq!(event_message.kind(), super::OutboxKind::Event);
        assert_eq!(event_message.destination().as_str(), "orders.events");
        assert_eq!(job_message.kind(), super::OutboxKind::Job);
        assert_eq!(job_message.destination().as_str(), "jobs.receipts");
    }

    #[test]
    fn outbox_messages_default_to_normal_priority_and_can_be_overridden() {
        let event = EventEnvelope::with_metadata(
            rustee_events::EventId::new(),
            OrderPaid,
            "account-7",
            123,
        )
        .unwrap();
        let message =
            OutboxMessage::event(OutboxDestination::new("orders.events").unwrap(), &event).unwrap();

        assert_eq!(message.priority(), OutboxPriority::NORMAL);
        assert_eq!(message.priority().value(), 0);

        let prioritized = message.with_priority(OutboxPriority::new(200));
        assert_eq!(prioritized.priority().value(), 200);
    }

    #[test]
    fn relay_limits_are_bounded() {
        assert!(
            LeaseConfig::new(
                NonZeroUsize::new(1_001).unwrap(),
                std::time::Duration::from_secs(1),
            )
            .is_err()
        );
        assert!(
            RelayConfig::new(
                LeaseConfig::default(),
                std::time::Duration::from_secs(3_601)
            )
            .is_err()
        );
    }

    #[test]
    fn delayed_job_schedules_are_positive_and_bounded() {
        assert_eq!(
            JobSchedule::after(Duration::ZERO),
            Err(JobScheduleError::ZeroDelay)
        );
        assert_eq!(
            JobSchedule::after(Duration::from_hours(8_808)),
            Err(JobScheduleError::DelayTooLong)
        );
        let schedule = JobSchedule::after(Duration::from_mins(1)).unwrap();
        assert_eq!(schedule.delay(), Duration::from_mins(1));
    }

    #[test]
    fn delayed_event_schedules_are_positive_and_bounded() {
        assert_eq!(
            EventSchedule::after(Duration::ZERO),
            Err(EventScheduleError::ZeroDelay)
        );
        assert_eq!(
            EventSchedule::after(Duration::from_hours(8_808)),
            Err(EventScheduleError::DelayTooLong)
        );
        let schedule = EventSchedule::after(Duration::from_mins(1)).unwrap();
        assert_eq!(schedule.delay(), Duration::from_mins(1));
    }

    #[test]
    fn relay_loop_config_requires_a_bounded_non_zero_idle_delay() {
        assert_eq!(
            RelayLoopConfig::new(Duration::ZERO),
            Err(RelayLoopConfigError::ZeroIdleDelay)
        );
        assert_eq!(
            RelayLoopConfig::new(Duration::from_secs(60 * 60 + 1)),
            Err(RelayLoopConfigError::IdleDelayTooLong)
        );
        let config = RelayLoopConfig::new(Duration::from_millis(25)).unwrap();
        assert_eq!(config.idle_delay(), Duration::from_millis(25));
    }

    #[test]
    fn inbox_keys_are_scoped_and_bounded() {
        let event_id = rustee_events::EventId::new();
        let job_id = rustee_jobs::JobId::new();
        assert_eq!(
            InboxMessageId::event(event_id).as_str(),
            event_id.to_string()
        );
        assert_eq!(InboxMessageId::job(job_id).as_str(), job_id.to_string());
        assert!(InboxConsumer::new(" ").is_err());
        assert!(InboxMessageId::new("\0").is_err());
    }
}
