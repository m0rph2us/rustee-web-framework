//! Validated durable outbox message models and storage-boundary admission.

use std::fmt;

use uuid::Uuid;

const EVENT_KIND: &str = "event";
const JOB_KIND: &str = "job";
const MAX_DESTINATION_BYTES: usize = 255;
const MAX_MESSAGE_ID_BYTES: usize = 255;
const MAX_MESSAGE_TYPE_BYTES: usize = 255;
const MAX_ORDERING_KEY_BYTES: usize = 512;
const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;

/// A validated logical broker destination used to isolate a relay's leased records.
#[derive(Clone, Eq, Hash, PartialEq)]
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

impl fmt::Debug for OutboxDestination {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OutboxDestination([REDACTED])")
    }
}

/// Unique identifier for one durable outbox row.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct OutboxId(pub(crate) Uuid);

impl OutboxId {
    /// Creates an identifier for one newly staged outbox record.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub(crate) fn from_uuid(id: Uuid) -> Self {
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

impl fmt::Debug for OutboxId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OutboxId([REDACTED])")
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

impl OutboxKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Event => EVENT_KIND,
            Self::Job => JOB_KIND,
        }
    }
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

/// A validated, serialized event or job awaiting a `PostgreSQL` transaction commit.
#[derive(Clone, Eq, PartialEq)]
pub struct OutboxMessage {
    pub(crate) id: OutboxId,
    pub(crate) kind: OutboxKind,
    pub(crate) destination: OutboxDestination,
    pub(crate) message_id: String,
    pub(crate) message_type: String,
    pub(crate) schema_version: u16,
    pub(crate) ordering_key: String,
    pub(crate) delivery_attempt: u16,
    pub(crate) priority: OutboxPriority,
    pub(crate) payload: Vec<u8>,
}

pub(crate) struct OutboxMessageInput {
    pub(crate) kind: OutboxKind,
    pub(crate) destination: OutboxDestination,
    pub(crate) message_id: String,
    pub(crate) message_type: String,
    pub(crate) schema_version: u16,
    pub(crate) ordering_key: String,
    pub(crate) delivery_attempt: u16,
    pub(crate) payload: Vec<u8>,
}

impl OutboxMessage {
    pub(crate) fn new(input: OutboxMessageInput) -> Result<Self, OutboxMessageError> {
        validate_durable_message_fields(
            &input.message_id,
            &input.message_type,
            &input.ordering_key,
            input.delivery_attempt,
            &input.payload,
        )?;
        Ok(Self {
            id: OutboxId::new(),
            kind: input.kind,
            destination: input.destination,
            message_id: input.message_id,
            message_type: input.message_type,
            schema_version: input.schema_version,
            ordering_key: input.ordering_key,
            delivery_attempt: input.delivery_attempt,
            priority: OutboxPriority::NORMAL,
            payload: input.payload,
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

/// Shared durable-storage boundary for staging and leased-record reconstruction.
pub(crate) fn validate_durable_message_fields(
    message_id: &str,
    message_type: &str,
    ordering_key: &str,
    delivery_attempt: u16,
    payload: &[u8],
) -> Result<(), OutboxMessageError> {
    validate_text(
        message_id,
        MAX_MESSAGE_ID_BYTES,
        OutboxMessageError::InvalidMessageId,
    )?;
    validate_text(
        message_type,
        MAX_MESSAGE_TYPE_BYTES,
        OutboxMessageError::InvalidMessageType,
    )?;
    validate_text(
        ordering_key,
        MAX_ORDERING_KEY_BYTES,
        OutboxMessageError::InvalidOrderingKey,
    )?;
    if delivery_attempt == 0 {
        return Err(OutboxMessageError::InvalidDeliveryAttempt);
    }
    if payload.is_empty() || payload.len() > MAX_PAYLOAD_BYTES {
        return Err(OutboxMessageError::InvalidPayload);
    }
    Ok(())
}

impl fmt::Debug for OutboxMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OutboxMessage")
            .field("id", &"[REDACTED]")
            .field("kind", &self.kind)
            .field("destination", &"[REDACTED]")
            .field("message_id", &"[REDACTED]")
            .field("message_type", &"[REDACTED]")
            .field("schema_version", &self.schema_version)
            .field("ordering_key", &"[REDACTED]")
            .field("delivery_attempt", &self.delivery_attempt)
            .field("priority", &self.priority)
            .field("payload_byte_len", &self.payload.len())
            .finish()
    }
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
