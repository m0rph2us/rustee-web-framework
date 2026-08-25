//! Event envelope, trace-carrier, and compatibility-upcast model.

use std::{
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use uuid::Uuid;

use super::EventMessage;

/// Maximum serialized size accepted for one event-stream envelope.
///
/// Provider consumers and durable relays share this limit so malformed broker records do not
/// reach JSON decoding after exceeding the framework's supported in-memory envelope size.
pub const MAX_EVENT_ENVELOPE_BYTES: usize = 1024 * 1024;
/// Maximum UTF-8 byte length accepted for one provider-visible event partition key.
///
/// Partition keys are retained separately from serialized envelopes by providers and durable
/// relays, so they use the same bounded record budget as one serialized envelope.
pub const MAX_EVENT_PARTITION_KEY_BYTES: usize = MAX_EVENT_ENVELOPE_BYTES;
/// Maximum UTF-8 byte length accepted for one provider-visible stable event type.
///
/// Event types cross provider headers and durable outbox records, so they use the shared
/// 255-byte metadata budget rather than the full serialized-envelope allowance.
pub const MAX_EVENT_TYPE_BYTES: usize = 255;
/// Maximum UTF-8 byte length accepted for one correlation or causation identifier.
///
/// These identifiers are envelope metadata, not application payload. Keeping a separate bound
/// prevents untrusted routing metadata from consuming the full event-body budget.
pub const MAX_EVENT_METADATA_ID_BYTES: usize = 255;

mod codec;
mod compatibility;
mod trace;

pub use compatibility::{CompatibleDecodeError, EventUpcaster};
pub use trace::EventTraceContext;

use trace::SerializedEventTraceContext;

/// A serializable event payload with a stable type and schema version.
pub trait Event: DeserializeOwned + Serialize + Send + Sync + 'static {
    /// Non-blank, bounded stable event type name.
    const TYPE: &'static str;
    /// Schema version supported by this event reader.
    const VERSION: u16;
}

/// Globally unique event identifier.
#[derive(Clone, Copy, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct EventId(Uuid);

impl EventId {
    /// Creates a new random UUID v4 event identifier.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Wraps a UUID recovered from a trusted durable event store.
    #[must_use]
    pub const fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }
}

impl Default for EventId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl fmt::Debug for EventId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EventId([REDACTED])")
    }
}

/// A versioned event plus routing and correlation metadata.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct EventEnvelope<E> {
    id: EventId,
    event_type: String,
    version: u16,
    key: String,
    payload: E,
    occurred_at_unix_ms: u64,
    correlation_id: Option<String>,
    causation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_context: Option<EventTraceContext>,
}

#[derive(Deserialize)]
struct SerializedEventEnvelope<E> {
    id: EventId,
    event_type: String,
    version: u16,
    key: String,
    payload: E,
    occurred_at_unix_ms: u64,
    correlation_id: Option<String>,
    causation_id: Option<String>,
    trace_context: Option<SerializedEventTraceContext>,
}

impl<'de, E> Deserialize<'de> for EventEnvelope<E>
where
    E: Event,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serialized = SerializedEventEnvelope::deserialize(deserializer)?;
        Self::from_serialized(serialized).map_err(serde::de::Error::custom)
    }
}

impl<E> EventEnvelope<E>
where
    E: Event,
{
    fn from_serialized(serialized: SerializedEventEnvelope<E>) -> Result<Self, EnvelopeError> {
        let envelope = Self {
            id: serialized.id,
            event_type: serialized.event_type,
            version: serialized.version,
            key: serialized.key,
            payload: serialized.payload,
            occurred_at_unix_ms: serialized.occurred_at_unix_ms,
            correlation_id: serialized.correlation_id,
            causation_id: serialized.causation_id,
            trace_context: serialized
                .trace_context
                .map(EventTraceContext::from_serialized)
                .transpose()?,
        };
        envelope.validate()?;
        Ok(envelope)
    }
}

impl<E> EventEnvelope<E>
where
    E: Event,
{
    /// Creates an event with an explicit non-blank partition key.
    ///
    /// # Errors
    ///
    /// Returns [`EnvelopeError::InvalidEventType`] when the event type is invalid,
    /// [`EnvelopeError::BlankKey`] when `key` is blank, or
    /// [`EnvelopeError::KeyTooLarge`] when it exceeds the provider-visible key bound.
    pub fn new(payload: E, key: impl Into<String>) -> Result<Self, EnvelopeError> {
        Self::with_metadata(EventId::new(), payload, key, unix_time_ms())
    }

    /// Creates a deterministic event envelope for outbox relays and tests.
    ///
    /// # Errors
    ///
    /// Returns [`EnvelopeError::InvalidEventType`] when the event type is invalid,
    /// [`EnvelopeError::BlankKey`] when `key` is blank, or
    /// [`EnvelopeError::KeyTooLarge`] when it exceeds the provider-visible key bound.
    pub fn with_metadata(
        id: EventId,
        payload: E,
        key: impl Into<String>,
        occurred_at_unix_ms: u64,
    ) -> Result<Self, EnvelopeError> {
        let key = key.into();
        validate_event_type(E::TYPE)?;
        validate_partition_key(&key)?;
        Ok(Self {
            id,
            event_type: E::TYPE.to_owned(),
            version: E::VERSION,
            key,
            payload,
            occurred_at_unix_ms,
            correlation_id: None,
            causation_id: None,
            trace_context: None,
        })
    }

    /// Adds a correlation identifier without treating it as a trusted authorization value.
    ///
    /// # Errors
    ///
    /// Returns [`EnvelopeError::BlankCorrelationId`] when `correlation_id` is blank or
    /// [`EnvelopeError::CorrelationIdTooLarge`] when it exceeds the metadata identifier bound.
    pub fn with_correlation_id(
        mut self,
        correlation_id: impl Into<String>,
    ) -> Result<Self, EnvelopeError> {
        let correlation_id = correlation_id.into();
        validate_optional_bounded_identifier(
            Some(&correlation_id),
            EnvelopeError::BlankCorrelationId,
            EnvelopeError::CorrelationIdTooLarge,
        )?;
        self.correlation_id = Some(correlation_id);
        Ok(self)
    }

    /// Adds the identifier of the event or command that caused this event.
    ///
    /// # Errors
    ///
    /// Returns [`EnvelopeError::BlankCausationId`] when `causation_id` is blank or
    /// [`EnvelopeError::CausationIdTooLarge`] when it exceeds the metadata identifier bound.
    pub fn with_causation_id(
        mut self,
        causation_id: impl Into<String>,
    ) -> Result<Self, EnvelopeError> {
        let causation_id = causation_id.into();
        validate_optional_bounded_identifier(
            Some(&causation_id),
            EnvelopeError::BlankCausationId,
            EnvelopeError::CausationIdTooLarge,
        )?;
        self.causation_id = Some(causation_id);
        Ok(self)
    }

    /// Adds a bounded W3C trace-context carrier for an optional telemetry integration.
    #[must_use]
    pub fn with_trace_context(mut self, trace_context: EventTraceContext) -> Self {
        self.trace_context = Some(trace_context);
        self
    }

    /// Serializes the envelope as a provider message body.
    ///
    /// # Errors
    ///
    /// Returns an error when the payload cannot be encoded as JSON or the encoded envelope
    /// exceeds [`MAX_EVENT_ENVELOPE_BYTES`].
    pub fn encode(&self) -> Result<Vec<u8>, EnvelopeError> {
        codec::encode(self)
    }

    /// Decodes and validates an envelope for this event type and version.
    ///
    /// # Errors
    ///
    /// Returns an error when the body exceeds [`MAX_EVENT_ENVELOPE_BYTES`], JSON is malformed, or
    /// metadata does not match the expected event type.
    pub fn decode(bytes: &[u8]) -> Result<Self, EnvelopeError> {
        codec::validate_envelope_bytes(bytes)?;
        let serialized = serde_json::from_slice::<SerializedEventEnvelope<E>>(bytes)
            .map_err(EnvelopeError::Deserialize)?;
        Self::from_serialized(serialized)
    }

    /// Builds a provider message without exposing event payload fields as headers.
    ///
    /// # Errors
    ///
    /// Returns an error when the envelope cannot be encoded as JSON.
    pub fn message(&self) -> Result<EventMessage, EnvelopeError> {
        Ok(EventMessage::from_envelope(
            self.id,
            self.event_type.clone(),
            self.version,
            self.key.clone(),
            self.encode()?,
        ))
    }

    /// Returns the event identifier.
    #[must_use]
    pub const fn id(&self) -> EventId {
        self.id
    }

    /// Returns the stable event type.
    #[must_use]
    pub fn event_type(&self) -> &str {
        &self.event_type
    }

    /// Returns the schema version.
    #[must_use]
    pub const fn version(&self) -> u16 {
        self.version
    }

    /// Returns the explicit partition key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the timestamp in Unix milliseconds.
    #[must_use]
    pub const fn occurred_at_unix_ms(&self) -> u64 {
        self.occurred_at_unix_ms
    }

    /// Returns the optional correlation identifier.
    #[must_use]
    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }

    /// Returns the optional causation identifier.
    #[must_use]
    pub fn causation_id(&self) -> Option<&str> {
        self.causation_id.as_deref()
    }

    /// Returns the optional W3C trace-context carrier.
    #[must_use]
    pub fn trace_context(&self) -> Option<&EventTraceContext> {
        self.trace_context.as_ref()
    }

    /// Consumes the envelope and returns the typed payload.
    #[must_use]
    pub fn into_payload(self) -> E {
        self.payload
    }

    fn validate(&self) -> Result<(), EnvelopeError> {
        validate_metadata::<E>(
            &self.event_type,
            &self.key,
            self.correlation_id.as_deref(),
            self.causation_id.as_deref(),
            self.trace_context.as_ref(),
        )?;
        if self.version != E::VERSION {
            return Err(EnvelopeError::UnsupportedVersion {
                expected: E::VERSION,
                actual: self.version,
            });
        }
        Ok(())
    }
}

impl<E> fmt::Debug for EventEnvelope<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventEnvelope")
            .field("id", &self.id)
            .field("event_type", &self.event_type)
            .field("version", &self.version)
            .field("key", &"[REDACTED]")
            .field("payload", &"[APPLICATION-OWNED]")
            .field("occurred_at_unix_ms", &self.occurred_at_unix_ms)
            .field("has_correlation_id", &self.correlation_id.is_some())
            .field("has_causation_id", &self.causation_id.is_some())
            .field("has_trace_context", &self.trace_context.is_some())
            .finish()
    }
}

pub(super) fn validate_metadata<E>(
    event_type: &str,
    key: &str,
    correlation_id: Option<&str>,
    causation_id: Option<&str>,
    trace_context: Option<&EventTraceContext>,
) -> Result<(), EnvelopeError>
where
    E: Event,
{
    if event_type != E::TYPE {
        return Err(EnvelopeError::UnexpectedEventType {
            expected: E::TYPE,
            actual: event_type.to_owned(),
        });
    }
    validate_event_type(event_type)?;
    validate_partition_key(key)?;
    validate_optional_bounded_identifier(
        correlation_id,
        EnvelopeError::BlankCorrelationId,
        EnvelopeError::CorrelationIdTooLarge,
    )?;
    validate_optional_bounded_identifier(
        causation_id,
        EnvelopeError::BlankCausationId,
        EnvelopeError::CausationIdTooLarge,
    )?;
    if let Some(trace_context) = trace_context {
        trace_context.validate()?;
    }
    Ok(())
}

fn validate_optional_bounded_identifier(
    value: Option<&str>,
    blank_error: EnvelopeError,
    too_large_error: EnvelopeError,
) -> Result<(), EnvelopeError> {
    if let Some(value) = value {
        if value.trim().is_empty() {
            return Err(blank_error);
        }
        if value.len() > MAX_EVENT_METADATA_ID_BYTES {
            return Err(too_large_error);
        }
    }
    Ok(())
}

fn validate_partition_key(key: &str) -> Result<(), EnvelopeError> {
    if key.trim().is_empty() {
        return Err(EnvelopeError::BlankKey);
    }
    if key.len() > MAX_EVENT_PARTITION_KEY_BYTES {
        return Err(EnvelopeError::KeyTooLarge);
    }
    Ok(())
}

/// Returns whether a stable event type satisfies the shared provider and durable-storage contract.
///
/// This basic boundary intentionally does not impose a provider-specific character grammar.
/// Adapters such as a schema registry may apply their own stricter identifier rules.
#[must_use]
pub fn is_valid_event_type(event_type: &str) -> bool {
    !event_type.trim().is_empty()
        && event_type.len() <= MAX_EVENT_TYPE_BYTES
        && !event_type.contains('\0')
}

fn validate_event_type(event_type: &str) -> Result<(), EnvelopeError> {
    if !is_valid_event_type(event_type) {
        return Err(EnvelopeError::InvalidEventType);
    }
    Ok(())
}

/// Failed event serialization or envelope validation.
#[derive(thiserror::Error)]
pub enum EnvelopeError {
    /// JSON encoding failed.
    #[error("event envelope serialization failed")]
    Serialize(#[source] serde_json::Error),
    /// JSON decoding failed.
    #[error("event envelope deserialization failed")]
    Deserialize(#[source] serde_json::Error),
    /// The serialized envelope exceeded the framework's bounded event-message size.
    #[error("event envelope exceeded the framework byte limit")]
    TooLarge,
    /// A reader received a different event type.
    #[error("event envelope type did not match the registered handler")]
    UnexpectedEventType {
        /// Expected stable event type.
        expected: &'static str,
        /// Received event type.
        actual: String,
    },
    /// The declared stable event type was blank, contained NUL, or exceeded the framework byte limit.
    #[error("event type must be non-blank, NUL-free, and within the framework byte limit")]
    InvalidEventType,
    /// A reader received an unsupported schema version.
    #[error("event envelope version is not supported by the registered handler")]
    UnsupportedVersion {
        /// Expected schema version.
        expected: u16,
        /// Received schema version.
        actual: u16,
    },
    /// The partition key was blank.
    #[error("event partition key must not be blank")]
    BlankKey,
    /// The partition key exceeded the framework's provider-visible key limit.
    #[error("event partition key exceeded the framework byte limit")]
    KeyTooLarge,
    /// The correlation ID was blank.
    #[error("event correlation ID must not be blank")]
    BlankCorrelationId,
    /// The correlation ID exceeded the bounded metadata identifier contract.
    #[error("event correlation ID exceeded the framework metadata byte limit")]
    CorrelationIdTooLarge,
    /// The causation ID was blank.
    #[error("event causation ID must not be blank")]
    BlankCausationId,
    /// The causation ID exceeded the bounded metadata identifier contract.
    #[error("event causation ID exceeded the framework metadata byte limit")]
    CausationIdTooLarge,
    /// A W3C trace-context carrier was blank, non-ASCII, or too large.
    #[error("event trace context must be bounded non-empty ASCII")]
    InvalidTraceContext,
}

impl fmt::Debug for EnvelopeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Serialize(_) => "serialization_failed",
            Self::Deserialize(_) => "deserialization_failed",
            Self::TooLarge => "too_large",
            Self::UnexpectedEventType { .. } => "unexpected_event_type",
            Self::InvalidEventType => "invalid_event_type",
            Self::UnsupportedVersion { .. } => "unsupported_version",
            Self::BlankKey => "blank_key",
            Self::KeyTooLarge => "key_too_large",
            Self::BlankCorrelationId => "blank_correlation_id",
            Self::CorrelationIdTooLarge => "correlation_id_too_large",
            Self::BlankCausationId => "blank_causation_id",
            Self::CausationIdTooLarge => "causation_id_too_large",
            Self::InvalidTraceContext => "invalid_trace_context",
        };
        formatter
            .debug_struct("EnvelopeError")
            .field("kind", &kind)
            .finish()
    }
}

fn unix_time_ms() -> u64 {
    let milliseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(milliseconds).unwrap_or(u64::MAX)
}
