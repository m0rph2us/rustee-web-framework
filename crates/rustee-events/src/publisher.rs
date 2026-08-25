use std::{error::Error as StdError, fmt};

use futures_util::future::BoxFuture;

use super::{
    EnvelopeError, Event, EventEnvelope, EventId, MAX_EVENT_ENVELOPE_BYTES,
    MAX_EVENT_PARTITION_KEY_BYTES, MAX_EVENT_TYPE_BYTES, is_valid_event_type,
};

/// Serialized event content plus metadata a provider uses for routing and observability.
#[derive(Clone, Eq, PartialEq)]
pub struct EventMessage {
    id: EventId,
    event_type: String,
    version: u16,
    key: String,
    payload: Vec<u8>,
}

impl EventMessage {
    pub(super) fn from_envelope(
        id: EventId,
        event_type: String,
        version: u16,
        key: String,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            id,
            event_type,
            version,
            key,
            payload,
        }
    }

    /// Reconstructs a provider message from metadata stored by a trusted durable relay.
    ///
    /// # Errors
    ///
    /// Returns [`EventMessageError`] when routing metadata is blank or oversized, or the
    /// serialized body is empty or exceeds the framework byte limit. Event payload schema
    /// validation still belongs to the typed consumer.
    pub fn from_parts(
        id: EventId,
        event_type: impl Into<String>,
        version: u16,
        key: impl Into<String>,
        payload: Vec<u8>,
    ) -> Result<Self, EventMessageError> {
        let event_type = event_type.into();
        let key = key.into();
        if !is_valid_event_type(&event_type) {
            return if event_type.trim().is_empty() {
                Err(EventMessageError::BlankEventType)
            } else if event_type.len() > MAX_EVENT_TYPE_BYTES {
                Err(EventMessageError::EventTypeTooLarge)
            } else {
                Err(EventMessageError::InvalidEventType)
            };
        }
        if key.trim().is_empty() {
            return Err(EventMessageError::BlankKey);
        }
        if key.len() > MAX_EVENT_PARTITION_KEY_BYTES {
            return Err(EventMessageError::KeyTooLarge);
        }
        if payload.is_empty() {
            return Err(EventMessageError::EmptyPayload);
        }
        if payload.len() > MAX_EVENT_ENVELOPE_BYTES {
            return Err(EventMessageError::PayloadTooLarge);
        }
        Ok(Self {
            id,
            event_type,
            version,
            key,
            payload,
        })
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

    /// Consumes the message and returns the serialized envelope body.
    #[must_use]
    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }
}

impl fmt::Debug for EventMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventMessage")
            .field("id", &self.id)
            .field("event_type", &self.event_type)
            .field("version", &self.version)
            .field("key", &"[REDACTED]")
            .field("payload_byte_len", &self.payload.len())
            .finish()
    }
}

/// Invalid metadata recovered for a provider event message.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EventMessageError {
    /// The stored stable event type was blank.
    #[error("event message type must not be blank")]
    BlankEventType,
    /// The stored stable event type exceeded the framework's provider-visible type limit.
    #[error("event message type exceeded the framework byte limit")]
    EventTypeTooLarge,
    /// The stored stable event type contained NUL and cannot cross durable provider boundaries.
    #[error("event message type must be NUL-free")]
    InvalidEventType,
    /// The stored partition key was blank.
    #[error("event message partition key must not be blank")]
    BlankKey,
    /// The stored partition key exceeded the framework's provider-visible key limit.
    #[error("event message partition key exceeded the framework byte limit")]
    KeyTooLarge,
    /// The stored serialized envelope body was empty.
    #[error("event message payload must not be empty")]
    EmptyPayload,
    /// The stored serialized envelope body exceeded the framework's event-message limit.
    #[error("event message payload exceeded the framework byte limit")]
    PayloadTooLarge,
}

/// Provider-facing contract for appending one serialized event to a configured stream topic.
pub trait EventPublisher: Clone + Send + Sync + 'static {
    /// Provider-specific append failure.
    type Error: StdError + Send + Sync + 'static;

    /// Appends an event and waits for the provider's configured delivery acknowledgement.
    fn publish(&self, message: EventMessage) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// Typed event producer built on a provider-specific publisher.
///
/// Debug output retains the publisher type without invoking publisher diagnostics.
#[derive(Clone)]
pub struct EventClient<P> {
    publisher: P,
}

impl<P> fmt::Debug for EventClient<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventClient")
            .field("publisher_type", &std::any::type_name::<P>())
            .finish_non_exhaustive()
    }
}

impl<P> EventClient<P> {
    /// Creates an event client from one provider-specific publisher.
    #[must_use]
    pub fn new(publisher: P) -> Self {
        Self { publisher }
    }
}

impl<P> EventClient<P>
where
    P: EventPublisher,
{
    /// Encodes and appends an already-configured event envelope.
    ///
    /// # Errors
    ///
    /// Returns an envelope serialization failure or provider append failure.
    pub async fn publish<E>(
        &self,
        envelope: &EventEnvelope<E>,
    ) -> Result<(), PublishError<P::Error>>
    where
        E: Event,
    {
        let message = envelope.message().map_err(PublishError::Envelope)?;
        self.publisher
            .publish(message)
            .await
            .map_err(PublishError::Provider)
    }
}

/// Failure while appending an event.
#[derive(thiserror::Error)]
pub enum PublishError<E> {
    /// The envelope could not be encoded.
    #[error("event envelope serialization failed")]
    Envelope(#[source] EnvelopeError),
    /// The provider could not append the event.
    #[error("event provider publish failed")]
    Provider(#[source] E),
}

impl<E> fmt::Debug for PublishError<E>
where
    E: StdError + Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Envelope(_) => "envelope_serialization_failed",
            Self::Provider(_) => "provider_publish_failed",
        };
        formatter
            .debug_struct("PublishError")
            .field("kind", &kind)
            .finish()
    }
}
