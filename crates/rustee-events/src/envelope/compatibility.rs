//! Backward-compatible event payload decoding and upcasting.

use std::{error::Error as StdError, fmt};

use serde::Deserialize;
use serde_json::Value;

use super::{
    EnvelopeError, Event, EventEnvelope, EventId, EventTraceContext, SerializedEventTraceContext,
    codec::validate_envelope_bytes, validate_metadata,
};

/// A pure, application-defined conversion from an older payload version to the current event type.
///
/// The upcaster receives only an older JSON payload and its source version. Envelope metadata,
/// event type, key, trace carrier, and newer-version rejection remain controlled by
/// [`EventEnvelope::decode_compatible`].
pub trait EventUpcaster<E>: Send + Sync
where
    E: Event,
{
    /// Application-specific conversion failure.
    type Error: StdError + Send + Sync + 'static;

    /// Converts one lower source version into the current event payload type.
    ///
    /// # Errors
    ///
    /// Returns the application-defined error when the older payload cannot be converted safely.
    fn upcast(&self, source_version: u16, payload: Value) -> Result<E, Self::Error>;
}

impl<E, F, Error> EventUpcaster<E> for F
where
    E: Event,
    F: Fn(u16, Value) -> Result<E, Error> + Send + Sync,
    Error: StdError + Send + Sync + 'static,
{
    type Error = Error;

    fn upcast(&self, source_version: u16, payload: Value) -> Result<E, Self::Error> {
        self(source_version, payload)
    }
}

/// Failed strict or compatible event-envelope decoding.
#[derive(thiserror::Error)]
pub enum CompatibleDecodeError<E>
where
    E: StdError + Send + Sync + 'static,
{
    /// Stable envelope metadata was malformed or unsupported.
    #[error("event envelope decoding failed")]
    Envelope(#[source] EnvelopeError),
    /// The current-version payload could not be decoded into the typed event.
    #[error("current event payload deserialization failed")]
    Payload(#[source] serde_json::Error),
    /// Application-defined upcasting of an older payload failed.
    #[error("event payload upcast failed")]
    Upcaster(#[source] E),
}

impl<E> fmt::Debug for CompatibleDecodeError<E>
where
    E: StdError + Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Envelope(_) => "envelope_invalid",
            Self::Payload(_) => "payload_deserialization_failed",
            Self::Upcaster(_) => "upcast_failed",
        };
        formatter
            .debug_struct("CompatibleDecodeError")
            .field("kind", &kind)
            .finish()
    }
}

impl<E> EventEnvelope<E>
where
    E: Event,
{
    /// Decodes this event's current version or explicitly upcasts an older payload version.
    ///
    /// The normal [`Self::decode`] path remains strict. This method accepts only versions lower
    /// than [`Event::VERSION`] and invokes `upcaster` for them. It never accepts a newer producer
    /// version, because a typed consumer cannot safely infer compatibility in that direction.
    ///
    /// # Errors
    ///
    /// Returns [`CompatibleDecodeError`] when envelope metadata is invalid, the current payload
    /// cannot be decoded, the upcaster rejects an older payload, or the producer version is newer
    /// than this consumer supports.
    pub fn decode_compatible<U>(
        bytes: &[u8],
        upcaster: &U,
    ) -> Result<Self, CompatibleDecodeError<U::Error>>
    where
        U: EventUpcaster<E>,
    {
        validate_envelope_bytes(bytes).map_err(CompatibleDecodeError::Envelope)?;
        let raw = serde_json::from_slice::<RawEventEnvelope>(bytes)
            .map_err(EnvelopeError::Deserialize)
            .map_err(CompatibleDecodeError::Envelope)?;
        let trace_context = raw
            .trace_context
            .map(EventTraceContext::from_serialized)
            .transpose()
            .map_err(CompatibleDecodeError::Envelope)?;
        validate_metadata::<E>(
            &raw.event_type,
            &raw.key,
            raw.correlation_id.as_deref(),
            raw.causation_id.as_deref(),
            trace_context.as_ref(),
        )
        .map_err(CompatibleDecodeError::Envelope)?;
        if raw.version > E::VERSION {
            return Err(CompatibleDecodeError::Envelope(
                EnvelopeError::UnsupportedVersion {
                    expected: E::VERSION,
                    actual: raw.version,
                },
            ));
        }
        let payload = if raw.version == E::VERSION {
            serde_json::from_value(raw.payload).map_err(CompatibleDecodeError::Payload)?
        } else {
            upcaster
                .upcast(raw.version, raw.payload)
                .map_err(CompatibleDecodeError::Upcaster)?
        };
        Ok(Self {
            id: raw.id,
            event_type: raw.event_type,
            version: E::VERSION,
            key: raw.key,
            payload,
            occurred_at_unix_ms: raw.occurred_at_unix_ms,
            correlation_id: raw.correlation_id,
            causation_id: raw.causation_id,
            trace_context,
        })
    }
}

#[derive(Deserialize)]
struct RawEventEnvelope {
    id: EventId,
    event_type: String,
    version: u16,
    key: String,
    payload: Value,
    occurred_at_unix_ms: u64,
    correlation_id: Option<String>,
    causation_id: Option<String>,
    trace_context: Option<SerializedEventTraceContext>,
}
