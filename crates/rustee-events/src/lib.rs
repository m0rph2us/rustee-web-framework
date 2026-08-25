//! Versioned event envelopes for append-only event streams.
//!
//! Events are not background jobs: one event may be replayed by multiple consumer groups. Topic,
//! partition key, offset, and retention choices remain visible in provider adapters.

mod delivery;
mod envelope;
mod handler;
mod publisher;

pub use delivery::{
    EventDeliveryFinished, EventDeliveryObservation, EventDeliveryObserver, EventDeliveryOutcome,
    EventDeliveryStarted, NoopEventDeliveryObserver,
};
pub use envelope::{
    CompatibleDecodeError, EnvelopeError, Event, EventEnvelope, EventId, EventTraceContext,
    EventUpcaster, MAX_EVENT_ENVELOPE_BYTES, MAX_EVENT_METADATA_ID_BYTES,
    MAX_EVENT_PARTITION_KEY_BYTES, MAX_EVENT_TYPE_BYTES, is_valid_event_type,
};
pub use handler::{EventContext, EventHandler, dispatch};
pub use publisher::{EventClient, EventMessage, EventMessageError, EventPublisher, PublishError};

#[cfg(test)]
mod tests;
