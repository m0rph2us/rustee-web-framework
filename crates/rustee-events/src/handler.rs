//! Private typed-handler dispatch for durable event envelopes.

use std::{error::Error as StdError, fmt};

use futures_util::future::BoxFuture;

use super::{Event, EventEnvelope, EventId, EventTraceContext};

/// Metadata supplied to an event handler without exposing provider offset or partition handles.
#[derive(Clone, Eq, PartialEq)]
pub struct EventContext {
    id: EventId,
    event_type: String,
    version: u16,
    key: String,
    correlation_id: Option<String>,
    causation_id: Option<String>,
    trace_context: Option<EventTraceContext>,
}

impl EventContext {
    pub(crate) fn from_envelope<E>(envelope: &EventEnvelope<E>) -> Self
    where
        E: Event,
    {
        Self {
            id: envelope.id(),
            event_type: envelope.event_type().to_owned(),
            version: envelope.version(),
            key: envelope.key().to_owned(),
            correlation_id: envelope.correlation_id().map(ToOwned::to_owned),
            causation_id: envelope.causation_id().map(ToOwned::to_owned),
            trace_context: envelope.trace_context().cloned(),
        }
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

    /// Returns the event schema version.
    #[must_use]
    pub const fn version(&self) -> u16 {
        self.version
    }

    /// Returns the producer-selected partition key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the correlation identifier when supplied.
    #[must_use]
    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }

    /// Returns the identifier of the event or command that caused this event.
    #[must_use]
    pub fn causation_id(&self) -> Option<&str> {
        self.causation_id.as_deref()
    }

    /// Returns the optional W3C trace-context carrier attached by the event producer.
    #[must_use]
    pub fn trace_context(&self) -> Option<&EventTraceContext> {
        self.trace_context.as_ref()
    }
}

impl fmt::Debug for EventContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventContext")
            .field("id", &self.id)
            .field("event_type", &self.event_type)
            .field("version", &self.version)
            .field("key", &"[REDACTED]")
            .field("has_correlation_id", &self.correlation_id.is_some())
            .field("has_causation_id", &self.causation_id.is_some())
            .field("has_trace_context", &self.trace_context.is_some())
            .finish()
    }
}

/// A typed asynchronous event handler.
pub trait EventHandler<E>: Clone + Send + Sync + 'static
where
    E: Event,
{
    /// Handler-specific failure. Providers preserve the uncommitted offset when this fails.
    type Error: StdError + Send + Sync + 'static;

    /// Processes one deserialized event payload and its stable metadata.
    fn handle(
        &self,
        payload: E,
        context: EventContext,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

impl<E, F, Future, Error> EventHandler<E> for F
where
    E: Event,
    F: Fn(E, EventContext) -> Future + Clone + Send + Sync + 'static,
    Future: std::future::Future<Output = Result<(), Error>> + Send + 'static,
    Error: StdError + Send + Sync + 'static,
{
    type Error = Error;

    fn handle(
        &self,
        payload: E,
        context: EventContext,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(self(payload, context))
    }
}

/// Runs one typed event handler before a provider commits its source position.
///
/// # Errors
///
/// Returns the handler failure without committing or otherwise advancing the provider position.
pub async fn dispatch<E, H>(envelope: EventEnvelope<E>, handler: &H) -> Result<(), H::Error>
where
    E: Event,
    H: EventHandler<E>,
{
    let context = EventContext::from_envelope(&envelope);
    handler.handle(envelope.into_payload(), context).await
}
