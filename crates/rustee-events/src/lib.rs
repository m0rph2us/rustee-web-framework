//! Versioned event envelopes for append-only event streams.
//!
//! Events are not background jobs: one event may be replayed by multiple consumer groups. Topic,
//! partition key, offset, and retention choices remain visible in provider adapters.

use std::{
    error::Error as StdError,
    fmt,
    num::NonZeroU16,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use uuid::Uuid;

const MAX_TRACEPARENT_LEN: usize = 512;
const MAX_TRACESTATE_LEN: usize = 512;

/// A serializable event payload with a stable type and schema version.
pub trait Event: DeserializeOwned + Serialize + Send + Sync + 'static {
    /// Stable event type name.
    const TYPE: &'static str;
    /// Schema version supported by this event reader.
    const VERSION: u16;
}

/// Globally unique event identifier.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
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

/// A bounded W3C trace-context carrier attached to an event envelope.
///
/// `rustee-events` deliberately stores only the transport-neutral carrier. An optional telemetry
/// integration validates it against its propagator and decides whether it becomes a parent span.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EventTraceContext {
    traceparent: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    tracestate: Option<String>,
}

impl EventTraceContext {
    /// Creates a bounded ASCII W3C trace-context carrier.
    ///
    /// # Errors
    ///
    /// Returns [`EnvelopeError::InvalidTraceContext`] when a value is blank, non-ASCII, or larger
    /// than the documented W3C carrier bounds. Syntax and sampling are validated by the optional
    /// telemetry propagator that consumes this carrier.
    pub fn new(
        traceparent: impl Into<String>,
        tracestate: Option<String>,
    ) -> Result<Self, EnvelopeError> {
        let traceparent = traceparent.into();
        let context = Self {
            traceparent,
            tracestate,
        };
        context.validate()?;
        Ok(context)
    }

    /// Returns the W3C traceparent carrier value.
    #[must_use]
    pub fn traceparent(&self) -> &str {
        &self.traceparent
    }

    /// Returns the optional W3C tracestate carrier value.
    #[must_use]
    pub fn tracestate(&self) -> Option<&str> {
        self.tracestate.as_deref()
    }

    fn validate(&self) -> Result<(), EnvelopeError> {
        if self.traceparent.trim().is_empty()
            || !self.traceparent.is_ascii()
            || self.traceparent.len() > MAX_TRACEPARENT_LEN
            || self.tracestate.as_deref().is_some_and(|value| {
                value.trim().is_empty() || !value.is_ascii() || value.len() > MAX_TRACESTATE_LEN
            })
        {
            return Err(EnvelopeError::InvalidTraceContext);
        }
        Ok(())
    }
}

/// A versioned event plus routing and correlation metadata.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

impl<E> EventEnvelope<E>
where
    E: Event,
{
    /// Creates an event with an explicit non-blank partition key.
    ///
    /// # Errors
    ///
    /// Returns [`EnvelopeError::BlankKey`] when `key` is blank.
    pub fn new(payload: E, key: impl Into<String>) -> Result<Self, EnvelopeError> {
        Self::with_metadata(EventId::new(), payload, key, unix_time_ms())
    }

    /// Creates a deterministic event envelope for outbox relays and tests.
    ///
    /// # Errors
    ///
    /// Returns [`EnvelopeError::BlankKey`] when `key` is blank.
    pub fn with_metadata(
        id: EventId,
        payload: E,
        key: impl Into<String>,
        occurred_at_unix_ms: u64,
    ) -> Result<Self, EnvelopeError> {
        let key = key.into();
        ensure_not_blank(&key, EnvelopeError::BlankKey)?;
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
    /// Returns [`EnvelopeError::BlankCorrelationId`] when `correlation_id` is blank.
    pub fn with_correlation_id(
        mut self,
        correlation_id: impl Into<String>,
    ) -> Result<Self, EnvelopeError> {
        let correlation_id = correlation_id.into();
        ensure_not_blank(&correlation_id, EnvelopeError::BlankCorrelationId)?;
        self.correlation_id = Some(correlation_id);
        Ok(self)
    }

    /// Adds the identifier of the event or command that caused this event.
    ///
    /// # Errors
    ///
    /// Returns [`EnvelopeError::BlankCausationId`] when `causation_id` is blank.
    pub fn with_causation_id(
        mut self,
        causation_id: impl Into<String>,
    ) -> Result<Self, EnvelopeError> {
        let causation_id = causation_id.into();
        ensure_not_blank(&causation_id, EnvelopeError::BlankCausationId)?;
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
    /// Returns an error when the payload cannot be encoded as JSON.
    pub fn encode(&self) -> Result<Vec<u8>, EnvelopeError> {
        serde_json::to_vec(self).map_err(EnvelopeError::Serialize)
    }

    /// Decodes and validates an envelope for this event type and version.
    ///
    /// # Errors
    ///
    /// Returns an error when JSON is malformed or metadata does not match the expected event type.
    pub fn decode(bytes: &[u8]) -> Result<Self, EnvelopeError> {
        let envelope = serde_json::from_slice::<Self>(bytes).map_err(EnvelopeError::Deserialize)?;
        envelope.validate()?;
        Ok(envelope)
    }

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
        let raw = serde_json::from_slice::<RawEventEnvelope>(bytes)
            .map_err(EnvelopeError::Deserialize)
            .map_err(CompatibleDecodeError::Envelope)?;
        validate_metadata::<E>(
            &raw.event_type,
            &raw.key,
            raw.correlation_id.as_deref(),
            raw.causation_id.as_deref(),
            raw.trace_context.as_ref(),
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
            trace_context: raw.trace_context,
        })
    }

    /// Builds a provider message without exposing event payload fields as headers.
    ///
    /// # Errors
    ///
    /// Returns an error when the envelope cannot be encoded as JSON.
    pub fn message(&self) -> Result<EventMessage, EnvelopeError> {
        Ok(EventMessage {
            id: self.id,
            event_type: self.event_type.clone(),
            version: self.version,
            key: self.key.clone(),
            payload: self.encode()?,
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
    trace_context: Option<EventTraceContext>,
}

fn validate_metadata<E>(
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
    ensure_not_blank(key, EnvelopeError::BlankKey)?;
    if correlation_id.is_some_and(|value| value.trim().is_empty()) {
        return Err(EnvelopeError::BlankCorrelationId);
    }
    if causation_id.is_some_and(|value| value.trim().is_empty()) {
        return Err(EnvelopeError::BlankCausationId);
    }
    if let Some(trace_context) = trace_context {
        trace_context.validate()?;
    }
    Ok(())
}

fn ensure_not_blank<T>(value: &str, error: T) -> Result<(), T> {
    if value.trim().is_empty() {
        return Err(error);
    }
    Ok(())
}

/// Failed event serialization or envelope validation.
#[derive(Debug, thiserror::Error)]
pub enum EnvelopeError {
    /// JSON encoding failed.
    #[error("event envelope serialization failed: {0}")]
    Serialize(serde_json::Error),
    /// JSON decoding failed.
    #[error("event envelope deserialization failed: {0}")]
    Deserialize(serde_json::Error),
    /// A reader received a different event type.
    #[error("expected event {expected}, received {actual}")]
    UnexpectedEventType {
        /// Expected stable event type.
        expected: &'static str,
        /// Received event type.
        actual: String,
    },
    /// A reader received an unsupported schema version.
    #[error("expected event version {expected}, received {actual}")]
    UnsupportedVersion {
        /// Expected schema version.
        expected: u16,
        /// Received schema version.
        actual: u16,
    },
    /// The partition key was blank.
    #[error("event partition key must not be blank")]
    BlankKey,
    /// The correlation ID was blank.
    #[error("event correlation ID must not be blank")]
    BlankCorrelationId,
    /// The causation ID was blank.
    #[error("event causation ID must not be blank")]
    BlankCausationId,
    /// A W3C trace-context carrier was blank, non-ASCII, or too large.
    #[error("event trace context must be bounded non-empty ASCII")]
    InvalidTraceContext,
}

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
#[derive(Debug, thiserror::Error)]
pub enum CompatibleDecodeError<E>
where
    E: StdError + Send + Sync + 'static,
{
    /// Stable envelope metadata was malformed or unsupported.
    #[error(transparent)]
    Envelope(EnvelopeError),
    /// The current-version payload could not be decoded into the typed event.
    #[error("current event payload deserialization failed: {0}")]
    Payload(serde_json::Error),
    /// Application-defined upcasting of an older payload failed.
    #[error("event payload upcast failed: {0}")]
    Upcaster(E),
}

/// Serialized event content plus metadata a provider uses for routing and observability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventMessage {
    id: EventId,
    event_type: String,
    version: u16,
    key: String,
    payload: Vec<u8>,
}

impl EventMessage {
    /// Reconstructs a provider message from metadata stored by a trusted durable relay.
    ///
    /// # Errors
    ///
    /// Returns [`EventMessageError`] when routing metadata is blank or the serialized body is
    /// empty. Event payload schema validation still belongs to the typed consumer.
    pub fn from_parts(
        id: EventId,
        event_type: impl Into<String>,
        version: u16,
        key: impl Into<String>,
        payload: Vec<u8>,
    ) -> Result<Self, EventMessageError> {
        let event_type = event_type.into();
        let key = key.into();
        if event_type.trim().is_empty() {
            return Err(EventMessageError::BlankEventType);
        }
        if key.trim().is_empty() {
            return Err(EventMessageError::BlankKey);
        }
        if payload.is_empty() {
            return Err(EventMessageError::EmptyPayload);
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

/// Invalid metadata recovered for a provider event message.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EventMessageError {
    /// The stored stable event type was blank.
    #[error("event message type must not be blank")]
    BlankEventType,
    /// The stored partition key was blank.
    #[error("event message partition key must not be blank")]
    BlankKey,
    /// The stored serialized envelope body was empty.
    #[error("event message payload must not be empty")]
    EmptyPayload,
}

/// Provider-facing contract for appending one serialized event to a configured stream topic.
pub trait EventPublisher: Clone + Send + Sync + 'static {
    /// Provider-specific append failure.
    type Error: StdError + Send + Sync + 'static;

    /// Appends an event and waits for the provider's configured delivery acknowledgement.
    fn publish(&self, message: EventMessage) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// Typed event producer built on a provider-specific publisher.
#[derive(Clone, Debug)]
pub struct EventClient<P> {
    publisher: P,
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

/// Metadata supplied to an event handler without exposing provider offset or partition handles.
#[derive(Clone, Debug, Eq, PartialEq)]
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
    fn from_envelope<E>(envelope: &EventEnvelope<E>) -> Self
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

/// The terminal settlement result observed by an event-stream consumer.
///
/// Event streams remain at-least-once systems. `Acknowledged`, `Retried`, and `DeadLettered`
/// mean the provider reported the named source-position settlement; they do not prove that an
/// external handler side effect occurred exactly once.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum EventDeliveryOutcome {
    /// The source position was acknowledged after the typed handler completed successfully.
    Acknowledged,
    /// The source delivery was routed to its retry destination and then acknowledged.
    Retried,
    /// The source delivery was routed to its dead-letter destination and then acknowledged.
    DeadLettered,
    /// The consumer could not durably settle the source delivery.
    Unsettled,
}

impl EventDeliveryOutcome {
    /// Returns the bounded, exporter-safe outcome label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Acknowledged => "acknowledged",
            Self::Retried => "retried",
            Self::DeadLettered => "dead_lettered",
            Self::Unsettled => "unsettled",
        }
    }
}

/// Metadata emitted when one provider consumer starts processing a delivery.
///
/// `provider` must be a stable implementation identifier such as `apache_kafka`; it is not a
/// broker endpoint, topic, consumer group, or application-configured route.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventDeliveryStarted {
    provider: &'static str,
}

impl EventDeliveryStarted {
    /// Returns the stable provider identifier.
    #[must_use]
    pub const fn provider(self) -> &'static str {
        self.provider
    }
}

/// Metadata emitted after a provider consumer settles or abandons one delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventDeliveryFinished {
    provider: &'static str,
    attempt: Option<NonZeroU16>,
    outcome: EventDeliveryOutcome,
    duration: Duration,
}

impl EventDeliveryFinished {
    /// Returns the stable provider identifier.
    #[must_use]
    pub const fn provider(self) -> &'static str {
        self.provider
    }

    /// Returns the one-based attempt when the provider could recover it without changing its
    /// ordinary delivery semantics.
    #[must_use]
    pub const fn attempt(self) -> Option<NonZeroU16> {
        self.attempt
    }

    /// Returns the terminal source-position settlement observed by the consumer.
    #[must_use]
    pub const fn outcome(self) -> EventDeliveryOutcome {
        self.outcome
    }

    /// Returns elapsed consumer processing time, including durable source settlement.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.duration
    }
}

/// Synchronous, non-blocking observer for durable event-delivery lifecycle events.
///
/// Implementations should aggregate locally or hand off work to a bounded exporter queue. The
/// framework catches observer panics so telemetry cannot change consumer settlement semantics.
pub trait EventDeliveryObserver: Send + Sync + 'static {
    /// Records the start of one received delivery.
    fn on_delivery_started(&self, delivery: EventDeliveryStarted);

    /// Records a completed source settlement or an unsettled consumer failure.
    fn on_delivery_finished(&self, delivery: EventDeliveryFinished);
}

/// No-op observer used by consumers that have not opted into event-delivery telemetry.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopEventDeliveryObserver;

impl EventDeliveryObserver for NoopEventDeliveryObserver {
    fn on_delivery_started(&self, _delivery: EventDeliveryStarted) {}

    fn on_delivery_finished(&self, _delivery: EventDeliveryFinished) {}
}

/// In-progress delivery observation owned by one provider consumer task.
///
/// Dropping this value without [`Self::finish`] records an `unsettled` result, including task
/// cancellation while the consumer is waiting for a handler, retry route, or source settlement.
pub struct EventDeliveryObservation {
    observer: Arc<dyn EventDeliveryObserver>,
    provider: &'static str,
    started_at: Instant,
    finished: bool,
}

impl EventDeliveryObservation {
    /// Starts observing a delivery for one stable provider implementation identifier.
    #[must_use]
    pub fn start(observer: Arc<dyn EventDeliveryObserver>, provider: &'static str) -> Self {
        notify_delivery_started(&observer, EventDeliveryStarted { provider });
        Self {
            observer,
            provider,
            started_at: Instant::now(),
            finished: false,
        }
    }

    /// Emits the final result after the provider settles the source position.
    pub fn finish(mut self, attempt: Option<NonZeroU16>, outcome: EventDeliveryOutcome) {
        self.finished = true;
        notify_delivery_finished(
            &self.observer,
            EventDeliveryFinished {
                provider: self.provider,
                attempt,
                outcome,
                duration: self.started_at.elapsed(),
            },
        );
    }
}

impl Drop for EventDeliveryObservation {
    fn drop(&mut self) {
        if !self.finished {
            notify_delivery_finished(
                &self.observer,
                EventDeliveryFinished {
                    provider: self.provider,
                    attempt: None,
                    outcome: EventDeliveryOutcome::Unsettled,
                    duration: self.started_at.elapsed(),
                },
            );
        }
    }
}

impl fmt::Debug for EventDeliveryObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventDeliveryObservation")
            .field("provider", &self.provider)
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

fn notify_delivery_started(
    observer: &Arc<dyn EventDeliveryObserver>,
    delivery: EventDeliveryStarted,
) {
    let _ = catch_unwind(AssertUnwindSafe(|| observer.on_delivery_started(delivery)));
}

fn notify_delivery_finished(
    observer: &Arc<dyn EventDeliveryObserver>,
    delivery: EventDeliveryFinished,
) {
    let _ = catch_unwind(AssertUnwindSafe(|| observer.on_delivery_finished(delivery)));
}

/// Failure while appending an event.
#[derive(Debug, thiserror::Error)]
pub enum PublishError<E> {
    /// The envelope could not be encoded.
    #[error(transparent)]
    Envelope(EnvelopeError),
    /// The provider could not append the event.
    #[error("event provider publish failed: {0}")]
    Provider(E),
}

fn unix_time_ms() -> u64 {
    let milliseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(milliseconds).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    use super::{
        CompatibleDecodeError, EnvelopeError, Event, EventContext, EventDeliveryFinished,
        EventDeliveryObservation, EventDeliveryObserver, EventDeliveryOutcome,
        EventDeliveryStarted, EventEnvelope, EventId, EventTraceContext, dispatch,
    };

    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    struct OrderPaid {
        order_id: u64,
    }

    impl Event for OrderPaid {
        const TYPE: &'static str = "orders.paid";
        const VERSION: u16 = 1;
    }

    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    struct OrderPaidV2 {
        order_id: u64,
        currency: String,
    }

    impl Event for OrderPaidV2 {
        const TYPE: &'static str = "orders.paid";
        const VERSION: u16 = 2;
    }

    struct PanickingDeliveryObserver;

    impl EventDeliveryObserver for PanickingDeliveryObserver {
        fn on_delivery_started(&self, _delivery: EventDeliveryStarted) {
            panic!("observer panic must not escape a consumer");
        }

        fn on_delivery_finished(&self, _delivery: EventDeliveryFinished) {
            panic!("observer panic must not escape a consumer");
        }
    }

    #[test]
    fn envelope_round_trip_preserves_key_and_correlation() {
        let envelope =
            EventEnvelope::with_metadata(EventId::new(), OrderPaid { order_id: 7 }, "acct-1", 123)
                .unwrap()
                .with_correlation_id("request-1")
                .unwrap();

        let decoded = EventEnvelope::<OrderPaid>::decode(&envelope.encode().unwrap()).unwrap();
        assert_eq!(decoded.key(), "acct-1");
        assert_eq!(decoded.correlation_id(), Some("request-1"));
        assert_eq!(decoded.into_payload(), OrderPaid { order_id: 7 });
    }

    #[test]
    fn blank_partition_key_is_rejected() {
        assert!(EventEnvelope::new(OrderPaid { order_id: 7 }, " ").is_err());
    }

    #[test]
    fn trace_context_round_trip_preserves_the_bounded_carrier() {
        let envelope = EventEnvelope::new(OrderPaid { order_id: 7 }, "7")
            .unwrap()
            .with_trace_context(
                EventTraceContext::new(
                    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                    Some("vendor=one".to_owned()),
                )
                .unwrap(),
            );
        let decoded = EventEnvelope::<OrderPaid>::decode(&envelope.encode().unwrap()).unwrap();
        assert_eq!(
            decoded.trace_context().unwrap().traceparent(),
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        );
        assert_eq!(
            decoded.trace_context().unwrap().tracestate(),
            Some("vendor=one")
        );
    }

    #[test]
    fn invalid_trace_context_is_rejected_before_or_during_decoding() {
        assert!(matches!(
            EventTraceContext::new(" ", None),
            Err(EnvelopeError::InvalidTraceContext)
        ));
        let invalid = r#"{
            "id":"550e8400-e29b-41d4-a716-446655440000",
            "event_type":"orders.paid",
            "version":1,
            "key":"7",
            "payload":{"order_id":7},
            "occurred_at_unix_ms":123,
            "correlation_id":null,
            "causation_id":null,
            "trace_context":{"traceparent":" ","tracestate":null}
        }"#;
        assert!(matches!(
            EventEnvelope::<OrderPaid>::decode(invalid.as_bytes()),
            Err(EnvelopeError::InvalidTraceContext)
        ));
    }

    #[test]
    fn compatible_decode_explicitly_upcasts_only_an_older_payload() {
        let id = EventId::new();
        let legacy = serde_json::json!({
            "id": id,
            "event_type": "orders.paid",
            "version": 1,
            "key": "acct-7",
            "payload": { "order_id": 7 },
            "occurred_at_unix_ms": 123,
            "correlation_id": "request-7",
            "causation_id": null,
            "trace_context": null
        });
        let upcaster =
            |source_version, payload: Value| -> Result<OrderPaidV2, std::convert::Infallible> {
                assert_eq!(source_version, 1);
                Ok(OrderPaidV2 {
                    order_id: payload["order_id"].as_u64().unwrap(),
                    currency: "KRW".to_owned(),
                })
            };

        let decoded = EventEnvelope::<OrderPaidV2>::decode_compatible(
            &serde_json::to_vec(&legacy).unwrap(),
            &upcaster,
        )
        .unwrap();
        assert_eq!(decoded.id(), id);
        assert_eq!(decoded.key(), "acct-7");
        assert_eq!(decoded.correlation_id(), Some("request-7"));
        assert_eq!(
            decoded.into_payload(),
            OrderPaidV2 {
                order_id: 7,
                currency: "KRW".to_owned(),
            }
        );
    }

    #[test]
    fn compatible_decode_rejects_a_newer_producer_version_before_upcasting() {
        let newer = serde_json::json!({
            "id": EventId::new(),
            "event_type": "orders.paid",
            "version": 3,
            "key": "acct-7",
            "payload": { "order_id": 7, "currency": "KRW" },
            "occurred_at_unix_ms": 123,
            "correlation_id": null,
            "causation_id": null,
            "trace_context": null
        });
        let upcaster =
            |_source_version, _payload: Value| -> Result<OrderPaidV2, std::convert::Infallible> {
                unreachable!("newer versions must not reach the upcaster")
            };

        assert!(matches!(
            EventEnvelope::<OrderPaidV2>::decode_compatible(
                &serde_json::to_vec(&newer).unwrap(),
                &upcaster,
            ),
            Err(CompatibleDecodeError::Envelope(
                EnvelopeError::UnsupportedVersion {
                    expected: 2,
                    actual: 3,
                }
            ))
        ));
    }

    #[test]
    fn delivery_observation_isolates_observer_panics() {
        EventDeliveryObservation::start(Arc::new(PanickingDeliveryObserver), "test_provider")
            .finish(None, EventDeliveryOutcome::Acknowledged);
    }

    #[tokio::test]
    async fn dispatch_passes_event_metadata_before_a_provider_commit() {
        let envelope =
            EventEnvelope::with_metadata(EventId::new(), OrderPaid { order_id: 7 }, "7", 123)
                .unwrap()
                .with_correlation_id("trace-7")
                .unwrap();

        dispatch(
            envelope,
            &|event: OrderPaid, context: EventContext| async move {
                assert_eq!(event.order_id, 7);
                assert_eq!(context.key(), "7");
                assert_eq!(context.correlation_id(), Some("trace-7"));
                Ok::<_, std::convert::Infallible>(())
            },
        )
        .await
        .unwrap();
    }
}
