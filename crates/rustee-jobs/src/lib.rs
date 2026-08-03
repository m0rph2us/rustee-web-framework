//! Typed durable job envelopes and provider-neutral delivery rules.
//!
//! Providers own broker-specific publish, receive, acknowledgement, delay, and dead-letter
//! behavior. This crate defines only the metadata every durable job must preserve.

use std::{
    collections::BTreeMap,
    error::Error as StdError,
    fmt,
    num::{NonZeroU16, NonZeroUsize},
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

/// A serializable, versioned job payload.
pub trait Job: DeserializeOwned + Serialize + Send + Sync + 'static {
    /// Stable job type name used for provider routing and backward-compatible decoding.
    const NAME: &'static str;
    /// Payload schema version understood by this handler.
    const VERSION: u16;
}

/// A durable, globally unique job identifier.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct JobId(Uuid);

impl JobId {
    /// Creates a new random UUID v4 job identifier.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Wraps a UUID recovered from a trusted durable job store.
    #[must_use]
    pub const fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A bounded W3C trace-context carrier attached to a durable job envelope.
///
/// `rustee-jobs` persists only transport-neutral carrier values. An optional telemetry adapter
/// decides whether a valid carrier becomes the parent of a job-handling span.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct JobTraceContext {
    traceparent: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    tracestate: Option<String>,
}

impl JobTraceContext {
    /// Creates a bounded ASCII W3C trace-context carrier.
    ///
    /// # Errors
    ///
    /// Returns [`EnvelopeError::InvalidTraceContext`] when a value is blank, non-ASCII, or larger
    /// than the documented W3C carrier bounds. The optional telemetry propagator validates syntax
    /// and sampling when it consumes this carrier.
    pub fn new(
        traceparent: impl Into<String>,
        tracestate: Option<String>,
    ) -> Result<Self, EnvelopeError> {
        let context = Self {
            traceparent: traceparent.into(),
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

/// A JSON-encoded durable job payload plus transport-independent delivery metadata.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct JobEnvelope<J> {
    id: JobId,
    name: String,
    version: u16,
    payload: J,
    idempotency_key: Option<String>,
    enqueued_at_unix_ms: u64,
    attempt: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_context: Option<JobTraceContext>,
}

impl<J> JobEnvelope<J>
where
    J: Job,
{
    /// Creates a first-delivery envelope using the current system timestamp.
    #[must_use]
    pub fn new(payload: J) -> Self {
        Self::with_metadata(JobId::new(), payload, unix_time_ms())
    }

    /// Creates a deterministic envelope for tests, outbox relays, or provider recovery code.
    #[must_use]
    pub fn with_metadata(id: JobId, payload: J, enqueued_at_unix_ms: u64) -> Self {
        Self {
            id,
            name: J::NAME.to_owned(),
            version: J::VERSION,
            payload,
            idempotency_key: None,
            enqueued_at_unix_ms,
            attempt: 1,
            trace_context: None,
        }
    }

    /// Adds an application-defined idempotency key.
    ///
    /// # Errors
    ///
    /// Returns [`EnvelopeError::BlankIdempotencyKey`] when `key` is blank.
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Result<Self, EnvelopeError> {
        let key = key.into();
        if key.trim().is_empty() {
            return Err(EnvelopeError::BlankIdempotencyKey);
        }
        self.idempotency_key = Some(key);
        Ok(self)
    }

    /// Adds a bounded W3C trace-context carrier for an optional telemetry integration.
    #[must_use]
    pub fn with_trace_context(mut self, trace_context: JobTraceContext) -> Self {
        self.trace_context = Some(trace_context);
        self
    }

    /// Serializes this envelope for a provider message body.
    ///
    /// # Errors
    ///
    /// Returns an error when the payload cannot be serialized as JSON.
    pub fn encode(&self) -> Result<Vec<u8>, EnvelopeError> {
        serde_json::to_vec(self).map_err(EnvelopeError::Serialize)
    }

    /// Builds the provider message while retaining non-sensitive delivery metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the payload cannot be serialized as JSON.
    pub fn message(&self) -> Result<JobMessage, EnvelopeError> {
        Ok(JobMessage {
            id: self.id(),
            name: self.name().to_owned(),
            version: self.version(),
            attempt: self.attempt(),
            payload: self.encode()?,
        })
    }

    /// Decodes and validates an envelope for the expected job type and version.
    ///
    /// # Errors
    ///
    /// Returns an error when JSON is malformed, the job type/version does not match, or the
    /// attempt counter is invalid.
    pub fn decode(bytes: &[u8]) -> Result<Self, EnvelopeError> {
        let envelope = serde_json::from_slice::<Self>(bytes).map_err(EnvelopeError::Deserialize)?;
        envelope.validate()?;
        Ok(envelope)
    }

    /// Decodes this job's current version or explicitly upcasts an older payload version.
    ///
    /// The normal [`Self::decode`] path remains strict. This method accepts only versions lower
    /// than [`Job::VERSION`] and invokes `upcaster` for them. It never accepts a newer producer
    /// version because a typed worker cannot safely infer compatibility in that direction.
    ///
    /// # Errors
    ///
    /// Returns [`CompatibleDecodeError`] when envelope metadata is invalid, the current payload
    /// cannot be decoded, the upcaster rejects an older payload, or the producer version is newer
    /// than this worker supports.
    pub fn decode_compatible<U>(
        bytes: &[u8],
        upcaster: &U,
    ) -> Result<Self, CompatibleDecodeError<U::Error>>
    where
        U: JobUpcaster<J>,
    {
        let raw = serde_json::from_slice::<RawJobEnvelope>(bytes)
            .map_err(EnvelopeError::Deserialize)
            .map_err(CompatibleDecodeError::Envelope)?;
        validate_metadata::<J>(
            &raw.name,
            raw.idempotency_key.as_deref(),
            raw.attempt,
            raw.trace_context.as_ref(),
        )
        .map_err(CompatibleDecodeError::Envelope)?;
        if raw.version > J::VERSION {
            return Err(CompatibleDecodeError::Envelope(
                EnvelopeError::UnsupportedVersion {
                    expected: J::VERSION,
                    actual: raw.version,
                },
            ));
        }
        let payload = if raw.version == J::VERSION {
            serde_json::from_value(raw.payload).map_err(CompatibleDecodeError::Payload)?
        } else {
            upcaster
                .upcast(raw.version, raw.payload)
                .map_err(CompatibleDecodeError::Upcaster)?
        };
        Ok(Self {
            id: raw.id,
            name: raw.name,
            version: J::VERSION,
            payload,
            idempotency_key: raw.idempotency_key,
            enqueued_at_unix_ms: raw.enqueued_at_unix_ms,
            attempt: raw.attempt,
            trace_context: raw.trace_context,
        })
    }

    /// Returns the stable job ID.
    #[must_use]
    pub const fn id(&self) -> JobId {
        self.id
    }

    /// Returns the stable job type name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the job schema version.
    #[must_use]
    pub const fn version(&self) -> u16 {
        self.version
    }

    /// Returns the durable idempotency key when the producer supplied one.
    #[must_use]
    pub fn idempotency_key(&self) -> Option<&str> {
        self.idempotency_key.as_deref()
    }

    /// Returns the optional W3C trace-context carrier attached by the producer.
    #[must_use]
    pub fn trace_context(&self) -> Option<&JobTraceContext> {
        self.trace_context.as_ref()
    }

    /// Returns the enqueue timestamp in Unix milliseconds.
    #[must_use]
    pub const fn enqueued_at_unix_ms(&self) -> u64 {
        self.enqueued_at_unix_ms
    }

    /// Returns the one-based delivery attempt number.
    #[must_use]
    pub const fn attempt(&self) -> u16 {
        self.attempt
    }

    /// Consumes the envelope and returns the application payload.
    #[must_use]
    pub fn into_payload(self) -> J {
        self.payload
    }

    /// Increments the delivery attempt after a provider has chosen to retry.
    ///
    /// # Errors
    ///
    /// Returns [`EnvelopeError::AttemptOverflow`] when the `u16` attempt counter is exhausted.
    pub fn next_attempt(mut self) -> Result<Self, EnvelopeError> {
        self.attempt = self
            .attempt
            .checked_add(1)
            .ok_or(EnvelopeError::AttemptOverflow)?;
        Ok(self)
    }

    /// Replaces the delivery attempt with the provider-observed one-based attempt number.
    ///
    /// Providers use this before dispatch so [`JobContext`] reflects redeliveries even when the
    /// stored envelope body has not changed.
    ///
    /// # Errors
    ///
    /// Returns [`EnvelopeError::InvalidAttempt`] when `attempt` is zero.
    pub fn with_attempt(mut self, attempt: u16) -> Result<Self, EnvelopeError> {
        if attempt == 0 {
            return Err(EnvelopeError::InvalidAttempt);
        }
        self.attempt = attempt;
        Ok(self)
    }

    fn validate(&self) -> Result<(), EnvelopeError> {
        validate_metadata::<J>(
            &self.name,
            self.idempotency_key.as_deref(),
            self.attempt,
            self.trace_context.as_ref(),
        )?;
        if self.version != J::VERSION {
            return Err(EnvelopeError::UnsupportedVersion {
                expected: J::VERSION,
                actual: self.version,
            });
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct RawJobEnvelope {
    id: JobId,
    name: String,
    version: u16,
    payload: Value,
    idempotency_key: Option<String>,
    enqueued_at_unix_ms: u64,
    attempt: u16,
    trace_context: Option<JobTraceContext>,
}

fn validate_metadata<J>(
    name: &str,
    idempotency_key: Option<&str>,
    attempt: u16,
    trace_context: Option<&JobTraceContext>,
) -> Result<(), EnvelopeError>
where
    J: Job,
{
    if name != J::NAME {
        return Err(EnvelopeError::UnexpectedJobName {
            expected: J::NAME,
            actual: name.to_owned(),
        });
    }
    if attempt == 0 {
        return Err(EnvelopeError::InvalidAttempt);
    }
    if idempotency_key.is_some_and(|key| key.trim().is_empty()) {
        return Err(EnvelopeError::BlankIdempotencyKey);
    }
    if let Some(trace_context) = trace_context {
        trace_context.validate()?;
    }
    Ok(())
}

/// Failed durable job serialization or envelope validation.
#[derive(Debug, thiserror::Error)]
pub enum EnvelopeError {
    /// JSON encoding failed.
    #[error("job envelope serialization failed: {0}")]
    Serialize(serde_json::Error),
    /// JSON decoding failed.
    #[error("job envelope deserialization failed: {0}")]
    Deserialize(serde_json::Error),
    /// The provider message addressed a different job handler.
    #[error("expected job {expected}, received {actual}")]
    UnexpectedJobName {
        /// Expected stable job name.
        expected: &'static str,
        /// Received stable job name.
        actual: String,
    },
    /// The provider message used a version this handler cannot process.
    #[error("expected job version {expected}, received {actual}")]
    UnsupportedVersion {
        /// Expected job schema version.
        expected: u16,
        /// Received job schema version.
        actual: u16,
    },
    /// The serialized attempt counter was zero.
    #[error("job delivery attempt must be at least one")]
    InvalidAttempt,
    /// The attempt counter cannot be incremented further.
    #[error("job delivery attempt counter overflowed")]
    AttemptOverflow,
    /// The idempotency key was blank.
    #[error("job idempotency key must not be blank")]
    BlankIdempotencyKey,
    /// The serialized W3C trace carrier was unsafe or outside the bounded format.
    #[error("job trace context is invalid")]
    InvalidTraceContext,
}

/// A pure application-defined conversion from an older payload version to the current job type.
///
/// The upcaster receives only an older JSON payload and its source version. Envelope metadata,
/// idempotency key, delivery attempt, and newer-version rejection remain controlled by
/// [`JobEnvelope::decode_compatible`].
pub trait JobUpcaster<J>: Send + Sync
where
    J: Job,
{
    /// Application-specific conversion failure.
    type Error: StdError + Send + Sync + 'static;

    /// Converts one lower source version into the current job payload type.
    ///
    /// # Errors
    ///
    /// Returns the application-defined error when the older payload cannot be converted safely.
    fn upcast(&self, source_version: u16, payload: Value) -> Result<J, Self::Error>;
}

impl<J, F, Error> JobUpcaster<J> for F
where
    J: Job,
    F: Fn(u16, Value) -> Result<J, Error> + Send + Sync,
    Error: StdError + Send + Sync + 'static,
{
    type Error = Error;

    fn upcast(&self, source_version: u16, payload: Value) -> Result<J, Self::Error> {
        self(source_version, payload)
    }
}

/// Failed strict or compatible job-envelope decoding.
#[derive(Debug, thiserror::Error)]
pub enum CompatibleDecodeError<E>
where
    E: StdError + Send + Sync + 'static,
{
    /// Stable envelope metadata was malformed or unsupported.
    #[error(transparent)]
    Envelope(EnvelopeError),
    /// The current-version payload could not be decoded into the typed job.
    #[error("current job payload deserialization failed: {0}")]
    Payload(serde_json::Error),
    /// Application-defined upcasting of an older payload failed.
    #[error("job payload upcast failed: {0}")]
    Upcaster(E),
}

/// Serialized job content plus metadata a provider may use for deduplication and observability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobMessage {
    id: JobId,
    name: String,
    version: u16,
    attempt: u16,
    payload: Vec<u8>,
}

impl JobMessage {
    /// Reconstructs a provider message from metadata stored by a trusted durable relay.
    ///
    /// # Errors
    ///
    /// Returns [`JobMessageError`] when the stored metadata cannot represent a durable job
    /// delivery. Payload schema validation still belongs to the typed worker.
    pub fn from_parts(
        id: JobId,
        name: impl Into<String>,
        version: u16,
        attempt: u16,
        payload: Vec<u8>,
    ) -> Result<Self, JobMessageError> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(JobMessageError::BlankName);
        }
        if attempt == 0 {
            return Err(JobMessageError::InvalidAttempt);
        }
        if payload.is_empty() {
            return Err(JobMessageError::EmptyPayload);
        }
        Ok(Self {
            id,
            name,
            version,
            attempt,
            payload,
        })
    }

    /// Returns the durable job ID, suitable for a provider deduplication key.
    #[must_use]
    pub const fn id(&self) -> JobId {
        self.id
    }

    /// Returns the stable job type name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the schema version.
    #[must_use]
    pub const fn version(&self) -> u16 {
        self.version
    }

    /// Returns the one-based delivery attempt number.
    #[must_use]
    pub const fn attempt(&self) -> u16 {
        self.attempt
    }

    /// Consumes the message and returns its serialized envelope body.
    #[must_use]
    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }
}

/// Invalid metadata recovered for a provider job message.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum JobMessageError {
    /// The stored stable job name was blank.
    #[error("job message name must not be blank")]
    BlankName,
    /// The stored attempt was zero.
    #[error("job message delivery attempt must be at least one")]
    InvalidAttempt,
    /// The stored serialized envelope body was empty.
    #[error("job message payload must not be empty")]
    EmptyPayload,
}

/// Provider-facing contract for publishing one serialized durable job message.
pub trait JobPublisher: Clone + Send + Sync + 'static {
    /// Provider-specific publish failure.
    type Error: StdError + Send + Sync + 'static;

    /// Persists a serialized envelope for later delivery.
    fn publish(&self, message: JobMessage) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// Typed durable-job producer built on a provider-specific publisher.
#[derive(Clone, Debug)]
pub struct JobClient<P> {
    publisher: P,
}

impl<P> JobClient<P> {
    /// Creates a job client from one provider-specific publisher.
    #[must_use]
    pub fn new(publisher: P) -> Self {
        Self { publisher }
    }
}

impl<P> JobClient<P>
where
    P: JobPublisher,
{
    /// Serializes and persists an already-configured job envelope.
    ///
    /// # Errors
    ///
    /// Returns an envelope serialization failure or a provider publish failure.
    pub async fn enqueue<J>(&self, envelope: &JobEnvelope<J>) -> Result<(), EnqueueError<P::Error>>
    where
        J: Job,
    {
        let message = envelope.message().map_err(EnqueueError::Envelope)?;
        self.publisher
            .publish(message)
            .await
            .map_err(EnqueueError::Provider)
    }
}

/// Failure while publishing a durable job.
#[derive(Debug, thiserror::Error)]
pub enum EnqueueError<E> {
    /// The envelope could not be serialized.
    #[error(transparent)]
    Envelope(EnvelopeError),
    /// The provider could not durably publish the message.
    #[error("job provider publish failed: {0}")]
    Provider(E),
}

/// Metadata supplied to a handler without exposing provider delivery handles.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobContext {
    id: JobId,
    name: String,
    version: u16,
    idempotency_key: Option<String>,
    attempt: u16,
    trace_context: Option<JobTraceContext>,
}

impl JobContext {
    fn from_envelope<J>(envelope: &JobEnvelope<J>) -> Self
    where
        J: Job,
    {
        Self {
            id: envelope.id(),
            name: envelope.name().to_owned(),
            version: envelope.version(),
            idempotency_key: envelope.idempotency_key().map(ToOwned::to_owned),
            attempt: envelope.attempt(),
            trace_context: envelope.trace_context().cloned(),
        }
    }

    /// Returns the durable job ID.
    #[must_use]
    pub const fn id(&self) -> JobId {
        self.id
    }

    /// Returns the job type name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the job schema version.
    #[must_use]
    pub const fn version(&self) -> u16 {
        self.version
    }

    /// Returns the producer's idempotency key when supplied.
    #[must_use]
    pub fn idempotency_key(&self) -> Option<&str> {
        self.idempotency_key.as_deref()
    }

    /// Returns the optional W3C trace-context carrier attached by the producer.
    #[must_use]
    pub fn trace_context(&self) -> Option<&JobTraceContext> {
        self.trace_context.as_ref()
    }

    /// Returns the one-based delivery attempt number.
    #[must_use]
    pub const fn attempt(&self) -> u16 {
        self.attempt
    }
}

/// A typed asynchronous job handler.
pub trait JobHandler<J>: Clone + Send + Sync + 'static
where
    J: Job,
{
    /// Handler-specific failure type, kept available for provider logs and metrics.
    type Error: StdError + Send + Sync + 'static;

    /// Processes one deserialized job payload.
    fn handle(
        &self,
        payload: J,
        context: JobContext,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

impl<J, F, Future, E> JobHandler<J> for F
where
    J: Job,
    F: Fn(J, JobContext) -> Future + Clone + Send + Sync + 'static,
    Future: std::future::Future<Output = Result<(), E>> + Send + 'static,
    E: StdError + Send + Sync + 'static,
{
    type Error = E;

    fn handle(
        &self,
        payload: J,
        context: JobContext,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(self(payload, context))
    }
}

/// Runs one typed handler invocation and maps success to an acknowledgement decision.
///
/// A provider must call this before acknowledging a delivery. On error it must use its own error
/// reporting and [`RetryPolicy`] to choose retry or dead-letter behavior.
///
/// # Errors
///
/// Returns the handler's error without acknowledging, retrying, or discarding the delivery.
pub async fn dispatch<J, H>(
    envelope: JobEnvelope<J>,
    handler: &H,
) -> Result<DeliveryAction, H::Error>
where
    J: Job,
    H: JobHandler<J>,
{
    let context = JobContext::from_envelope(&envelope);
    handler.handle(envelope.into_payload(), context).await?;
    Ok(DeliveryAction::Acknowledge)
}

/// Result of one broker delivery after its source message has been durably settled.
///
/// The value intentionally excludes payload bytes, job IDs, queue routes, broker handles, and
/// handler error text. Those values either create unbounded metrics labels or can carry secrets.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum JobDeliveryOutcome {
    /// The source delivery was acknowledged or deleted after successful handling.
    Acknowledged,
    /// The source delivery was durably scheduled for a later attempt.
    Retried,
    /// The source delivery was copied to its dead-letter route and then acknowledged or deleted.
    DeadLettered,
    /// The worker could not durably settle the source delivery.
    Unsettled,
}

impl JobDeliveryOutcome {
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

/// Metadata emitted when one provider worker starts processing a delivery.
///
/// `provider` must be a stable implementation identifier such as `nats_jetstream`; it is not a
/// broker URL, queue name, or user-configurable route.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobDeliveryStarted {
    provider: &'static str,
}

impl JobDeliveryStarted {
    /// Returns the stable provider identifier.
    #[must_use]
    pub const fn provider(self) -> &'static str {
        self.provider
    }
}

/// Metadata emitted after a provider worker completes or abandons one delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobDeliveryFinished {
    provider: &'static str,
    attempt: Option<NonZeroU16>,
    outcome: JobDeliveryOutcome,
    duration: Duration,
}

impl JobDeliveryFinished {
    /// Returns the stable provider identifier.
    #[must_use]
    pub const fn provider(self) -> &'static str {
        self.provider
    }

    /// Returns the one-based delivery attempt when the provider could recover it safely.
    #[must_use]
    pub const fn attempt(self) -> Option<NonZeroU16> {
        self.attempt
    }

    /// Returns the terminal settlement result observed by the worker.
    #[must_use]
    pub const fn outcome(self) -> JobDeliveryOutcome {
        self.outcome
    }

    /// Returns the elapsed worker processing time, including durable settlement.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.duration
    }
}

/// Synchronous, non-blocking observer for durable job delivery lifecycle events.
///
/// Implementations should aggregate locally or hand off work to a bounded exporter queue. The
/// framework catches observer panics so telemetry cannot change acknowledgement semantics.
pub trait JobDeliveryObserver: Send + Sync + 'static {
    /// Records the start of one received delivery.
    fn on_delivery_started(&self, delivery: JobDeliveryStarted);

    /// Records a completed delivery settlement or an unsettled worker failure.
    fn on_delivery_finished(&self, delivery: JobDeliveryFinished);
}

/// No-op observer used by workers that have not opted into job delivery telemetry.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopJobDeliveryObserver;

impl JobDeliveryObserver for NoopJobDeliveryObserver {
    fn on_delivery_started(&self, _delivery: JobDeliveryStarted) {}

    fn on_delivery_finished(&self, _delivery: JobDeliveryFinished) {}
}

/// In-progress delivery observation owned by one provider worker task.
///
/// Dropping this value without [`Self::finish`] records an `unsettled` result, including task
/// cancellation during a bounded worker drain.
pub struct JobDeliveryObservation {
    observer: Arc<dyn JobDeliveryObserver>,
    provider: &'static str,
    started_at: Instant,
    finished: bool,
}

impl JobDeliveryObservation {
    /// Starts observing a delivery for one stable provider implementation identifier.
    #[must_use]
    pub fn start(observer: Arc<dyn JobDeliveryObserver>, provider: &'static str) -> Self {
        notify_started(&observer, JobDeliveryStarted { provider });
        Self {
            observer,
            provider,
            started_at: Instant::now(),
            finished: false,
        }
    }

    /// Emits the final result after durable provider settlement completes.
    pub fn finish(mut self, attempt: Option<NonZeroU16>, outcome: JobDeliveryOutcome) {
        self.finished = true;
        notify_finished(
            &self.observer,
            JobDeliveryFinished {
                provider: self.provider,
                attempt,
                outcome,
                duration: self.started_at.elapsed(),
            },
        );
    }
}

impl Drop for JobDeliveryObservation {
    fn drop(&mut self) {
        if !self.finished {
            notify_finished(
                &self.observer,
                JobDeliveryFinished {
                    provider: self.provider,
                    attempt: None,
                    outcome: JobDeliveryOutcome::Unsettled,
                    duration: self.started_at.elapsed(),
                },
            );
        }
    }
}

impl fmt::Debug for JobDeliveryObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobDeliveryObservation")
            .field("provider", &self.provider)
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

fn notify_started(observer: &Arc<dyn JobDeliveryObserver>, delivery: JobDeliveryStarted) {
    let _ = catch_unwind(AssertUnwindSafe(|| observer.on_delivery_started(delivery)));
}

fn notify_finished(observer: &Arc<dyn JobDeliveryObserver>, delivery: JobDeliveryFinished) {
    let _ = catch_unwind(AssertUnwindSafe(|| observer.on_delivery_finished(delivery)));
}

const MAX_REGISTRY_ENVELOPE_BYTES: usize = 1024 * 1024;
const MAX_REGISTERED_JOB_NAME_BYTES: usize = 255;

/// A fixed set of typed job handlers that can dispatch one provider delivery by its envelope name.
///
/// Build the registry during application startup, then clone it into provider workers. The
/// registry is immutable after startup: registration detects duplicate names so a deployment never
/// depends on handler insertion order. It retains only stable handler metadata and never exposes
/// raw job payloads or handler errors through its error values.
#[derive(Clone, Default)]
pub struct JobRegistry {
    handlers: BTreeMap<String, Arc<dyn RegisteredJobHandler>>,
}

impl JobRegistry {
    /// Creates an empty immutable-after-startup job registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one typed handler under its stable [`Job::NAME`].
    ///
    /// # Errors
    ///
    /// Returns [`JobRegistryRegistrationError::InvalidJobName`] for a blank or oversized job
    /// name, or [`JobRegistryRegistrationError::DuplicateJobName`] when the name is already
    /// registered.
    pub fn register<J, H>(&mut self, handler: H) -> Result<(), JobRegistryRegistrationError>
    where
        J: Job,
        H: JobHandler<J>,
    {
        self.insert_handler::<J>(Arc::new(StrictRegisteredJobHandler::<J, H> {
            handler,
            marker: std::marker::PhantomData,
        }))
    }

    /// Registers one typed handler with an explicit compatibility upcaster for older payloads.
    ///
    /// Current-version payloads decode normally. Only lower producer versions reach `upcaster`;
    /// newer versions remain rejected. Use [`Self::register`] when the worker must stay strict.
    ///
    /// # Errors
    ///
    /// Returns [`JobRegistryRegistrationError`] when the stable job name is invalid or another
    /// handler already owns it.
    pub fn register_with_upcaster<J, H, U>(
        &mut self,
        handler: H,
        upcaster: U,
    ) -> Result<(), JobRegistryRegistrationError>
    where
        J: Job,
        H: JobHandler<J>,
        U: JobUpcaster<J> + 'static,
    {
        self.insert_handler::<J>(Arc::new(CompatibleRegisteredJobHandler::<J, H, U> {
            handler,
            upcaster,
            marker: std::marker::PhantomData,
        }))
    }

    fn insert_handler<J>(
        &mut self,
        handler: Arc<dyn RegisteredJobHandler>,
    ) -> Result<(), JobRegistryRegistrationError>
    where
        J: Job,
    {
        validate_registered_job_name(J::NAME)?;
        if self.handlers.contains_key(J::NAME) {
            return Err(JobRegistryRegistrationError::DuplicateJobName);
        }
        self.handlers.insert(J::NAME.to_owned(), handler);
        Ok(())
    }

    /// Returns the registered stable job names in deterministic lexical order.
    pub fn registered_names(&self) -> impl Iterator<Item = &str> {
        self.handlers.keys().map(String::as_str)
    }

    /// Decodes and dispatches one provider payload using its registered envelope name.
    ///
    /// `attempt` comes from the provider delivery metadata and replaces the attempt serialized in
    /// the envelope before the typed handler receives its [`JobContext`]. Unknown or malformed
    /// deliveries return a sanitized error so providers can dead-letter them without retrying an
    /// arbitrary payload. Typed handler failures return [`JobRegistryError::Handler`] so providers
    /// can apply their configured retry policy.
    #[must_use]
    pub fn dispatch(
        &self,
        payload: &[u8],
        attempt: u16,
    ) -> BoxFuture<'static, Result<DeliveryAction, JobRegistryError>> {
        if attempt == 0 {
            return Box::pin(async { Err(JobRegistryError::InvalidAttempt) });
        }
        if payload.is_empty() || payload.len() > MAX_REGISTRY_ENVELOPE_BYTES {
            return Box::pin(async { Err(JobRegistryError::InvalidEnvelope) });
        }
        let Ok(identity) = serde_json::from_slice::<RegistryEnvelopeIdentity>(payload) else {
            return Box::pin(async { Err(JobRegistryError::InvalidEnvelope) });
        };
        let Some(handler) = self.handlers.get(&identity.name).cloned() else {
            return Box::pin(async { Err(JobRegistryError::UnknownJob) });
        };
        handler.dispatch(payload.to_vec(), attempt)
    }
}

impl fmt::Debug for JobRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobRegistry")
            .field(
                "registered_names",
                &self.handlers.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[derive(Deserialize)]
struct RegistryEnvelopeIdentity {
    name: String,
}

trait RegisteredJobHandler: Send + Sync {
    fn dispatch(
        &self,
        payload: Vec<u8>,
        attempt: u16,
    ) -> BoxFuture<'static, Result<DeliveryAction, JobRegistryError>>;
}

struct StrictRegisteredJobHandler<J, H>
where
    J: Job,
    H: JobHandler<J>,
{
    handler: H,
    marker: std::marker::PhantomData<fn() -> J>,
}

impl<J, H> RegisteredJobHandler for StrictRegisteredJobHandler<J, H>
where
    J: Job,
    H: JobHandler<J>,
{
    fn dispatch(
        &self,
        payload: Vec<u8>,
        attempt: u16,
    ) -> BoxFuture<'static, Result<DeliveryAction, JobRegistryError>> {
        let handler = self.handler.clone();
        Box::pin(async move {
            let envelope = JobEnvelope::<J>::decode(&payload)
                .map_err(|_| JobRegistryError::InvalidEnvelope)?
                .with_attempt(attempt)
                .map_err(|_| JobRegistryError::InvalidAttempt)?;
            let id = envelope.id();
            let name = envelope.name().to_owned();
            dispatch(envelope, &handler)
                .await
                .map_err(|_| JobRegistryError::Handler { id, name })
        })
    }
}

struct CompatibleRegisteredJobHandler<J, H, U>
where
    J: Job,
    H: JobHandler<J>,
    U: JobUpcaster<J>,
{
    handler: H,
    upcaster: U,
    marker: std::marker::PhantomData<fn() -> J>,
}

impl<J, H, U> RegisteredJobHandler for CompatibleRegisteredJobHandler<J, H, U>
where
    J: Job,
    H: JobHandler<J>,
    U: JobUpcaster<J> + 'static,
{
    fn dispatch(
        &self,
        payload: Vec<u8>,
        attempt: u16,
    ) -> BoxFuture<'static, Result<DeliveryAction, JobRegistryError>> {
        let handler = self.handler.clone();
        let envelope = JobEnvelope::<J>::decode_compatible(&payload, &self.upcaster)
            .map_err(|_| JobRegistryError::InvalidEnvelope)
            .and_then(|envelope| {
                envelope
                    .with_attempt(attempt)
                    .map_err(|_| JobRegistryError::InvalidAttempt)
            });
        Box::pin(async move {
            let envelope = envelope?;
            let id = envelope.id();
            let name = envelope.name().to_owned();
            dispatch(envelope, &handler)
                .await
                .map_err(|_| JobRegistryError::Handler { id, name })
        })
    }
}

/// Registration failure for one typed [`JobRegistry`] handler.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum JobRegistryRegistrationError {
    /// The static job name was blank, contained whitespace, or exceeded its bounded registry key.
    #[error("registered job name must be non-blank, whitespace-free, and bounded")]
    InvalidJobName,
    /// A startup registry attempted to replace a handler with the same stable name.
    #[error("a handler is already registered for this job name")]
    DuplicateJobName,
}

/// Sanitized outcome of dispatching one registry-selected job delivery.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum JobRegistryError {
    /// The provider payload was empty, oversized, malformed, or did not match its registered type.
    #[error("registered job envelope was invalid")]
    InvalidEnvelope,
    /// The provider did not supply a valid one-based delivery attempt.
    #[error("registered job delivery attempt was invalid")]
    InvalidAttempt,
    /// No handler was registered for the stable job name in the provider envelope.
    #[error("no handler is registered for this job")]
    UnknownJob,
    /// The registered typed handler failed; raw handler text is intentionally not retained.
    #[error("registered job handler failed")]
    Handler {
        /// Stable job ID useful for bounded operational correlation.
        id: JobId,
        /// Registered stable job name.
        name: String,
    },
}

fn validate_registered_job_name(name: &str) -> Result<(), JobRegistryRegistrationError> {
    if name.trim().is_empty()
        || name.len() > MAX_REGISTERED_JOB_NAME_BYTES
        || name.chars().any(char::is_whitespace)
    {
        return Err(JobRegistryRegistrationError::InvalidJobName);
    }
    Ok(())
}

/// A provider-neutral retry policy for failures after a delivery attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    /// Maximum total deliveries, including the first delivery.
    pub max_deliveries: u16,
    /// Delay before the first retry.
    pub initial_backoff: Duration,
    /// Maximum retry delay after exponential backoff.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_deliveries: 5,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_mins(5),
        }
    }
}

impl RetryPolicy {
    /// Chooses a retry or dead-letter action after a failed one-based delivery attempt.
    #[must_use]
    pub fn after_failure(self, attempt: u16) -> DeliveryAction {
        if attempt == 0 || attempt >= self.max_deliveries {
            return DeliveryAction::DeadLetter;
        }

        let exponent = u32::from(attempt.saturating_sub(1));
        let multiplier = 2_u32.saturating_pow(exponent);
        let delay = self
            .initial_backoff
            .saturating_mul(multiplier)
            .min(self.max_backoff);
        DeliveryAction::Retry {
            next_attempt: attempt.saturating_add(1),
            delay,
        }
    }
}

/// The next provider delivery action after handling a job.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryAction {
    /// Acknowledge only after the handler's side effect has completed successfully.
    Acknowledge,
    /// Retry after an explicit delay, preserving the next delivery attempt number.
    Retry {
        /// One-based delivery attempt number to use on the retry.
        next_attempt: u16,
        /// Minimum delay before another delivery attempt.
        delay: Duration,
    },
    /// Move the message to a provider-specific dead-letter path without automatic replay.
    DeadLetter,
}

/// Worker settings shared by provider runtimes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerConfig {
    /// Maximum number of handler invocations running in this worker process.
    pub concurrency: NonZeroUsize,
    /// Time allowed to drain active handler invocations during shutdown.
    pub drain_timeout: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            concurrency: NonZeroUsize::new(8).expect("8 is non-zero"),
            drain_timeout: Duration::from_secs(30),
        }
    }
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
    use std::{
        convert::Infallible,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    use super::{
        CompatibleDecodeError, DeliveryAction, EnvelopeError, Job, JobClient, JobEnvelope, JobId,
        JobPublisher, JobRegistry, JobRegistryError, JobRegistryRegistrationError, JobTraceContext,
        RetryPolicy, dispatch,
    };

    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    struct WelcomeEmail {
        user_id: u64,
    }

    impl Job for WelcomeEmail {
        const NAME: &'static str = "email.welcome";
        const VERSION: u16 = 1;
    }

    #[derive(Clone, Default)]
    struct CapturingPublisher {
        messages: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl JobPublisher for CapturingPublisher {
        type Error = Infallible;

        fn publish(
            &self,
            message: super::JobMessage,
        ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
            let messages = self.messages.clone();
            Box::pin(async move {
                messages.lock().unwrap().push(message.into_payload());
                Ok(())
            })
        }
    }

    #[test]
    fn envelope_round_trip_preserves_idempotency_metadata() {
        let id = JobId::new();
        let envelope = JobEnvelope::with_metadata(id, WelcomeEmail { user_id: 7 }, 123)
            .with_idempotency_key("welcome:7")
            .unwrap()
            .with_trace_context(
                JobTraceContext::new(
                    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                    Some("vendor=one".to_owned()),
                )
                .unwrap(),
            );

        let decoded = JobEnvelope::<WelcomeEmail>::decode(&envelope.encode().unwrap()).unwrap();
        assert_eq!(decoded.id(), id);
        assert_eq!(decoded.idempotency_key(), Some("welcome:7"));
        assert_eq!(decoded.enqueued_at_unix_ms(), 123);
        assert_eq!(decoded.attempt(), 1);
        assert_eq!(
            decoded.trace_context().map(JobTraceContext::traceparent),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        );
    }

    #[test]
    fn trace_context_rejects_unsafe_carrier_values() {
        assert!(matches!(
            JobTraceContext::new(" ", None),
            Err(EnvelopeError::InvalidTraceContext)
        ));
        assert!(matches!(
            JobTraceContext::new("traceparent", Some("not-ascii-\u{2603}".to_owned())),
            Err(EnvelopeError::InvalidTraceContext)
        ));
    }

    #[tokio::test]
    async fn typed_client_publishes_the_envelope_bytes() {
        let publisher = CapturingPublisher::default();
        let client = JobClient::new(publisher.clone());
        let envelope = JobEnvelope::with_metadata(JobId::new(), WelcomeEmail { user_id: 7 }, 123)
            .with_trace_context(
                JobTraceContext::new(
                    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                    None,
                )
                .unwrap(),
            );

        client.enqueue(&envelope).await.unwrap();
        let message = publisher.messages.lock().unwrap().pop().unwrap();
        let decoded = JobEnvelope::<WelcomeEmail>::decode(&message).unwrap();
        assert_eq!(decoded.into_payload(), WelcomeEmail { user_id: 7 });
    }

    #[test]
    fn retry_policy_uses_bounded_exponential_backoff_then_dead_letters() {
        let policy = RetryPolicy {
            max_deliveries: 3,
            initial_backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(3),
        };

        assert_eq!(
            policy.after_failure(1),
            DeliveryAction::Retry {
                next_attempt: 2,
                delay: Duration::from_secs(2),
            }
        );
        assert_eq!(
            policy.after_failure(2),
            DeliveryAction::Retry {
                next_attempt: 3,
                delay: Duration::from_secs(3),
            }
        );
        assert_eq!(policy.after_failure(3), DeliveryAction::DeadLetter);
    }

    #[test]
    fn provider_delivery_attempt_replaces_the_stored_attempt() {
        let envelope = JobEnvelope::with_metadata(JobId::new(), WelcomeEmail { user_id: 7 }, 123)
            .with_attempt(3)
            .unwrap();

        assert_eq!(envelope.attempt(), 3);
        assert!(matches!(
            envelope.with_attempt(0),
            Err(super::EnvelopeError::InvalidAttempt)
        ));
    }

    #[tokio::test]
    async fn dispatch_acknowledges_only_after_the_handler_succeeds() {
        let envelope = JobEnvelope::with_metadata(JobId::new(), WelcomeEmail { user_id: 7 }, 123)
            .with_trace_context(
                JobTraceContext::new(
                    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                    None,
                )
                .unwrap(),
            );
        let action = dispatch(
            envelope,
            &|job: WelcomeEmail, context: super::JobContext| async move {
                assert_eq!(job.user_id, 7);
                assert_eq!(context.attempt(), 1);
                assert_eq!(
                    context.trace_context().map(JobTraceContext::traceparent),
                    Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
                );
                Ok::<_, Infallible>(())
            },
        )
        .await
        .unwrap();

        assert_eq!(action, DeliveryAction::Acknowledge);
    }

    #[tokio::test]
    async fn registry_routes_a_typed_envelope_and_uses_the_provider_attempt() {
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let handler_attempts = Arc::clone(&attempts);
        let mut registry = JobRegistry::new();
        registry
            .register::<WelcomeEmail, _>(move |job: WelcomeEmail, context: super::JobContext| {
                let attempts = Arc::clone(&handler_attempts);
                async move {
                    assert_eq!(job.user_id, 7);
                    attempts.lock().unwrap().push(context.attempt());
                    Ok::<_, Infallible>(())
                }
            })
            .unwrap();
        let envelope = JobEnvelope::with_metadata(JobId::new(), WelcomeEmail { user_id: 7 }, 123);

        assert_eq!(
            registry
                .dispatch(&envelope.encode().unwrap(), 3)
                .await
                .unwrap(),
            DeliveryAction::Acknowledge
        );
        assert_eq!(*attempts.lock().unwrap(), vec![3]);
        assert_eq!(
            registry.registered_names().collect::<Vec<_>>(),
            ["email.welcome"]
        );
    }

    #[tokio::test]
    async fn registry_rejects_duplicate_and_unknown_job_types_without_leaking_payload_data() {
        let mut registry = JobRegistry::new();
        registry
            .register::<WelcomeEmail, _>(|_job: WelcomeEmail, _context: super::JobContext| async {
                Ok::<_, Infallible>(())
            })
            .unwrap();
        assert_eq!(
            registry
                .register::<WelcomeEmail, _>(
                    |_job: WelcomeEmail, _context: super::JobContext| async {
                        Ok::<_, Infallible>(())
                    },
                )
                .unwrap_err(),
            JobRegistryRegistrationError::DuplicateJobName
        );

        let unknown = JobEnvelope::with_metadata(JobId::new(), Newsletter { user_id: 8 }, 123);
        assert_eq!(
            registry.dispatch(&unknown.encode().unwrap(), 1).await,
            Err(JobRegistryError::UnknownJob)
        );
        assert_eq!(
            registry.dispatch(b"not-json", 1).await,
            Err(JobRegistryError::InvalidEnvelope)
        );
    }

    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    struct Newsletter {
        user_id: u64,
    }

    impl Job for Newsletter {
        const NAME: &'static str = "email.newsletter";
        const VERSION: u16 = 1;
    }

    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    struct WelcomeEmailV2 {
        user_id: u64,
        locale: String,
    }

    impl Job for WelcomeEmailV2 {
        const NAME: &'static str = "email.welcome";
        const VERSION: u16 = 2;
    }

    #[test]
    fn compatible_decode_explicitly_upcasts_only_an_older_job_payload() {
        let id = JobId::new();
        let legacy = serde_json::json!({
            "id": id,
            "name": "email.welcome",
            "version": 1,
            "payload": { "user_id": 7 },
            "idempotency_key": "welcome:7",
            "enqueued_at_unix_ms": 123,
            "attempt": 2,
        });
        let upcaster =
            |source_version, payload: Value| -> Result<WelcomeEmailV2, std::convert::Infallible> {
                assert_eq!(source_version, 1);
                Ok(WelcomeEmailV2 {
                    user_id: payload["user_id"].as_u64().unwrap(),
                    locale: "ko-KR".to_owned(),
                })
            };

        let decoded = JobEnvelope::<WelcomeEmailV2>::decode_compatible(
            &serde_json::to_vec(&legacy).unwrap(),
            &upcaster,
        )
        .unwrap();
        assert_eq!(decoded.id(), id);
        assert_eq!(decoded.version(), 2);
        assert_eq!(decoded.attempt(), 2);
        assert_eq!(decoded.idempotency_key(), Some("welcome:7"));
        assert_eq!(
            decoded.into_payload(),
            WelcomeEmailV2 {
                user_id: 7,
                locale: "ko-KR".to_owned(),
            }
        );
    }

    #[test]
    fn compatible_decode_rejects_a_newer_job_before_upcasting() {
        let newer = serde_json::json!({
            "id": JobId::new(),
            "name": "email.welcome",
            "version": 3,
            "payload": { "user_id": 7, "locale": "ko-KR" },
            "idempotency_key": null,
            "enqueued_at_unix_ms": 123,
            "attempt": 1,
        });
        let upcaster = |_source_version,
                        _payload: Value|
         -> Result<WelcomeEmailV2, std::convert::Infallible> {
            unreachable!("newer job versions must not reach the upcaster")
        };

        assert!(matches!(
            JobEnvelope::<WelcomeEmailV2>::decode_compatible(
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

    #[tokio::test]
    async fn registry_can_dispatch_an_explicitly_upcast_older_job_payload() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let handler_received = Arc::clone(&received);
        let mut registry = JobRegistry::new();
        registry
            .register_with_upcaster::<WelcomeEmailV2, _, _>(
                move |job: WelcomeEmailV2, context: super::JobContext| {
                    let received = Arc::clone(&handler_received);
                    async move {
                        received
                            .lock()
                            .unwrap()
                            .push((job.user_id, job.locale, context.attempt()));
                        Ok::<_, Infallible>(())
                    }
                },
                |source_version, payload: Value| -> Result<WelcomeEmailV2, Infallible> {
                    assert_eq!(source_version, 1);
                    Ok(WelcomeEmailV2 {
                        user_id: payload["user_id"].as_u64().unwrap(),
                        locale: "ko-KR".to_owned(),
                    })
                },
            )
            .unwrap();
        let legacy = serde_json::json!({
            "id": JobId::new(),
            "name": "email.welcome",
            "version": 1,
            "payload": { "user_id": 7 },
            "idempotency_key": null,
            "enqueued_at_unix_ms": 123,
            "attempt": 1,
        });

        assert_eq!(
            registry
                .dispatch(&serde_json::to_vec(&legacy).unwrap(), 3)
                .await
                .unwrap(),
            DeliveryAction::Acknowledge
        );
        assert_eq!(*received.lock().unwrap(), vec![(7, "ko-KR".to_owned(), 3)]);
    }
}
