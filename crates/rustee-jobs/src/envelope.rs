//! Durable job envelope, trace-carrier, and compatibility-upcast model.

use std::{
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use uuid::Uuid;

use super::JobMessage;

mod codec;
mod compatibility;
mod trace;

pub use compatibility::{CompatibleDecodeError, JobUpcaster};
pub use trace::JobTraceContext;

use trace::SerializedJobTraceContext;

/// Maximum serialized size accepted for one durable job envelope.
///
/// Provider adapters and direct typed workers share this limit so malformed broker deliveries do
/// not reach JSON decoding after exceeding the framework's supported in-memory envelope size.
pub const MAX_JOB_ENVELOPE_BYTES: usize = 1024 * 1024;

/// Maximum byte length of a stable job name used for provider routing and durable storage.
pub const MAX_JOB_NAME_BYTES: usize = 255;
/// Maximum UTF-8 byte length accepted for one application-defined idempotency key.
///
/// Idempotency metadata remains separate from the serialized job payload so it cannot consume the
/// full durable-message budget.
pub const MAX_JOB_IDEMPOTENCY_KEY_BYTES: usize = 255;

/// A serializable, versioned job payload.
pub trait Job: DeserializeOwned + Serialize + Send + Sync + 'static {
    /// Stable job type name used for provider routing and backward-compatible decoding.
    ///
    /// Names must be non-empty, no more than [`MAX_JOB_NAME_BYTES`] bytes, and contain neither
    /// whitespace nor NUL bytes. The contract is enforced before durable encoding, provider
    /// message reconstruction, and handler registration.
    const NAME: &'static str;
    /// Payload schema version understood by this handler.
    const VERSION: u16;
}

/// A durable, globally unique job identifier.
#[derive(Clone, Copy, Deserialize, Eq, Hash, PartialEq, Serialize)]
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

impl fmt::Debug for JobId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JobId([REDACTED])")
    }
}

/// A JSON-encoded durable job payload plus transport-independent delivery metadata.
#[derive(Clone, Eq, PartialEq, Serialize)]
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

#[derive(Deserialize)]
struct SerializedJobEnvelope<J> {
    id: JobId,
    name: String,
    version: u16,
    payload: J,
    idempotency_key: Option<String>,
    enqueued_at_unix_ms: u64,
    attempt: u16,
    trace_context: Option<SerializedJobTraceContext>,
}

impl<'de, J> Deserialize<'de> for JobEnvelope<J>
where
    J: Job,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serialized = SerializedJobEnvelope::deserialize(deserializer)?;
        Self::from_serialized(serialized).map_err(serde::de::Error::custom)
    }
}

impl<J> JobEnvelope<J>
where
    J: Job,
{
    fn from_serialized(serialized: SerializedJobEnvelope<J>) -> Result<Self, EnvelopeError> {
        let envelope = Self {
            id: serialized.id,
            name: serialized.name,
            version: serialized.version,
            payload: serialized.payload,
            idempotency_key: serialized.idempotency_key,
            enqueued_at_unix_ms: serialized.enqueued_at_unix_ms,
            attempt: serialized.attempt,
            trace_context: serialized
                .trace_context
                .map(JobTraceContext::from_serialized)
                .transpose()?,
        };
        envelope.validate()?;
        Ok(envelope)
    }
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
    /// Returns [`EnvelopeError::BlankIdempotencyKey`] when `key` is blank or
    /// [`EnvelopeError::IdempotencyKeyTooLarge`] when it exceeds the metadata identifier bound.
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Result<Self, EnvelopeError> {
        let key = key.into();
        validate_optional_bounded_identifier(
            Some(&key),
            EnvelopeError::BlankIdempotencyKey,
            EnvelopeError::IdempotencyKeyTooLarge,
        )?;
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
    /// Returns an error when the payload cannot be serialized as JSON or the encoded envelope
    /// exceeds [`MAX_JOB_ENVELOPE_BYTES`]. It also rejects an invalid static [`Job::NAME`].
    pub fn encode(&self) -> Result<Vec<u8>, EnvelopeError> {
        self.validate()?;
        codec::encode(self)
    }

    /// Builds the provider message while retaining non-sensitive delivery metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the payload cannot be serialized as JSON.
    pub fn message(&self) -> Result<JobMessage, EnvelopeError> {
        Ok(JobMessage::from_envelope(
            self.id(),
            self.name().to_owned(),
            self.version(),
            self.attempt(),
            self.encode()?,
        ))
    }

    /// Decodes and validates an envelope for the expected job type and version.
    ///
    /// # Errors
    ///
    /// Returns an error when the body exceeds [`MAX_JOB_ENVELOPE_BYTES`], JSON is malformed, the
    /// job type/version does not match, or the attempt counter is invalid.
    pub fn decode(bytes: &[u8]) -> Result<Self, EnvelopeError> {
        codec::validate_envelope_bytes(bytes)?;
        let serialized = serde_json::from_slice::<SerializedJobEnvelope<J>>(bytes)
            .map_err(EnvelopeError::Deserialize)?;
        Self::from_serialized(serialized)
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
    /// Providers use this before dispatch so [`crate::JobContext`] reflects redeliveries even when
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

impl<J> fmt::Debug for JobEnvelope<J> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobEnvelope")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("version", &self.version)
            .field("payload", &"[APPLICATION-OWNED]")
            .field("has_idempotency_key", &self.idempotency_key.is_some())
            .field("enqueued_at_unix_ms", &self.enqueued_at_unix_ms)
            .field("attempt", &self.attempt)
            .field("has_trace_context", &self.trace_context.is_some())
            .finish()
    }
}

pub(super) fn validate_metadata<J>(
    name: &str,
    idempotency_key: Option<&str>,
    attempt: u16,
    trace_context: Option<&JobTraceContext>,
) -> Result<(), EnvelopeError>
where
    J: Job,
{
    if !is_valid_job_name(J::NAME) || !is_valid_job_name(name) {
        return Err(EnvelopeError::InvalidJobName);
    }
    if name != J::NAME {
        return Err(EnvelopeError::UnexpectedJobName {
            expected: J::NAME,
            actual: name.to_owned(),
        });
    }
    if attempt == 0 {
        return Err(EnvelopeError::InvalidAttempt);
    }
    validate_optional_bounded_identifier(
        idempotency_key,
        EnvelopeError::BlankIdempotencyKey,
        EnvelopeError::IdempotencyKeyTooLarge,
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
        if value.len() > MAX_JOB_IDEMPOTENCY_KEY_BYTES {
            return Err(too_large_error);
        }
    }
    Ok(())
}

/// Returns whether a stable job name satisfies the shared provider and durable-storage contract.
///
/// Use this when an adapter restores a job declaration from durable storage before it creates a
/// [`JobMessage`] or asks a handler registry to dispatch the job.
#[must_use]
pub fn is_valid_job_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_JOB_NAME_BYTES
        && !name.contains('\0')
        && !name.chars().any(char::is_whitespace)
}

/// Failed durable job serialization or envelope validation.
#[derive(thiserror::Error)]
pub enum EnvelopeError {
    /// JSON encoding failed.
    #[error("job envelope serialization failed")]
    Serialize(#[source] serde_json::Error),
    /// JSON decoding failed.
    #[error("job envelope deserialization failed")]
    Deserialize(#[source] serde_json::Error),
    /// The serialized envelope exceeded the framework's bounded durable-message size.
    #[error("job envelope exceeded the framework byte limit")]
    TooLarge,
    /// The stable job name was unsafe or outside the shared provider and storage contract.
    #[error("job envelope name was invalid")]
    InvalidJobName,
    /// The provider message addressed a different job handler.
    #[error("job envelope name did not match the registered handler")]
    UnexpectedJobName {
        /// Expected stable job name.
        expected: &'static str,
        /// Received stable job name.
        actual: String,
    },
    /// The provider message used a version this handler cannot process.
    #[error("job envelope version is not supported by the registered handler")]
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
    /// The idempotency key exceeded the bounded metadata identifier contract.
    #[error("job idempotency key exceeded the framework metadata byte limit")]
    IdempotencyKeyTooLarge,
    /// The serialized W3C trace carrier was unsafe or outside the bounded format.
    #[error("job trace context is invalid")]
    InvalidTraceContext,
}

impl fmt::Debug for EnvelopeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Serialize(_) => "serialization_failed",
            Self::Deserialize(_) => "deserialization_failed",
            Self::TooLarge => "too_large",
            Self::InvalidJobName => "invalid_job_name",
            Self::UnexpectedJobName { .. } => "unexpected_job_name",
            Self::UnsupportedVersion { .. } => "unsupported_version",
            Self::InvalidAttempt => "invalid_attempt",
            Self::AttemptOverflow => "attempt_overflow",
            Self::BlankIdempotencyKey => "blank_idempotency_key",
            Self::IdempotencyKeyTooLarge => "idempotency_key_too_large",
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
