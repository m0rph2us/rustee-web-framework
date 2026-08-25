//! Compatible job-envelope decoding and explicit payload upcasting.

use std::{error::Error as StdError, fmt};

use serde::Deserialize;
use serde_json::Value;

use super::{
    EnvelopeError, Job, JobEnvelope, JobId, JobTraceContext, SerializedJobTraceContext,
    codec::validate_envelope_bytes, validate_metadata,
};

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
#[derive(thiserror::Error)]
pub enum CompatibleDecodeError<E>
where
    E: StdError + Send + Sync + 'static,
{
    /// Stable envelope metadata was malformed or unsupported.
    #[error("job envelope decoding failed")]
    Envelope(#[source] EnvelopeError),
    /// The current-version payload could not be decoded into the typed job.
    #[error("current job payload deserialization failed")]
    Payload(#[source] serde_json::Error),
    /// Application-defined upcasting of an older payload failed.
    #[error("job payload upcast failed")]
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

impl<J> JobEnvelope<J>
where
    J: Job,
{
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
        validate_envelope_bytes(bytes).map_err(CompatibleDecodeError::Envelope)?;
        let raw = serde_json::from_slice::<RawJobEnvelope>(bytes)
            .map_err(EnvelopeError::Deserialize)
            .map_err(CompatibleDecodeError::Envelope)?;
        let trace_context = raw
            .trace_context
            .map(JobTraceContext::from_serialized)
            .transpose()
            .map_err(CompatibleDecodeError::Envelope)?;
        validate_metadata::<J>(
            &raw.name,
            raw.idempotency_key.as_deref(),
            raw.attempt,
            trace_context.as_ref(),
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
            trace_context,
        })
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
    trace_context: Option<SerializedJobTraceContext>,
}
