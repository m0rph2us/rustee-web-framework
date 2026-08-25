use std::{error::Error as StdError, fmt};

use futures_util::future::BoxFuture;

use super::envelope::is_valid_job_name;
use super::{EnvelopeError, Job, JobEnvelope, JobId, MAX_JOB_ENVELOPE_BYTES};

/// Serialized job content plus metadata a provider may use for deduplication and observability.
#[derive(Clone, Eq, PartialEq)]
pub struct JobMessage {
    id: JobId,
    name: String,
    version: u16,
    attempt: u16,
    payload: Vec<u8>,
}

impl JobMessage {
    pub(super) fn from_envelope(
        id: JobId,
        name: String,
        version: u16,
        attempt: u16,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            id,
            name,
            version,
            attempt,
            payload,
        }
    }

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
        if !is_valid_job_name(&name) {
            return Err(JobMessageError::InvalidName);
        }
        if attempt == 0 {
            return Err(JobMessageError::InvalidAttempt);
        }
        if payload.is_empty() {
            return Err(JobMessageError::EmptyPayload);
        }
        if payload.len() > MAX_JOB_ENVELOPE_BYTES {
            return Err(JobMessageError::PayloadTooLarge);
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

impl fmt::Debug for JobMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobMessage")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("version", &self.version)
            .field("attempt", &self.attempt)
            .field("payload_byte_len", &self.payload.len())
            .finish()
    }
}

/// Invalid metadata recovered for a provider job message.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum JobMessageError {
    /// The stored stable job name was blank.
    #[error("job message name must not be blank")]
    BlankName,
    /// The stored stable job name was unsafe or outside the shared provider and storage contract.
    #[error("job message name was invalid")]
    InvalidName,
    /// The stored attempt was zero.
    #[error("job message delivery attempt must be at least one")]
    InvalidAttempt,
    /// The stored serialized envelope body was empty.
    #[error("job message payload must not be empty")]
    EmptyPayload,
    /// The stored serialized envelope body exceeded the framework's durable-message limit.
    #[error("job message payload exceeded the framework byte limit")]
    PayloadTooLarge,
}

/// Provider-facing contract for publishing one serialized durable job message.
pub trait JobPublisher: Clone + Send + Sync + 'static {
    /// Provider-specific publish failure.
    type Error: StdError + Send + Sync + 'static;

    /// Persists a serialized envelope for later delivery.
    fn publish(&self, message: JobMessage) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// Typed durable-job producer built on a provider-specific publisher.
///
/// Debug output retains the publisher type without invoking publisher diagnostics.
#[derive(Clone)]
pub struct JobClient<P> {
    publisher: P,
}

impl<P> fmt::Debug for JobClient<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobClient")
            .field("publisher_type", &std::any::type_name::<P>())
            .finish_non_exhaustive()
    }
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
#[derive(thiserror::Error)]
pub enum EnqueueError<E> {
    /// The envelope could not be serialized.
    #[error("job envelope serialization failed")]
    Envelope(#[source] EnvelopeError),
    /// The provider could not durably publish the message.
    #[error("job provider publish failed")]
    Provider(#[source] E),
}

impl<E> fmt::Debug for EnqueueError<E>
where
    E: StdError + Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Envelope(_) => "envelope_serialization_failed",
            Self::Provider(_) => "provider_publish_failed",
        };
        formatter
            .debug_struct("EnqueueError")
            .field("kind", &kind)
            .finish()
    }
}
