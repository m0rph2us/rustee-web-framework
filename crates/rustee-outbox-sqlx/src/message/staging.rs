//! Event and job envelope conversion for durable outbox staging.

use std::fmt;

use rustee_events::{EnvelopeError as EventEnvelopeError, Event, EventEnvelope, EventMessage};
use rustee_jobs::{EnvelopeError as JobEnvelopeError, Job, JobEnvelope, JobMessage};

use super::{
    OutboxDestination, OutboxId, OutboxKind, OutboxMessage, OutboxMessageError,
    model::OutboxMessageInput,
};

impl OutboxMessage {
    /// Serializes one event envelope for an outbox transaction.
    ///
    /// # Errors
    ///
    /// Returns an event envelope encoding error or [`OutboxMessageError`] when its provider
    /// metadata cannot fit within the durable outbox contract.
    pub fn event<E>(
        destination: OutboxDestination,
        envelope: &EventEnvelope<E>,
    ) -> Result<Self, StageEventError>
    where
        E: Event,
    {
        let message = envelope.message().map_err(StageEventError::Envelope)?;
        Self::from_event_message(destination, message).map_err(StageEventError::Outbox)
    }

    /// Builds an outbox record from an already serialized event message.
    ///
    /// This is useful when an application has intentionally separated envelope construction from
    /// its database transaction. The message must still be staged before that transaction commits.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxMessageError`] when stored metadata exceeds its bounded contract.
    pub fn from_event_message(
        destination: OutboxDestination,
        message: EventMessage,
    ) -> Result<Self, OutboxMessageError> {
        let id = message.id().to_string();
        let message_type = message.event_type().to_owned();
        let schema_version = message.version();
        let ordering_key = message.key().to_owned();
        let payload = message.into_payload();
        Self::new(OutboxMessageInput {
            kind: OutboxKind::Event,
            destination,
            message_id: id,
            message_type,
            schema_version,
            ordering_key,
            delivery_attempt: 1,
            payload,
        })
    }

    /// Serializes one durable job envelope for an outbox transaction.
    ///
    /// The job's stable ID becomes the ordering key because job providers do not share an event
    /// partition-key contract.
    ///
    /// # Errors
    ///
    /// Returns a job envelope encoding error or [`OutboxMessageError`] when metadata cannot fit
    /// within the durable outbox contract.
    pub fn job<J>(
        destination: OutboxDestination,
        envelope: &JobEnvelope<J>,
    ) -> Result<Self, StageJobError>
    where
        J: Job,
    {
        let message = envelope.message().map_err(StageJobError::Envelope)?;
        Self::from_job_message(destination, message).map_err(StageJobError::Outbox)
    }

    /// Builds an outbox record from an already serialized durable job message.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxMessageError`] when stored metadata exceeds its bounded contract.
    pub fn from_job_message(
        destination: OutboxDestination,
        message: JobMessage,
    ) -> Result<Self, OutboxMessageError> {
        let id = message.id().to_string();
        let message_type = message.name().to_owned();
        let schema_version = message.version();
        let delivery_attempt = message.attempt();
        let ordering_key = id.clone();
        let payload = message.into_payload();
        Self::new(OutboxMessageInput {
            kind: OutboxKind::Job,
            destination,
            message_id: id,
            message_type,
            schema_version,
            ordering_key,
            delivery_attempt,
            payload,
        })
    }
}

/// Failed serialization or validation while staging an event.
///
/// Display and debug output retain only a safe failure category. The underlying envelope or
/// metadata error remains available through [`std::error::Error::source`] for trusted handling.
#[derive(thiserror::Error)]
pub enum StageEventError {
    /// The event envelope could not be encoded.
    #[error("event outbox staging failed")]
    Envelope(#[source] EventEnvelopeError),
    /// The serialized event metadata could not be stored safely.
    #[error("event outbox staging failed")]
    Outbox(#[source] OutboxMessageError),
}

impl fmt::Debug for StageEventError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Envelope(_) => "envelope_invalid",
            Self::Outbox(_) => "outbox_metadata_invalid",
        };
        formatter
            .debug_struct("StageEventError")
            .field("kind", &kind)
            .finish()
    }
}

/// Failed serialization or validation while staging a durable job.
///
/// Display and debug output retain only a safe failure category. The underlying envelope or
/// metadata error remains available through [`std::error::Error::source`] for trusted handling.
#[derive(thiserror::Error)]
pub enum StageJobError {
    /// The job envelope could not be encoded.
    #[error("job outbox staging failed")]
    Envelope(#[source] JobEnvelopeError),
    /// The serialized job metadata could not be stored safely.
    #[error("job outbox staging failed")]
    Outbox(#[source] OutboxMessageError),
}

impl fmt::Debug for StageJobError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Envelope(_) => "envelope_invalid",
            Self::Outbox(_) => "outbox_metadata_invalid",
        };
        formatter
            .debug_struct("StageJobError")
            .field("kind", &kind)
            .finish()
    }
}

/// Outcome of an insert that is deduplicated by kind, destination, and source message ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StageOutcome {
    /// The event or job was inserted into the still-open transaction.
    Inserted(OutboxId),
    /// An earlier transaction already staged the same source message for this destination.
    AlreadyPresent,
}
