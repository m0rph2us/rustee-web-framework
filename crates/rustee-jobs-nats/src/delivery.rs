use std::{fmt, time::Duration};

use async_nats::{
    HeaderMap,
    jetstream::{self, AckKind, message::PublishMessage},
};
use bytes::Bytes;
use rustee_jobs::{
    DeliveryAction, Job, JobDeliveryOutcome, JobEnvelope, JobHandler, JobId, JobRegistry,
    JobRegistryError, RetryPolicy, dispatch,
};

use crate::NatsError;

/// An owned `NATS` delivery whose acknowledgement stays explicit at the worker boundary.
pub struct JetStreamDelivery {
    message: jetstream::Message,
}

impl fmt::Debug for JetStreamDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        JetStreamDeliveryDebug {
            payload: &self.message.payload,
            has_headers: self.message.headers.is_some(),
        }
        .fmt(formatter)
    }
}

struct JetStreamDeliveryDebug<'a> {
    payload: &'a [u8],
    has_headers: bool,
}

impl fmt::Debug for JetStreamDeliveryDebug<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JetStreamDelivery")
            .field("payload_byte_len", &self.payload.len())
            .field("has_headers", &self.has_headers)
            .finish()
    }
}

impl JetStreamDelivery {
    /// Wraps one pull-consumer message after a provider has selected it for processing.
    #[must_use]
    pub fn new(message: jetstream::Message) -> Self {
        Self { message }
    }

    /// Returns the serialized `Rustee` job envelope without exposing acknowledgement internals.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.message.payload
    }

    /// Returns the one-based delivery attempt reported by `JetStream` metadata.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::DeliveryMetadata`] when the message does not carry usable `JetStream`
    /// acknowledgement metadata.
    pub fn delivery_attempt(&self) -> Result<u16, NatsError> {
        let delivered = self
            .message
            .info()
            .map_err(|_| NatsError::DeliveryMetadata)?
            .delivered;
        u16::try_from(delivered).map_err(|_| NatsError::DeliveryMetadata)
    }

    /// Acknowledges a successfully completed handler execution.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::Acknowledge`] when `NATS` cannot accept the acknowledgement.
    pub async fn acknowledge(&self) -> Result<(), NatsError> {
        self.message.ack().await.map_err(|_| NatsError::Acknowledge)
    }

    /// Requests delayed redelivery after a retryable handler failure.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::NegativeAcknowledge`] when `NATS` cannot accept the negative acknowledgement.
    pub async fn retry_after(&self, delay: Duration) -> Result<(), NatsError> {
        self.message
            .ack_with(AckKind::Nak(Some(delay)))
            .await
            .map_err(|_| NatsError::NegativeAcknowledge)
    }

    /// Returns `NATS` headers for provider-level correlation only; job payload data is not logged here.
    #[must_use]
    pub fn headers(&self) -> Option<&HeaderMap> {
        self.message.headers.as_ref()
    }
}

pub(crate) async fn process_delivery<J, H>(
    context: jetstream::Context,
    dead_letter_subject: String,
    message: jetstream::Message,
    handler: H,
    retry_policy: RetryPolicy,
) -> Result<(u16, JobDeliveryOutcome), NatsError>
where
    J: Job,
    H: JobHandler<J>,
{
    let delivery = JetStreamDelivery::new(message);
    let attempt = delivery.delivery_attempt()?;
    let envelope = match JobEnvelope::<J>::decode(delivery.payload()) {
        Ok(envelope) => envelope
            .with_attempt(attempt)
            .map_err(|_| NatsError::DeliveryMetadata)?,
        Err(_) => {
            return dead_letter_and_acknowledge(
                &context,
                &dead_letter_subject,
                &delivery,
                None,
                attempt,
            )
            .await
            .map(|()| (attempt, JobDeliveryOutcome::DeadLettered));
        }
    };
    let job_id = envelope.id();

    let action = match dispatch(envelope, &handler).await {
        Ok(action) => action,
        Err(_) => retry_policy.after_failure(attempt),
    };
    settle_action(
        &context,
        &dead_letter_subject,
        &delivery,
        action,
        Some(job_id),
        attempt,
    )
    .await
    .map(|outcome| (attempt, outcome))
}

pub(crate) async fn process_registry_delivery(
    context: jetstream::Context,
    dead_letter_subject: String,
    message: jetstream::Message,
    registry: JobRegistry,
    retry_policy: RetryPolicy,
) -> Result<(u16, JobDeliveryOutcome), NatsError> {
    let delivery = JetStreamDelivery::new(message);
    let attempt = delivery.delivery_attempt()?;
    let (action, job_id) = match registry.dispatch(delivery.payload(), attempt).await {
        Ok(action) => (action, None),
        Err(JobRegistryError::Handler { id, .. }) => {
            (retry_policy.after_failure(attempt), Some(id))
        }
        Err(_) => (DeliveryAction::DeadLetter, None),
    };
    settle_action(
        &context,
        &dead_letter_subject,
        &delivery,
        action,
        job_id,
        attempt,
    )
    .await
    .map(|outcome| (attempt, outcome))
}

async fn settle_action(
    context: &jetstream::Context,
    dead_letter_subject: &str,
    delivery: &JetStreamDelivery,
    action: DeliveryAction,
    job_id: Option<JobId>,
    attempt: u16,
) -> Result<JobDeliveryOutcome, NatsError> {
    match action {
        DeliveryAction::Acknowledge => delivery
            .acknowledge()
            .await
            .map(|()| JobDeliveryOutcome::Acknowledged),
        DeliveryAction::Retry { delay, .. } => delivery
            .retry_after(delay)
            .await
            .map(|()| JobDeliveryOutcome::Retried),
        DeliveryAction::DeadLetter => {
            dead_letter_and_acknowledge(context, dead_letter_subject, delivery, job_id, attempt)
                .await
                .map(|()| JobDeliveryOutcome::DeadLettered)
        }
    }
}

async fn dead_letter_and_acknowledge(
    context: &jetstream::Context,
    dead_letter_subject: &str,
    delivery: &JetStreamDelivery,
    job_id: Option<JobId>,
    attempt: u16,
) -> Result<(), NatsError> {
    let mut publish = PublishMessage::build()
        .payload(Bytes::copy_from_slice(delivery.payload()))
        .header("Rustee-Delivery-Attempt", attempt.to_string());
    if let Some(job_id) = job_id {
        publish = publish.message_id(job_id.to_string());
    }
    context
        .send_publish(dead_letter_subject.to_owned(), publish)
        .await
        .map_err(|_| NatsError::DeadLetterPublish)?
        .await
        .map_err(|_| NatsError::DeadLetterPublishAcknowledgement)?;
    delivery.acknowledge().await
}

#[cfg(test)]
mod tests {
    use super::JetStreamDeliveryDebug;

    #[test]
    fn delivery_debug_output_excludes_payload_and_provider_metadata() {
        let secret = b"private job payload";
        let output = format!(
            "{:?}",
            JetStreamDeliveryDebug {
                payload: secret,
                has_headers: true,
            }
        );

        assert!(output.contains("payload_byte_len: 19"));
        assert!(output.contains("has_headers: true"));
        assert!(!output.contains("private job payload"));
    }
}
