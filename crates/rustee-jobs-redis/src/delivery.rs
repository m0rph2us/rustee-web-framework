use std::{fmt, time::Duration};

use rustee_jobs::{
    DeliveryAction, Job, JobDeliveryOutcome, JobEnvelope, JobHandler, JobRegistry,
    JobRegistryError, RetryPolicy, dispatch,
};

use crate::{RedisStreamsError, worker::RedisStreamsWorker};

/// One Redis pending delivery with settlement kept private to the provider.
#[derive(Clone)]
pub struct RedisStreamsDelivery {
    worker: RedisStreamsWorker,
    entry_id: String,
    payload: Vec<u8>,
    attempt: Option<u16>,
}

impl fmt::Debug for RedisStreamsDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        RedisStreamsDeliveryDebug {
            entry_id: &self.entry_id,
            payload: &self.payload,
            attempt: self.attempt,
        }
        .fmt(formatter)
    }
}

struct RedisStreamsDeliveryDebug<'a> {
    entry_id: &'a str,
    payload: &'a [u8],
    attempt: Option<u16>,
}

impl fmt::Debug for RedisStreamsDeliveryDebug<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisStreamsDelivery")
            .field("entry_id", &redacted(self.entry_id))
            .field("payload_byte_len", &self.payload.len())
            .field("attempt", &self.attempt)
            .finish()
    }
}

fn redacted(_value: &str) -> &'static str {
    "[REDACTED]"
}

impl RedisStreamsDelivery {
    pub(crate) fn new(
        worker: RedisStreamsWorker,
        entry_id: String,
        payload: Vec<u8>,
        attempt: Option<u16>,
    ) -> Self {
        Self {
            worker,
            entry_id,
            payload,
            attempt,
        }
    }

    /// Returns the serialized job envelope bytes. A malformed external stream entry may be empty.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Returns the end-to-end one-based attempt, including reclaimed pending deliveries.
    ///
    /// # Errors
    ///
    /// Returns [`RedisStreamsError::DeliveryMetadata`] when a producer omitted a valid base
    /// attempt or reclaim count could not be represented safely.
    pub fn delivery_attempt(&self) -> Result<u16, RedisStreamsError> {
        self.attempt.ok_or(RedisStreamsError::DeliveryMetadata)
    }

    /// Settles this entry only when this configured consumer remains its current PEL owner.
    ///
    /// # Errors
    ///
    /// Returns [`RedisStreamsError::DeliveryOwnershipLost`] when another consumer reclaimed the
    /// entry, or [`RedisStreamsError::Acknowledge`] when Redis cannot execute the settlement.
    pub async fn acknowledge(&self) -> Result<(), RedisStreamsError> {
        self.worker.acknowledge(&self.entry_id).await
    }

    /// Stores one durable delayed retry before acknowledging this source entry.
    ///
    /// # Errors
    ///
    /// Returns [`RedisStreamsError::DeliveryOwnershipLost`] when another consumer reclaimed the
    /// entry, or [`RedisStreamsError::RetrySchedule`] when Redis cannot atomically store retry
    /// state and acknowledge this source delivery.
    pub async fn retry_after(
        &self,
        next_attempt: u16,
        delay: Duration,
    ) -> Result<(), RedisStreamsError> {
        self.worker
            .schedule_retry(&self.entry_id, &self.payload, next_attempt, delay)
            .await
    }

    /// Publishes this payload to the configured DLQ before acknowledging its source entry.
    ///
    /// # Errors
    ///
    /// Returns [`RedisStreamsError::DeliveryOwnershipLost`] when another consumer reclaimed the
    /// entry, or [`RedisStreamsError::DeadLetter`] when Redis cannot atomically write the DLQ
    /// record and acknowledge this source delivery.
    pub async fn dead_letter(&self) -> Result<(), RedisStreamsError> {
        self.worker
            .dead_letter(&self.entry_id, &self.payload, self.attempt.unwrap_or(1))
            .await
    }
}

pub(crate) async fn process_delivery<J, H>(
    delivery: RedisStreamsDelivery,
    handler: H,
    retry_policy: RetryPolicy,
) -> Result<(u16, JobDeliveryOutcome), RedisStreamsError>
where
    J: Job,
    H: JobHandler<J>,
{
    let Ok(attempt) = delivery.delivery_attempt() else {
        return delivery
            .dead_letter()
            .await
            .map(|()| (1, JobDeliveryOutcome::DeadLettered));
    };
    let envelope = match JobEnvelope::<J>::decode(delivery.payload()) {
        Ok(envelope) => envelope
            .with_attempt(attempt)
            .map_err(|_| RedisStreamsError::DeliveryMetadata)?,
        Err(_) => {
            return delivery
                .dead_letter()
                .await
                .map(|()| (attempt, JobDeliveryOutcome::DeadLettered));
        }
    };

    let action = match dispatch(envelope, &handler).await {
        Ok(action) => action,
        Err(_) => retry_policy.after_failure(attempt),
    };
    settle_action(delivery, action)
        .await
        .map(|outcome| (attempt, outcome))
}

pub(crate) async fn process_registry_delivery(
    delivery: RedisStreamsDelivery,
    registry: JobRegistry,
    retry_policy: RetryPolicy,
) -> Result<(u16, JobDeliveryOutcome), RedisStreamsError> {
    let Ok(attempt) = delivery.delivery_attempt() else {
        return delivery
            .dead_letter()
            .await
            .map(|()| (1, JobDeliveryOutcome::DeadLettered));
    };
    let action = match registry.dispatch(delivery.payload(), attempt).await {
        Ok(action) => action,
        Err(JobRegistryError::Handler { .. }) => retry_policy.after_failure(attempt),
        Err(_) => DeliveryAction::DeadLetter,
    };
    settle_action(delivery, action)
        .await
        .map(|outcome| (attempt, outcome))
}

async fn settle_action(
    delivery: RedisStreamsDelivery,
    action: DeliveryAction,
) -> Result<JobDeliveryOutcome, RedisStreamsError> {
    match action {
        DeliveryAction::Acknowledge => delivery
            .acknowledge()
            .await
            .map(|()| JobDeliveryOutcome::Acknowledged),
        DeliveryAction::Retry {
            next_attempt,
            delay,
        } => delivery
            .retry_after(next_attempt, delay)
            .await
            .map(|()| JobDeliveryOutcome::Retried),
        DeliveryAction::DeadLetter => delivery
            .dead_letter()
            .await
            .map(|()| JobDeliveryOutcome::DeadLettered),
    }
}

#[cfg(test)]
mod tests {
    use super::RedisStreamsDeliveryDebug;

    #[test]
    fn delivery_debug_format_redacts_provider_metadata_and_payload() {
        let entry_id = "1741012345678-42";
        let payload = b"private job payload";
        let output = format!(
            "{:?}",
            RedisStreamsDeliveryDebug {
                entry_id,
                payload,
                attempt: Some(2),
            }
        );

        assert!(!output.contains(entry_id));
        assert!(!output.contains("private job payload"));
        assert!(output.contains("payload_byte_len"));
        assert!(output.contains("attempt: Some(2)"));
    }
}
