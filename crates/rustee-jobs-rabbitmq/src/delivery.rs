use std::{fmt, time::Duration};

use lapin::{
    Channel,
    message::Delivery,
    options::{BasicAckOptions, BasicRejectOptions},
    types::AMQPValue,
};
use rustee_jobs::{
    DeliveryAction, Job, JobDeliveryOutcome, JobEnvelope, JobHandler, JobRegistry,
    JobRegistryError, RetryPolicy, dispatch,
};

use crate::{
    RabbitMqError, RabbitMqWorkerConfig,
    publisher::{PublishKind, publish_confirmed},
};

pub(crate) const ACQUIRED_COUNT_HEADER: &str = "x-acquired-count";

/// A received `RabbitMQ` delivery whose acknowledgement remains explicit.
pub struct RabbitMqDelivery {
    message: Delivery,
}

impl fmt::Debug for RabbitMqDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        RabbitMqDeliveryDebug {
            payload: &self.message.data,
            redelivered: self.message.redelivered,
        }
        .fmt(formatter)
    }
}

struct RabbitMqDeliveryDebug<'a> {
    payload: &'a [u8],
    redelivered: bool,
}

impl fmt::Debug for RabbitMqDeliveryDebug<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RabbitMqDelivery")
            .field("payload_byte_len", &self.payload.len())
            .field("redelivered", &self.redelivered)
            .finish()
    }
}

impl RabbitMqDelivery {
    /// Wraps one manual-ack AMQP delivery after worker selection.
    #[must_use]
    pub fn new(message: Delivery) -> Self {
        Self { message }
    }

    /// Returns the serialized Rustee job envelope without acknowledgement internals.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.message.data
    }

    /// Returns `RabbitMQ` 4.3 quorum queue's one-based acquired delivery count, defaulting absent
    /// headers to the first attempt.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqError::DeliveryMetadata`] when the broker header is zero, exceeds the
    /// core attempt bound, or has another AMQP type.
    pub fn delivery_attempt(&self) -> Result<u16, RabbitMqError> {
        let Some(headers) = self.message.properties.headers().as_ref() else {
            return Ok(1);
        };
        let Some(value) = headers.inner().get(ACQUIRED_COUNT_HEADER) else {
            return Ok(1);
        };
        let attempt = match value {
            AMQPValue::ShortShortUInt(value) => u16::from(*value),
            AMQPValue::ShortUInt(value) => *value,
            AMQPValue::LongUInt(value) => {
                u16::try_from(*value).map_err(|_| RabbitMqError::DeliveryMetadata)?
            }
            AMQPValue::LongInt(value) => {
                u16::try_from(*value).map_err(|_| RabbitMqError::DeliveryMetadata)?
            }
            AMQPValue::LongLongInt(value) => {
                u16::try_from(*value).map_err(|_| RabbitMqError::DeliveryMetadata)?
            }
            _ => return Err(RabbitMqError::DeliveryMetadata),
        };
        if attempt == 0 {
            return Err(RabbitMqError::DeliveryMetadata);
        }
        Ok(attempt)
    }

    /// Acknowledges a completed delivery on its original consumer channel.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqError::Acknowledge`] when `RabbitMQ` rejects or cannot receive the ack.
    pub async fn acknowledge(&self) -> Result<(), RabbitMqError> {
        self.message
            .ack(BasicAckOptions::default())
            .await
            .map_err(|_| RabbitMqError::Acknowledge)?
            .then_some(())
            .ok_or(RabbitMqError::Acknowledge)
    }

    /// Returns this message to the source quorum queue so its native delayed-retry policy applies.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqError::RetryReturn`] when `RabbitMQ` rejects or cannot receive the return.
    pub async fn return_for_retry(&self) -> Result<(), RabbitMqError> {
        self.message
            .reject(BasicRejectOptions { requeue: true })
            .await
            .map_err(|_| RabbitMqError::RetryReturn)?
            .then_some(())
            .ok_or(RabbitMqError::RetryReturn)
    }

    fn message_id(&self) -> String {
        self.message
            .properties
            .message_id()
            .as_ref()
            .map_or_else(|| "rustee-job".to_owned(), ToString::to_string)
    }
}

pub(super) async fn process_delivery<J, H>(
    settlement_channel: Channel,
    config: RabbitMqWorkerConfig,
    message: Delivery,
    handler: H,
    retry_policy: RetryPolicy,
) -> Result<(u16, JobDeliveryOutcome), RabbitMqError>
where
    J: Job,
    H: JobHandler<J>,
{
    let delivery = RabbitMqDelivery::new(message);
    let Ok(attempt) = delivery.delivery_attempt() else {
        return dead_letter_and_acknowledge(&settlement_channel, &config, &delivery, 1)
            .await
            .map(|()| (1, JobDeliveryOutcome::DeadLettered));
    };
    let envelope = match JobEnvelope::<J>::decode(delivery.payload()) {
        Ok(envelope) => envelope
            .with_attempt(attempt)
            .map_err(|_| RabbitMqError::DeliveryMetadata)?,
        Err(_) => {
            return dead_letter_and_acknowledge(&settlement_channel, &config, &delivery, attempt)
                .await
                .map(|()| (attempt, JobDeliveryOutcome::DeadLettered));
        }
    };

    let action = match dispatch(envelope, &handler).await {
        Ok(action) => action,
        Err(_) => retry_policy.after_failure(attempt),
    };
    settle_action(&settlement_channel, &config, &delivery, action, attempt)
        .await
        .map(|outcome| (attempt, outcome))
}

pub(super) async fn process_registry_delivery(
    settlement_channel: Channel,
    config: RabbitMqWorkerConfig,
    message: Delivery,
    registry: JobRegistry,
    retry_policy: RetryPolicy,
) -> Result<(u16, JobDeliveryOutcome), RabbitMqError> {
    let delivery = RabbitMqDelivery::new(message);
    let Ok(attempt) = delivery.delivery_attempt() else {
        return dead_letter_and_acknowledge(&settlement_channel, &config, &delivery, 1)
            .await
            .map(|()| (1, JobDeliveryOutcome::DeadLettered));
    };
    let action = match registry.dispatch(delivery.payload(), attempt).await {
        Ok(action) => action,
        Err(JobRegistryError::Handler { .. }) => retry_policy.after_failure(attempt),
        Err(_) => DeliveryAction::DeadLetter,
    };
    settle_action(&settlement_channel, &config, &delivery, action, attempt)
        .await
        .map(|outcome| (attempt, outcome))
}

async fn settle_action(
    settlement_channel: &Channel,
    config: &RabbitMqWorkerConfig,
    delivery: &RabbitMqDelivery,
    action: DeliveryAction,
    attempt: u16,
) -> Result<JobDeliveryOutcome, RabbitMqError> {
    match action {
        DeliveryAction::Acknowledge => delivery
            .acknowledge()
            .await
            .map(|()| JobDeliveryOutcome::Acknowledged),
        DeliveryAction::Retry {
            next_attempt,
            delay,
        } => retry_and_return(config, delivery, next_attempt, delay)
            .await
            .map(|()| JobDeliveryOutcome::Retried),
        DeliveryAction::DeadLetter => {
            dead_letter_and_acknowledge(settlement_channel, config, delivery, attempt)
                .await
                .map(|()| JobDeliveryOutcome::DeadLettered)
        }
    }
}

async fn retry_and_return(
    config: &RabbitMqWorkerConfig,
    delivery: &RabbitMqDelivery,
    next_attempt: u16,
    delay: Duration,
) -> Result<(), RabbitMqError> {
    if next_attempt < 2 {
        return Err(RabbitMqError::RetryPolicyMismatch);
    }
    let expected_delay = config.native_retry().delay_for(next_attempt);
    if delay != expected_delay {
        return Err(RabbitMqError::RetryPolicyMismatch);
    }
    delivery.return_for_retry().await
}

async fn dead_letter_and_acknowledge(
    settlement_channel: &Channel,
    config: &RabbitMqWorkerConfig,
    delivery: &RabbitMqDelivery,
    _attempt: u16,
) -> Result<(), RabbitMqError> {
    publish_confirmed(
        settlement_channel,
        config.dead_letter_exchange(),
        config.dead_letter_routing_key(),
        delivery.payload(),
        &delivery.message_id(),
        config.publish_timeout(),
        PublishKind::DeadLetter,
    )
    .await?;
    delivery.acknowledge().await
}

#[cfg(test)]
mod tests {
    use lapin::message::Delivery;

    use super::RabbitMqDelivery;

    #[test]
    fn delivery_debug_output_excludes_payload_and_provider_metadata() {
        let secret = b"private job payload";
        let delivery = RabbitMqDelivery::new(Delivery::mock(
            42,
            "private.exchange".into(),
            "private.route".into(),
            true,
            secret.to_vec(),
        ));

        let output = format!("{delivery:?}");

        assert!(output.contains("payload_byte_len: 19"));
        assert!(output.contains("redelivered: true"));
        assert!(!output.contains("private job payload"));
        assert!(!output.contains("private.exchange"));
        assert!(!output.contains("private.route"));
        assert!(!output.contains("42"));
    }
}
