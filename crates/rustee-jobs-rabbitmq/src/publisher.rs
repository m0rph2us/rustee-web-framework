use std::{fmt, time::Duration};

use futures_util::future::BoxFuture;
use lapin::{
    BasicProperties, Channel, Confirmation, ExchangeKind,
    options::{BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions},
    types::FieldTable,
};
use rustee_jobs::{JobMessage, JobPublisher};
use tokio::time::timeout;

use crate::{
    RabbitMqConnection, RabbitMqError, RabbitMqPublisherConfig, worker::bounded_readiness,
};

const CONTENT_TYPE: &str = "application/json";
const PERSISTENT_DELIVERY_MODE: u8 = 2;

/// A publisher-confirming `RabbitMQ` job producer.
#[derive(Clone)]
pub struct RabbitMqPublisher {
    channel: Channel,
    config: RabbitMqPublisherConfig,
}

impl RabbitMqPublisher {
    /// Opens a dedicated publisher-confirm channel for one direct-exchange route.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqError::PublisherChannel`] when `RabbitMQ` cannot create the channel or
    /// enable publisher confirms.
    pub async fn new(
        connection: RabbitMqConnection,
        config: RabbitMqPublisherConfig,
    ) -> Result<Self, RabbitMqError> {
        let channel = open_confirm_channel(&connection).await?;
        Ok(Self { channel, config })
    }

    /// Verifies that the configured direct exchange exists within a caller-supplied deadline.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqError::InvalidReadinessTimeout`] when the deadline is zero,
    /// [`RabbitMqError::ReadinessTimeout`] when it expires, or [`RabbitMqError::Readiness`] when
    /// the exchange cannot be inspected.
    pub async fn readiness(&self, readiness_timeout: Duration) -> Result<(), RabbitMqError> {
        bounded_readiness(readiness_timeout, async {
            self.channel
                .exchange_declare(
                    self.config.exchange().into(),
                    ExchangeKind::Direct,
                    ExchangeDeclareOptions {
                        passive: true,
                        ..ExchangeDeclareOptions::default()
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|_| RabbitMqError::Readiness)
        })
        .await
    }

    /// Returns the configured direct-exchange route.
    #[must_use]
    pub fn config(&self) -> &RabbitMqPublisherConfig {
        &self.config
    }
}

impl fmt::Debug for RabbitMqPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RabbitMqPublisher")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl JobPublisher for RabbitMqPublisher {
    type Error = RabbitMqError;

    fn publish(&self, message: JobMessage) -> BoxFuture<'static, Result<(), Self::Error>> {
        let channel = self.channel.clone();
        let exchange = self.config.exchange().to_owned();
        let routing_key = self.config.routing_key().to_owned();
        let publish_timeout = self.config.publish_timeout();
        let message_id = message.id().to_string();
        let payload = message.into_payload();
        Box::pin(async move {
            publish_confirmed(
                &channel,
                &exchange,
                &routing_key,
                &payload,
                &message_id,
                publish_timeout,
                PublishKind::Job,
            )
            .await
        })
    }
}

pub(crate) async fn open_confirm_channel(
    connection: &RabbitMqConnection,
) -> Result<Channel, RabbitMqError> {
    let channel = connection
        .inner
        .create_channel()
        .await
        .map_err(|_| RabbitMqError::PublisherChannel)?;
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .map_err(|_| RabbitMqError::PublisherChannel)?;
    Ok(channel)
}

pub(crate) async fn publish_confirmed(
    channel: &Channel,
    exchange: &str,
    routing_key: &str,
    payload: &[u8],
    message_id: &str,
    publish_timeout: Duration,
    kind: PublishKind,
) -> Result<(), RabbitMqError> {
    let properties = persistent_properties(message_id);
    let publish = async {
        let confirmation = channel
            .basic_publish(
                exchange.into(),
                routing_key.into(),
                BasicPublishOptions {
                    mandatory: true,
                    ..BasicPublishOptions::default()
                },
                payload,
                properties,
            )
            .await
            .map_err(|_| kind.publish_error())?
            .await
            .map_err(|_| kind.confirm_error())?;
        match confirmation {
            Confirmation::Ack(None) => Ok(()),
            Confirmation::Ack(Some(_)) | Confirmation::Nack(Some(_)) => {
                Err(kind.unroutable_error())
            }
            Confirmation::Nack(None) => Err(kind.nack_error()),
            Confirmation::NotRequested => Err(kind.confirm_error()),
        }
    };
    match timeout(publish_timeout, publish).await {
        Ok(result) => result,
        Err(_) => Err(kind.timeout_error()),
    }
}

pub(crate) fn persistent_properties(message_id: &str) -> BasicProperties {
    BasicProperties::default()
        .with_content_type(CONTENT_TYPE.into())
        .with_delivery_mode(PERSISTENT_DELIVERY_MODE)
        .with_message_id(message_id.into())
}

#[derive(Clone, Copy)]
pub(crate) enum PublishKind {
    Job,
    DeadLetter,
}

impl PublishKind {
    const fn publish_error(self) -> RabbitMqError {
        match self {
            Self::Job => RabbitMqError::Publish,
            Self::DeadLetter => RabbitMqError::DeadLetterPublish,
        }
    }

    const fn confirm_error(self) -> RabbitMqError {
        match self {
            Self::Job => RabbitMqError::PublishConfirmation,
            Self::DeadLetter => RabbitMqError::DeadLetterConfirmation,
        }
    }

    const fn nack_error(self) -> RabbitMqError {
        match self {
            Self::Job => RabbitMqError::PublishNack,
            Self::DeadLetter => RabbitMqError::DeadLetterNack,
        }
    }

    const fn unroutable_error(self) -> RabbitMqError {
        match self {
            Self::Job => RabbitMqError::PublishUnroutable,
            Self::DeadLetter => RabbitMqError::DeadLetterUnroutable,
        }
    }

    const fn timeout_error(self) -> RabbitMqError {
        match self {
            Self::Job => RabbitMqError::PublishTimeout,
            Self::DeadLetter => RabbitMqError::DeadLetterTimeout,
        }
    }
}
