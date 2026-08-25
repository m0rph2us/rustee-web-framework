use std::fmt;

use async_nats::jetstream::{self, message::PublishMessage};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use rustee_jobs::{JobMessage, JobPublisher};
use tokio::time::timeout;

use crate::{ConfigError, NatsConfig, NatsError, config::validate_subject};

/// Acknowledged `JetStream` publisher for serialized `Rustee` jobs.
///
/// Its `Debug` output keeps the deployment-specific publish subject redacted.
#[derive(Clone)]
pub struct JetStreamPublisher {
    context: jetstream::Context,
    subject: String,
}

impl JetStreamPublisher {
    /// Connects to NATS and creates a `JetStream` context without provisioning infrastructure.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::Connect`] when the NATS server cannot be reached.
    pub async fn connect(config: &NatsConfig) -> Result<Self, NatsError> {
        let client = timeout(config.connect_timeout(), async_nats::connect(config.url()))
            .await
            .map_err(|_| NatsError::Connect)?
            .map_err(|_| NatsError::Connect)?;
        let mut context = jetstream::new(client);
        context.set_timeout(config.request_timeout());
        Ok(Self {
            context,
            subject: config.subject().to_owned(),
        })
    }

    /// Wraps an already-configured `JetStream` context for dependency injection and testing.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidSubject`] when `subject` is not a concrete publish subject.
    pub fn new(
        context: jetstream::Context,
        subject: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        let subject = subject.into();
        validate_subject(&subject)?;
        Ok(Self { context, subject })
    }

    /// Verifies access to the NATS `JetStream` account without creating a stream or consumer.
    ///
    /// # Errors
    ///
    /// Returns [`NatsError::Readiness`] when the account cannot be queried.
    pub async fn readiness(&self) -> Result<(), NatsError> {
        self.context
            .query_account()
            .await
            .map(|_| ())
            .map_err(|_| NatsError::Readiness)
    }
}

impl fmt::Debug for JetStreamPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JetStreamPublisher")
            .field("subject", &"[REDACTED]")
            .field("subject_length", &self.subject.len())
            .finish_non_exhaustive()
    }
}

impl JobPublisher for JetStreamPublisher {
    type Error = NatsError;

    fn publish(&self, message: JobMessage) -> BoxFuture<'static, Result<(), Self::Error>> {
        let context = self.context.clone();
        let subject = self.subject.clone();
        let message_id = message.id().to_string();
        let payload = Bytes::from(message.into_payload());
        Box::pin(async move {
            let publish = PublishMessage::build()
                .payload(payload)
                .message_id(message_id);
            context
                .send_publish(subject, publish)
                .await
                .map_err(|_| NatsError::Publish)?
                .await
                .map_err(|_| NatsError::PublishAcknowledgement)?;
            Ok(())
        })
    }
}
