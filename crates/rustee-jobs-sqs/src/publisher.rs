use std::{fmt, time::Duration};

use aws_sdk_sqs::Client;
use futures_util::future::BoxFuture;
use rustee_jobs::{JobMessage, JobPublisher};
use tokio::time::timeout;

use crate::{
    ConfigError, SqsError, SqsQueueTarget,
    config::{DEFAULT_REQUEST_TIMEOUT, validate_request_timeout},
    readiness::verify_queue_kind,
};

pub(crate) const MAX_MESSAGE_BYTES: usize = 1_048_576;

/// A response-acknowledged SQS job publisher.
#[derive(Clone)]
pub struct SqsPublisher {
    client: Client,
    target: SqsQueueTarget,
    request_timeout: Duration,
}

impl SqsPublisher {
    /// Wraps an explicitly configured AWS SDK client and one deployment-provisioned target queue.
    #[must_use]
    pub fn new(client: Client, target: SqsQueueTarget) -> Self {
        Self {
            client,
            target,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }

    /// Sets the maximum time one SQS readiness or publish request may occupy this adapter.
    ///
    /// The injected AWS SDK client's retry policy remains application-owned, but Rustee returns a
    /// sanitized provider error once this deadline expires.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroRequestTimeout`] when the deadline is zero.
    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Result<Self, ConfigError> {
        validate_request_timeout(request_timeout)?;
        self.request_timeout = request_timeout;
        Ok(self)
    }

    /// Verifies queue access and the configured Standard/FIFO mode without changing queue state.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError::Readiness`] when the queue is unavailable, inaccessible, or has a
    /// different type than this publisher configuration.
    pub async fn readiness(&self) -> Result<(), SqsError> {
        verify_queue_kind(&self.client, &self.target, self.request_timeout)
            .await
            .map_err(|_| SqsError::Readiness)
    }

    /// Returns the deployment-provisioned destination target.
    #[must_use]
    pub fn target(&self) -> &SqsQueueTarget {
        &self.target
    }

    /// Returns the maximum time one SQS readiness or publish request may use.
    #[must_use]
    pub const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }
}

impl fmt::Debug for SqsPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqsPublisher")
            .field("target", &self.target)
            .field("request_timeout", &self.request_timeout)
            .finish_non_exhaustive()
    }
}

impl JobPublisher for SqsPublisher {
    type Error = SqsError;

    fn publish(&self, message: JobMessage) -> BoxFuture<'static, Result<(), Self::Error>> {
        let client = self.client.clone();
        let target = self.target.clone();
        let request_timeout = self.request_timeout;
        let deduplication_id = message.id().to_string();
        let payload = message.into_payload();
        Box::pin(async move {
            let payload = String::from_utf8(payload).map_err(|_| SqsError::InvalidMessageBody)?;
            send_payload(
                &client,
                &target,
                payload,
                &deduplication_id,
                request_timeout,
            )
            .await
            .map_err(|()| SqsError::Publish)
        })
    }
}

pub(crate) async fn send_payload(
    client: &Client,
    target: &SqsQueueTarget,
    payload: String,
    deduplication_id: &str,
    request_timeout: Duration,
) -> Result<(), ()> {
    if payload.is_empty() || payload.len() > MAX_MESSAGE_BYTES {
        return Err(());
    }
    let request = client
        .send_message()
        .queue_url(target.queue_url())
        .message_body(payload);
    let response = match target.kind().message_group_id() {
        Some(message_group_id) => request
            .message_group_id(message_group_id)
            .message_deduplication_id(deduplication_id)
            .send(),
        None => request.send(),
    };
    timeout(request_timeout, response)
        .await
        .map_err(|_| ())?
        .map(|_| ())
        .map_err(|_| ())
}
