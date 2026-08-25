//! Private typed-handler dispatch for durable job envelopes.

use std::{error::Error as StdError, fmt};

use futures_util::future::BoxFuture;

use super::{DeliveryAction, Job, JobEnvelope, JobId, JobTraceContext};

/// Metadata supplied to a handler without exposing provider delivery handles.
#[derive(Clone, Eq, PartialEq)]
pub struct JobContext {
    id: JobId,
    name: String,
    version: u16,
    idempotency_key: Option<String>,
    attempt: u16,
    trace_context: Option<JobTraceContext>,
}

impl JobContext {
    pub(crate) fn from_envelope<J>(envelope: &JobEnvelope<J>) -> Self
    where
        J: Job,
    {
        Self {
            id: envelope.id(),
            name: envelope.name().to_owned(),
            version: envelope.version(),
            idempotency_key: envelope.idempotency_key().map(ToOwned::to_owned),
            attempt: envelope.attempt(),
            trace_context: envelope.trace_context().cloned(),
        }
    }

    /// Returns the durable job ID.
    #[must_use]
    pub const fn id(&self) -> JobId {
        self.id
    }

    /// Returns the job type name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the job schema version.
    #[must_use]
    pub const fn version(&self) -> u16 {
        self.version
    }

    /// Returns the producer's idempotency key when supplied.
    #[must_use]
    pub fn idempotency_key(&self) -> Option<&str> {
        self.idempotency_key.as_deref()
    }

    /// Returns the optional W3C trace-context carrier attached by the producer.
    #[must_use]
    pub fn trace_context(&self) -> Option<&JobTraceContext> {
        self.trace_context.as_ref()
    }

    /// Returns the one-based delivery attempt number.
    #[must_use]
    pub const fn attempt(&self) -> u16 {
        self.attempt
    }
}

impl fmt::Debug for JobContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobContext")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("version", &self.version)
            .field("has_idempotency_key", &self.idempotency_key.is_some())
            .field("attempt", &self.attempt)
            .field("has_trace_context", &self.trace_context.is_some())
            .finish()
    }
}

/// A typed asynchronous job handler.
pub trait JobHandler<J>: Clone + Send + Sync + 'static
where
    J: Job,
{
    /// Handler-specific failure type, kept available for provider logs and metrics.
    type Error: StdError + Send + Sync + 'static;

    /// Processes one deserialized job payload.
    fn handle(
        &self,
        payload: J,
        context: JobContext,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

impl<J, F, Future, E> JobHandler<J> for F
where
    J: Job,
    F: Fn(J, JobContext) -> Future + Clone + Send + Sync + 'static,
    Future: std::future::Future<Output = Result<(), E>> + Send + 'static,
    E: StdError + Send + Sync + 'static,
{
    type Error = E;

    fn handle(
        &self,
        payload: J,
        context: JobContext,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(self(payload, context))
    }
}

/// Runs one typed handler invocation and maps success to an acknowledgement decision.
///
/// A provider must call this before acknowledging a delivery. On error it must use its own error
/// reporting and [`super::RetryPolicy`] to choose retry or dead-letter behavior.
///
/// # Errors
///
/// Returns the handler's error without acknowledging, retrying, or discarding the delivery.
pub async fn dispatch<J, H>(
    envelope: JobEnvelope<J>,
    handler: &H,
) -> Result<DeliveryAction, H::Error>
where
    J: Job,
    H: JobHandler<J>,
{
    let context = JobContext::from_envelope(&envelope);
    handler.handle(envelope.into_payload(), context).await?;
    Ok(DeliveryAction::Acknowledge)
}
