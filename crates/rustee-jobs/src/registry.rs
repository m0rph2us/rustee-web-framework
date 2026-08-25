//! Startup-owned typed job handler registration and sanitized provider dispatch.

use std::{collections::BTreeMap, fmt, sync::Arc};

use futures_util::future::BoxFuture;
use serde::Deserialize;

use super::envelope::is_valid_job_name;
use super::{
    DeliveryAction, Job, JobEnvelope, JobHandler, JobId, JobUpcaster, MAX_JOB_ENVELOPE_BYTES,
    dispatch,
};

/// A fixed set of typed job handlers that can dispatch one provider delivery by its envelope name.
///
/// Build the registry during application startup, then clone it into provider workers. The
/// registry is immutable after startup: registration detects duplicate names so a deployment never
/// depends on handler insertion order. It retains only stable handler metadata and never exposes
/// raw job payloads or handler errors through its error values.
#[derive(Clone, Default)]
pub struct JobRegistry {
    handlers: BTreeMap<String, Arc<dyn RegisteredJobHandler>>,
}

impl JobRegistry {
    /// Creates an empty immutable-after-startup job registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one typed handler under its stable [`Job::NAME`].
    ///
    /// # Errors
    ///
    /// Returns [`JobRegistryRegistrationError::InvalidJobName`] when the static name is outside
    /// the shared provider and storage contract, or
    /// [`JobRegistryRegistrationError::DuplicateJobName`] when the name is already registered.
    pub fn register<J, H>(&mut self, handler: H) -> Result<(), JobRegistryRegistrationError>
    where
        J: Job,
        H: JobHandler<J>,
    {
        self.insert_handler::<J>(Arc::new(StrictRegisteredJobHandler::<J, H> {
            handler,
            marker: std::marker::PhantomData,
        }))
    }

    /// Registers one typed handler with an explicit compatibility upcaster for older payloads.
    ///
    /// Current-version payloads decode normally. Only lower producer versions reach `upcaster`;
    /// newer versions remain rejected. Use [`Self::register`] when the worker must stay strict.
    ///
    /// # Errors
    ///
    /// Returns [`JobRegistryRegistrationError`] when the stable job name is invalid or another
    /// handler already owns it.
    pub fn register_with_upcaster<J, H, U>(
        &mut self,
        handler: H,
        upcaster: U,
    ) -> Result<(), JobRegistryRegistrationError>
    where
        J: Job,
        H: JobHandler<J>,
        U: JobUpcaster<J> + 'static,
    {
        self.insert_handler::<J>(Arc::new(CompatibleRegisteredJobHandler::<J, H, U> {
            handler,
            upcaster,
            marker: std::marker::PhantomData,
        }))
    }

    fn insert_handler<J>(
        &mut self,
        handler: Arc<dyn RegisteredJobHandler>,
    ) -> Result<(), JobRegistryRegistrationError>
    where
        J: Job,
    {
        if !is_valid_job_name(J::NAME) {
            return Err(JobRegistryRegistrationError::InvalidJobName);
        }
        if self.handlers.contains_key(J::NAME) {
            return Err(JobRegistryRegistrationError::DuplicateJobName);
        }
        self.handlers.insert(J::NAME.to_owned(), handler);
        Ok(())
    }

    /// Returns the registered stable job names in deterministic lexical order.
    pub fn registered_names(&self) -> impl Iterator<Item = &str> {
        self.handlers.keys().map(String::as_str)
    }

    /// Decodes and dispatches one provider payload using its registered envelope name.
    ///
    /// `attempt` comes from the provider delivery metadata and replaces the attempt serialized in
    /// the envelope before the typed handler receives its [`super::JobContext`]. Unknown or
    /// malformed deliveries return a sanitized error so providers can dead-letter them without
    /// retrying an arbitrary payload. Typed handler failures return
    /// [`JobRegistryError::Handler`] so providers can apply their configured retry policy.
    #[must_use]
    pub fn dispatch(
        &self,
        payload: &[u8],
        attempt: u16,
    ) -> BoxFuture<'static, Result<DeliveryAction, JobRegistryError>> {
        if attempt == 0 {
            return Box::pin(async { Err(JobRegistryError::InvalidAttempt) });
        }
        if payload.is_empty() || payload.len() > MAX_JOB_ENVELOPE_BYTES {
            return Box::pin(async { Err(JobRegistryError::InvalidEnvelope) });
        }
        let Ok(identity) = serde_json::from_slice::<RegistryEnvelopeIdentity>(payload) else {
            return Box::pin(async { Err(JobRegistryError::InvalidEnvelope) });
        };
        let Some(handler) = self.handlers.get(&identity.name).cloned() else {
            return Box::pin(async { Err(JobRegistryError::UnknownJob) });
        };
        handler.dispatch(payload.to_vec(), attempt)
    }
}

impl fmt::Debug for JobRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobRegistry")
            .field("registered_job_count", &self.handlers.len())
            .finish()
    }
}

#[derive(Deserialize)]
struct RegistryEnvelopeIdentity {
    name: String,
}

trait RegisteredJobHandler: Send + Sync {
    fn dispatch(
        &self,
        payload: Vec<u8>,
        attempt: u16,
    ) -> BoxFuture<'static, Result<DeliveryAction, JobRegistryError>>;
}

struct StrictRegisteredJobHandler<J, H>
where
    J: Job,
    H: JobHandler<J>,
{
    handler: H,
    marker: std::marker::PhantomData<fn() -> J>,
}

impl<J, H> RegisteredJobHandler for StrictRegisteredJobHandler<J, H>
where
    J: Job,
    H: JobHandler<J>,
{
    fn dispatch(
        &self,
        payload: Vec<u8>,
        attempt: u16,
    ) -> BoxFuture<'static, Result<DeliveryAction, JobRegistryError>> {
        let handler = self.handler.clone();
        Box::pin(async move {
            let envelope = JobEnvelope::<J>::decode(&payload)
                .map_err(|_| JobRegistryError::InvalidEnvelope)?
                .with_attempt(attempt)
                .map_err(|_| JobRegistryError::InvalidAttempt)?;
            let id = envelope.id();
            let name = envelope.name().to_owned();
            dispatch(envelope, &handler)
                .await
                .map_err(|_| JobRegistryError::Handler { id, name })
        })
    }
}

struct CompatibleRegisteredJobHandler<J, H, U>
where
    J: Job,
    H: JobHandler<J>,
    U: JobUpcaster<J>,
{
    handler: H,
    upcaster: U,
    marker: std::marker::PhantomData<fn() -> J>,
}

impl<J, H, U> RegisteredJobHandler for CompatibleRegisteredJobHandler<J, H, U>
where
    J: Job,
    H: JobHandler<J>,
    U: JobUpcaster<J> + 'static,
{
    fn dispatch(
        &self,
        payload: Vec<u8>,
        attempt: u16,
    ) -> BoxFuture<'static, Result<DeliveryAction, JobRegistryError>> {
        let handler = self.handler.clone();
        let envelope = JobEnvelope::<J>::decode_compatible(&payload, &self.upcaster)
            .map_err(|_| JobRegistryError::InvalidEnvelope)
            .and_then(|envelope| {
                envelope
                    .with_attempt(attempt)
                    .map_err(|_| JobRegistryError::InvalidAttempt)
            });
        Box::pin(async move {
            let envelope = envelope?;
            let id = envelope.id();
            let name = envelope.name().to_owned();
            dispatch(envelope, &handler)
                .await
                .map_err(|_| JobRegistryError::Handler { id, name })
        })
    }
}

/// Registration failure for one typed [`JobRegistry`] handler.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum JobRegistryRegistrationError {
    /// The static job name was outside the shared provider and storage contract.
    #[error("registered job name was invalid")]
    InvalidJobName,
    /// A startup registry attempted to replace a handler with the same stable name.
    #[error("a handler is already registered for this job name")]
    DuplicateJobName,
}

/// Sanitized outcome of dispatching one registry-selected job delivery.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum JobRegistryError {
    /// The provider payload was empty, oversized, malformed, or did not match its registered type.
    #[error("registered job envelope was invalid")]
    InvalidEnvelope,
    /// The provider did not supply a valid one-based delivery attempt.
    #[error("registered job delivery attempt was invalid")]
    InvalidAttempt,
    /// No handler was registered for the stable job name in the provider envelope.
    #[error("no handler is registered for this job")]
    UnknownJob,
    /// The registered typed handler failed; raw handler text is intentionally not retained.
    #[error("registered job handler failed")]
    Handler {
        /// Stable job ID useful for bounded operational correlation.
        id: JobId,
        /// Registered stable job name.
        name: String,
    },
}
