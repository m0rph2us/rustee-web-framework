//! Typed durable job envelopes and provider-neutral delivery rules.
//!
//! Providers own broker-specific publish, receive, acknowledgement, delay, and dead-letter
//! behavior. This crate defines only the metadata every durable job must preserve.

mod delivery;
mod envelope;
mod handler;
mod publisher;
mod registry;
mod worker_config;

pub use delivery::{
    DeliveryAction, JobDeliveryFinished, JobDeliveryObservation, JobDeliveryObserver,
    JobDeliveryOutcome, JobDeliveryStarted, NoopJobDeliveryObserver, RetryPolicy,
};
pub use envelope::{
    CompatibleDecodeError, EnvelopeError, Job, JobEnvelope, JobId, JobTraceContext, JobUpcaster,
    MAX_JOB_ENVELOPE_BYTES, MAX_JOB_IDEMPOTENCY_KEY_BYTES, MAX_JOB_NAME_BYTES, is_valid_job_name,
};
pub use handler::{JobContext, JobHandler, dispatch};
pub use publisher::{EnqueueError, JobClient, JobMessage, JobMessageError, JobPublisher};
pub use registry::{JobRegistry, JobRegistryError, JobRegistryRegistrationError};
pub use worker_config::WorkerConfig;
#[cfg(test)]
mod tests;
