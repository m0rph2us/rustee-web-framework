//! Reference-only durable jobs for AI provider batch submission and artifact reconciliation.
//!
//! The job payload serializes an [`rustee_ai_batch::AiBatchReference`] and never raw provider
//! work. Each handler requires an exact durable idempotency-key binding before delegating to its
//! application-owned runner.

mod artifact;
mod submission;

pub use artifact::{
    AiBatchArtifactReconciliationHandler, AiBatchArtifactReconciliationJob,
    AiBatchArtifactReconciliationJobError, AiBatchArtifactReconciliationRunner,
};
pub use submission::{
    AiBatchSubmissionHandler, AiBatchSubmissionJob, AiBatchSubmissionJobError,
    AiBatchSubmissionRunner,
};

#[cfg(test)]
mod tests;
