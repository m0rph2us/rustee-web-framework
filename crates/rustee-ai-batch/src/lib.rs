//! Explicit control-plane contracts for provider asynchronous AI batches.
//!
//! A durable delivery contains an [`AiBatchReference`] only. The application catalog resolves raw
//! prompts, expected answers, documents, and provider-specific work after an authorized worker
//! starts. The submission ledger preserves an ambiguous provider submission as pending, so this
//! crate never retries or resubmits a batch automatically.

mod artifact;
mod in_memory;
mod submission;

pub use artifact::{
    AiBatchArtifactKind, AiBatchArtifactLedger, AiBatchArtifactLoader, AiBatchArtifactProcessor,
    AiBatchArtifactReconciler, AiBatchArtifactReconciliationDisposition,
    AiBatchArtifactReconciliationError, AiBatchArtifactReference, AiBatchArtifactReservation,
};
pub use in_memory::{
    InMemoryAiBatchArtifactLedger, InMemoryAiBatchArtifactLedgerError, InMemoryAiBatchLedger,
    InMemoryAiBatchLedgerError,
};
pub use submission::{
    AiBatchCatalog, AiBatchConfigError, AiBatchProvider, AiBatchReceipt, AiBatchReference,
    AiBatchReservation, AiBatchSubmission, AiBatchSubmissionDisposition, AiBatchSubmissionError,
    AiBatchSubmissionLedger, AiBatchSubmitter, MAX_BATCH_IDENTIFIER_BYTES,
};

pub(crate) use submission::valid_identifier;

#[cfg(test)]
mod tests;
