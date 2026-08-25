//! Deterministic AI provider fakes and content-free invocation records for application tests.
//!
//! [`RecordedAiProvider`] returns only responses and stream events explicitly queued by a test. It
//! records sanitized invocation metadata, never prompt, completion, tool arguments, or tool result
//! content. Real provider protocol behavior remains the responsibility of provider adapter tests.

mod model;
mod provider;

pub use model::{RecordedAiError, RecordedAiOperation, RecordedAiRequest};
pub use provider::RecordedAiProvider;

#[cfg(test)]
mod tests;
