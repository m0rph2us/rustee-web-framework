//! Error types for provider-neutral AI protocol values.

use std::{error::Error as StdError, fmt};

/// Invalid request construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RequestError {
    /// Alias was blank.
    #[error("AI model alias must not be blank")]
    BlankModel,
    /// Alias was longer than durable usage metadata supports.
    #[error("AI model alias exceeded the supported length")]
    ModelAliasTooLong,
    /// Alias contained a NUL byte.
    #[error("AI model alias must not contain a NUL byte")]
    ModelAliasContainsNul,
    /// No messages were supplied.
    #[error("AI request must contain at least one message")]
    EmptyMessages,
    /// A message was blank.
    #[error("AI message content must not be blank")]
    BlankMessage,
}

/// Invalid provider response metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ResponseError {
    /// Response ID was blank.
    #[error("AI provider response ID must not be blank")]
    BlankId,
    /// Model was blank.
    #[error("AI provider model must not be blank")]
    BlankModel,
    /// Model alias exceeded the shared durable metadata limit.
    #[error("AI provider model alias exceeded the supported length")]
    ModelAliasTooLong,
    /// Model alias contained a NUL byte.
    #[error("AI provider model alias must not contain a NUL byte")]
    ModelAliasContainsNul,
}

/// Structured JSON parsing failure.
///
/// Its display and debug forms deliberately omit model content. The parser source remains
/// available through [`std::error::Error::source`] for trusted diagnostics.
pub struct StructuredOutputError {
    source: serde_json::Error,
}

impl StructuredOutputError {
    pub(crate) fn deserialize(source: serde_json::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for StructuredOutputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AI structured output was invalid JSON")
    }
}

impl fmt::Debug for StructuredOutputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StructuredOutputError::Deserialize")
    }
}

impl StdError for StructuredOutputError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.source)
    }
}
