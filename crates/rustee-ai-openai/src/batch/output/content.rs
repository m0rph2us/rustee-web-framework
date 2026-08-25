//! Explicitly downloaded, content-redacted Batch file bytes.

use std::fmt;

use super::super::OPENAI_BATCH_MAX_REQUESTS;
use super::{OpenAiBatchOutputParseError, OpenAiBatchOutputRows};

/// Explicitly downloaded unparsed Batch file content.
///
/// The caller owns row decoding, authorization, partial-result handling, retention, and erase.
pub struct OpenAiBatchFileContent {
    bytes: Vec<u8>,
}

impl OpenAiBatchFileContent {
    /// Returns the downloaded byte length without exposing the content.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns whether the provider file was empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn from_bytes(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    pub(crate) fn from_download(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    /// Consumes the explicitly selected file content for application-owned processing.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Creates a bounded, fail-closed iterator over provider Batch output rows.
    ///
    /// The iterator parses only row structure and returns raw response bodies through an explicit
    /// consuming API. It never invokes a model, cache, evaluator, ledger, or durable job.
    #[must_use]
    pub fn output_rows(&self) -> OpenAiBatchOutputRows<'_> {
        OpenAiBatchOutputRows::new(&self.bytes, OPENAI_BATCH_MAX_REQUESTS)
    }

    /// Creates a Batch output iterator with an application-selected lower row bound.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiBatchOutputParseError::InvalidRowLimit`] when `max_rows` is zero or exceeds
    /// the provider's documented maximum.
    pub fn output_rows_with_limit(
        &self,
        max_rows: usize,
    ) -> Result<OpenAiBatchOutputRows<'_>, OpenAiBatchOutputParseError> {
        if max_rows == 0 || max_rows > OPENAI_BATCH_MAX_REQUESTS {
            return Err(OpenAiBatchOutputParseError::InvalidRowLimit);
        }
        Ok(OpenAiBatchOutputRows::new(&self.bytes, max_rows))
    }
}

impl fmt::Debug for OpenAiBatchFileContent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiBatchFileContent")
            .field("bytes", &"[REDACTED]")
            .field("length", &self.bytes.len())
            .finish()
    }
}
