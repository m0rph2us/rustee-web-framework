//! Short-lived, content-redacted Batch input-file bytes.

use std::fmt;

use super::{OPENAI_BATCH_FILE_MAX_BYTES, OpenAiBatchInputError};

/// Short-lived application-created JSONL content for one explicit Batch input-file upload.
///
/// This value is neither serializable nor cloneable. It must not be placed in a job payload,
/// cache, trace, or debug record.
pub struct OpenAiBatchInputJsonl {
    bytes: Vec<u8>,
}

impl OpenAiBatchInputJsonl {
    /// Validates bounded non-empty JSONL bytes before an explicit upload request.
    ///
    /// JSON line shape, authorization, and retention are application-owned. The `OpenAI` API
    /// remains the authority for provider-specific batch-row validation.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiBatchInputError`] when `bytes` is empty or exceeds the documented provider
    /// file limit.
    pub fn new(bytes: Vec<u8>) -> Result<Self, OpenAiBatchInputError> {
        if bytes.is_empty() {
            return Err(OpenAiBatchInputError::Empty);
        }
        if bytes.len() > OPENAI_BATCH_FILE_MAX_BYTES {
            return Err(OpenAiBatchInputError::TooLarge);
        }
        Ok(Self { bytes })
    }

    /// Returns the application-provided byte length without exposing its content.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns whether the application attempted to construct an empty input.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub(crate) fn into_upload_bytes(self) -> Vec<u8> {
        self.bytes
    }

    #[cfg(test)]
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl fmt::Debug for OpenAiBatchInputJsonl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiBatchInputJsonl")
            .field("bytes", &"[REDACTED]")
            .field("length", &self.bytes.len())
            .finish()
    }
}
