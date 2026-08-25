//! Batch submission request model.

use std::fmt;

use super::{
    OpenAiBatchEndpoint, OpenAiBatchInputFile, OpenAiBatchOutputExpiration,
    valid_provider_identifier,
};
use crate::OpenAiError;

/// Application-uploaded `OpenAI` JSONL input file selected for one batch submission.
#[derive(Clone, Eq, PartialEq)]
pub struct OpenAiBatchRequest {
    pub(super) input_file_id: String,
    pub(super) endpoint: OpenAiBatchEndpoint,
    pub(super) output_expiration: Option<OpenAiBatchOutputExpiration>,
}

impl fmt::Debug for OpenAiBatchRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiBatchRequest")
            .field("input_file_id", &"[REDACTED]")
            .field("endpoint", &self.endpoint)
            .field("output_expiration", &self.output_expiration)
            .finish()
    }
}

impl OpenAiBatchRequest {
    /// Creates a request from an already uploaded `OpenAI` file with purpose `batch`.
    ///
    /// The catalog/provider-work adapter owns JSONL creation and file upload. This type never
    /// accepts prompt text or request bodies.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiError::MalformedBatch`] when the provider file ID is not a bounded safe
    /// identifier.
    pub fn new(
        input_file_id: impl Into<String>,
        endpoint: OpenAiBatchEndpoint,
    ) -> Result<Self, OpenAiError> {
        let input_file_id = input_file_id.into();
        if !valid_provider_identifier(&input_file_id) {
            return Err(OpenAiError::MalformedBatch);
        }
        Ok(Self {
            input_file_id,
            endpoint,
            output_expiration: None,
        })
    }

    /// Creates a submission request from a file uploaded through [`super::OpenAiBatchProvider`].
    #[must_use]
    pub fn from_uploaded_input(
        input_file: &OpenAiBatchInputFile,
        endpoint: OpenAiBatchEndpoint,
    ) -> Self {
        Self {
            input_file_id: input_file.id().to_owned(),
            endpoint,
            output_expiration: None,
        }
    }

    /// Returns the selected uploaded input file ID.
    #[must_use]
    pub fn input_file_id(&self) -> &str {
        &self.input_file_id
    }

    /// Returns the single endpoint encoded by every JSONL line.
    #[must_use]
    pub const fn endpoint(&self) -> OpenAiBatchEndpoint {
        self.endpoint
    }

    /// Requests an explicit provider expiration for generated output and error files.
    ///
    /// This does not delete the input file or any application-owned result data. The application
    /// still records the retention decision and must reconcile a cancelled batch before deleting
    /// any provider file.
    #[must_use]
    pub const fn with_output_expiration(mut self, expiration: OpenAiBatchOutputExpiration) -> Self {
        self.output_expiration = Some(expiration);
        self
    }

    /// Returns the caller-selected output/error-file expiration policy, if any.
    #[must_use]
    pub const fn output_expiration(&self) -> Option<OpenAiBatchOutputExpiration> {
        self.output_expiration
    }
}
