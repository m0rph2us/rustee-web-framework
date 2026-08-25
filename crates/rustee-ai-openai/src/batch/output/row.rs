//! Typed Batch output-row models and explicit response-body conversion.

use std::fmt;

use rustee_ai::ChatResponse;
use rustee_ai_rag::Embedding;
use serde_json::Value;

use crate::{OpenAiError, provider::decode_embeddings, response::decode_response};

use super::super::valid_provider_identifier;

/// One validated provider Batch output row with an application correlation ID.
pub struct OpenAiBatchOutputRow {
    custom_id: String,
    outcome: OpenAiBatchRowOutcome,
}

impl OpenAiBatchOutputRow {
    /// Returns the bounded application correlation ID chosen in the input JSONL row.
    #[must_use]
    pub fn custom_id(&self) -> &str {
        &self.custom_id
    }

    /// Consumes the row so the caller explicitly selects response or failure handling.
    #[must_use]
    pub fn into_outcome(self) -> OpenAiBatchRowOutcome {
        self.outcome
    }
}

impl fmt::Debug for OpenAiBatchOutputRow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (outcome_kind, response_status) = match &self.outcome {
            OpenAiBatchRowOutcome::Response(response) => ("response", Some(response.status_code)),
            OpenAiBatchRowOutcome::Error(_) => ("error", None),
        };
        formatter
            .debug_struct("OpenAiBatchOutputRow")
            .field("custom_id", &"[REDACTED]")
            .field("outcome_kind", &outcome_kind)
            .field("response_status", &response_status)
            .finish()
    }
}

/// Mutually exclusive outcome for one `OpenAI` Batch output row.
pub enum OpenAiBatchRowOutcome {
    /// The provider returned an HTTP response body for this row.
    Response(OpenAiBatchResponse),
    /// The provider could not produce an HTTP response for this row.
    Error(OpenAiBatchRowError),
}

impl fmt::Debug for OpenAiBatchRowOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Response(response) => formatter.debug_tuple("Response").field(response).finish(),
            Self::Error(error) => formatter.debug_tuple("Error").field(error).finish(),
        }
    }
}

/// Safe response metadata plus an explicitly consumable provider response body.
pub struct OpenAiBatchResponse {
    status_code: u16,
    request_id: String,
    body: OpenAiBatchResponseBody,
}

impl OpenAiBatchResponse {
    /// Returns the provider HTTP status code.
    #[must_use]
    pub const fn status_code(&self) -> u16 {
        self.status_code
    }

    /// Returns the bounded provider request ID for support/reconciliation.
    #[must_use]
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    /// Consumes response metadata and body for application-owned response handling.
    #[must_use]
    pub fn into_body(self) -> OpenAiBatchResponseBody {
        self.body
    }
}

impl fmt::Debug for OpenAiBatchResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiBatchResponse")
            .field("status_code", &self.status_code)
            .field("request_id", &"[REDACTED]")
            .field("body", &"[REDACTED]")
            .finish()
    }
}

/// Unparsed provider response body retained only after an explicit output-row parse.
pub struct OpenAiBatchResponseBody {
    body: Value,
}

impl OpenAiBatchResponseBody {
    #[cfg(test)]
    pub(crate) fn from_json(body: Value) -> Self {
        Self { body }
    }

    /// Consumes and validates one Responses API body as a Rustee chat response.
    ///
    /// This is an explicit model-specific conversion for rows sent to `/v1/responses`; callers
    /// still own tenant authorization, structured/domain validation, partial-result policy, and
    /// every side effect. Bodies from another Batch endpoint fail with [`OpenAiError`].
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiError::MalformedResponse`] when the body is not a completed Responses API
    /// response accepted by Rustee's ordinary Responses adapter.
    pub fn into_chat_response(self) -> Result<ChatResponse, OpenAiError> {
        decode_response(&self.body)
    }

    /// Consumes and validates one Embeddings API body in the input row's declared order.
    ///
    /// This is an explicit model-specific conversion for rows sent to `/v1/embeddings`. The
    /// caller supplies the input count from its authorized catalog so duplicate, missing, or
    /// out-of-range provider indexes fail closed. Tenant authorization, chunk-to-domain mapping,
    /// partial-result policy, and every vector-store side effect remain application-owned.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiError::MalformedEmbeddingResponse`] when the body does not contain exactly
    /// `expected_embeddings` finite vectors with each provider index represented once.
    pub fn into_embeddings(
        self,
        expected_embeddings: usize,
    ) -> Result<Vec<Embedding>, OpenAiError> {
        decode_embeddings(&self.body, expected_embeddings)
    }

    /// Consumes the untrusted provider body for application-selected decoding and validation.
    #[must_use]
    pub fn into_json(self) -> Value {
        self.body
    }
}

impl fmt::Debug for OpenAiBatchResponseBody {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiBatchResponseBody")
            .field("body", &"[REDACTED]")
            .finish()
    }
}

/// Safe provider failure code for one `OpenAI` Batch output row.
pub struct OpenAiBatchRowError {
    code: String,
}

impl OpenAiBatchRowError {
    /// Returns the bounded provider failure code; provider error message text is not retained.
    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }
}

impl fmt::Debug for OpenAiBatchRowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiBatchRowError")
            .field("code", &"[REDACTED]")
            .finish()
    }
}

pub(super) fn decode_batch_output_row(value: &Value) -> Result<OpenAiBatchOutputRow, ()> {
    let custom_id = value
        .get("custom_id")
        .and_then(Value::as_str)
        .filter(|custom_id| valid_provider_identifier(custom_id))
        .ok_or(())?
        .to_owned();
    let response = value.get("response").filter(|response| !response.is_null());
    let error = value.get("error").filter(|error| !error.is_null());
    let outcome = match (response, error) {
        (Some(response), None) => {
            let status_code = response
                .get("status_code")
                .and_then(Value::as_u64)
                .and_then(|status_code| u16::try_from(status_code).ok())
                .filter(|status_code| (100..=599).contains(status_code))
                .ok_or(())?;
            let request_id = response
                .get("request_id")
                .and_then(Value::as_str)
                .filter(|request_id| valid_provider_identifier(request_id))
                .ok_or(())?
                .to_owned();
            let body = response
                .get("body")
                .filter(|body| body.is_object())
                .ok_or(())?
                .clone();
            OpenAiBatchRowOutcome::Response(OpenAiBatchResponse {
                status_code,
                request_id,
                body: OpenAiBatchResponseBody { body },
            })
        }
        (None, Some(error)) => {
            let code = error
                .get("code")
                .and_then(Value::as_str)
                .filter(|code| valid_provider_identifier(code))
                .ok_or(())?
                .to_owned();
            OpenAiBatchRowOutcome::Error(OpenAiBatchRowError { code })
        }
        _ => return Err(()),
    };
    Ok(OpenAiBatchOutputRow { custom_id, outcome })
}
