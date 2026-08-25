//! Safe `OpenAI` Batch lifecycle snapshots and provider-response decoding.

use std::fmt;

use rustee_ai_batch::AiBatchReceipt;
use serde_json::Value;

use crate::OpenAiError;

use super::{OPENAI_BATCH_MAX_REQUESTS, valid_provider_path_identifier};

/// Safe progress snapshot for an `OpenAI` batch.
///
/// Request counts are accepted only when they fit the adapter's documented Batch input limit.
#[derive(Clone, Eq, PartialEq)]
pub struct OpenAiBatchSnapshot {
    receipt: AiBatchReceipt,
    status: OpenAiBatchStatus,
    completed_requests: u64,
    failed_requests: u64,
    total_requests: u64,
    output_file_id: Option<String>,
    error_file_id: Option<String>,
}

impl fmt::Debug for OpenAiBatchSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiBatchSnapshot")
            .field("receipt", &"[REDACTED]")
            .field("status", &self.status)
            .field("completed_requests", &self.completed_requests)
            .field("failed_requests", &self.failed_requests)
            .field("total_requests", &self.total_requests)
            .field("has_output_file", &self.output_file_id.is_some())
            .field("has_error_file", &self.error_file_id.is_some())
            .finish()
    }
}

impl OpenAiBatchSnapshot {
    /// Returns the safe batch receipt.
    #[must_use]
    pub const fn receipt(&self) -> &AiBatchReceipt {
        &self.receipt
    }

    /// Consumes the snapshot for the provider submission receipt.
    pub(super) fn into_receipt(self) -> AiBatchReceipt {
        self.receipt
    }

    /// Returns the normalized provider lifecycle status.
    #[must_use]
    pub const fn status(&self) -> OpenAiBatchStatus {
        self.status
    }

    /// Returns completed requests reported by the provider.
    #[must_use]
    pub const fn completed_requests(&self) -> u64 {
        self.completed_requests
    }

    /// Returns failed requests reported by the provider.
    #[must_use]
    pub const fn failed_requests(&self) -> u64 {
        self.failed_requests
    }

    /// Returns total requests reported by the provider.
    #[must_use]
    pub const fn total_requests(&self) -> u64 {
        self.total_requests
    }

    /// Returns the optional successful-output file ID; download remains explicit.
    #[must_use]
    pub fn output_file_id(&self) -> Option<&str> {
        self.output_file_id.as_deref()
    }

    /// Returns the optional error-output file ID; download remains explicit.
    #[must_use]
    pub fn error_file_id(&self) -> Option<&str> {
        self.error_file_id.as_deref()
    }
}

/// `OpenAI` batch lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenAiBatchStatus {
    /// The provider is validating the uploaded input file.
    Validating,
    /// The provider is running batch requests.
    InProgress,
    /// The provider is finalizing output files.
    Finalizing,
    /// The batch completed and may have output/error files.
    Completed,
    /// The batch failed before normal completion.
    Failed,
    /// The provider completion window elapsed.
    Expired,
    /// The provider is processing a cancellation request.
    Cancelling,
    /// The batch was cancelled and may have partial output.
    Cancelled,
}

pub(crate) fn decode_batch(value: &Value) -> Result<OpenAiBatchSnapshot, OpenAiError> {
    let receipt = AiBatchReceipt::new(
        value
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| valid_provider_path_identifier(id))
            .ok_or(OpenAiError::MalformedBatch)?,
    )
    .map_err(|_| OpenAiError::MalformedBatch)?;
    let status = match value
        .get("status")
        .and_then(Value::as_str)
        .ok_or(OpenAiError::MalformedBatch)?
    {
        "validating" => OpenAiBatchStatus::Validating,
        "in_progress" => OpenAiBatchStatus::InProgress,
        "finalizing" => OpenAiBatchStatus::Finalizing,
        "completed" => OpenAiBatchStatus::Completed,
        "failed" => OpenAiBatchStatus::Failed,
        "expired" => OpenAiBatchStatus::Expired,
        "cancelling" => OpenAiBatchStatus::Cancelling,
        "cancelled" => OpenAiBatchStatus::Cancelled,
        _ => return Err(OpenAiError::MalformedBatch),
    };
    let request_counts = value
        .get("request_counts")
        .and_then(Value::as_object)
        .ok_or(OpenAiError::MalformedBatch)?;
    let count = |name| {
        request_counts
            .get(name)
            .and_then(Value::as_u64)
            .ok_or(OpenAiError::MalformedBatch)
    };
    let completed_requests = count("completed")?;
    let failed_requests = count("failed")?;
    let total_requests = count("total")?;
    if total_requests > OPENAI_BATCH_MAX_REQUESTS as u64
        || completed_requests.saturating_add(failed_requests) > total_requests
    {
        return Err(OpenAiError::MalformedBatch);
    }
    Ok(OpenAiBatchSnapshot {
        receipt,
        status,
        completed_requests,
        failed_requests,
        total_requests,
        output_file_id: optional_provider_identifier(value.get("output_file_id"))?,
        error_file_id: optional_provider_identifier(value.get("error_file_id"))?,
    })
}

fn optional_provider_identifier(value: Option<&Value>) -> Result<Option<String>, OpenAiError> {
    match value {
        Some(Value::Null) | None => Ok(None),
        Some(Value::String(value)) if valid_provider_path_identifier(value) => {
            Ok(Some(value.clone()))
        }
        _ => Err(OpenAiError::MalformedBatch),
    }
}
