//! Explicit Batch file-artifact transport, retention policy, and bounded byte handling.

use std::{fmt, time::Duration};

use futures_util::StreamExt;
use reqwest::{
    Response,
    multipart::{Form, Part},
};
use serde_json::Value;

use super::super::{OpenAiError, response::decode_json_response};
use super::output::OpenAiBatchFileContent;
use super::valid_provider_path_identifier;
use super::{OpenAiBatchInputJsonl, OpenAiBatchProvider};

const OPENAI_BATCH_OUTPUT_EXPIRATION_MIN_SECONDS: u64 = 60 * 60;
const OPENAI_BATCH_OUTPUT_EXPIRATION_MAX_SECONDS: u64 = 30 * 24 * 60 * 60;

/// Safe provider file reference returned after an explicit Batch input upload.
#[derive(Clone, Eq, PartialEq)]
pub struct OpenAiBatchInputFile {
    id: String,
}

impl OpenAiBatchInputFile {
    /// Returns the provider-assigned input-file ID.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }
}

impl fmt::Debug for OpenAiBatchInputFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiBatchInputFile")
            .field("id", &"[REDACTED]")
            .finish()
    }
}

/// Safe acknowledgement of one explicitly deleted provider Batch artifact.
#[derive(Clone, Eq, PartialEq)]
pub struct OpenAiBatchFileDeletion {
    id: String,
}

impl fmt::Debug for OpenAiBatchFileDeletion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiBatchFileDeletion")
            .field("id", &"[REDACTED]")
            .finish()
    }
}

impl OpenAiBatchFileDeletion {
    /// Returns the provider file ID confirmed as deleted.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }
}

/// Caller-selected expiration policy for one `OpenAI` Batch output/error artifact pair.
///
/// The provider anchors expiration at batch creation. This policy never deletes an input file,
/// settles billing, or replaces the application retention decision for copied domain results.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OpenAiBatchOutputExpiration {
    seconds: u64,
}

impl OpenAiBatchOutputExpiration {
    /// Creates an `OpenAI`-supported output/error-file expiration of one hour through 30 days.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiBatchOutputExpirationError::InvalidDuration`] when `duration` is not a
    /// whole number of seconds in the provider-supported range.
    pub fn new(duration: Duration) -> Result<Self, OpenAiBatchOutputExpirationError> {
        let seconds = duration.as_secs();
        if duration.subsec_nanos() != 0
            || !(OPENAI_BATCH_OUTPUT_EXPIRATION_MIN_SECONDS
                ..=OPENAI_BATCH_OUTPUT_EXPIRATION_MAX_SECONDS)
                .contains(&seconds)
        {
            return Err(OpenAiBatchOutputExpirationError::InvalidDuration);
        }
        Ok(Self { seconds })
    }

    /// Returns the approved output/error artifact lifetime as whole seconds.
    #[must_use]
    pub const fn seconds(self) -> u64 {
        self.seconds
    }
}

/// Invalid `OpenAI` Batch output/error artifact expiration configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OpenAiBatchOutputExpirationError {
    /// The selected duration was outside the provider-supported whole-second range.
    #[error(
        "OpenAI Batch output expiration must be a whole duration from one hour through 30 days"
    )]
    InvalidDuration,
}

impl OpenAiBatchProvider {
    /// Uploads one application-created JSONL file with `batch` purpose for later submission.
    ///
    /// This method is explicit and short-lived: it does not create a durable job, batch request,
    /// cache entry, or application result record.
    ///
    /// # Errors
    ///
    /// Returns an [`OpenAiError`] when the configured bound is exceeded, the upload cannot be
    /// sent, or the provider does not return a valid Batch-purpose file ID.
    pub async fn upload_batch_input(
        &self,
        input: OpenAiBatchInputJsonl,
    ) -> Result<OpenAiBatchInputFile, OpenAiError> {
        if input.len() > self.config.max_batch_file_bytes {
            return Err(OpenAiError::BatchFileTooLarge);
        }
        let input = input.into_upload_bytes();
        let endpoint = self
            .config
            .base_url
            .join("files")
            .map_err(|_| OpenAiError::InvalidEndpoint)?;
        let form = Form::new().text("purpose", "batch").part(
            "file",
            Part::bytes(input).file_name("rustee-batch-input.jsonl"),
        );
        let response = self
            .client
            .post(endpoint)
            .timeout(self.config.request_timeout)
            .bearer_auth(&self.config.api_key)
            .multipart(form)
            .send()
            .await
            .map_err(|_| OpenAiError::Transport)?;
        if !response.status().is_success() {
            return Err(OpenAiError::HttpStatus(response.status()));
        }
        let value = decode_json_response(
            response,
            self.config.max_response_bytes,
            OpenAiError::MalformedBatchFile,
        )
        .await?;
        decode_batch_input_file(&value)
    }

    /// Explicitly downloads one Batch input, output, or error file without parsing its rows.
    ///
    /// The caller must select an authorized file ID and process the returned bytes. No batch
    /// status transition, cache insertion, evaluation, or retry is triggered by this method.
    ///
    /// # Errors
    ///
    /// Returns an [`OpenAiError`] when `file_id` is unsafe, the configured bound is exceeded, or
    /// the download cannot be completed.
    pub async fn download_batch_file(
        &self,
        file_id: &str,
    ) -> Result<OpenAiBatchFileContent, OpenAiError> {
        if !valid_provider_path_identifier(file_id) {
            return Err(OpenAiError::MalformedBatchFile);
        }
        let endpoint = self
            .config
            .base_url
            .join(&format!("files/{file_id}/content"))
            .map_err(|_| OpenAiError::InvalidEndpoint)?;
        let response = self
            .client
            .get(endpoint)
            .timeout(self.config.request_timeout)
            .bearer_auth(&self.config.api_key)
            .send()
            .await
            .map_err(|_| OpenAiError::Transport)?;
        if !response.status().is_success() {
            return Err(OpenAiError::HttpStatus(response.status()));
        }
        Ok(OpenAiBatchFileContent::from_download(
            read_bounded_batch_file(response, self.config.max_batch_file_bytes).await?,
        ))
    }

    /// Explicitly deletes one authorized Batch input, output, or error file.
    ///
    /// No retention schedule, retry, batch cancellation, or local ledger mutation is implied.
    ///
    /// # Errors
    ///
    /// Returns an [`OpenAiError`] when `file_id` is unsafe, the request fails, or the provider
    /// does not confirm deletion of that exact file ID.
    pub async fn delete_batch_file(
        &self,
        file_id: &str,
    ) -> Result<OpenAiBatchFileDeletion, OpenAiError> {
        if !valid_provider_path_identifier(file_id) {
            return Err(OpenAiError::MalformedBatchFile);
        }
        let endpoint = self
            .config
            .base_url
            .join(&format!("files/{file_id}"))
            .map_err(|_| OpenAiError::InvalidEndpoint)?;
        let response = self
            .client
            .delete(endpoint)
            .timeout(self.config.request_timeout)
            .bearer_auth(&self.config.api_key)
            .send()
            .await
            .map_err(|_| OpenAiError::Transport)?;
        if !response.status().is_success() {
            return Err(OpenAiError::HttpStatus(response.status()));
        }
        let value = decode_json_response(
            response,
            self.config.max_response_bytes,
            OpenAiError::MalformedBatchFile,
        )
        .await?;
        decode_batch_file_deletion(&value, file_id)
    }
}

pub(crate) fn decode_batch_input_file(value: &Value) -> Result<OpenAiBatchInputFile, OpenAiError> {
    let id = value
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| valid_provider_path_identifier(id))
        .ok_or(OpenAiError::MalformedBatchFile)?;
    if value.get("purpose").and_then(Value::as_str) != Some("batch") {
        return Err(OpenAiError::MalformedBatchFile);
    }
    Ok(OpenAiBatchInputFile { id: id.to_owned() })
}

fn decode_batch_file_deletion(
    value: &Value,
    expected_file_id: &str,
) -> Result<OpenAiBatchFileDeletion, OpenAiError> {
    let id = value
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| valid_provider_path_identifier(id) && *id == expected_file_id)
        .ok_or(OpenAiError::MalformedBatchFile)?;
    if value.get("deleted").and_then(Value::as_bool) != Some(true) {
        return Err(OpenAiError::MalformedBatchFile);
    }
    Ok(OpenAiBatchFileDeletion { id: id.to_owned() })
}

async fn read_bounded_batch_file(
    response: Response,
    max_batch_file_bytes: usize,
) -> Result<Vec<u8>, OpenAiError> {
    let max_batch_file_bytes_u64 =
        u64::try_from(max_batch_file_bytes).map_err(|_| OpenAiError::BatchFileTooLarge)?;
    if response
        .content_length()
        .is_some_and(|length| length > max_batch_file_bytes_u64)
    {
        return Err(OpenAiError::BatchFileTooLarge);
    }
    let mut content = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| OpenAiError::Transport)?;
        if chunk.len() > max_batch_file_bytes.saturating_sub(content.len()) {
            return Err(OpenAiError::BatchFileTooLarge);
        }
        content.extend_from_slice(&chunk);
    }
    Ok(content)
}
