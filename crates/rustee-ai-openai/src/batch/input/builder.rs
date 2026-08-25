//! Endpoint-homogeneous JSONL accumulation and bounded serialization.

use std::{
    collections::HashSet,
    fmt,
    io::{self, Write},
};

use serde::Serialize;
use serde_json::Value;

use super::{
    OPENAI_BATCH_FILE_MAX_BYTES, OPENAI_BATCH_MAX_REQUESTS, OpenAiBatchEndpoint,
    OpenAiBatchInputError, OpenAiBatchInputJsonl, OpenAiBatchInputRow,
};

/// Builder for one endpoint-homogeneous, content-redacted `OpenAI` Batch input JSONL file.
pub struct OpenAiBatchJsonlBuilder {
    endpoint: OpenAiBatchEndpoint,
    custom_ids: HashSet<String>,
    jsonl: Vec<u8>,
    row_count: usize,
}

impl OpenAiBatchJsonlBuilder {
    /// Starts an input file whose rows must all target `endpoint`.
    #[must_use]
    pub fn new(endpoint: OpenAiBatchEndpoint) -> Self {
        Self {
            endpoint,
            custom_ids: HashSet::new(),
            jsonl: Vec::new(),
            row_count: 0,
        }
    }

    /// Serializes and adds one provider request row after correlation, endpoint, and count validation.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiBatchInputError`] for mismatched endpoint, duplicate correlation ID,
    /// provider row-count limit exhaustion, serialization failure, or file-size exhaustion. On
    /// failure, this builder retains exactly its prior accepted rows.
    pub fn push(&mut self, row: OpenAiBatchInputRow) -> Result<(), OpenAiBatchInputError> {
        if row.endpoint != self.endpoint {
            return Err(OpenAiBatchInputError::EndpointMismatch);
        }
        if self.row_count >= OPENAI_BATCH_MAX_REQUESTS {
            return Err(OpenAiBatchInputError::TooManyRows);
        }
        if self.custom_ids.contains(&row.custom_id) {
            return Err(OpenAiBatchInputError::DuplicateCustomId);
        }
        append_jsonl_row(&mut self.jsonl, &row, OPENAI_BATCH_FILE_MAX_BYTES)?;
        self.custom_ids.insert(row.custom_id);
        self.row_count += 1;
        Ok(())
    }

    /// Moves the accepted JSONL bytes into a short-lived upload value.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiBatchInputError::Empty`] when no row was accepted. Row serialization and
    /// file-size admission occur in [`Self::push`].
    pub fn build(self) -> Result<OpenAiBatchInputJsonl, OpenAiBatchInputError> {
        if self.row_count == 0 {
            return Err(OpenAiBatchInputError::Empty);
        }
        OpenAiBatchInputJsonl::new(self.jsonl)
    }
}

impl fmt::Debug for OpenAiBatchJsonlBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiBatchJsonlBuilder")
            .field("endpoint", &self.endpoint)
            .field("rows", &self.row_count)
            .field("serialized_bytes", &self.jsonl.len())
            .finish_non_exhaustive()
    }
}

#[derive(Serialize)]
struct BatchInputRowWire<'a> {
    custom_id: &'a str,
    method: &'static str,
    url: &'static str,
    body: &'a Value,
}

fn append_jsonl_row(
    jsonl: &mut Vec<u8>,
    row: &OpenAiBatchInputRow,
    max_bytes: usize,
) -> Result<(), OpenAiBatchInputError> {
    let initial_len = jsonl.len();
    let wire = BatchInputRowWire {
        custom_id: &row.custom_id,
        method: "POST",
        url: row.endpoint.path(),
        body: &row.body,
    };
    let (result, exceeded) = {
        let mut writer = BoundedJsonlWriter::new(jsonl, max_bytes);
        let result = serde_json::to_writer(&mut writer, &wire);
        (result, writer.exceeded)
    };

    if exceeded {
        jsonl.truncate(initial_len);
        return Err(OpenAiBatchInputError::TooLarge);
    }
    if result.is_err() {
        jsonl.truncate(initial_len);
        return Err(OpenAiBatchInputError::Serialization);
    }
    if jsonl.len() == max_bytes {
        jsonl.truncate(initial_len);
        return Err(OpenAiBatchInputError::TooLarge);
    }
    jsonl.push(b'\n');
    Ok(())
}

struct BoundedJsonlWriter<'a> {
    bytes: &'a mut Vec<u8>,
    max_bytes: usize,
    exceeded: bool,
}

impl<'a> BoundedJsonlWriter<'a> {
    fn new(bytes: &'a mut Vec<u8>, max_bytes: usize) -> Self {
        Self {
            bytes,
            max_bytes,
            exceeded: false,
        }
    }
}

impl Write for BoundedJsonlWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.len() > self.max_bytes.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "OpenAI Batch JSONL exceeds the configured limit",
            ));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        OpenAiBatchEndpoint, OpenAiBatchInputError, OpenAiBatchInputRow, append_jsonl_row,
    };

    #[test]
    fn row_append_reverts_partial_jsonl_when_the_bound_is_exceeded() {
        let row = OpenAiBatchInputRow::new(
            "row-1",
            OpenAiBatchEndpoint::Responses,
            json!({"input":"x".repeat(64)}),
        )
        .expect("test row is valid");
        let mut jsonl = b"previous-row\n".to_vec();
        let original = jsonl.clone();

        assert_eq!(
            append_jsonl_row(&mut jsonl, &row, 32).unwrap_err(),
            OpenAiBatchInputError::TooLarge
        );
        assert_eq!(jsonl, original);

        append_jsonl_row(&mut jsonl, &row, 256).expect("row fits the test limit");
        assert!(jsonl.ends_with(b"\n"));
    }
}
