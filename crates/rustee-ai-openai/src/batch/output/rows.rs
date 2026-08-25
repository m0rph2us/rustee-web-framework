//! Fail-closed bounded JSONL iteration for Batch output rows.

use std::fmt;

use serde_json::Value;

use super::OpenAiBatchOutputRow;
use super::row::decode_batch_output_row;

/// Fail-closed iterator over one explicitly downloaded `OpenAI` Batch JSONL output file.
pub struct OpenAiBatchOutputRows<'a> {
    bytes: &'a [u8],
    offset: usize,
    rows_seen: usize,
    max_rows: usize,
    failed: bool,
}

impl<'a> OpenAiBatchOutputRows<'a> {
    pub(super) const fn new(bytes: &'a [u8], max_rows: usize) -> Self {
        Self {
            bytes,
            offset: 0,
            rows_seen: 0,
            max_rows,
            failed: false,
        }
    }
}

impl Iterator for OpenAiBatchOutputRows<'_> {
    type Item = Result<OpenAiBatchOutputRow, OpenAiBatchOutputParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed || self.offset >= self.bytes.len() {
            return None;
        }
        let remaining = &self.bytes[self.offset..];
        let (line, next_offset) = match remaining.iter().position(|byte| *byte == b'\n') {
            Some(index) => (&remaining[..index], self.offset + index + 1),
            None => (remaining, self.bytes.len()),
        };
        self.offset = next_offset;
        self.rows_seen += 1;
        let line_number = self.rows_seen;
        if self.rows_seen > self.max_rows {
            self.failed = true;
            return Some(Err(OpenAiBatchOutputParseError::TooManyRows));
        }
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            self.failed = true;
            return Some(Err(OpenAiBatchOutputParseError::MalformedRow {
                line_number,
            }));
        }
        let Ok(value) = serde_json::from_slice::<Value>(line) else {
            self.failed = true;
            return Some(Err(OpenAiBatchOutputParseError::MalformedRow {
                line_number,
            }));
        };
        if let Ok(row) = decode_batch_output_row(&value) {
            Some(Ok(row))
        } else {
            self.failed = true;
            Some(Err(OpenAiBatchOutputParseError::MalformedRow {
                line_number,
            }))
        }
    }
}

impl fmt::Debug for OpenAiBatchOutputRows<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiBatchOutputRows")
            .field("rows_seen", &self.rows_seen)
            .field("max_rows", &self.max_rows)
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

/// Safe structural parse failure for an `OpenAI` Batch output file.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OpenAiBatchOutputParseError {
    /// The caller supplied a row bound outside the provider's supported range.
    #[error("OpenAI Batch output row limit is outside the supported range")]
    InvalidRowLimit,
    /// A JSONL line was malformed or did not meet the safe output-row contract.
    #[error("OpenAI Batch output row {line_number} was malformed")]
    MalformedRow {
        /// One-based position in the explicitly selected output file.
        line_number: usize,
    },
    /// The downloaded file exceeded the caller-selected row bound.
    #[error("OpenAI Batch output exceeded the configured row limit")]
    TooManyRows,
}
