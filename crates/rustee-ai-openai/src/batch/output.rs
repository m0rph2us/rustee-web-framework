//! Explicit `OpenAI` Batch output facade with bounded parsing and response consumption.

mod content;
mod row;
mod rows;

pub use content::OpenAiBatchFileContent;
pub use row::{
    OpenAiBatchOutputRow, OpenAiBatchResponse, OpenAiBatchResponseBody, OpenAiBatchRowError,
    OpenAiBatchRowOutcome,
};
pub use rows::{OpenAiBatchOutputParseError, OpenAiBatchOutputRows};
