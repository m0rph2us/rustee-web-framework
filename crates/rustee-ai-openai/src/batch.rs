//! `OpenAI` Batch public facade.

mod files;
mod input;
mod lifecycle;
mod output;
mod provider;
mod request;

pub use files::{
    OpenAiBatchFileDeletion, OpenAiBatchInputFile, OpenAiBatchOutputExpiration,
    OpenAiBatchOutputExpirationError,
};
pub use input::{
    OPENAI_BATCH_FILE_MAX_BYTES, OPENAI_BATCH_MAX_REQUESTS, OpenAiBatchEmbeddingsRowError,
    OpenAiBatchEndpoint, OpenAiBatchInputError, OpenAiBatchInputJsonl, OpenAiBatchInputRow,
    OpenAiBatchJsonlBuilder, OpenAiBatchResponsesRowError,
};
pub use lifecycle::{OpenAiBatchSnapshot, OpenAiBatchStatus};
pub use output::{
    OpenAiBatchFileContent, OpenAiBatchOutputParseError, OpenAiBatchOutputRow,
    OpenAiBatchOutputRows, OpenAiBatchResponse, OpenAiBatchResponseBody, OpenAiBatchRowError,
    OpenAiBatchRowOutcome,
};
pub use provider::OpenAiBatchProvider;
pub use request::OpenAiBatchRequest;

#[cfg(test)]
pub(super) use files::decode_batch_input_file;
pub(super) use input::valid_provider_identifier;
pub(super) use lifecycle::decode_batch;

pub(super) fn valid_provider_path_identifier(value: &str) -> bool {
    valid_provider_identifier(value) && !matches!(value, "." | "..")
}
