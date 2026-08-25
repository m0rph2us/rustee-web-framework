//! Validated `OpenAI` Batch input JSONL models and content-redacted serialization.

use std::fmt;

use crate::OpenAiError;

mod builder;
mod content;
mod row;

pub use builder::OpenAiBatchJsonlBuilder;
pub use content::OpenAiBatchInputJsonl;
pub(crate) use row::valid_provider_identifier;
pub use row::{OpenAiBatchEndpoint, OpenAiBatchInputRow};

/// Maximum Batch input or result file size accepted by the current `OpenAI` API contract.
pub const OPENAI_BATCH_FILE_MAX_BYTES: usize = 200 * 1024 * 1024;

/// Maximum request/output rows accepted by the current `OpenAI` Batch API contract.
pub const OPENAI_BATCH_MAX_REQUESTS: usize = 50_000;

/// Invalid local `OpenAI` Batch input-file content.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OpenAiBatchInputError {
    /// The provider cannot receive an empty input file.
    #[error("OpenAI Batch input JSONL must not be empty")]
    Empty,
    /// The local input exceeds the provider's documented maximum Batch file size.
    #[error("OpenAI Batch input JSONL exceeds the provider file limit")]
    TooLarge,
    /// The application correlation ID was not a bounded safe identifier.
    #[error("OpenAI Batch custom ID was unsafe")]
    UnsafeCustomId,
    /// The generic Batch envelope requires an object-shaped provider request body.
    #[error("OpenAI Batch request body must be a JSON object")]
    BodyMustBeObject,
    /// All rows in one provider Batch must target the same endpoint.
    #[error("OpenAI Batch row endpoint did not match the builder endpoint")]
    EndpointMismatch,
    /// A provider Batch cannot contain the same application correlation ID twice.
    #[error("OpenAI Batch custom ID was duplicated")]
    DuplicateCustomId,
    /// The builder reached the provider's documented per-file request limit.
    #[error("OpenAI Batch input exceeded the provider row limit")]
    TooManyRows,
    /// The generic Batch envelope could not be serialized.
    #[error("OpenAI Batch input could not be serialized")]
    Serialization,
}

/// Failure while mapping a typed Rustee request into an `OpenAI` Responses Batch row.
#[derive(thiserror::Error)]
pub enum OpenAiBatchResponsesRowError {
    /// The generic Batch row validation did not accept its content-free envelope metadata.
    #[error("OpenAI Responses Batch row was invalid")]
    Input {
        /// Generic Batch row validation failure.
        #[source]
        source: OpenAiBatchInputError,
    },
    /// Rustee's typed chat request could not map to the Responses request shape.
    #[error("Rustee chat request could not map to an OpenAI Responses Batch row")]
    Request {
        /// Safe adapter mapping failure.
        #[source]
        source: OpenAiError,
    },
}

impl fmt::Debug for OpenAiBatchResponsesRowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Input { .. } => "input",
            Self::Request { .. } => "request",
        };
        formatter
            .debug_struct("OpenAiBatchResponsesRowError")
            .field("kind", &kind)
            .finish()
    }
}

/// Failure while mapping typed Rustee embedding inputs into an `OpenAI` Embeddings Batch row.
#[derive(thiserror::Error)]
pub enum OpenAiBatchEmbeddingsRowError {
    /// An embedding Batch row requires a bounded provider model alias.
    #[error("OpenAI Batch embedding model was unsafe")]
    UnsafeModel,
    /// An embedding Batch row needs at least one input in its preserved order.
    #[error("OpenAI Batch embedding row must contain at least one input")]
    EmptyInputs,
    /// The generic Batch row validation did not accept its content-free envelope metadata.
    #[error("OpenAI Embeddings Batch row was invalid")]
    Input {
        /// Generic Batch row validation failure.
        #[source]
        source: OpenAiBatchInputError,
    },
}

impl fmt::Debug for OpenAiBatchEmbeddingsRowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::UnsafeModel => "unsafe_model",
            Self::EmptyInputs => "empty_inputs",
            Self::Input { .. } => "input",
        };
        formatter
            .debug_struct("OpenAiBatchEmbeddingsRowError")
            .field("kind", &kind)
            .finish()
    }
}
