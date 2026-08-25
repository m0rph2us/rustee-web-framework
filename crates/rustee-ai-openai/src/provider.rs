//! Stable `OpenAI` provider facade and internal request-mapping helpers.

mod embeddings;
mod responses;

pub use embeddings::OpenAiEmbeddingsProvider;
pub use responses::OpenAiResponsesProvider;

#[cfg(test)]
pub(crate) use embeddings::validate_embedding_batch;
pub(crate) use embeddings::{decode_embeddings, embedding_request_body};
pub(crate) use responses::request_body;
