//! Redis adapter for explicit Rustee AI response-cache entries.
//!
//! Redis retains serialized response content. Deployments must separately choose encrypted
//! transport/storage, tenant erase, credential rotation, and retention controls. This adapter
//! accepts only the opaque exact keys and non-tool-call entries validated by `rustee-ai-cache`.
//! It neither hashes prompts nor offers SCAN/wildcard namespace deletion.

mod config;
mod store;

pub use config::{
    DEFAULT_MAX_ENTRY_BYTES, DEFAULT_NAMESPACE, RedisAiResponseCache,
    RedisAiResponseCacheConfigError,
};
pub use store::RedisAiResponseCacheError;
