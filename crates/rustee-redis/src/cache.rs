//! Stable JSON cache facade and content-free failure diagnostics.

use std::fmt;

use redis::RedisError;

mod bounded;
mod read;
mod write;

pub use bounded::{get_json_bounded, take_json_bounded};
pub use read::{delete, get_json, take_json};
pub use write::{set_json, set_json_bounded, set_json_bounded_if_absent, set_json_if_absent};

/// Maximum whole-second expiry accepted by Rustee's Redis helpers.
///
/// Redis stores expiry deadlines in signed milliseconds. This leaves several million years of
/// clock headroom while ensuring seconds-to-milliseconds conversion cannot overflow that deadline.
pub const MAX_REDIS_TTL_SECONDS: u64 = 9_000_000_000_000_000;

/// Errors returned by the explicit JSON cache helpers.
#[derive(thiserror::Error)]
pub enum CacheError {
    /// A cache entry cannot be written with an immediate expiry.
    #[error("Redis cache TTL must be greater than zero")]
    ZeroTtl,
    /// A cache entry cannot be written with a TTL Redis cannot represent safely.
    #[error("Redis cache TTL exceeds the supported range")]
    TtlOutOfRange,
    /// A create-only cache write found an existing value.
    #[error("Redis cache entry already exists")]
    EntryExists,
    /// Redis command failure.
    #[error("Redis cache command failed")]
    Redis(#[from] RedisError),
    /// JSON serialization or deserialization failure.
    #[error("Redis cache JSON processing failed")]
    Json(#[from] serde_json::Error),
    /// A bounded read observed more serialized bytes than its caller allows.
    #[error("Redis cache value exceeded its size limit")]
    ValueTooLarge,
    /// A trusted Redis command returned a reply outside this helper's contract.
    #[error("Redis cache command returned an unexpected response")]
    UnexpectedResponse,
}

impl fmt::Debug for CacheError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::ZeroTtl => "zero_ttl",
            Self::TtlOutOfRange => "ttl_out_of_range",
            Self::EntryExists => "entry_exists",
            Self::Redis(_) => "redis_failed",
            Self::Json(_) => "json_failed",
            Self::ValueTooLarge => "value_too_large",
            Self::UnexpectedResponse => "unexpected_response",
        };
        formatter
            .debug_struct("CacheError")
            .field("kind", &kind)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use serde_json::Value;

    use super::CacheError;

    #[test]
    fn json_processing_errors_are_content_free_and_preserve_their_source() {
        let error =
            CacheError::Json(serde_json::from_str::<Value>("private-invalid-json").unwrap_err());

        assert!(!error.to_string().contains("private-invalid-json"));
        assert!(!format!("{error:?}").contains("private-invalid-json"));
        assert!(StdError::source(&error).is_some());
    }
}
