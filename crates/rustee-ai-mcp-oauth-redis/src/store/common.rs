use std::time::Duration;

use rustee_ai_mcp_oauth::McpOAuthTokenStoreKey;
use rustee_redis::{MAX_REDIS_TTL_SECONDS, is_valid_key_namespace};

pub(super) const TRANSACTION_PURPOSE: &str = "transaction";
pub(super) const TOKEN_PURPOSE: &str = "token";

const TRANSACTION_STORAGE_KEY_NAMESPACE: &str = "rustee:mcp:oauth:transaction-key:v1";
const TOKEN_STORAGE_KEY_NAMESPACE: &str = "rustee:mcp:oauth:token-key:v1";

/// Redis OAuth-store configuration failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RedisMcpOAuthStoreConfigError {
    /// Redis namespaces must be bounded ASCII key prefixes without hash-tag syntax.
    #[error(
        "Redis MCP OAuth namespace must use bounded ASCII letters, digits, colon, underscore, hyphen, or dot"
    )]
    InvalidNamespace,
    /// Token retention must be finite and no shorter than one second.
    #[error("Redis MCP OAuth token retention TTL must be at least one second")]
    ZeroTokenTtl,
    /// Token retention must match Redis's exact second-based expiry semantics.
    #[error("Redis MCP OAuth token retention TTL must be a whole number of seconds")]
    FractionalTokenTtl,
    /// Token retention exceeds the common Redis expiry range.
    #[error("Redis MCP OAuth token retention TTL exceeds the Redis-supported range")]
    TokenTtlOutOfRange,
}

pub(crate) fn redacted_namespace_debug_fields(namespace: &str) -> (&'static str, usize) {
    ("[REDACTED]", namespace.len())
}

pub(crate) fn validate_namespace(
    namespace: impl Into<String>,
) -> Result<String, RedisMcpOAuthStoreConfigError> {
    let namespace = namespace.into();
    if !is_valid_key_namespace(&namespace) {
        return Err(RedisMcpOAuthStoreConfigError::InvalidNamespace);
    }
    Ok(namespace)
}

pub(crate) fn validate_token_ttl(token_ttl: Duration) -> Result<(), RedisMcpOAuthStoreConfigError> {
    if token_ttl.as_secs() == 0 {
        return Err(RedisMcpOAuthStoreConfigError::ZeroTokenTtl);
    }
    if token_ttl.subsec_nanos() != 0 {
        return Err(RedisMcpOAuthStoreConfigError::FractionalTokenTtl);
    }
    if token_ttl.as_secs() > MAX_REDIS_TTL_SECONDS {
        return Err(RedisMcpOAuthStoreConfigError::TokenTtlOutOfRange);
    }
    Ok(())
}

pub(crate) fn transaction_storage_key(namespace: &str, state: &str) -> String {
    format!(
        "{TRANSACTION_STORAGE_KEY_NAMESPACE}:{}:{namespace}:{}:{state}",
        namespace.len(),
        state.len(),
    )
}

pub(crate) fn token_storage_key(namespace: &str, key: &McpOAuthTokenStoreKey) -> String {
    let key = key.as_str();
    format!(
        "{TOKEN_STORAGE_KEY_NAMESPACE}:{}:{namespace}:{}:{key}",
        namespace.len(),
        key.len(),
    )
}
