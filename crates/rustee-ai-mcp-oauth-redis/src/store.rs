//! Stable facade for encrypted Redis MCP OAuth transaction and token persistence.

mod common;
mod token;
mod transaction;

pub use common::RedisMcpOAuthStoreConfigError;
pub use token::{DEFAULT_TOKEN_NAMESPACE, RedisMcpOAuthTokenStore, RedisMcpOAuthTokenStoreError};
pub use transaction::{
    DEFAULT_TRANSACTION_NAMESPACE, RedisMcpOAuthTransactionStore,
    RedisMcpOAuthTransactionStoreError,
};

#[cfg(test)]
pub(super) use common::{
    redacted_namespace_debug_fields, token_storage_key, transaction_storage_key,
    validate_namespace, validate_token_ttl,
};
