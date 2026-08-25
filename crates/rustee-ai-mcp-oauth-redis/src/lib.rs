//! Encrypted Redis persistence for MCP OAuth authorization transactions and token sets.
//!
//! Redis stores only versioned AES-256-GCM envelopes. The envelope authentication data binds each
//! ciphertext to its record kind and full Redis key, preventing a valid transaction or token from
//! being moved to another namespace or subject slot. Applications supply an active encryption key
//! and optional retired decryption keys; Redis never receives those key values.

mod crypto;
mod store;

pub use crypto::{
    McpOAuthSecretCipher, McpOAuthSecretCipherError, McpOAuthSecretKeyRing,
    McpOAuthSecretKeyRingError,
};
pub use store::{
    DEFAULT_TOKEN_NAMESPACE, DEFAULT_TRANSACTION_NAMESPACE, RedisMcpOAuthStoreConfigError,
    RedisMcpOAuthTokenStore, RedisMcpOAuthTokenStoreError, RedisMcpOAuthTransactionStore,
    RedisMcpOAuthTransactionStoreError,
};

#[cfg(test)]
mod tests;
