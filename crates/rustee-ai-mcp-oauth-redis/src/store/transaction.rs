use std::fmt;

use futures_util::future::BoxFuture;
use rustee_ai_mcp_oauth::{McpOAuthPendingAuthorization, McpOAuthTransactionStore};
use rustee_redis::CacheError;

use crate::crypto::{
    EncryptedEnvelope, MAX_SERIALIZED_ENVELOPE_BYTES, McpOAuthSecretCipher,
    McpOAuthSecretCipherError,
};

use super::common::{
    RedisMcpOAuthStoreConfigError, TRANSACTION_PURPOSE, redacted_namespace_debug_fields,
    transaction_storage_key, validate_namespace,
};

/// Default, versioned Redis namespace for encrypted MCP OAuth PKCE transactions.
pub const DEFAULT_TRANSACTION_NAMESPACE: &str = "rustee:mcp:oauth:transaction:v1";

/// Redis-backed encrypted storage for one-time MCP OAuth authorization transactions.
#[derive(Clone)]
pub struct RedisMcpOAuthTransactionStore {
    connection: rustee_redis::redis::aio::ConnectionManager,
    namespace: String,
    cipher: McpOAuthSecretCipher,
}

impl RedisMcpOAuthTransactionStore {
    /// Creates a transaction store using [`DEFAULT_TRANSACTION_NAMESPACE`].
    #[must_use]
    pub fn new(
        connection: rustee_redis::redis::aio::ConnectionManager,
        cipher: McpOAuthSecretCipher,
    ) -> Self {
        Self {
            connection,
            namespace: DEFAULT_TRANSACTION_NAMESPACE.to_owned(),
            cipher,
        }
    }

    /// Creates a store with an explicit versioned Redis namespace.
    ///
    /// # Errors
    ///
    /// Returns [`RedisMcpOAuthStoreConfigError::InvalidNamespace`] for blank, oversized, or unsafe
    /// namespaces.
    pub fn with_namespace(
        connection: rustee_redis::redis::aio::ConnectionManager,
        cipher: McpOAuthSecretCipher,
        namespace: impl Into<String>,
    ) -> Result<Self, RedisMcpOAuthStoreConfigError> {
        let namespace = validate_namespace(namespace)?;
        Ok(Self {
            connection,
            namespace,
            cipher,
        })
    }

    fn key(&self, state: &str) -> String {
        transaction_storage_key(&self.namespace, state)
    }
}

impl fmt::Debug for RedisMcpOAuthTransactionStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (namespace, namespace_length) = redacted_namespace_debug_fields(&self.namespace);
        formatter
            .debug_struct("RedisMcpOAuthTransactionStore")
            .field("namespace", &namespace)
            .field("namespace_length", &namespace_length)
            .field("cipher", &self.cipher)
            .finish_non_exhaustive()
    }
}

impl McpOAuthTransactionStore for RedisMcpOAuthTransactionStore {
    type Error = RedisMcpOAuthTransactionStoreError;

    fn save(
        &self,
        transaction: McpOAuthPendingAuthorization,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let connection = self.connection.clone();
        let key = self.key(transaction.state());
        let cipher = self.cipher.clone();
        Box::pin(async move {
            let ttl_seconds = transaction
                .remaining_ttl_seconds()
                .ok_or(RedisMcpOAuthTransactionStoreError::ExpiredTransaction)?;
            let envelope = cipher
                .seal(TRANSACTION_PURPOSE, &key, &transaction)
                .map_err(RedisMcpOAuthTransactionStoreError::Encrypt)?;
            rustee_redis::set_json_bounded_if_absent(
                &connection,
                &key,
                &envelope,
                ttl_seconds,
                MAX_SERIALIZED_ENVELOPE_BYTES,
            )
            .await
            .map_err(RedisMcpOAuthTransactionStoreError::Save)
        })
    }

    fn take(
        &self,
        state: String,
    ) -> BoxFuture<'static, Result<Option<McpOAuthPendingAuthorization>, Self::Error>> {
        let connection = self.connection.clone();
        let key = self.key(&state);
        let cipher = self.cipher.clone();
        Box::pin(async move {
            let envelope = rustee_redis::take_json_bounded::<EncryptedEnvelope>(
                &connection,
                &key,
                MAX_SERIALIZED_ENVELOPE_BYTES,
            )
            .await
            .map_err(RedisMcpOAuthTransactionStoreError::Take)?;
            envelope
                .map(|envelope| {
                    cipher
                        .open(TRANSACTION_PURPOSE, &key, envelope)
                        .map_err(RedisMcpOAuthTransactionStoreError::Decrypt)
                })
                .transpose()
        })
    }
}

/// Redis transaction-store failures with no state or ciphertext detail.
#[derive(thiserror::Error)]
pub enum RedisMcpOAuthTransactionStoreError {
    /// The transaction elapsed before the store could persist it.
    #[error("MCP OAuth authorization transaction expired before storage")]
    ExpiredTransaction,
    /// The transaction could not be encrypted.
    #[error("MCP OAuth authorization transaction encryption failed")]
    Encrypt(#[source] McpOAuthSecretCipherError),
    /// Redis could not save the encrypted transaction envelope.
    #[error("MCP OAuth authorization transaction save failed")]
    Save(#[source] CacheError),
    /// Redis could not atomically consume the encrypted transaction envelope.
    #[error("MCP OAuth authorization transaction take failed")]
    Take(#[source] CacheError),
    /// The consumed transaction envelope could not be authenticated or decoded.
    #[error("MCP OAuth authorization transaction was rejected")]
    Decrypt(#[source] McpOAuthSecretCipherError),
}

impl fmt::Debug for RedisMcpOAuthTransactionStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::ExpiredTransaction => "expired_transaction",
            Self::Encrypt(_) => "encryption_failed",
            Self::Save(_) => "save_failed",
            Self::Take(_) => "take_failed",
            Self::Decrypt(_) => "decryption_failed",
        };
        formatter
            .debug_struct("RedisMcpOAuthTransactionStoreError")
            .field("kind", &kind)
            .finish()
    }
}
