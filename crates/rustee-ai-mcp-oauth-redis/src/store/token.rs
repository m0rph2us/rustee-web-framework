use std::{fmt, time::Duration};

use futures_util::future::BoxFuture;
use rustee_ai_mcp_oauth::{
    McpOAuthTokenSecrets, McpOAuthTokenSet, McpOAuthTokenStore, McpOAuthTokenStoreKey,
};
use rustee_redis::CacheError;

use crate::crypto::{
    EncryptedEnvelope, MAX_SERIALIZED_ENVELOPE_BYTES, McpOAuthSecretCipher,
    McpOAuthSecretCipherError,
};

use super::common::{
    RedisMcpOAuthStoreConfigError, TOKEN_PURPOSE, redacted_namespace_debug_fields,
    token_storage_key, validate_namespace, validate_token_ttl,
};

/// Default, versioned Redis namespace for encrypted MCP OAuth token sets.
pub const DEFAULT_TOKEN_NAMESPACE: &str = "rustee:mcp:oauth:token:v1";

const DEFAULT_TOKEN_TTL: Duration = Duration::from_secs(60 * 60 * 24 * 30);

/// Redis-backed encrypted persistence for MCP OAuth access/refresh token sets.
#[derive(Clone)]
pub struct RedisMcpOAuthTokenStore {
    connection: rustee_redis::redis::aio::ConnectionManager,
    namespace: String,
    cipher: McpOAuthSecretCipher,
    token_ttl: Duration,
}

impl RedisMcpOAuthTokenStore {
    /// Creates a token store using [`DEFAULT_TOKEN_NAMESPACE`] and a 30-day retention TTL.
    #[must_use]
    pub fn new(
        connection: rustee_redis::redis::aio::ConnectionManager,
        cipher: McpOAuthSecretCipher,
    ) -> Self {
        Self {
            connection,
            namespace: DEFAULT_TOKEN_NAMESPACE.to_owned(),
            cipher,
            token_ttl: DEFAULT_TOKEN_TTL,
        }
    }

    /// Creates a token store with an explicit versioned namespace.
    ///
    /// # Errors
    ///
    /// Returns [`RedisMcpOAuthStoreConfigError::InvalidNamespace`] for a blank, oversized, or unsafe
    /// namespace.
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
            token_ttl: DEFAULT_TOKEN_TTL,
        })
    }

    /// Sets the explicit maximum retention period for one token set.
    ///
    /// # Errors
    ///
    /// Returns [`RedisMcpOAuthStoreConfigError::ZeroTokenTtl`] for a sub-second or zero TTL,
    /// [`RedisMcpOAuthStoreConfigError::FractionalTokenTtl`] when the value cannot be represented
    /// as an exact Redis second-based expiry, or
    /// [`RedisMcpOAuthStoreConfigError::TokenTtlOutOfRange`] when Redis cannot represent it.
    pub fn with_token_ttl(
        mut self,
        token_ttl: Duration,
    ) -> Result<Self, RedisMcpOAuthStoreConfigError> {
        validate_token_ttl(token_ttl)?;
        self.token_ttl = token_ttl;
        Ok(self)
    }

    /// Returns the configured maximum Redis retention period.
    #[must_use]
    pub const fn token_ttl(&self) -> Duration {
        self.token_ttl
    }

    fn key(&self, key: &McpOAuthTokenStoreKey) -> String {
        token_storage_key(&self.namespace, key)
    }
}

impl fmt::Debug for RedisMcpOAuthTokenStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (namespace, namespace_length) = redacted_namespace_debug_fields(&self.namespace);
        formatter
            .debug_struct("RedisMcpOAuthTokenStore")
            .field("namespace", &namespace)
            .field("namespace_length", &namespace_length)
            .field("token_ttl", &self.token_ttl)
            .field("cipher", &self.cipher)
            .finish_non_exhaustive()
    }
}

impl McpOAuthTokenStore for RedisMcpOAuthTokenStore {
    type Error = RedisMcpOAuthTokenStoreError;

    fn load(
        &self,
        token_key: McpOAuthTokenStoreKey,
    ) -> BoxFuture<'static, Result<Option<McpOAuthTokenSet>, Self::Error>> {
        let connection = self.connection.clone();
        let key = self.key(&token_key);
        let cipher = self.cipher.clone();
        Box::pin(async move {
            let envelope = rustee_redis::get_json_bounded::<EncryptedEnvelope>(
                &connection,
                &key,
                MAX_SERIALIZED_ENVELOPE_BYTES,
            )
            .await
            .map_err(RedisMcpOAuthTokenStoreError::Load)?;
            envelope
                .map(|envelope| {
                    let secrets: McpOAuthTokenSecrets = cipher
                        .open(TOKEN_PURPOSE, &key, envelope)
                        .map_err(RedisMcpOAuthTokenStoreError::Decrypt)?;
                    secrets
                        .into_token_set()
                        .map_err(|_| RedisMcpOAuthTokenStoreError::TokenRejected)
                })
                .transpose()
        })
    }

    fn save(
        &self,
        token_key: McpOAuthTokenStoreKey,
        tokens: McpOAuthTokenSet,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let connection = self.connection.clone();
        let key = self.key(&token_key);
        let cipher = self.cipher.clone();
        let ttl_seconds = self.token_ttl.as_secs();
        Box::pin(async move {
            let secrets = tokens.into_secrets();
            let envelope = cipher
                .seal(TOKEN_PURPOSE, &key, &secrets)
                .map_err(RedisMcpOAuthTokenStoreError::Encrypt)?;
            rustee_redis::set_json_bounded(
                &connection,
                &key,
                &envelope,
                ttl_seconds,
                MAX_SERIALIZED_ENVELOPE_BYTES,
            )
            .await
            .map_err(RedisMcpOAuthTokenStoreError::Save)
        })
    }

    fn remove(
        &self,
        token_key: McpOAuthTokenStoreKey,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let connection = self.connection.clone();
        let key = self.key(&token_key);
        Box::pin(async move {
            rustee_redis::delete(&connection, &key)
                .await
                .map_err(RedisMcpOAuthTokenStoreError::Delete)
        })
    }
}

/// Redis token-store failures with no token, key, or ciphertext detail.
#[derive(thiserror::Error)]
pub enum RedisMcpOAuthTokenStoreError {
    /// The token record could not be encrypted.
    #[error("MCP OAuth token encryption failed")]
    Encrypt(#[source] McpOAuthSecretCipherError),
    /// Redis could not write the encrypted token envelope.
    #[error("MCP OAuth token save failed")]
    Save(#[source] CacheError),
    /// Redis could not load the encrypted token envelope.
    #[error("MCP OAuth token load failed")]
    Load(#[source] CacheError),
    /// The envelope could not be authenticated or decrypted.
    #[error("MCP OAuth token was rejected")]
    Decrypt(#[source] McpOAuthSecretCipherError),
    /// Decrypted content did not satisfy the OAuth token-set invariant.
    #[error("MCP OAuth token was rejected")]
    TokenRejected,
    /// Redis could not remove the token envelope.
    #[error("MCP OAuth token removal failed")]
    Delete(#[source] CacheError),
}

impl fmt::Debug for RedisMcpOAuthTokenStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Encrypt(_) => "encryption_failed",
            Self::Save(_) => "save_failed",
            Self::Load(_) => "load_failed",
            Self::Decrypt(_) => "decryption_failed",
            Self::TokenRejected => "token_rejected",
            Self::Delete(_) => "delete_failed",
        };
        formatter
            .debug_struct("RedisMcpOAuthTokenStoreError")
            .field("kind", &kind)
            .finish()
    }
}
