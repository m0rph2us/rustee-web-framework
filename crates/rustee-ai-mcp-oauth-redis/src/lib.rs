//! Encrypted Redis persistence for MCP OAuth authorization transactions and token sets.
//!
//! Redis stores only versioned AES-256-GCM envelopes. The envelope authentication data binds each
//! ciphertext to its record kind and full Redis key, preventing a valid transaction or token from
//! being moved to another namespace or subject slot. Applications supply an active encryption key
//! and optional retired decryption keys; Redis never receives those key values.

use std::{collections::BTreeMap, fmt, time::Duration};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::future::BoxFuture;
use ring::{
    aead::{AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey},
    rand::{SecureRandom, SystemRandom},
};
use rustee_ai_mcp_oauth::{
    McpOAuthPendingAuthorization, McpOAuthTokenSecrets, McpOAuthTokenSet, McpOAuthTokenStore,
    McpOAuthTokenStoreKey, McpOAuthTransactionStore,
};
use rustee_redis::{CacheError, redis::RedisError};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use zeroize::Zeroizing;

/// Default, versioned Redis namespace for encrypted MCP OAuth PKCE transactions.
pub const DEFAULT_TRANSACTION_NAMESPACE: &str = "rustee:mcp:oauth:transaction:v1";
/// Default, versioned Redis namespace for encrypted MCP OAuth token sets.
pub const DEFAULT_TOKEN_NAMESPACE: &str = "rustee:mcp:oauth:token:v1";

const DEFAULT_TOKEN_TTL: Duration = Duration::from_secs(60 * 60 * 24 * 30);
const ENVELOPE_VERSION: u8 = 1;
const MAX_KEY_ID_BYTES: usize = 128;
const MAX_ENCRYPTED_PLAINTEXT_BYTES: usize = 64 * 1024;
const MAX_ENCODED_ENVELOPE_BYTES: usize = 96 * 1024;
const TRANSACTION_PURPOSE: &str = "transaction";
const TOKEN_PURPOSE: &str = "token";

/// One active encryption key plus optional retired keys that may decrypt existing records.
///
/// Callers rotate by creating a new ring with the new active key and every still-valid former key
/// supplied through [`Self::with_retired_key`]. New records always use the active key ID.
#[derive(Clone)]
pub struct McpOAuthSecretKeyRing {
    active_key_id: String,
    keys: BTreeMap<String, Zeroizing<[u8; 32]>>,
}

impl McpOAuthSecretKeyRing {
    /// Creates a key ring with one active 256-bit AES-GCM key.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthSecretKeyRingError::InvalidKeyId`] for a blank, oversized, or unsafe key
    /// identifier.
    pub fn new(
        active_key_id: impl Into<String>,
        active_key: [u8; 32],
    ) -> Result<Self, McpOAuthSecretKeyRingError> {
        let active_key_id = active_key_id.into();
        if !valid_key_id(&active_key_id) {
            return Err(McpOAuthSecretKeyRingError::InvalidKeyId);
        }
        let mut keys = BTreeMap::new();
        keys.insert(active_key_id.clone(), Zeroizing::new(active_key));
        Ok(Self {
            active_key_id,
            keys,
        })
    }

    /// Adds a previous key that may decrypt persisted records but can never encrypt new ones.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthSecretKeyRingError::InvalidKeyId`] for an unsafe identifier or
    /// [`McpOAuthSecretKeyRingError::DuplicateKeyId`] when it collides with the active or another
    /// retired key.
    pub fn with_retired_key(
        mut self,
        key_id: impl Into<String>,
        key: [u8; 32],
    ) -> Result<Self, McpOAuthSecretKeyRingError> {
        let key_id = key_id.into();
        if !valid_key_id(&key_id) {
            return Err(McpOAuthSecretKeyRingError::InvalidKeyId);
        }
        if self.keys.contains_key(&key_id) {
            return Err(McpOAuthSecretKeyRingError::DuplicateKeyId);
        }
        self.keys.insert(key_id, Zeroizing::new(key));
        Ok(self)
    }

    /// Returns the identifier that will encrypt newly written records.
    #[must_use]
    pub fn active_key_id(&self) -> &str {
        &self.active_key_id
    }

    fn active_key(&self) -> &[u8; 32] {
        self.keys
            .get(&self.active_key_id)
            .map(|key| &**key)
            .expect("MCP OAuth key ring must retain its active key")
    }

    fn decryption_key(&self, key_id: &str) -> Option<&[u8; 32]> {
        self.keys.get(key_id).map(|key| &**key)
    }
}

impl fmt::Debug for McpOAuthSecretKeyRing {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthSecretKeyRing")
            .field("active_key_id", &self.active_key_id)
            .field("decryption_key_count", &self.keys.len())
            .finish()
    }
}

/// Key-ring configuration failures that do not reveal key material.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum McpOAuthSecretKeyRingError {
    /// Key IDs must be bounded ASCII identifiers without whitespace or control characters.
    #[error("MCP OAuth encryption key ID was invalid")]
    InvalidKeyId,
    /// Active and retired key IDs must be unique.
    #[error("MCP OAuth encryption key ID was duplicated")]
    DuplicateKeyId,
}

/// Authenticated encryption and decryption failures with no plaintext or ciphertext detail.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum McpOAuthSecretCipherError {
    /// The operating system could not supply a random AES-GCM nonce.
    #[error("MCP OAuth encryption randomness was unavailable")]
    RandomnessUnavailable,
    /// The serialized value exceeded the bounded persistence envelope.
    #[error("MCP OAuth encrypted record exceeded its size limit")]
    PayloadTooLarge,
    /// A record could not be serialized before encryption.
    #[error("MCP OAuth encrypted record could not be serialized")]
    SerializationRejected,
    /// The stored envelope was malformed, oversized, or used an unsupported version.
    #[error("MCP OAuth encrypted record was rejected")]
    EnvelopeRejected,
    /// The envelope named a key that is no longer available for decryption.
    #[error("MCP OAuth encrypted record key was unavailable")]
    KeyUnavailable,
    /// Envelope authentication failed or the ciphertext could not be decrypted.
    #[error("MCP OAuth encrypted record could not be authenticated")]
    AuthenticationRejected,
    /// The decrypted plaintext could not be decoded as its expected typed record.
    #[error("MCP OAuth encrypted record payload was rejected")]
    DeserializationRejected,
}

/// AES-256-GCM envelope codec shared by the Redis transaction and token stores.
#[derive(Clone)]
pub struct McpOAuthSecretCipher {
    key_ring: McpOAuthSecretKeyRing,
}

impl McpOAuthSecretCipher {
    /// Creates a cipher that uses the supplied active/retired key ring.
    #[must_use]
    pub fn new(key_ring: McpOAuthSecretKeyRing) -> Self {
        Self { key_ring }
    }

    fn seal<T: Serialize>(
        &self,
        purpose: &str,
        record_key: &str,
        value: &T,
    ) -> Result<EncryptedEnvelope, McpOAuthSecretCipherError> {
        let mut plaintext = Zeroizing::new(
            serde_json::to_vec(value)
                .map_err(|_| McpOAuthSecretCipherError::SerializationRejected)?,
        );
        if plaintext.len() > MAX_ENCRYPTED_PLAINTEXT_BYTES {
            return Err(McpOAuthSecretCipherError::PayloadTooLarge);
        }
        let mut nonce_bytes = [0_u8; NONCE_LEN];
        SystemRandom::new()
            .fill(&mut nonce_bytes)
            .map_err(|_| McpOAuthSecretCipherError::RandomnessUnavailable)?;
        let less_safe_key = LessSafeKey::new(
            UnboundKey::new(&AES_256_GCM, self.key_ring.active_key())
                .expect("32-byte AES-256-GCM key must be accepted"),
        );
        let associated_data = associated_data(purpose, record_key);
        less_safe_key
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce_bytes),
                Aad::from(associated_data.as_slice()),
                &mut *plaintext,
            )
            .map_err(|_| McpOAuthSecretCipherError::AuthenticationRejected)?;
        if plaintext.len() > MAX_ENCRYPTED_PLAINTEXT_BYTES + 16 {
            return Err(McpOAuthSecretCipherError::PayloadTooLarge);
        }
        Ok(EncryptedEnvelope {
            version: ENVELOPE_VERSION,
            key_id: self.key_ring.active_key_id.clone(),
            nonce: URL_SAFE_NO_PAD.encode(nonce_bytes),
            ciphertext: URL_SAFE_NO_PAD.encode(plaintext),
        })
    }

    fn open<T: DeserializeOwned>(
        &self,
        purpose: &str,
        record_key: &str,
        envelope: EncryptedEnvelope,
    ) -> Result<T, McpOAuthSecretCipherError> {
        if envelope.version != ENVELOPE_VERSION
            || !valid_key_id(&envelope.key_id)
            || envelope.nonce.len() > 64
            || envelope.ciphertext.len() > MAX_ENCODED_ENVELOPE_BYTES
        {
            return Err(McpOAuthSecretCipherError::EnvelopeRejected);
        }
        let nonce = URL_SAFE_NO_PAD
            .decode(envelope.nonce)
            .map_err(|_| McpOAuthSecretCipherError::EnvelopeRejected)?;
        let nonce: [u8; NONCE_LEN] = nonce
            .try_into()
            .map_err(|_| McpOAuthSecretCipherError::EnvelopeRejected)?;
        let mut ciphertext = Zeroizing::new(
            URL_SAFE_NO_PAD
                .decode(envelope.ciphertext)
                .map_err(|_| McpOAuthSecretCipherError::EnvelopeRejected)?,
        );
        if ciphertext.len() > MAX_ENCRYPTED_PLAINTEXT_BYTES + 16 {
            return Err(McpOAuthSecretCipherError::EnvelopeRejected);
        }
        let material = self
            .key_ring
            .decryption_key(&envelope.key_id)
            .ok_or(McpOAuthSecretCipherError::KeyUnavailable)?;
        let less_safe_key = LessSafeKey::new(
            UnboundKey::new(&AES_256_GCM, material)
                .expect("32-byte AES-256-GCM key must be accepted"),
        );
        let associated_data = associated_data(purpose, record_key);
        let plaintext = less_safe_key
            .open_in_place(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(associated_data.as_slice()),
                &mut ciphertext,
            )
            .map_err(|_| McpOAuthSecretCipherError::AuthenticationRejected)?;
        serde_json::from_slice(plaintext)
            .map_err(|_| McpOAuthSecretCipherError::DeserializationRejected)
    }
}

impl fmt::Debug for McpOAuthSecretCipher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthSecretCipher")
            .field("key_ring", &self.key_ring)
            .finish()
    }
}

#[derive(Clone, Deserialize, Serialize)]
struct EncryptedEnvelope {
    version: u8,
    key_id: String,
    nonce: String,
    ciphertext: String,
}

impl fmt::Debug for EncryptedEnvelope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EncryptedEnvelope")
            .field("version", &self.version)
            .field("key_id", &self.key_id)
            .field("nonce", &"[REDACTED]")
            .field("ciphertext", &"[REDACTED]")
            .finish()
    }
}

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
    /// Returns [`RedisMcpOAuthStoreConfigError::InvalidNamespace`] for blank or whitespace-bearing
    /// namespaces.
    pub fn with_namespace(
        connection: rustee_redis::redis::aio::ConnectionManager,
        cipher: McpOAuthSecretCipher,
        namespace: impl Into<String>,
    ) -> Result<Self, RedisMcpOAuthStoreConfigError> {
        let namespace = namespace.into();
        if !valid_namespace(&namespace) {
            return Err(RedisMcpOAuthStoreConfigError::InvalidNamespace);
        }
        Ok(Self {
            connection,
            namespace,
            cipher,
        })
    }

    fn key(&self, state: &str) -> String {
        format!("{}:{state}", self.namespace)
    }
}

impl fmt::Debug for RedisMcpOAuthTransactionStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisMcpOAuthTransactionStore")
            .field("namespace", &self.namespace)
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
            rustee_redis::set_json(&connection, &key, &envelope, ttl_seconds)
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
            let envelope = rustee_redis::take_json::<EncryptedEnvelope>(&connection, &key)
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
    /// Returns [`RedisMcpOAuthStoreConfigError::InvalidNamespace`] for a blank or unsafe namespace.
    pub fn with_namespace(
        connection: rustee_redis::redis::aio::ConnectionManager,
        cipher: McpOAuthSecretCipher,
        namespace: impl Into<String>,
    ) -> Result<Self, RedisMcpOAuthStoreConfigError> {
        let namespace = namespace.into();
        if !valid_namespace(&namespace) {
            return Err(RedisMcpOAuthStoreConfigError::InvalidNamespace);
        }
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
    /// Returns [`RedisMcpOAuthStoreConfigError::ZeroTokenTtl`] for a sub-second TTL.
    pub fn with_token_ttl(
        mut self,
        token_ttl: Duration,
    ) -> Result<Self, RedisMcpOAuthStoreConfigError> {
        if !valid_token_ttl(token_ttl) {
            return Err(RedisMcpOAuthStoreConfigError::ZeroTokenTtl);
        }
        self.token_ttl = token_ttl;
        Ok(self)
    }

    /// Returns the configured maximum Redis retention period.
    #[must_use]
    pub const fn token_ttl(&self) -> Duration {
        self.token_ttl
    }

    fn key(&self, key: &McpOAuthTokenStoreKey) -> String {
        format!("{}:{}", self.namespace, key.as_str())
    }
}

impl fmt::Debug for RedisMcpOAuthTokenStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisMcpOAuthTokenStore")
            .field("namespace", &self.namespace)
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
            let envelope = rustee_redis::get_json::<EncryptedEnvelope>(&connection, &key)
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
            rustee_redis::set_json(&connection, &key, &envelope, ttl_seconds)
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

/// Redis OAuth-store configuration failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RedisMcpOAuthStoreConfigError {
    /// Redis namespaces must be explicit and whitespace-free.
    #[error("Redis MCP OAuth namespace must be non-blank and contain no whitespace")]
    InvalidNamespace,
    /// Token retention must be finite and no shorter than one second.
    #[error("Redis MCP OAuth token retention TTL must be at least one second")]
    ZeroTokenTtl,
}

/// Redis transaction-store failures with no state or ciphertext detail.
#[derive(Debug, thiserror::Error)]
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

/// Redis token-store failures with no token, key, or ciphertext detail.
#[derive(Debug, thiserror::Error)]
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
    Delete(#[source] RedisError),
}

fn associated_data(purpose: &str, key: &str) -> Vec<u8> {
    format!("rustee:mcp-oauth:v{ENVELOPE_VERSION}:{purpose}:{key}").into_bytes()
}

fn valid_namespace(namespace: &str) -> bool {
    !namespace.trim().is_empty() && !namespace.chars().any(char::is_whitespace)
}

fn valid_key_id(key_id: &str) -> bool {
    !key_id.is_empty()
        && key_id.len() <= MAX_KEY_ID_BYTES
        && key_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
}

fn valid_token_ttl(token_ttl: Duration) -> bool {
    token_ttl.as_secs() > 0
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde::{Deserialize, Serialize};

    use super::{
        DEFAULT_TOKEN_NAMESPACE, DEFAULT_TRANSACTION_NAMESPACE, McpOAuthSecretCipher,
        McpOAuthSecretCipherError, McpOAuthSecretKeyRing, McpOAuthSecretKeyRingError, valid_key_id,
        valid_namespace, valid_token_ttl,
    };

    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    struct TestPayload {
        value: String,
    }

    #[test]
    fn encrypted_envelope_binds_its_purpose_and_redis_key() {
        let cipher =
            McpOAuthSecretCipher::new(McpOAuthSecretKeyRing::new("current", [7_u8; 32]).unwrap());
        let envelope = cipher
            .seal(
                "transaction",
                "rustee:test:transaction:state-a",
                &TestPayload {
                    value: "sensitive payload".to_owned(),
                },
            )
            .unwrap();
        assert!(!format!("{envelope:?}").contains("sensitive payload"));
        let decoded: TestPayload = cipher
            .open(
                "transaction",
                "rustee:test:transaction:state-a",
                envelope.clone(),
            )
            .unwrap();
        assert_eq!(decoded.value, "sensitive payload");
        assert_eq!(
            cipher
                .open::<TestPayload>("token", "rustee:test:token:user-a", envelope)
                .unwrap_err(),
            McpOAuthSecretCipherError::AuthenticationRejected
        );
    }

    #[test]
    fn key_rotation_reads_retired_envelopes_but_writes_with_the_new_active_key() {
        let old_cipher =
            McpOAuthSecretCipher::new(McpOAuthSecretKeyRing::new("old", [1_u8; 32]).unwrap());
        let old_envelope = old_cipher
            .seal(
                "token",
                "rustee:test:token:user-a",
                &TestPayload {
                    value: "old".to_owned(),
                },
            )
            .unwrap();
        let rotated = McpOAuthSecretCipher::new(
            McpOAuthSecretKeyRing::new("new", [2_u8; 32])
                .unwrap()
                .with_retired_key("old", [1_u8; 32])
                .unwrap(),
        );
        let decoded: TestPayload = rotated
            .open("token", "rustee:test:token:user-a", old_envelope)
            .unwrap();
        assert_eq!(decoded.value, "old");
        let new_envelope = rotated
            .seal(
                "token",
                "rustee:test:token:user-a",
                &TestPayload {
                    value: "new".to_owned(),
                },
            )
            .unwrap();
        assert_eq!(new_envelope.key_id, "new");
    }

    #[test]
    fn key_ring_and_redis_configuration_reject_unsafe_values() {
        assert_eq!(
            DEFAULT_TRANSACTION_NAMESPACE,
            "rustee:mcp:oauth:transaction:v1"
        );
        assert_eq!(DEFAULT_TOKEN_NAMESPACE, "rustee:mcp:oauth:token:v1");
        assert!(valid_namespace("customer-a:mcp:oauth:v1"));
        assert!(!valid_namespace(""));
        assert!(!valid_namespace("mcp oauth"));
        assert!(valid_key_id("kms-2026.08"));
        assert!(!valid_key_id("key id"));
        assert_eq!(
            McpOAuthSecretKeyRing::new("", [0_u8; 32]).unwrap_err(),
            McpOAuthSecretKeyRingError::InvalidKeyId
        );
        assert_eq!(
            McpOAuthSecretKeyRing::new("current", [0_u8; 32])
                .unwrap()
                .with_retired_key("current", [1_u8; 32])
                .unwrap_err(),
            McpOAuthSecretKeyRingError::DuplicateKeyId
        );
        assert!(!valid_token_ttl(Duration::ZERO));
    }
}
