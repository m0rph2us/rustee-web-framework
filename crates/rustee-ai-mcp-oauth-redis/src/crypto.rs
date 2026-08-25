//! Bounded AES-256-GCM envelopes for MCP OAuth Redis records.

use std::{
    collections::BTreeMap,
    fmt,
    io::{self, Write},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::{
    aead::{AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey},
    rand::{SecureRandom, SystemRandom},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use zeroize::Zeroizing;

const ENVELOPE_VERSION: u8 = 1;
const MAX_KEY_ID_BYTES: usize = 128;
pub(super) const MAX_ENCRYPTED_PLAINTEXT_BYTES: usize = 64 * 1024;
const MAX_ENCODED_CIPHERTEXT_BYTES: usize = 96 * 1024;
/// Maximum serialized Redis JSON record for one encrypted envelope.
///
/// The budget includes base64 ciphertext, the bounded key ID and nonce, plus JSON framing.
pub(super) const MAX_SERIALIZED_ENVELOPE_BYTES: usize = 128 * 1024;

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

    pub(super) fn seal<T: Serialize>(
        &self,
        purpose: &str,
        record_key: &str,
        value: &T,
    ) -> Result<EncryptedEnvelope, McpOAuthSecretCipherError> {
        let mut plaintext = bounded_plaintext(value)?;
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

    pub(super) fn open<T: DeserializeOwned>(
        &self,
        purpose: &str,
        record_key: &str,
        envelope: EncryptedEnvelope,
    ) -> Result<T, McpOAuthSecretCipherError> {
        if envelope.version != ENVELOPE_VERSION
            || !valid_key_id(&envelope.key_id)
            || envelope.nonce.len() > 64
            || envelope.ciphertext.len() > MAX_ENCODED_CIPHERTEXT_BYTES
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

fn bounded_plaintext<T: Serialize>(
    value: &T,
) -> Result<Zeroizing<Vec<u8>>, McpOAuthSecretCipherError> {
    let mut buffer = BoundedZeroizingJsonBuffer::new(MAX_ENCRYPTED_PLAINTEXT_BYTES);
    let result = serde_json::to_writer(&mut buffer, value);

    if buffer.exceeded {
        return Err(McpOAuthSecretCipherError::PayloadTooLarge);
    }

    result.map_err(|_| McpOAuthSecretCipherError::SerializationRejected)?;
    Ok(buffer.into_inner())
}

struct BoundedZeroizingJsonBuffer {
    bytes: Zeroizing<Vec<u8>>,
    max_bytes: usize,
    exceeded: bool,
}

impl BoundedZeroizingJsonBuffer {
    fn new(max_bytes: usize) -> Self {
        Self {
            bytes: Zeroizing::new(Vec::new()),
            max_bytes,
            exceeded: false,
        }
    }

    fn into_inner(self) -> Zeroizing<Vec<u8>> {
        self.bytes
    }
}

impl Write for BoundedZeroizingJsonBuffer {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.len() > self.max_bytes.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "encrypted plaintext limit exceeded",
            ));
        }

        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
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
pub(super) struct EncryptedEnvelope {
    version: u8,
    key_id: String,
    nonce: String,
    ciphertext: String,
}

impl EncryptedEnvelope {
    #[cfg(test)]
    pub(super) fn key_id(&self) -> &str {
        &self.key_id
    }
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

fn associated_data(purpose: &str, key: &str) -> Vec<u8> {
    format!("rustee:mcp-oauth:v{ENVELOPE_VERSION}:{purpose}:{key}").into_bytes()
}

pub(super) fn valid_key_id(key_id: &str) -> bool {
    !key_id.is_empty()
        && key_id.len() <= MAX_KEY_ID_BYTES
        && key_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
}
