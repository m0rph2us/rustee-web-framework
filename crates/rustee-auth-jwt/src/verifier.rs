use std::{fmt, sync::Arc};

use futures_util::future::BoxFuture;
use jsonwebtoken::{DecodingKey, Validation, decode};
use rustee_auth::{AuthError, BearerAuthenticator, Principal};

use crate::{
    JwtConfig, JwtConfigurationError,
    claims::VerifiedClaims,
    config::{
        hmac_secret_minimum_bytes, is_ec_algorithm, is_eddsa_algorithm, is_hmac_algorithm,
        is_rsa_algorithm, require_algorithm,
    },
};

const MAX_HMAC_SECRET_BYTES: usize = 4 * 1024;
const MAX_PUBLIC_KEY_PEM_BYTES: usize = 16 * 1024;

/// A static-key JWT verifier that implements Rustee bearer authentication.
#[derive(Clone)]
pub struct JwtAuthenticator {
    key: Arc<DecodingKey>,
    validation: Validation,
}

impl JwtAuthenticator {
    /// Creates an HMAC JWT verifier.
    ///
    /// Only HS256, HS384, and HS512 configurations are accepted. The secret is consumed by the
    /// decoding key and is never exposed through this type's debug output.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured algorithm is incompatible or the secret is blank,
    /// shorter than the JWA minimum for that algorithm, or oversized.
    pub fn from_hmac_secret(
        config: JwtConfig,
        secret: impl AsRef<[u8]>,
    ) -> Result<Self, JwtConfigurationError> {
        require_algorithm(config.algorithm(), "an HMAC secret", is_hmac_algorithm)?;
        let secret = secret.as_ref();
        if secret.is_empty() {
            return Err(JwtConfigurationError::BlankVerificationKey);
        }
        let minimum_bytes = hmac_secret_minimum_bytes(config.algorithm()).ok_or(
            JwtConfigurationError::AlgorithmKeyMismatch {
                algorithm: config.algorithm(),
                key_kind: "an HMAC secret",
            },
        )?;
        if secret.len() < minimum_bytes {
            return Err(JwtConfigurationError::VerificationKeyTooShort { minimum_bytes });
        }
        ensure_key_len(secret, MAX_HMAC_SECRET_BYTES)?;
        Ok(Self::with_key(config, DecodingKey::from_secret(secret)))
    }

    /// Creates an RSA or RSASSA-PSS JWT verifier from a PEM-encoded public key.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured algorithm is incompatible, the PEM is oversized, or
    /// the PEM is invalid.
    pub fn from_rsa_pem(
        config: JwtConfig,
        public_key_pem: &[u8],
    ) -> Result<Self, JwtConfigurationError> {
        require_algorithm(config.algorithm(), "an RSA public key", is_rsa_algorithm)?;
        ensure_key_len(public_key_pem, MAX_PUBLIC_KEY_PEM_BYTES)?;
        let key = DecodingKey::from_rsa_pem(public_key_pem)
            .map_err(|_| JwtConfigurationError::InvalidVerificationKey)?;
        Ok(Self::with_key(config, key))
    }

    /// Creates an ECDSA JWT verifier from a PEM-encoded public key.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured algorithm is incompatible, the PEM is oversized, or
    /// the PEM is invalid.
    pub fn from_ec_pem(
        config: JwtConfig,
        public_key_pem: &[u8],
    ) -> Result<Self, JwtConfigurationError> {
        require_algorithm(config.algorithm(), "an ECDSA public key", is_ec_algorithm)?;
        ensure_key_len(public_key_pem, MAX_PUBLIC_KEY_PEM_BYTES)?;
        let key = DecodingKey::from_ec_pem(public_key_pem)
            .map_err(|_| JwtConfigurationError::InvalidVerificationKey)?;
        Ok(Self::with_key(config, key))
    }

    /// Creates an `EdDSA` JWT verifier from a PEM-encoded public key.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured algorithm is incompatible, the PEM is oversized, or
    /// the PEM is invalid.
    pub fn from_ed_pem(
        config: JwtConfig,
        public_key_pem: &[u8],
    ) -> Result<Self, JwtConfigurationError> {
        require_algorithm(
            config.algorithm(),
            "an EdDSA public key",
            is_eddsa_algorithm,
        )?;
        ensure_key_len(public_key_pem, MAX_PUBLIC_KEY_PEM_BYTES)?;
        let key = DecodingKey::from_ed_pem(public_key_pem)
            .map_err(|_| JwtConfigurationError::InvalidVerificationKey)?;
        Ok(Self::with_key(config, key))
    }

    fn with_key(config: JwtConfig, key: DecodingKey) -> Self {
        Self {
            key: Arc::new(key),
            validation: config.into_validation(),
        }
    }
}

fn ensure_key_len(key: &[u8], maximum_bytes: usize) -> Result<(), JwtConfigurationError> {
    if key.len() > maximum_bytes {
        return Err(JwtConfigurationError::VerificationKeyTooLarge);
    }
    Ok(())
}

impl fmt::Debug for JwtAuthenticator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JwtAuthenticator")
            .field("algorithms", &self.validation.algorithms)
            .field("issuer_configured", &self.validation.iss.is_some())
            .field("audience_configured", &self.validation.aud.is_some())
            .finish_non_exhaustive()
    }
}

impl BearerAuthenticator for JwtAuthenticator {
    fn authenticate(&self, token: &str) -> BoxFuture<'static, Result<Principal, AuthError>> {
        let key = Arc::clone(&self.key);
        let validation = self.validation.clone();
        let token = token.to_owned();
        Box::pin(async move {
            let claims = decode::<VerifiedClaims>(&token, key.as_ref(), &validation)
                .map_err(|_| AuthError::RejectedBearerToken)?
                .claims;
            claims.into_principal()
        })
    }
}
