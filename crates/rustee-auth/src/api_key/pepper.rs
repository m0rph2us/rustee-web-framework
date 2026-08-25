use std::fmt;

use hmac::{Hmac, Mac};
use sha2::Sha256;
use zeroize::Zeroize;

use super::authenticator::{ApiKeyError, is_valid_api_key_value};

const API_KEY_PEPPER_BYTES: usize = 32;

/// Maximum number of prior peppers retained by [`ApiKeyPepperRing`].
///
/// The active pepper is always tried first, so this limit bounds one authentication to at most
/// three opaque store lookups during a deployment-managed migration window.
pub const MAX_RETIRED_API_KEY_PEPPERS: usize = 2;

/// Secret material used to derive API-key lookup fingerprints.
///
/// Load this value from a secret manager or a deployment-owned protected configuration source.
/// It is deliberately not serializable or printable.
pub struct ApiKeyPepper([u8; API_KEY_PEPPER_BYTES]);

impl ApiKeyPepper {
    /// Creates a pepper from exactly 256 bits of deployment-owned secret material.
    ///
    /// # Errors
    ///
    /// Returns [`ApiKeyPepperError::AllZero`] for the all-zero value, which would not provide a
    /// deployment-held secret for the keyed derivation.
    pub fn new(bytes: [u8; API_KEY_PEPPER_BYTES]) -> Result<Self, ApiKeyPepperError> {
        if bytes.iter().all(|byte| *byte == 0) {
            return Err(ApiKeyPepperError::AllZero);
        }
        Ok(Self(bytes))
    }

    /// Derives a bounded, keyed lookup fingerprint without exposing the API key to a store.
    ///
    /// # Errors
    ///
    /// Returns [`ApiKeyError::InvalidApiKey`] when `api_key` cannot appear as a valid API-key
    /// header value. A failure to initialize the HMAC implementation is mapped to
    /// [`ApiKeyError::ProviderUnavailable`].
    pub fn fingerprint(&self, api_key: &str) -> Result<ApiKeyFingerprint, ApiKeyError> {
        if !is_valid_api_key_value(api_key) {
            return Err(ApiKeyError::InvalidApiKey);
        }

        self.fingerprint_valid_api_key(api_key)
    }

    fn fingerprint_valid_api_key(&self, api_key: &str) -> Result<ApiKeyFingerprint, ApiKeyError> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.0)
            .map_err(|_| ApiKeyError::ProviderUnavailable)?;
        mac.update(api_key.as_bytes());
        Ok(ApiKeyFingerprint(mac.finalize().into_bytes().into()))
    }

    fn same_material(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Clone for ApiKeyPepper {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

impl Drop for ApiKeyPepper {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl fmt::Debug for ApiKeyPepper {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApiKeyPepper([redacted])")
    }
}

/// Invalid deployment-owned API-key pepper material.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ApiKeyPepperError {
    /// The all-zero value is not a deployment-held secret.
    #[error("API-key pepper must not be all zero")]
    AllZero,
}

/// A bounded active-and-retired API-key pepper set for a deployment-managed migration window.
///
/// This type derives fingerprints only; it does not persist raw keys, add replacement
/// fingerprints to a store, select a KMS, or decide when a retired pepper can be removed.
#[derive(Clone)]
pub struct ApiKeyPepperRing {
    active: ApiKeyPepper,
    retired: Vec<ApiKeyPepper>,
}

impl ApiKeyPepperRing {
    /// Creates a ring with one active pepper and no dual-read migration window.
    #[must_use]
    pub fn new(active: ApiKeyPepper) -> Self {
        Self {
            active,
            retired: Vec::new(),
        }
    }

    /// Creates a ring that tries the active pepper before distinct retained peppers.
    ///
    /// # Errors
    ///
    /// Returns [`ApiKeyPepperRingError::TooManyRetired`] when `retired` exceeds
    /// [`MAX_RETIRED_API_KEY_PEPPERS`], or [`ApiKeyPepperRingError::DuplicatePepper`] when the
    /// active or any retained pepper repeats.
    pub fn with_retired(
        active: ApiKeyPepper,
        retired: impl IntoIterator<Item = ApiKeyPepper>,
    ) -> Result<Self, ApiKeyPepperRingError> {
        let mut retained = Vec::with_capacity(MAX_RETIRED_API_KEY_PEPPERS);
        for candidate in retired {
            if retained.len() == MAX_RETIRED_API_KEY_PEPPERS {
                return Err(ApiKeyPepperRingError::TooManyRetired);
            }
            if candidate.same_material(&active)
                || retained
                    .iter()
                    .any(|previous| candidate.same_material(previous))
            {
                return Err(ApiKeyPepperRingError::DuplicatePepper);
            }
            retained.push(candidate);
        }
        Ok(Self {
            active,
            retired: retained,
        })
    }

    pub(super) fn fingerprints(
        &self,
        api_key: &str,
    ) -> Result<Vec<ApiKeyFingerprint>, ApiKeyError> {
        if !is_valid_api_key_value(api_key) {
            return Err(ApiKeyError::InvalidApiKey);
        }

        let mut fingerprints = Vec::with_capacity(1 + self.retired.len());
        fingerprints.push(self.active.fingerprint_valid_api_key(api_key)?);
        for pepper in &self.retired {
            fingerprints.push(pepper.fingerprint_valid_api_key(api_key)?);
        }
        Ok(fingerprints)
    }
}

impl fmt::Debug for ApiKeyPepperRing {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApiKeyPepperRing")
            .field("retired_pepper_count", &self.retired.len())
            .finish_non_exhaustive()
    }
}

/// Invalid active-and-retired pepper configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ApiKeyPepperRingError {
    /// Retaining more peppers would exceed the bounded authentication lookup contract.
    #[error("API-key pepper ring has too many retired peppers")]
    TooManyRetired,
    /// Repeated pepper material would create redundant store lookups.
    #[error("API-key pepper ring peppers must be distinct")]
    DuplicatePepper,
}

/// Opaque HMAC-SHA-256 lookup value for one API key.
///
/// The value is not the API key and has no text rendering. It can be used as the primary lookup
/// key in a provider store, alongside that provider's active/revoked state and audit transaction.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiKeyFingerprint([u8; 32]);

impl ApiKeyFingerprint {
    /// Returns the fixed-size binary value for a protected provider lookup.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for ApiKeyFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApiKeyFingerprint([redacted])")
    }
}
