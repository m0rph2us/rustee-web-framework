//! API-key value admission and provider-facing authentication contracts.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    fmt,
};

use crate::Principal;
use futures_util::future::BoxFuture;

use super::pepper::{ApiKeyFingerprint, ApiKeyPepper, ApiKeyPepperRing};

const MAX_API_KEY_BYTES: usize = 4 * 1024;

/// A production-facing API-key store that receives only a keyed fingerprint.
///
/// The store owns persistence and maps unknown, revoked, expired, or disabled keys to
/// [`ApiKeyError::RejectedApiKey`]. It may atomically update last-used/audit records with a
/// successful lookup, but must not record the raw API key or fingerprint in general logs.
pub trait ApiKeyFingerprintStore: Clone + Send + Sync + 'static {
    /// Resolves one keyed fingerprint to a validated principal.
    fn authenticate(
        &self,
        fingerprint: ApiKeyFingerprint,
    ) -> BoxFuture<'static, Result<Principal, ApiKeyError>>;
}

/// [`ApiKeyAuthenticator`] implementation that sends only a keyed fingerprint to its store.
///
/// Rotation and revocation are represented by store records: deployments can keep multiple active
/// fingerprints for one principal during client-key rotation and reject a revoked record without
/// changing the HTTP layer.
#[derive(Clone)]
pub struct KeyedApiKeyAuthenticator<S> {
    pepper: ApiKeyPepper,
    store: S,
}

impl<S> KeyedApiKeyAuthenticator<S> {
    /// Creates an API-key authenticator that derives HMAC-SHA-256 lookup fingerprints.
    #[must_use]
    pub fn new(pepper: ApiKeyPepper, store: S) -> Self {
        Self { pepper, store }
    }
}

impl<S> fmt::Debug for KeyedApiKeyAuthenticator<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KeyedApiKeyAuthenticator")
            .field("store", &std::any::type_name::<S>())
            .finish_non_exhaustive()
    }
}

/// [`ApiKeyAuthenticator`] implementation with a bounded active-and-retired pepper window.
///
/// The active fingerprint is looked up first. A retained fingerprint is attempted only after a
/// store returns [`ApiKeyError::RejectedApiKey`]; any other store result, including provider
/// unavailability, stops the sequence immediately. This supports dual-read during a
/// deployment-managed pepper migration without turning a provider outage into a credential miss.
#[derive(Clone)]
pub struct RotatingKeyedApiKeyAuthenticator<S> {
    peppers: ApiKeyPepperRing,
    store: S,
}

impl<S> RotatingKeyedApiKeyAuthenticator<S> {
    /// Creates an API-key authenticator for a bounded pepper migration window.
    #[must_use]
    pub fn new(peppers: ApiKeyPepperRing, store: S) -> Self {
        Self { peppers, store }
    }
}

impl<S> fmt::Debug for RotatingKeyedApiKeyAuthenticator<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RotatingKeyedApiKeyAuthenticator")
            .field("store", &std::any::type_name::<S>())
            .finish_non_exhaustive()
    }
}

/// A failure that is safe to render as an API-key authentication rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ApiKeyError {
    /// The configured API-key header was not present.
    #[error("missing API key")]
    MissingApiKey,
    /// The API-key header was malformed, repeated, or outside the accepted bound.
    #[error("invalid API key")]
    InvalidApiKey,
    /// A provider rejected a syntactically valid API key.
    #[error("API key was rejected")]
    RejectedApiKey,
    /// Required API-key authentication infrastructure could not be reached safely.
    #[error("API-key authentication provider is unavailable")]
    ProviderUnavailable,
}

/// A provider-specific verifier of one API-key header value.
///
/// Implementations receive a bounded printable ASCII value from [`crate::ApiKeyLayer`] and return
/// only a validated [`Principal`]. They must not log the key and should use a constant-time
/// comparison or a secret-safe lookup when handling production credentials.
pub trait ApiKeyAuthenticator: Clone + Send + Sync + 'static {
    /// Verifies a raw API-key header value and returns only a validated principal.
    fn authenticate(&self, api_key: &str) -> BoxFuture<'static, Result<Principal, ApiKeyError>>;
}

impl<S> ApiKeyAuthenticator for KeyedApiKeyAuthenticator<S>
where
    S: ApiKeyFingerprintStore,
{
    fn authenticate(&self, api_key: &str) -> BoxFuture<'static, Result<Principal, ApiKeyError>> {
        let fingerprint = self.pepper.fingerprint(api_key);
        let store = self.store.clone();
        Box::pin(async move { store.authenticate(fingerprint?).await })
    }
}

impl<S> ApiKeyAuthenticator for RotatingKeyedApiKeyAuthenticator<S>
where
    S: ApiKeyFingerprintStore,
{
    fn authenticate(&self, api_key: &str) -> BoxFuture<'static, Result<Principal, ApiKeyError>> {
        let fingerprints = self.peppers.fingerprints(api_key);
        let store = self.store.clone();
        Box::pin(async move {
            for fingerprint in fingerprints? {
                match store.authenticate(fingerprint).await {
                    Ok(principal) => return Ok(principal),
                    Err(ApiKeyError::RejectedApiKey) => {}
                    Err(error) => return Err(error),
                }
            }
            Err(ApiKeyError::RejectedApiKey)
        })
    }
}

/// A deliberately simple static API-key authenticator for tests and local examples only.
///
/// Production applications should keep only a derived lookup value in their identity provider and
/// make its comparison and rotation policy explicit.
#[derive(Clone, Default)]
pub struct StaticApiKeyAuthenticator {
    keys: BTreeMap<String, Principal>,
}

impl StaticApiKeyAuthenticator {
    /// Creates an empty local API-key authenticator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one local API-key-to-principal mapping.
    ///
    /// # Errors
    ///
    /// Returns [`StaticApiKeyError::InvalidKey`] when `api_key` cannot appear as a bounded
    /// printable API-key header value, or [`StaticApiKeyError::DuplicateKey`] when `api_key` is
    /// already registered.
    pub fn insert(
        &mut self,
        api_key: impl Into<String>,
        principal: Principal,
    ) -> Result<(), StaticApiKeyError> {
        let api_key = api_key.into();
        if !is_valid_api_key_value(&api_key) {
            return Err(StaticApiKeyError::InvalidKey);
        }
        match self.keys.entry(api_key) {
            Entry::Vacant(entry) => {
                entry.insert(principal);
                Ok(())
            }
            Entry::Occupied(_) => Err(StaticApiKeyError::DuplicateKey),
        }
    }
}

impl fmt::Debug for StaticApiKeyAuthenticator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StaticApiKeyAuthenticator")
            .field("registered_keys", &self.keys.len())
            .finish()
    }
}

impl ApiKeyAuthenticator for StaticApiKeyAuthenticator {
    fn authenticate(&self, api_key: &str) -> BoxFuture<'static, Result<Principal, ApiKeyError>> {
        let principal = self.keys.get(api_key).cloned();
        Box::pin(async move { principal.ok_or(ApiKeyError::RejectedApiKey) })
    }
}

/// Invalid local static API-key authenticator configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StaticApiKeyError {
    /// A static key was blank, too large, or not printable ASCII.
    #[error("static API key must be printable ASCII and at most {MAX_API_KEY_BYTES} bytes")]
    InvalidKey,
    /// An API key already maps to one local principal.
    #[error("static API key is already registered")]
    DuplicateKey,
}

pub(super) fn is_valid_api_key_value(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_API_KEY_BYTES
        && value.bytes().all(|byte| (b'!'..=b'~').contains(&byte))
}
