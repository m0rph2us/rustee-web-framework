//! Trusted JWKS retrieval, key rotation, and signed-token verification.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures_util::future::BoxFuture;
use jsonwebtoken::{
    Algorithm, DecodingKey, decode_header,
    jwk::{Jwk, JwkSet, KeyAlgorithm, KeyOperations, PublicKeyUse},
};
use rustee_auth::{AuthError, BearerAuthenticator, Principal};
use tokio::sync::{Mutex, RwLock};

use super::resource_server_config::OidcResourceServerConfig;
use crate::claims::{Claims, IdTokenClaims};

mod transport;

pub use transport::{HttpJwksFetcher, JwksFetcher};

/// Verifies an OIDC ID token and proves that it belongs to the browser login transaction.
pub trait IdTokenVerifier: Clone + Send + Sync + 'static {
    /// Validates one ID token and its nonce.
    fn verify_id_token(
        &self,
        token: &str,
        expected_nonce: &str,
    ) -> BoxFuture<'static, Result<Principal, AuthError>>;
}

/// JWKS-backed bearer verifier with cache expiry and key-rotation behavior.
#[derive(Clone)]
pub struct JwksAuthenticator<F> {
    config: OidcResourceServerConfig,
    fetcher: F,
    cache: Arc<RwLock<JwksCache>>,
    refresh_gate: Arc<Mutex<()>>,
}

impl<F> JwksAuthenticator<F>
where
    F: JwksFetcher,
{
    /// Creates an empty verifier.
    ///
    /// Call [`Self::refresh`] during startup when an application must fail fast if its identity
    /// provider is unavailable. Otherwise, the first token validation loads the JWKS lazily.
    #[must_use]
    pub fn new(config: OidcResourceServerConfig, fetcher: F) -> Self {
        Self {
            config,
            fetcher,
            cache: Arc::new(RwLock::new(JwksCache::default())),
            refresh_gate: Arc::new(Mutex::new(())),
        }
    }

    /// Fetches and replaces the cached keys with compatible entries from the configured JWKS.
    ///
    /// Duplicate key IDs and JWKs with an incompatible algorithm, key use, or key operation are
    /// excluded rather than selecting an arbitrary remote key.
    ///
    /// # Errors
    ///
    /// Returns the fetcher's error when the remote JWKS cannot be retrieved or decoded.
    pub async fn refresh(&self) -> Result<(), F::Error> {
        let _refresh_guard = self.refresh_gate.lock().await;
        self.fetch_and_store(RefreshReason::Explicit).await
    }

    async fn key_for(&self, kid: &str) -> Result<Option<DecodingKey>, KeyLookupError<F::Error>> {
        match self.cache_lookup(kid).await {
            CacheLookup::Key(key) => return Ok(Some(key)),
            CacheLookup::Missing => return Ok(None),
            CacheLookup::Unavailable => return Err(KeyLookupError::Unavailable),
            CacheLookup::Refresh(_) => {}
        }

        let _refresh_guard = self.refresh_gate.lock().await;
        match self.cache_lookup(kid).await {
            CacheLookup::Key(key) => Ok(Some(key)),
            CacheLookup::Missing => Ok(None),
            CacheLookup::Unavailable => Err(KeyLookupError::Unavailable),
            CacheLookup::Refresh(reason) => {
                self.fetch_and_store(reason)
                    .await
                    .map_err(KeyLookupError::Fetch)?;
                Ok(self.cache.read().await.keys.get(kid).cloned())
            }
        }
    }

    async fn verification_key(&self, token: &str) -> Result<DecodingKey, AuthError> {
        let header = decode_header(token).map_err(|_| AuthError::RejectedBearerToken)?;
        if header.alg != self.config.algorithm() {
            return Err(AuthError::RejectedBearerToken);
        }
        let kid = header.kid.ok_or(AuthError::RejectedBearerToken)?;
        self.key_for(&kid)
            .await
            .map_err(|_| AuthError::ProviderUnavailable)?
            .ok_or(AuthError::RejectedBearerToken)
    }

    async fn cache_lookup(&self, kid: &str) -> CacheLookup {
        let cache = self.cache.read().await;
        let key = cache.keys.get(kid).cloned();
        let has_key = key.is_some();
        let fresh = cache
            .last_successful_refresh
            .is_some_and(|refreshed_at| refreshed_at.elapsed() < self.config.cache_ttl());

        if fresh && let Some(key) = key {
            return CacheLookup::Key(key);
        }
        if !has_key && !cache.unknown_key_refresh_allowed(self.config.minimum_refresh_interval()) {
            return if cache.last_refresh_failed {
                CacheLookup::Unavailable
            } else {
                CacheLookup::Missing
            };
        }
        if has_key && !cache.refresh_allowed(self.config.minimum_refresh_interval()) {
            return CacheLookup::Unavailable;
        }
        if has_key {
            CacheLookup::Refresh(RefreshReason::ExpiredCache)
        } else {
            CacheLookup::Refresh(RefreshReason::UnknownKey)
        }
    }

    async fn fetch_and_store(&self, reason: RefreshReason) -> Result<(), F::Error> {
        {
            let mut cache = self.cache.write().await;
            let now = Instant::now();
            cache.last_refresh_attempt = Some(now);
            if matches!(reason, RefreshReason::UnknownKey) {
                cache.last_unknown_key_refresh = Some(now);
            }
        }

        let result = self.fetcher.fetch().await;
        let mut cache = self.cache.write().await;
        match result {
            Ok(set) => {
                cache.keys = compatible_keys(set, self.config.algorithm());
                cache.last_successful_refresh = Some(Instant::now());
                cache.last_refresh_failed = false;
                Ok(())
            }
            Err(error) => {
                cache.last_refresh_failed = true;
                Err(error)
            }
        }
    }
}

impl<F> fmt::Debug for JwksAuthenticator<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JwksAuthenticator")
            .field("algorithm", &self.config.algorithm())
            .field("issuer", &"[REDACTED]")
            .field("cache_ttl", &self.config.cache_ttl())
            .finish_non_exhaustive()
    }
}

impl<F> BearerAuthenticator for JwksAuthenticator<F>
where
    F: JwksFetcher,
{
    fn authenticate(&self, token: &str) -> BoxFuture<'static, Result<Principal, AuthError>> {
        let this = self.clone();
        let token = token.to_owned();
        Box::pin(async move {
            let key = this.verification_key(&token).await?;
            let claims = jsonwebtoken::decode::<Claims>(&token, &key, &this.config.validation())
                .map_err(|_| AuthError::RejectedBearerToken)?
                .claims;
            claims.into_principal()
        })
    }
}

impl<F> IdTokenVerifier for JwksAuthenticator<F>
where
    F: JwksFetcher,
{
    fn verify_id_token(
        &self,
        token: &str,
        expected_nonce: &str,
    ) -> BoxFuture<'static, Result<Principal, AuthError>> {
        let this = self.clone();
        let token = token.to_owned();
        let expected_nonce = expected_nonce.to_owned();
        Box::pin(async move {
            if expected_nonce.is_empty() {
                return Err(AuthError::RejectedBearerToken);
            }
            let key = this.verification_key(&token).await?;
            let claims = jsonwebtoken::decode::<IdTokenClaims>(
                &token,
                &key,
                &this.config.id_token_validation(),
            )
            .map_err(|_| AuthError::RejectedBearerToken)?
            .claims;
            let latest_issued_at = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .map(|duration| {
                    duration
                        .as_secs()
                        .saturating_add(this.config.leeway_seconds())
                })
                .ok_or(AuthError::RejectedBearerToken)?;
            if !claims.matches_browser_login_binding(
                this.config.audience(),
                &expected_nonce,
                latest_issued_at,
            ) {
                return Err(AuthError::RejectedBearerToken);
            }
            claims.into_principal()
        })
    }
}

#[derive(Default)]
struct JwksCache {
    keys: BTreeMap<String, DecodingKey>,
    last_successful_refresh: Option<Instant>,
    last_refresh_attempt: Option<Instant>,
    last_unknown_key_refresh: Option<Instant>,
    last_refresh_failed: bool,
}

impl JwksCache {
    fn refresh_allowed(&self, minimum_refresh_interval: Duration) -> bool {
        self.last_refresh_attempt
            .is_none_or(|attempted_at| attempted_at.elapsed() >= minimum_refresh_interval)
    }

    fn unknown_key_refresh_allowed(&self, minimum_refresh_interval: Duration) -> bool {
        self.last_unknown_key_refresh
            .is_none_or(|attempted_at| attempted_at.elapsed() >= minimum_refresh_interval)
    }
}

enum CacheLookup {
    Key(DecodingKey),
    Missing,
    Refresh(RefreshReason),
    Unavailable,
}

#[derive(Clone, Copy)]
enum RefreshReason {
    Explicit,
    ExpiredCache,
    UnknownKey,
}

enum KeyLookupError<E> {
    Fetch(E),
    Unavailable,
}

fn compatible_keys(set: JwkSet, algorithm: Algorithm) -> BTreeMap<String, DecodingKey> {
    let mut keys = BTreeMap::new();
    let mut seen = BTreeSet::new();
    let mut duplicates = BTreeSet::new();

    for jwk in set.keys {
        let Some(kid) = jwk.common.key_id.clone() else {
            continue;
        };
        if !seen.insert(kid.clone()) {
            duplicates.insert(kid);
            continue;
        }
        if is_compatible_jwk(&jwk, algorithm)
            && let Ok(key) = DecodingKey::from_jwk(&jwk)
        {
            keys.insert(kid, key);
        }
    }
    for duplicate in duplicates {
        keys.remove(&duplicate);
    }
    keys
}

fn is_compatible_jwk(jwk: &Jwk, algorithm: Algorithm) -> bool {
    matches!(
        (algorithm, jwk.common.key_algorithm),
        (Algorithm::RS256, Some(KeyAlgorithm::RS256))
            | (Algorithm::RS384, Some(KeyAlgorithm::RS384))
            | (Algorithm::RS512, Some(KeyAlgorithm::RS512))
            | (Algorithm::PS256, Some(KeyAlgorithm::PS256))
            | (Algorithm::PS384, Some(KeyAlgorithm::PS384))
            | (Algorithm::PS512, Some(KeyAlgorithm::PS512))
            | (Algorithm::ES256, Some(KeyAlgorithm::ES256))
            | (Algorithm::ES384, Some(KeyAlgorithm::ES384))
            | (Algorithm::EdDSA, Some(KeyAlgorithm::EdDSA))
    ) && matches!(
        jwk.common.public_key_use,
        None | Some(PublicKeyUse::Signature)
    ) && jwk
        .common
        .key_operations
        .as_ref()
        .is_none_or(|operations| operations.contains(&KeyOperations::Verify))
}
