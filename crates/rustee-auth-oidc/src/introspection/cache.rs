//! Opaque token fingerprinting and bounded fail-closed principal caching.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rustee_auth::{AuthError, Principal};
use sha2::{Digest, Sha256};

/// Bounded cache keyed only by opaque token fingerprints.
#[derive(Clone)]
pub(super) struct OpaqueTokenCache {
    enabled: bool,
    max_entries: usize,
    entries: Arc<Mutex<BTreeMap<TokenFingerprint, CachedPrincipal>>>,
}

impl OpaqueTokenCache {
    pub(super) fn new(max_entries: usize, max_ttl: Duration) -> Self {
        Self {
            enabled: max_entries > 0 && !max_ttl.is_zero(),
            max_entries,
            entries: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub(super) fn fingerprint(token: &str) -> TokenFingerprint {
        TokenFingerprint(URL_SAFE_NO_PAD.encode(Sha256::digest(token.as_bytes())))
    }

    pub(super) fn cached_principal(
        &self,
        cache_key: &TokenFingerprint,
    ) -> Result<Option<Principal>, AuthError> {
        if !self.enabled {
            return Ok(None);
        }
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| AuthError::ProviderUnavailable)?;
        let now = Instant::now();
        entries.retain(|_, entry| entry.expires_at > now);
        Ok(entries.get(cache_key).map(|entry| entry.principal.clone()))
    }

    pub(super) fn cache_principal(
        &self,
        cache_key: TokenFingerprint,
        principal: Principal,
        ttl: Option<Duration>,
    ) -> Result<(), AuthError> {
        let Some(ttl) = ttl else {
            return Ok(());
        };
        if !self.enabled {
            return Ok(());
        }

        let mut entries = self
            .entries
            .lock()
            .map_err(|_| AuthError::ProviderUnavailable)?;
        let now = Instant::now();
        entries.retain(|_, entry| entry.expires_at > now);
        if entries.len() >= self.max_entries && !entries.contains_key(&cache_key) {
            return Ok(());
        }
        entries.insert(
            cache_key,
            CachedPrincipal {
                principal,
                expires_at: now + ttl,
            },
        );
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn poison_for_test(&self) {
        let entries = Arc::clone(&self.entries);
        let poison = std::thread::spawn(move || {
            let _guard = entries.lock().expect("new cache lock must be available");
            panic!("test must poison the opaque token cache lock");
        });
        assert!(poison.join().is_err());
    }
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct TokenFingerprint(String);

#[derive(Clone)]
struct CachedPrincipal {
    principal: Principal,
    expires_at: Instant,
}
