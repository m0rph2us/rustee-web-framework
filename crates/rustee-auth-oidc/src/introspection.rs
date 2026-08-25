//! OAuth 2.0 opaque access-token introspection support.
//!
//! The configured introspection endpoint is a trusted authentication dependency. A successful
//! response is still checked for its active state, issuer, audience, and any supplied time
//! bounds before it becomes a [`Principal`]. Only a SHA-256 fingerprint, never the raw token,
//! is used as a bounded in-memory cache key.

use std::{
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};

use futures_util::future::BoxFuture;
use rustee_auth::{AuthError, BearerAuthenticator, Principal};

mod cache;
mod config;
mod model;
mod transport;

use cache::OpaqueTokenCache;

pub use config::{OpaqueIntrospectionConfig, OpaqueIntrospectionConfigError};
pub use model::OpaqueTokenIntrospection;
pub use transport::{
    HttpOpaqueTokenIntrospector, OpaqueTokenIntrospectionRequest, OpaqueTokenIntrospector,
};

/// Opaque bearer authenticator with a bounded cache of active, expiring token fingerprints.
#[derive(Clone)]
pub struct OpaqueTokenAuthenticator<I> {
    config: OpaqueIntrospectionConfig,
    introspector: I,
    cache: OpaqueTokenCache,
}

impl<I> OpaqueTokenAuthenticator<I>
where
    I: OpaqueTokenIntrospector,
{
    /// Creates an opaque bearer authenticator with an empty response cache.
    #[must_use]
    pub fn new(config: OpaqueIntrospectionConfig, introspector: I) -> Self {
        let cache = OpaqueTokenCache::new(config.max_cache_entries, config.cache_ttl);
        Self {
            config,
            introspector,
            cache,
        }
    }
}

impl<I> fmt::Debug for OpaqueTokenAuthenticator<I> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpaqueTokenAuthenticator")
            .field("config", &self.config)
            .field("introspector", &std::any::type_name::<I>())
            .finish_non_exhaustive()
    }
}

impl<I> BearerAuthenticator for OpaqueTokenAuthenticator<I>
where
    I: OpaqueTokenIntrospector,
{
    fn authenticate(&self, token: &str) -> BoxFuture<'static, Result<Principal, AuthError>> {
        let this = self.clone();
        let token = token.to_owned();
        Box::pin(async move {
            let cache_key = OpaqueTokenCache::fingerprint(&token);
            if let Some(principal) = this.cache.cached_principal(&cache_key)? {
                return Ok(principal);
            }

            let request = OpaqueTokenIntrospectionRequest::new(
                token,
                this.config.client_id.clone(),
                this.config.authentication.clone(),
            );
            let result = this
                .introspector
                .introspect(this.config.endpoint.clone(), request)
                .await
                .map_err(|_| AuthError::ProviderUnavailable)?;
            let (principal, cache_ttl) = result.validated_principal(&this.config)?;
            this.cache
                .cache_principal(cache_key, principal.clone(), cache_ttl)?;
            Ok(principal)
        })
    }
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests;
