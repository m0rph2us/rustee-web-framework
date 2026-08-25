//! Token-slot identity, encrypted-store contracts, and bounded local persistence.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};

use futures_util::future::BoxFuture;

use super::McpOAuthTokenSet;
use crate::config::MAX_CLIENT_ID_BYTES;
use crate::{InMemoryMcpOAuthStoreError, McpOAuthConfigError};

pub(super) const MAX_IN_MEMORY_TOKEN_SETS: usize = 1_024;

/// Application-owned key that identifies one tenant/user/connection token slot.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct McpOAuthTokenStoreKey(String);

impl McpOAuthTokenStoreKey {
    /// Creates an opaque bounded key chosen by the application token-ownership policy.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthConfigError::InvalidTokenStoreKey`] for blank, oversized, or control
    /// character-bearing values.
    pub fn new(value: impl Into<String>) -> Result<Self, McpOAuthConfigError> {
        let value = value.into();
        if value.trim().is_empty()
            || value.len() > MAX_CLIENT_ID_BYTES
            || value.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(McpOAuthConfigError::InvalidTokenStoreKey);
        }
        Ok(Self(value))
    }

    /// Returns this application-owned key for a dedicated token-store adapter.
    ///
    /// Store adapters must treat the value as tenant/user/connection metadata and avoid logging
    /// it with token payloads or provider responses.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for McpOAuthTokenStoreKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpOAuthTokenStoreKey([REDACTED])")
    }
}

/// Encrypted, tenant/user-bound persistence boundary for MCP OAuth token sets.
///
/// Implementations own encryption, key rotation, tenant/user authorization, retention, and
/// cross-instance refresh coordination. The local in-memory implementation is deliberately not
/// a production credential store.
pub trait McpOAuthTokenStore: Clone + Send + Sync + 'static {
    /// Store-specific failure type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Loads the currently persisted token set for one application-owned key.
    fn load(
        &self,
        key: McpOAuthTokenStoreKey,
    ) -> BoxFuture<'static, Result<Option<McpOAuthTokenSet>, Self::Error>>;

    /// Atomically replaces the token set after authorization or a successful refresh.
    fn save(
        &self,
        key: McpOAuthTokenStoreKey,
        tokens: McpOAuthTokenSet,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;

    /// Deletes a revoked or disconnected token set.
    fn remove(&self, key: McpOAuthTokenStoreKey) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// Process-local plain-memory token store for tests and single-instance development only.
///
/// The store retains at most 1,024 application-owned token slots. It never evicts a token set:
/// callers must explicitly revoke or remove stale slots, while updates to an existing slot remain
/// available at capacity.
#[derive(Clone, Default)]
pub struct InMemoryMcpOAuthTokenStore {
    pub(super) tokens: Arc<Mutex<BTreeMap<McpOAuthTokenStoreKey, McpOAuthTokenSet>>>,
}

impl fmt::Debug for InMemoryMcpOAuthTokenStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemoryMcpOAuthTokenStore")
            .finish_non_exhaustive()
    }
}

impl McpOAuthTokenStore for InMemoryMcpOAuthTokenStore {
    type Error = InMemoryMcpOAuthStoreError;

    fn load(
        &self,
        key: McpOAuthTokenStoreKey,
    ) -> BoxFuture<'static, Result<Option<McpOAuthTokenSet>, Self::Error>> {
        let tokens = Arc::clone(&self.tokens);
        Box::pin(async move {
            Ok(tokens
                .lock()
                .map_err(|_| InMemoryMcpOAuthStoreError::StateUnavailable)?
                .get(&key)
                .cloned())
        })
    }

    fn save(
        &self,
        key: McpOAuthTokenStoreKey,
        token_set: McpOAuthTokenSet,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let tokens = Arc::clone(&self.tokens);
        Box::pin(async move {
            let mut tokens = tokens
                .lock()
                .map_err(|_| InMemoryMcpOAuthStoreError::StateUnavailable)?;
            if !tokens.contains_key(&key) && tokens.len() >= MAX_IN_MEMORY_TOKEN_SETS {
                return Err(InMemoryMcpOAuthStoreError::TokenCapacityExhausted);
            }
            tokens.insert(key, token_set);
            Ok(())
        })
    }

    fn remove(&self, key: McpOAuthTokenStoreKey) -> BoxFuture<'static, Result<(), Self::Error>> {
        let tokens = Arc::clone(&self.tokens);
        Box::pin(async move {
            tokens
                .lock()
                .map_err(|_| InMemoryMcpOAuthStoreError::StateUnavailable)?
                .remove(&key);
            Ok(())
        })
    }
}
