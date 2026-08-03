//! Redis-backed, one-time OIDC Authorization Code + PKCE transaction persistence.
//!
//! Each transaction is stored under a caller-visible versioned namespace with its remaining TTL.
//! Callback completion uses Redis `GETDEL`, atomically consuming `state`, nonce, and PKCE verifier
//! before any provider token exchange can occur.

use std::fmt;

use futures_util::future::BoxFuture;
use rustee_auth_oidc::{AuthorizationTransactionStore, PendingAuthorization};
use rustee_redis::CacheError;

/// The default, versioned Redis key namespace for browser OIDC transactions.
pub const DEFAULT_NAMESPACE: &str = "rustee:oidc:transaction:v1";

/// Redis storage for one-time OIDC authorization transactions.
#[derive(Clone)]
pub struct RedisAuthorizationTransactionStore {
    connection: rustee_redis::redis::aio::ConnectionManager,
    namespace: String,
}

impl RedisAuthorizationTransactionStore {
    /// Creates a store using [`DEFAULT_NAMESPACE`].
    #[must_use]
    pub fn new(connection: rustee_redis::redis::aio::ConnectionManager) -> Self {
        Self {
            connection,
            namespace: DEFAULT_NAMESPACE.to_owned(),
        }
    }

    /// Creates a store using an explicit, non-blank key namespace.
    ///
    /// # Errors
    ///
    /// Returns [`RedisAuthorizationTransactionStoreConfigError::InvalidNamespace`] when the
    /// namespace is blank or contains whitespace.
    pub fn with_namespace(
        connection: rustee_redis::redis::aio::ConnectionManager,
        namespace: impl Into<String>,
    ) -> Result<Self, RedisAuthorizationTransactionStoreConfigError> {
        let namespace = namespace.into();
        if !valid_namespace(&namespace) {
            return Err(RedisAuthorizationTransactionStoreConfigError::InvalidNamespace);
        }
        Ok(Self {
            connection,
            namespace,
        })
    }

    fn key(&self, state: &str) -> String {
        format!("{}:{state}", self.namespace)
    }
}

impl fmt::Debug for RedisAuthorizationTransactionStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisAuthorizationTransactionStore")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

impl AuthorizationTransactionStore for RedisAuthorizationTransactionStore {
    type Error = RedisAuthorizationTransactionStoreError;

    fn save(
        &self,
        transaction: PendingAuthorization,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let connection = self.connection.clone();
        let key = self.key(transaction.state());
        Box::pin(async move {
            let ttl_seconds = transaction
                .remaining_ttl_seconds()
                .ok_or(RedisAuthorizationTransactionStoreError::ExpiredTransaction)?;
            rustee_redis::set_json(&connection, &key, &transaction, ttl_seconds)
                .await
                .map_err(RedisAuthorizationTransactionStoreError::Save)
        })
    }

    fn take(
        &self,
        state: String,
    ) -> BoxFuture<'static, Result<Option<PendingAuthorization>, Self::Error>> {
        let connection = self.connection.clone();
        let key = self.key(&state);
        Box::pin(async move {
            rustee_redis::take_json(&connection, &key)
                .await
                .map_err(RedisAuthorizationTransactionStoreError::Take)
        })
    }
}

/// Invalid Redis authorization-transaction-store configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RedisAuthorizationTransactionStoreConfigError {
    /// A namespace must be explicit and whitespace-free.
    #[error("Redis OIDC transaction namespace must be non-blank and contain no whitespace")]
    InvalidNamespace,
}

/// Redis-backed OIDC authorization-transaction persistence failure.
#[derive(Debug, thiserror::Error)]
pub enum RedisAuthorizationTransactionStoreError {
    /// The transaction expired before it could be persisted.
    #[error("cannot persist an expired OIDC authorization transaction")]
    ExpiredTransaction,
    /// Redis failed while serializing or writing the transaction record.
    #[error("Redis OIDC authorization transaction save failed")]
    Save(#[source] CacheError),
    /// Redis failed while atomically consuming or decoding the transaction record.
    #[error("Redis OIDC authorization transaction take failed")]
    Take(#[source] CacheError),
}

fn valid_namespace(namespace: &str) -> bool {
    !namespace.trim().is_empty() && !namespace.chars().any(char::is_whitespace)
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_NAMESPACE, valid_namespace};

    #[test]
    fn namespace_is_versioned_and_invalid_values_are_rejected() {
        assert_eq!(DEFAULT_NAMESPACE, "rustee:oidc:transaction:v1");
        assert!(valid_namespace("customer-a:oidc:v1"));
        assert!(!valid_namespace(""));
        assert!(!valid_namespace("oidc transactions"));
    }
}
