//! Redis OIDC transaction namespace configuration and redacted adapter diagnostics.

use std::fmt;

use rustee_redis::is_valid_key_namespace;

/// The default, versioned Redis key namespace for browser OIDC transactions.
pub const DEFAULT_NAMESPACE: &str = "rustee:oidc:transaction:v1";

/// Redis storage for one-time OIDC authorization transactions.
///
/// Its `Debug` output exposes only the configured namespace length, never the key prefix.
#[derive(Clone)]
pub struct RedisAuthorizationTransactionStore {
    pub(super) connection: rustee_redis::redis::aio::ConnectionManager,
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

    /// Creates a store using an explicit, bounded key namespace.
    ///
    /// # Errors
    ///
    /// Returns [`RedisAuthorizationTransactionStoreConfigError::InvalidNamespace`] when the
    /// namespace is blank, oversized, or uses unsafe Redis key syntax.
    pub fn with_namespace(
        connection: rustee_redis::redis::aio::ConnectionManager,
        namespace: impl Into<String>,
    ) -> Result<Self, RedisAuthorizationTransactionStoreConfigError> {
        let namespace = namespace.into();
        if !is_valid_key_namespace(&namespace) {
            return Err(RedisAuthorizationTransactionStoreConfigError::InvalidNamespace);
        }
        Ok(Self {
            connection,
            namespace,
        })
    }

    pub(super) fn key(&self, state: &str) -> String {
        format!("{}:{state}", self.namespace)
    }
}

impl fmt::Debug for RedisAuthorizationTransactionStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisAuthorizationTransactionStore")
            .field("namespace", &"[REDACTED]")
            .field("namespace_length", &self.namespace.len())
            .finish_non_exhaustive()
    }
}

/// Invalid Redis authorization-transaction-store configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RedisAuthorizationTransactionStoreConfigError {
    /// A namespace must be a bounded ASCII Redis key prefix without hash-tag syntax.
    #[error(
        "Redis OIDC transaction namespace must use bounded ASCII letters, digits, colon, underscore, hyphen, or dot"
    )]
    InvalidNamespace,
}

#[cfg(test)]
mod tests {
    use rustee_redis::is_valid_key_namespace;

    use super::DEFAULT_NAMESPACE;

    #[test]
    fn namespace_is_versioned_and_invalid_values_are_rejected() {
        assert_eq!(DEFAULT_NAMESPACE, "rustee:oidc:transaction:v1");
        assert!(is_valid_key_namespace("customer-a:oidc:v1"));
        assert!(!is_valid_key_namespace(""));
        assert!(!is_valid_key_namespace("oidc transactions"));
        assert!(!is_valid_key_namespace("oidc{shared-slot}"));
    }
}
