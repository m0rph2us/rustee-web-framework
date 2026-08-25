//! Redis one-time OIDC authorization transaction persistence and sanitized failures.

use std::fmt;

use futures_util::future::BoxFuture;
use rustee_auth_oidc::{AuthorizationTransactionStore, PendingAuthorization};
use rustee_redis::CacheError;

use crate::RedisAuthorizationTransactionStore;

// Three bounded OAuth capabilities, one bounded HTTPS endpoint, and JSON framing fit comfortably
// within this persistence budget while leaving headroom for future non-sensitive metadata.
const MAX_TRANSACTION_RECORD_BYTES: usize = 16 * 1024;

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
            rustee_redis::set_json_bounded_if_absent(
                &connection,
                &key,
                &transaction,
                ttl_seconds,
                MAX_TRANSACTION_RECORD_BYTES,
            )
            .await
            .map_err(cache_save_error)
        })
    }

    fn take(
        &self,
        state: String,
    ) -> BoxFuture<'static, Result<Option<PendingAuthorization>, Self::Error>> {
        let connection = self.connection.clone();
        let key = self.key(&state);
        Box::pin(async move {
            rustee_redis::take_json_bounded(&connection, &key, MAX_TRANSACTION_RECORD_BYTES)
                .await
                .map_err(RedisAuthorizationTransactionStoreError::Take)
        })
    }
}

fn cache_save_error(error: CacheError) -> RedisAuthorizationTransactionStoreError {
    match error {
        CacheError::EntryExists => RedisAuthorizationTransactionStoreError::DuplicateState,
        error => RedisAuthorizationTransactionStoreError::Save(error),
    }
}

/// Redis-backed OIDC authorization-transaction persistence failure.
#[derive(thiserror::Error)]
pub enum RedisAuthorizationTransactionStoreError {
    /// The transaction expired before it could be persisted.
    #[error("cannot persist an expired OIDC authorization transaction")]
    ExpiredTransaction,
    /// A live transaction already occupies the one-time state capability.
    #[error("OIDC authorization transaction state already exists")]
    DuplicateState,
    /// Redis failed while serializing or writing the transaction record.
    #[error("Redis OIDC authorization transaction save failed")]
    Save(#[source] CacheError),
    /// Redis failed while atomically consuming or decoding the transaction record.
    #[error("Redis OIDC authorization transaction take failed")]
    Take(#[source] CacheError),
}

impl fmt::Debug for RedisAuthorizationTransactionStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::ExpiredTransaction => "expired_transaction",
            Self::DuplicateState => "duplicate_state",
            Self::Save(_) => "save_failed",
            Self::Take(_) => "take_failed",
        };
        formatter
            .debug_struct("RedisAuthorizationTransactionStoreError")
            .field("kind", &kind)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use rustee_redis::{CacheError, redis};

    use super::{RedisAuthorizationTransactionStoreError, cache_save_error};

    #[test]
    fn duplicate_transaction_state_is_not_reported_as_a_redis_failure() {
        assert!(matches!(
            cache_save_error(CacheError::EntryExists),
            RedisAuthorizationTransactionStoreError::DuplicateState
        ));
    }

    #[test]
    fn transaction_store_diagnostics_redact_redis_details_and_preserve_sources() {
        let error = RedisAuthorizationTransactionStoreError::Save(CacheError::Redis(
            redis::RedisError::from((
                redis::ErrorKind::InvalidClientConfig,
                "private-oidc-transaction-redis-detail",
            )),
        ));

        assert!(!format!("{error:?}").contains("private-oidc-transaction-redis-detail"));
        assert!(
            !error
                .to_string()
                .contains("private-oidc-transaction-redis-detail")
        );
        assert!(StdError::source(&error).is_some());
    }
}
