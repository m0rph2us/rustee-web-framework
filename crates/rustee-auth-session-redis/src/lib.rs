//! Redis-backed persistence for Rustee opaque server-side sessions.
//!
//! The adapter writes each session under a caller-visible, versioned namespace and sets the Redis
//! expiry from the remaining session lifetime. A Redis failure remains a store failure, allowing
//! [`rustee_auth_session::SessionLayer`] to return its fail-closed `503` response.

use std::fmt;

use futures_util::future::BoxFuture;
use rustee_auth_session::{Session, SessionId, SessionStore};
use rustee_redis::{CacheError, redis::RedisError};

/// The default, versioned Redis key namespace for sessions.
pub const DEFAULT_NAMESPACE: &str = "rustee:session:v1";

/// Redis storage for opaque Rustee sessions.
#[derive(Clone)]
pub struct RedisSessionStore {
    connection: rustee_redis::redis::aio::ConnectionManager,
    namespace: String,
}

impl RedisSessionStore {
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
    /// Returns [`RedisSessionStoreConfigError::InvalidNamespace`] when the namespace is blank or
    /// contains whitespace.
    pub fn with_namespace(
        connection: rustee_redis::redis::aio::ConnectionManager,
        namespace: impl Into<String>,
    ) -> Result<Self, RedisSessionStoreConfigError> {
        let namespace = namespace.into();
        if !valid_namespace(&namespace) {
            return Err(RedisSessionStoreConfigError::InvalidNamespace);
        }
        Ok(Self {
            connection,
            namespace,
        })
    }

    fn key(&self, id: SessionId) -> String {
        format!("{}:{id}", self.namespace)
    }
}

fn valid_namespace(namespace: &str) -> bool {
    !namespace.trim().is_empty() && !namespace.chars().any(char::is_whitespace)
}

impl fmt::Debug for RedisSessionStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisSessionStore")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

impl SessionStore for RedisSessionStore {
    type Error = RedisSessionStoreError;

    fn save(&self, session: Session) -> BoxFuture<'static, Result<(), Self::Error>> {
        let connection = self.connection.clone();
        let key = self.key(session.id());
        Box::pin(async move {
            let ttl_seconds = session
                .remaining_ttl_seconds()
                .ok_or(RedisSessionStoreError::ExpiredSession)?;
            rustee_redis::set_json(&connection, &key, &session, ttl_seconds)
                .await
                .map_err(RedisSessionStoreError::Save)
        })
    }

    fn load(&self, id: SessionId) -> BoxFuture<'static, Result<Option<Session>, Self::Error>> {
        let connection = self.connection.clone();
        let key = self.key(id);
        Box::pin(async move {
            rustee_redis::get_json(&connection, &key)
                .await
                .map_err(RedisSessionStoreError::Load)
        })
    }

    fn delete(&self, id: SessionId) -> BoxFuture<'static, Result<(), Self::Error>> {
        let connection = self.connection.clone();
        let key = self.key(id);
        Box::pin(async move {
            rustee_redis::delete(&connection, &key)
                .await
                .map_err(RedisSessionStoreError::Delete)
        })
    }
}

/// Invalid Redis session-store configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RedisSessionStoreConfigError {
    /// A namespace must be an explicit, whitespace-free Redis key prefix.
    #[error("Redis session namespace must be non-blank and contain no whitespace")]
    InvalidNamespace,
}

/// Redis-backed session persistence failure.
#[derive(Debug, thiserror::Error)]
pub enum RedisSessionStoreError {
    /// The session expired before it could be stored.
    #[error("cannot persist an expired session")]
    ExpiredSession,
    /// Redis failed while serializing or writing the session record.
    #[error("Redis session save failed")]
    Save(#[source] CacheError),
    /// Redis failed while reading or decoding the session record.
    #[error("Redis session load failed")]
    Load(#[source] CacheError),
    /// Redis failed while deleting the session record.
    #[error("Redis session delete failed")]
    Delete(#[source] RedisError),
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_NAMESPACE, valid_namespace};

    #[test]
    fn namespace_is_versioned_and_invalid_values_are_rejected() {
        assert_eq!(DEFAULT_NAMESPACE, "rustee:session:v1");
        assert!(valid_namespace("customer-a:session:v1"));
        assert!(!valid_namespace(""));
        assert!(!valid_namespace("tenant sessions"));
    }
}
