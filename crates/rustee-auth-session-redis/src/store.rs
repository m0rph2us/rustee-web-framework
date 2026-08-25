//! Redis `SessionStore` persistence and sanitized storage failures.

use std::fmt;

use futures_util::future::BoxFuture;
use rustee_auth_session::{Session, SessionId, SessionStore};
use rustee_redis::CacheError;

use crate::RedisSessionStore;

const MAX_SERIALIZED_SESSION_BYTES: usize = 512 * 1024;

impl SessionStore for RedisSessionStore {
    type Error = RedisSessionStoreError;

    fn save(&self, session: Session) -> BoxFuture<'static, Result<(), Self::Error>> {
        let connection = self.connection.clone();
        let key = self.key(session.id());
        Box::pin(async move {
            let ttl_seconds = session
                .remaining_ttl_seconds()
                .ok_or(RedisSessionStoreError::ExpiredSession)?;
            rustee_redis::set_json_bounded(
                &connection,
                &key,
                &session,
                ttl_seconds,
                MAX_SERIALIZED_SESSION_BYTES,
            )
            .await
            .map_err(cache_save_error)
        })
    }

    fn load(&self, id: SessionId) -> BoxFuture<'static, Result<Option<Session>, Self::Error>> {
        let connection = self.connection.clone();
        let key = self.key(id);
        Box::pin(async move {
            let session =
                rustee_redis::get_json_bounded(&connection, &key, MAX_SERIALIZED_SESSION_BYTES)
                    .await
                    .map_err(RedisSessionStoreError::Load)?;
            loaded_session(id, session)
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

fn loaded_session(
    id: SessionId,
    session: Option<Session>,
) -> Result<Option<Session>, RedisSessionStoreError> {
    let Some(session) = session else {
        return Ok(None);
    };
    if session.id() != id {
        return Err(RedisSessionStoreError::SessionIdMismatch);
    }
    Ok((!session.is_expired()).then_some(session))
}

fn cache_save_error(error: CacheError) -> RedisSessionStoreError {
    match error {
        CacheError::TtlOutOfRange => RedisSessionStoreError::TtlOutOfRange,
        error => RedisSessionStoreError::Save(error),
    }
}

/// Redis-backed session persistence failure.
#[derive(thiserror::Error)]
pub enum RedisSessionStoreError {
    /// The session expired before it could be stored.
    #[error("cannot persist an expired session")]
    ExpiredSession,
    /// The session has a remaining TTL Redis cannot represent safely.
    #[error("session TTL exceeds the Redis-supported range")]
    TtlOutOfRange,
    /// A stored record did not remain bound to its lookup key.
    #[error("stored Redis session does not match its lookup key")]
    SessionIdMismatch,
    /// Redis failed while serializing or writing the session record.
    #[error("Redis session save failed")]
    Save(#[source] CacheError),
    /// Redis failed while reading or decoding the session record.
    #[error("Redis session load failed")]
    Load(#[source] CacheError),
    /// Redis failed while deleting the session record.
    #[error("Redis session delete failed")]
    Delete(#[source] CacheError),
}

impl fmt::Debug for RedisSessionStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::ExpiredSession => "expired_session",
            Self::TtlOutOfRange => "ttl_out_of_range",
            Self::SessionIdMismatch => "session_id_mismatch",
            Self::Save(_) => "save_failed",
            Self::Load(_) => "load_failed",
            Self::Delete(_) => "delete_failed",
        };
        formatter
            .debug_struct("RedisSessionStoreError")
            .field("kind", &kind)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use rustee_auth_session::{Session, SessionId};
    use rustee_redis::{CacheError, redis};

    use super::{RedisSessionStoreError, cache_save_error, loaded_session};

    fn session_with_expiry(id: SessionId, expires_at_unix_seconds: u64) -> Session {
        serde_json::from_str(&format!(
            r#"{{"id":"{id}","principal":{{"subject":"alice","scopes":[]}},"csrf_token":"00000000-0000-4000-8000-000000000001","expires_at_unix_seconds":{expires_at_unix_seconds}}}"#,
        ))
        .expect("test session JSON must deserialize")
    }

    #[test]
    fn loaded_session_must_remain_bound_to_its_lookup_id_and_unexpired() {
        let lookup_id = SessionId::new();
        let session = session_with_expiry(SessionId::new(), u64::MAX);

        assert!(matches!(
            loaded_session(lookup_id, Some(session)),
            Err(RedisSessionStoreError::SessionIdMismatch)
        ));
        assert!(
            loaded_session(lookup_id, Some(session_with_expiry(lookup_id, 0)))
                .expect("expired record has a matching session ID")
                .is_none()
        );
        assert!(loaded_session(lookup_id, None).unwrap().is_none());
    }

    #[test]
    fn redis_ttl_range_rejection_stays_distinct_from_a_storage_failure() {
        assert!(matches!(
            cache_save_error(CacheError::TtlOutOfRange),
            RedisSessionStoreError::TtlOutOfRange
        ));
    }

    #[test]
    fn session_store_diagnostics_redact_redis_details_and_preserve_sources() {
        let save = RedisSessionStoreError::Save(CacheError::Redis(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "private-redis-session-save-detail",
        ))));
        let delete = RedisSessionStoreError::Delete(CacheError::Redis(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "private-redis-session-delete-detail",
        ))));

        for error in [&save as &dyn StdError, &delete as &dyn StdError] {
            assert!(!format!("{error:?}").contains("private-redis-session-save-detail"));
            assert!(!format!("{error:?}").contains("private-redis-session-delete-detail"));
            assert!(
                !error
                    .to_string()
                    .contains("private-redis-session-save-detail")
            );
            assert!(
                !error
                    .to_string()
                    .contains("private-redis-session-delete-detail")
            );
            assert!(StdError::source(error).is_some());
        }
    }
}
