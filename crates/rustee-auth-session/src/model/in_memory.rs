//! Bounded local session persistence.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};

use futures_util::future::BoxFuture;

use super::{Session, SessionId, SessionStore};

pub(super) const MAX_IN_MEMORY_SESSIONS: usize = 1_024;

/// In-memory session store for tests and local development only.
///
/// The store retains at most 1,024 unexpired sessions. Saving or loading prunes expired entries,
/// and a new session is rejected when the remaining fixed capacity is exhausted.
#[derive(Clone, Default)]
pub struct InMemorySessionStore {
    pub(super) sessions: Arc<Mutex<BTreeMap<SessionId, Session>>>,
}

impl fmt::Debug for InMemorySessionStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemorySessionStore")
            .finish_non_exhaustive()
    }
}

/// In-memory session-store failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InMemorySessionStoreError {
    /// A poisoned lock prevents safely reading or mutating local session state.
    #[error("in-memory session store state is unavailable")]
    StateUnavailable,
    /// A session expired before it could be stored.
    #[error("cannot persist an expired in-memory session")]
    ExpiredSession,
    /// Live sessions exhausted the fixed local-store capacity.
    #[error("in-memory session store capacity is exhausted")]
    CapacityExhausted,
}

impl SessionStore for InMemorySessionStore {
    type Error = InMemorySessionStoreError;

    fn save(&self, session: Session) -> BoxFuture<'static, Result<(), Self::Error>> {
        let sessions = Arc::clone(&self.sessions);
        Box::pin(async move {
            let mut sessions = sessions
                .lock()
                .map_err(|_| InMemorySessionStoreError::StateUnavailable)?;
            sessions.retain(|_, existing| !existing.is_expired());
            if session.is_expired() {
                return Err(InMemorySessionStoreError::ExpiredSession);
            }
            if !sessions.contains_key(&session.id) && sessions.len() >= MAX_IN_MEMORY_SESSIONS {
                return Err(InMemorySessionStoreError::CapacityExhausted);
            }
            sessions.insert(session.id, session);
            Ok(())
        })
    }

    fn load(&self, id: SessionId) -> BoxFuture<'static, Result<Option<Session>, Self::Error>> {
        let sessions = Arc::clone(&self.sessions);
        Box::pin(async move {
            let mut sessions = sessions
                .lock()
                .map_err(|_| InMemorySessionStoreError::StateUnavailable)?;
            sessions.retain(|_, session| !session.is_expired());
            Ok(sessions.get(&id).cloned())
        })
    }

    fn delete(&self, id: SessionId) -> BoxFuture<'static, Result<(), Self::Error>> {
        let sessions = Arc::clone(&self.sessions);
        Box::pin(async move {
            sessions
                .lock()
                .map_err(|_| InMemorySessionStoreError::StateUnavailable)?
                .remove(&id);
            Ok(())
        })
    }
}
