//! Session persistence contract and application-facing lifecycle coordinator.

use std::fmt;

use futures_util::future::BoxFuture;
use http::HeaderValue;
use rustee_auth::Principal;

use super::{IssuedSession, Session, SessionCookieConfig, SessionId};

/// Persistence contract for opaque server-side sessions.
pub trait SessionStore: Clone + Send + Sync + 'static {
    /// Store-specific failure type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Persists or replaces one session record.
    fn save(&self, session: Session) -> BoxFuture<'static, Result<(), Self::Error>>;
    /// Loads one session record by its opaque identifier.
    ///
    /// Implementations must return only an unexpired record whose [`Session::id`] matches `id`.
    fn load(&self, id: SessionId) -> BoxFuture<'static, Result<Option<Session>, Self::Error>>;
    /// Deletes one server-side session record.
    fn delete(&self, id: SessionId) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// Creates, rotates, and invalidates sessions without exposing store details to request handlers.
///
/// Debug output identifies the store type and cookie policy without invoking store diagnostics.
#[derive(Clone)]
pub struct SessionManager<S> {
    store: S,
    cookie: SessionCookieConfig,
}

impl<S> fmt::Debug for SessionManager<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionManager")
            .field("store_type", &std::any::type_name::<S>())
            .field("cookie", &self.cookie)
            .finish_non_exhaustive()
    }
}

impl<S> SessionManager<S>
where
    S: SessionStore,
{
    /// Creates a session manager from one durable session store and cookie policy.
    #[must_use]
    pub fn new(store: S, cookie: SessionCookieConfig) -> Self {
        Self { store, cookie }
    }

    /// Establishes a new session, suitable after login or privilege elevation.
    ///
    /// # Errors
    ///
    /// Returns the underlying store failure when the new session cannot be persisted.
    pub async fn establish(&self, principal: Principal) -> Result<IssuedSession, S::Error> {
        let session = Session::new(principal, self.cookie.ttl_seconds());
        self.store.save(session.clone()).await?;
        Ok(IssuedSession::new(
            session.csrf_token,
            self.cookie.set_cookie(session.id),
        ))
    }

    /// Rotates a session by deleting the old ID before issuing a new one.
    ///
    /// # Errors
    ///
    /// Returns the underlying store failure when deletion or new-session persistence fails.
    pub async fn rotate(
        &self,
        previous: SessionId,
        principal: Principal,
    ) -> Result<IssuedSession, S::Error> {
        self.store.delete(previous).await?;
        self.establish(principal).await
    }

    /// Invalidates a server-side session and expires the browser cookie.
    ///
    /// # Errors
    ///
    /// Returns the underlying store failure when the session cannot be deleted.
    pub async fn invalidate(&self, id: SessionId) -> Result<HeaderValue, S::Error> {
        self.store.delete(id).await?;
        Ok(self.cookie.clear_cookie())
    }
}
