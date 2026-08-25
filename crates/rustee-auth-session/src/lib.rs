//! Server-side browser sessions and CSRF protection for Rustee.
//!
//! Cookies contain only a random opaque session identifier. Identity and CSRF state remain in a
//! [`SessionStore`], which production applications replace with a durable provider adapter.

mod middleware;
mod model;

pub use middleware::{
    CsrfLayer, CsrfService, SessionContext, SessionLayer, SessionService, SessionUser,
};
pub use model::{
    CookieConfigError, InMemorySessionStore, InMemorySessionStoreError, IssuedSession,
    MAX_COOKIE_NAME_BYTES, SameSite, Session, SessionCookieConfig, SessionId, SessionManager,
    SessionStore,
};

#[cfg(test)]
mod tests;
