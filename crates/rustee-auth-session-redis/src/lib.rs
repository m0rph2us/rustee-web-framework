//! Redis-backed persistence for Rustee opaque server-side sessions.
//!
//! The adapter writes each session under a caller-visible, versioned namespace and sets the Redis
//! expiry from the remaining session lifetime. A Redis failure remains a store failure, allowing
//! [`rustee_auth_session::SessionLayer`] to return its fail-closed `503` response.

mod config;
mod store;

pub use config::{DEFAULT_NAMESPACE, RedisSessionStore, RedisSessionStoreConfigError};
pub use store::RedisSessionStoreError;
