//! Redis-backed, one-time OIDC Authorization Code + PKCE transaction persistence.
//!
//! Each transaction is stored under a caller-visible versioned namespace with its remaining TTL.
//! Callback completion uses Redis `GETDEL`, atomically consuming `state`, nonce, and PKCE verifier
//! before any provider token exchange can occur.

mod config;
mod store;

pub use config::{
    DEFAULT_NAMESPACE, RedisAuthorizationTransactionStore,
    RedisAuthorizationTransactionStoreConfigError,
};
pub use store::RedisAuthorizationTransactionStoreError;
