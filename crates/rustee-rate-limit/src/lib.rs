//! Explicit keyed rate-limit policy and Tower middleware contracts.
//!
//! Applications resolve a key from trusted request context. Storage adapters implement
//! [`RateLimitStore`], and every layer declares whether a storage outage is fail-open or
//! fail-closed.

mod middleware;
mod policy;

pub use middleware::{RateLimit, RateLimitLayer};
pub use policy::{
    FixedWindow, RateLimitConfigError, RateLimitDecision, RateLimitKey, RateLimitKeyResolver,
    RateLimitStore, StoreFailurePolicy,
};

#[cfg(test)]
mod tests;
