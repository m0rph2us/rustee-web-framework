//! Trusted reverse-proxy facade for policy, header parsing, and context injection.

mod forwarded;
mod layer;
mod policy;

pub use layer::{ForwardedContext, TrustedProxy, TrustedProxyLayer};
pub use policy::{
    MAX_TRUSTED_PROXY_NETWORKS, TrustedProxyError, TrustedProxyNetwork, TrustedProxyPolicy,
};

#[cfg(test)]
pub(super) use forwarded::{X_FORWARDED_FOR, X_FORWARDED_HOST, X_FORWARDED_PROTO};
#[cfg(test)]
pub(super) const MAX_FORWARDED_CHAIN_HOPS: usize = policy::MAX_FORWARDED_CHAIN_HOPS;
