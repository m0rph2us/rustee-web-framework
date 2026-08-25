//! Middleware primitives that preserve Tower's `Layer` and `Service` contracts.

pub use tower::ServiceBuilder;

mod compression;
mod cors;
mod panic;
mod trusted_proxy;

pub use compression::{Compression, CompressionLayer};
pub use cors::{Cors, CorsLayer};
pub use panic::{PanicCatch, PanicCatchLayer};
pub use trusted_proxy::{
    ForwardedContext, MAX_TRUSTED_PROXY_NETWORKS, TrustedProxy, TrustedProxyError,
    TrustedProxyLayer, TrustedProxyNetwork, TrustedProxyPolicy,
};
#[cfg(test)]
use trusted_proxy::{
    MAX_FORWARDED_CHAIN_HOPS, X_FORWARDED_FOR, X_FORWARDED_HOST, X_FORWARDED_PROTO,
};

#[cfg(test)]
mod tests;
