//! Formatted tracing subscriber installation.

use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Installs a formatted tracing subscriber using `RUST_LOG` when present.
///
/// Calling this more than once is harmless and returns `false` after the first subscriber wins.
#[must_use]
pub fn init() -> bool {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .try_init()
        .is_ok()
}
