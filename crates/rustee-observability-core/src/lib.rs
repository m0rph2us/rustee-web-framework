//! Bounded duration-histogram and Prometheus-text primitives for Rustee telemetry.
//!
//! Domain collectors retain their own labels, snapshots, and public configuration errors. This
//! crate owns only the invariant that duration buckets and their cumulative counts stay paired.

mod histogram;

pub mod prometheus;

pub use histogram::{
    DEFAULT_DURATION_BUCKETS, DurationHistogram, DurationHistogramConfigError, MAX_DURATION_BUCKETS,
};
