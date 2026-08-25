//! Exporter-neutral metrics for `Rustee` transactional-outbox relays.
//!
//! The collector implements [`rustee_outbox_sqlx::OutboxRelayObserver`] and is attached to an
//! event or job relay with
//! its `with_relay_observer` builder. It records only the fixed relay kind, terminal outcome,
//! bounded counts, and global duration. Destinations, message IDs, payloads, broker endpoints,
//! and error text never enter this collector.

mod collector;
mod model;

pub use collector::{
    DEFAULT_RELAY_PASS_DURATION_BUCKETS, OutboxRelayMetrics, OutboxRelayMetricsConfigError,
};
pub use model::{OutboxRelayMetricsSnapshot, RelayRowCount};

/// Stable names for relay metrics exported by an application adapter.
pub mod metric_names {
    /// Count of relay passes whose future started.
    pub const RELAY_PASSES_TOTAL: &str = "rustee_outbox_relay_passes_total";
    /// Number of relay pass futures currently executing in this process.
    pub const RELAY_PASSES_IN_FLIGHT: &str = "rustee_outbox_relay_passes_in_flight";
    /// Sum of completed relay pass durations in seconds.
    pub const RELAY_PASS_DURATION_SECONDS: &str = "rustee_outbox_relay_pass_duration_seconds";
    /// Count of relay pass outcomes by fixed kind and outcome labels.
    pub const RELAY_PASS_OUTCOMES_TOTAL: &str = "rustee_outbox_relay_pass_outcomes_total";
    /// Aggregate claimed, published, retry-scheduled, or lease-lost row counts by fixed kind.
    pub const RELAY_ROWS_TOTAL: &str = "rustee_outbox_relay_rows_total";
}

#[cfg(test)]
mod tests;
