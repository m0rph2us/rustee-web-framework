//! Exporter-neutral metrics for `Rustee` `PostgreSQL` recurring-scheduler passes.
//!
//! The collector implements [`rustee_jobs_cron_sqlx::RecurringJobFireObserver`] and is attached to a scheduler with
//! [`rustee_jobs_cron_sqlx::PostgresRecurringJobs::with_fire_observer`]. It records terminal outcomes, aggregate row
//! counts, and global duration only. Schedule keys, destinations, payloads, tenant context, and
//! storage errors never enter the collector.

mod collector;
mod model;

pub use collector::{
    DEFAULT_SCHEDULER_FIRE_PASS_DURATION_BUCKETS, RecurringJobFireMetrics,
    RecurringJobFireMetricsConfigError,
};
pub use model::{RecurringJobFireMetricsSnapshot, RecurringJobFireRowCount};

/// Stable names for recurring-scheduler metrics exported by an application adapter.
pub mod metric_names {
    /// Count of scheduler passes whose future started.
    pub const FIRE_PASSES_TOTAL: &str = "rustee_scheduler_fire_passes_total";
    /// Number of scheduler pass futures currently executing in this process.
    pub const FIRE_PASSES_IN_FLIGHT: &str = "rustee_scheduler_fire_passes_in_flight";
    /// Sum of completed scheduler pass durations in seconds.
    pub const FIRE_PASS_DURATION_SECONDS: &str = "rustee_scheduler_fire_pass_duration_seconds";
    /// Count of completed scheduler passes by fixed outcome label.
    pub const FIRE_PASS_OUTCOMES_TOTAL: &str = "rustee_scheduler_fire_pass_outcomes_total";
    /// Aggregate claimed, staged, or rate-limited schedule counts by fixed count label.
    pub const FIRE_ROWS_TOTAL: &str = "rustee_scheduler_fire_rows_total";
}

#[cfg(test)]
mod tests;
