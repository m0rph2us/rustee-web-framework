//! Exporter-neutral metrics for `Rustee` durable job workers.
//!
//! The collector implements [`rustee_jobs::JobDeliveryObserver`] and is attached to a provider
//! worker with its `with_delivery_observer` builder. It records only bounded provider and
//! settlement labels; payloads, job IDs, queue routes, delivery handles, and handler error text
//! never enter this collector.

mod collector;
mod model;

pub use collector::{DEFAULT_JOB_DELIVERY_DURATION_BUCKETS, JobMetrics, JobMetricsConfigError};
pub use model::JobMetricsSnapshot;

/// Stable names for job metrics exported by an application adapter.
pub mod metric_names {
    /// Count of provider deliveries whose worker task started.
    pub const JOB_DELIVERIES_TOTAL: &str = "rustee_job_deliveries_total";
    /// Number of provider deliveries currently executing in this process.
    pub const JOB_DELIVERIES_IN_FLIGHT: &str = "rustee_job_deliveries_in_flight";
    /// Sum of completed worker delivery durations in seconds.
    pub const JOB_DELIVERY_DURATION_SECONDS: &str = "rustee_job_delivery_duration_seconds";
    /// Count of settled and unsettled deliveries by bounded provider and outcome labels.
    pub const JOB_DELIVERY_OUTCOMES_TOTAL: &str = "rustee_job_delivery_outcomes_total";
}

#[cfg(test)]
mod tests;
