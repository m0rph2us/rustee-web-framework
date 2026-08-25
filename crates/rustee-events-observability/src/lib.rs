//! Exporter-neutral metrics for `Rustee` event-stream consumers.
//!
//! The collector implements [`rustee_events::EventDeliveryObserver`] and is attached to a
//! provider consumer with its `with_delivery_observer` builder. It records only bounded provider
//! and settlement labels; payloads, event identifiers, topics, partitions, consumer groups,
//! offsets, keys, and handler error text never enter this collector.

mod collector;
mod model;

pub use collector::{
    DEFAULT_EVENT_DELIVERY_DURATION_BUCKETS, EventMetrics, EventMetricsConfigError,
};
pub use model::EventMetricsSnapshot;

/// Stable names for event-delivery metrics exported by an application adapter.
pub mod metric_names {
    /// Count of provider deliveries whose consumer task started.
    pub const EVENT_DELIVERIES_TOTAL: &str = "rustee_event_deliveries_total";
    /// Number of provider deliveries currently executing in this process.
    pub const EVENT_DELIVERIES_IN_FLIGHT: &str = "rustee_event_deliveries_in_flight";
    /// Sum of completed consumer delivery durations in seconds.
    pub const EVENT_DELIVERY_DURATION_SECONDS: &str = "rustee_event_delivery_duration_seconds";
    /// Count of settled and unsettled deliveries by bounded provider and outcome labels.
    pub const EVENT_DELIVERY_OUTCOMES_TOTAL: &str = "rustee_event_delivery_outcomes_total";
}

#[cfg(test)]
mod tests;
