//! Bounded request metrics collection and Tower lifecycle instrumentation.

mod collector;
mod layer;
mod model;

#[cfg(test)]
mod tests;

pub use collector::{DEFAULT_REQUEST_DURATION_BUCKETS, RequestMetrics, RequestMetricsConfigError};
pub use layer::{MetricsLayer, MetricsService};
pub use model::RequestMetricsSnapshot;

/// Stable names for request metrics exported by an application adapter.
pub mod metric_names {
    /// Count of completed HTTP requests.
    pub const HTTP_REQUESTS_TOTAL: &str = "rustee_http_requests_total";
    /// Number of HTTP requests currently executing.
    pub const HTTP_REQUESTS_IN_FLIGHT: &str = "rustee_http_requests_in_flight";
    /// Sum of completed request durations in seconds.
    pub const HTTP_REQUEST_DURATION_SECONDS: &str = "rustee_http_request_duration_seconds";
    /// Count of completed HTTP requests by router classification and status class.
    pub const HTTP_ROUTE_REQUESTS_TOTAL: &str = "rustee_http_route_requests_total";
}
