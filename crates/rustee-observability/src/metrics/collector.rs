use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use rustee_core::{Response, RouteClassification};
use rustee_observability_core::{
    DEFAULT_DURATION_BUCKETS, DurationHistogram, DurationHistogramConfigError,
};

use super::model::RequestMetricsSnapshot;

/// Default cumulative upper bounds for request duration histograms.
///
/// Applications needing a different latency range can use
/// [`RequestMetrics::with_duration_buckets`]. Bucket values must stay bounded, non-zero, and
/// strictly increasing.
pub const DEFAULT_REQUEST_DURATION_BUCKETS: [Duration; 12] = DEFAULT_DURATION_BUCKETS;

/// Thread-safe, exporter-neutral request metric collector.
///
/// It deliberately records only bounded status-class labels and router classifications. A router
/// classification is either a configured route template or a framework-reserved outcome label.
/// Raw paths, credentials, hosts, and request IDs belong in an application-specific exporter
/// policy, not the framework default.
#[derive(Clone, Debug)]
pub struct RequestMetrics {
    pub(super) state: Arc<Mutex<RequestMetricsState>>,
}

impl Default for RequestMetrics {
    fn default() -> Self {
        Self::with_duration_buckets(DEFAULT_REQUEST_DURATION_BUCKETS)
            .expect("default request duration buckets must be valid")
    }
}

impl RequestMetrics {
    /// Creates an empty request metric collector.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a request metric collector with explicit duration histogram upper bounds.
    ///
    /// At most 32 non-zero durations are accepted, in strictly increasing order. Histogram bucket
    /// counts are global rather than route-labelled, keeping scrape cardinality bounded.
    ///
    /// # Errors
    ///
    /// Returns [`RequestMetricsConfigError`] when the bounds are empty, too numerous, zero, or
    /// not strictly increasing.
    pub fn with_duration_buckets(
        buckets: impl IntoIterator<Item = Duration>,
    ) -> Result<Self, RequestMetricsConfigError> {
        let duration_histogram =
            DurationHistogram::new(buckets).map_err(RequestMetricsConfigError::from)?;
        Ok(Self {
            state: Arc::new(Mutex::new(RequestMetricsState::new(duration_histogram))),
        })
    }

    /// Returns a point-in-time snapshot suitable for an exporter or readiness diagnostic.
    ///
    /// The collector recovers its state after a metrics-only panic so observation never
    /// interrupts application traffic.
    #[must_use]
    pub fn snapshot(&self) -> RequestMetricsSnapshot {
        let state = self.lock_state();
        RequestMetricsSnapshot::from_state(
            state.in_flight,
            state.completed,
            state.status_classes.clone(),
            state.route_classification_status_classes.clone(),
            state.duration_histogram.bucket_counts().collect(),
            state.duration_histogram.total_duration(),
        )
    }

    pub(super) fn started(&self) -> InFlightRequest {
        let mut state = self.lock_state();
        state.in_flight = state.in_flight.saturating_add(1);
        InFlightRequest {
            metrics: self.clone(),
            completed: false,
        }
    }

    pub(super) fn finished(&self, response: &Response, duration: Duration) {
        let mut state = self.lock_state();
        state.in_flight = state.in_flight.saturating_sub(1);
        state.completed = state.completed.saturating_add(1);
        let status = response.status();
        let status_count = state
            .status_classes
            .entry(status.as_u16() / 100)
            .or_default();
        *status_count = status_count.saturating_add(1);
        if let Some(route) = response.extensions().get::<RouteClassification>() {
            let route_status_count = state
                .route_classification_status_classes
                .entry((route.as_str().to_owned(), status.as_u16() / 100))
                .or_default();
            *route_status_count = route_status_count.saturating_add(1);
        }
        state.duration_histogram.observe(duration);
    }

    fn cancelled(&self) {
        let mut state = self.lock_state();
        state.in_flight = state.in_flight.saturating_sub(1);
    }

    pub(super) fn lock_state(&self) -> MutexGuard<'_, RequestMetricsState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Debug)]
pub(super) struct RequestMetricsState {
    pub(super) in_flight: u64,
    pub(super) completed: u64,
    pub(super) status_classes: BTreeMap<u16, u64>,
    pub(super) route_classification_status_classes: BTreeMap<(String, u16), u64>,
    pub(super) duration_histogram: DurationHistogram,
}

impl RequestMetricsState {
    fn new(duration_histogram: DurationHistogram) -> Self {
        Self {
            in_flight: 0,
            completed: 0,
            status_classes: BTreeMap::new(),
            route_classification_status_classes: BTreeMap::new(),
            duration_histogram,
        }
    }
}

/// Invalid duration histogram configuration rejected before collection starts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestMetricsConfigError {
    /// No finite histogram upper bound was configured.
    EmptyDurationBuckets,
    /// More than 32 finite histogram upper bounds were configured.
    TooManyDurationBuckets,
    /// A histogram upper bound was zero.
    ZeroDurationBucket,
    /// Histogram upper bounds were duplicated or out of ascending order.
    UnorderedDurationBuckets,
}

impl From<DurationHistogramConfigError> for RequestMetricsConfigError {
    fn from(error: DurationHistogramConfigError) -> Self {
        match error {
            DurationHistogramConfigError::EmptyBuckets => Self::EmptyDurationBuckets,
            DurationHistogramConfigError::TooManyBuckets => Self::TooManyDurationBuckets,
            DurationHistogramConfigError::ZeroBucket => Self::ZeroDurationBucket,
            DurationHistogramConfigError::UnorderedBuckets => Self::UnorderedDurationBuckets,
        }
    }
}

impl fmt::Display for RequestMetricsConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptyDurationBuckets => "at least one duration histogram bucket is required",
            Self::TooManyDurationBuckets => {
                "request duration histogram supports at most 32 finite buckets"
            }
            Self::ZeroDurationBucket => {
                "request duration histogram buckets must be greater than zero"
            }
            Self::UnorderedDurationBuckets => {
                "request duration histogram buckets must be strictly increasing"
            }
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for RequestMetricsConfigError {}

pub(super) struct InFlightRequest {
    metrics: RequestMetrics,
    completed: bool,
}

impl InFlightRequest {
    pub(super) fn finish(mut self, response: &Response, duration: Duration) {
        self.metrics.finished(response, duration);
        self.completed = true;
    }
}

impl Drop for InFlightRequest {
    fn drop(&mut self) {
        if !self.completed {
            self.metrics.cancelled();
        }
    }
}
