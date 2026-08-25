use std::{collections::BTreeMap, time::Duration};

/// Immutable view of request metrics collected by [`super::RequestMetrics`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestMetricsSnapshot {
    in_flight: u64,
    completed: u64,
    status_classes: BTreeMap<u16, u64>,
    route_classification_status_classes: BTreeMap<(String, u16), u64>,
    duration_bucket_counts: Vec<(Duration, u64)>,
    total_duration: Duration,
}

impl RequestMetricsSnapshot {
    pub(super) fn from_state(
        in_flight: u64,
        completed: u64,
        status_classes: BTreeMap<u16, u64>,
        route_classification_status_classes: BTreeMap<(String, u16), u64>,
        duration_bucket_counts: Vec<(Duration, u64)>,
        total_duration: Duration,
    ) -> Self {
        Self {
            in_flight,
            completed,
            status_classes,
            route_classification_status_classes,
            duration_bucket_counts,
            total_duration,
        }
    }

    /// Returns requests currently executing.
    #[must_use]
    pub const fn in_flight(&self) -> u64 {
        self.in_flight
    }

    /// Returns completed request count.
    #[must_use]
    pub const fn completed(&self) -> u64 {
        self.completed
    }

    /// Returns completed request count for a status class such as `2` or `5`.
    #[must_use]
    pub fn status_class(&self, class: u16) -> u64 {
        self.status_classes.get(&class).copied().unwrap_or(0)
    }

    /// Iterates completed request counts by status class in stable numeric order.
    pub fn status_class_counts(&self) -> impl Iterator<Item = (u16, u64)> + '_ {
        self.status_classes
            .iter()
            .map(|(&class, &count)| (class, count))
    }

    /// Returns completed request count for one router classification and status class.
    ///
    /// A request only contributes when the Rustee router attached a
    /// [`rustee_core::RouteClassification`] to its response. Raw request paths are never
    /// collected.
    #[must_use]
    pub fn route_classification_status_class(&self, route: &str, class: u16) -> u64 {
        self.route_classification_status_classes
            .get(&(route.to_owned(), class))
            .copied()
            .unwrap_or(0)
    }

    /// Iterates completed request counts by router classification and status class.
    ///
    /// The router classification is either a configured route template or a framework-reserved
    /// outcome. Values are returned in deterministic lexicographic/numeric order.
    pub fn route_classification_status_class_counts(
        &self,
    ) -> impl Iterator<Item = (&str, u16, u64)> + '_ {
        self.route_classification_status_classes
            .iter()
            .map(|((route, class), &count)| (route.as_str(), *class, count))
    }

    /// Iterates global cumulative request duration histogram buckets in ascending order.
    ///
    /// The implicit `+Inf` bucket always equals [`Self::completed`].
    pub fn duration_bucket_counts(&self) -> impl Iterator<Item = (Duration, u64)> + '_ {
        self.duration_bucket_counts.iter().copied()
    }

    /// Returns the cumulative count at one configured duration upper bound.
    #[must_use]
    pub fn duration_bucket_count(&self, upper_bound: Duration) -> Option<u64> {
        self.duration_bucket_counts
            .iter()
            .find_map(|(bound, count)| (*bound == upper_bound).then_some(*count))
    }

    /// Returns the total duration of completed requests.
    #[must_use]
    pub const fn total_duration(&self) -> Duration {
        self.total_duration
    }
}
