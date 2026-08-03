//! Conservative tracing initialization and request correlation for Rustee applications.

use std::{
    collections::BTreeMap,
    convert::Infallible,
    fmt,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures_util::{FutureExt, future::BoxFuture};
use http::{HeaderValue, header::HeaderName};
use rustee_core::{
    Error, FromRequest, Request, Response, RouteClassification, RouteParams, StateStore,
};
use tower::{Layer, Service, util::BoxCloneService};
use tracing::{Instrument, info};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");
const MAX_REQUEST_DURATION_BUCKETS: usize = 32;

/// Default cumulative upper bounds for request duration histograms.
///
/// Applications needing a different latency range can use
/// [`RequestMetrics::with_duration_buckets`]. Bucket values must stay bounded, non-zero, and
/// strictly increasing.
pub const DEFAULT_REQUEST_DURATION_BUCKETS: [Duration; 12] = [
    Duration::from_millis(1),
    Duration::from_millis(5),
    Duration::from_millis(10),
    Duration::from_millis(25),
    Duration::from_millis(50),
    Duration::from_millis(100),
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_millis(2500),
    Duration::from_secs(5),
    Duration::from_secs(10),
];

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

/// A generated request correlation identifier.
///
/// Rustee always generates this value at the application boundary. It deliberately does not
/// reuse a client-provided header until a future trusted-proxy policy proves its provenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestId(String);

impl RequestId {
    /// Creates a new unpredictable request ID.
    #[must_use]
    pub fn generate() -> Self {
        Self(Uuid::new_v4().simple().to_string())
    }

    /// Returns the identifier as an HTTP-safe ASCII string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromRequest for RequestId {
    fn from_request<'a>(
        request: &'a mut Request,
        _params: &'a RouteParams,
        _state: &'a StateStore,
    ) -> BoxFuture<'a, rustee_core::Result<Self>> {
        async move {
            request.extensions().get::<Self>().cloned().ok_or_else(|| {
                Error::new(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    "request_id_missing",
                    "request correlation middleware is required",
                )
            })
        }
        .boxed()
    }
}

/// Tower layer that injects a generated [`RequestId`] and emits one completion event per request.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[must_use = "a request ID layer must be applied to a service to have an effect"]
pub struct RequestIdLayer;

impl RequestIdLayer {
    /// Creates a request-correlation layer with the default `X-Request-Id` response header.
    pub const fn new() -> Self {
        Self
    }
}

/// Internal integration hook for assigning a parent before a Rustee request span starts.
///
/// Optional observability adapters place [`RequestSpanParent`] in the request extensions before
/// this layer. The hook runs after the request span is created but before it is entered, so an
/// adapter can associate it with a remote trace without making this crate depend on a telemetry
/// SDK.
#[doc(hidden)]
pub trait RequestSpanParentHook: Send + Sync + 'static {
    /// Associates `span` with one optional integration-specific parent.
    fn apply(&self, span: &tracing::Span);
}

/// Request extension carrying one [`RequestSpanParentHook`] for [`RequestIdLayer`].
///
/// This is public only so optional observability integration crates can install it. Applications
/// should use the integration crate's request layer instead of constructing this value directly.
#[doc(hidden)]
#[derive(Clone)]
pub struct RequestSpanParent(Arc<dyn RequestSpanParentHook>);

impl RequestSpanParent {
    /// Wraps an optional integration-specific parent hook.
    #[must_use]
    pub fn new(parent: impl RequestSpanParentHook) -> Self {
        Self(Arc::new(parent))
    }

    /// Applies the contained parent before a request span is entered.
    pub fn apply(&self, span: &tracing::Span) {
        self.0.apply(span);
    }
}

impl fmt::Debug for RequestSpanParent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequestSpanParent")
            .finish_non_exhaustive()
    }
}

/// Service produced by [`RequestIdLayer`].
#[derive(Clone, Debug)]
pub struct RequestIdService {
    inner: BoxCloneService<Request, Response, Infallible>,
}

impl<S> Layer<S> for RequestIdLayer
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = RequestIdService;

    fn layer(&self, inner: S) -> Self::Service {
        RequestIdService {
            inner: BoxCloneService::new(inner),
        }
    }
}

impl Service<Request> for RequestIdService {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, mut request: Request) -> Self::Future {
        let span_parent = request.extensions().get::<RequestSpanParent>().cloned();
        let request_id = RequestId::generate();
        let method = request.method().clone();
        request.extensions_mut().insert(request_id.clone());
        let mut inner = self.inner.clone();
        let span = tracing::info_span!(
            "rustee.request",
            otel.name = "HTTP request",
            otel.kind = "server",
            otel.status_code = tracing::field::Empty,
            request_id = %request_id,
            method = %method,
            http.request.method = %method,
            status = tracing::field::Empty,
            http.response.status_code = tracing::field::Empty,
            duration_ms = tracing::field::Empty,
            route = tracing::field::Empty,
        );
        if let Some(span_parent) = span_parent {
            span_parent.apply(&span);
        }
        let completion_span = span.clone();

        async move {
            let started_at = Instant::now();
            let mut response = inner.call(request).await?;
            let status = response.status();
            let duration_ms = started_at.elapsed().as_millis();
            completion_span.record("status", tracing::field::display(status.as_u16()));
            completion_span.record(
                "http.response.status_code",
                tracing::field::display(status.as_u16()),
            );
            completion_span.record(
                "otel.status_code",
                tracing::field::display(if status.is_server_error() {
                    "ERROR"
                } else {
                    "UNSET"
                }),
            );
            completion_span.record("duration_ms", tracing::field::display(duration_ms));
            if let Some(route) = response.extensions().get::<RouteClassification>() {
                completion_span.record("route", tracing::field::display(route.as_str()));
            }
            info!(parent: &completion_span, "request completed");
            response.headers_mut().insert(
                REQUEST_ID_HEADER,
                HeaderValue::from_str(request_id.as_str())
                    .expect("generated UUID request ID must be a valid header value"),
            );
            Ok(response)
        }
        .instrument(span)
        .boxed()
    }
}

/// Thread-safe, exporter-neutral request metric collector.
///
/// It deliberately records only bounded status-class labels and router classifications. A router
/// classification is either a configured route template or a framework-reserved outcome label.
/// Raw paths, credentials, hosts, and request IDs belong in an application-specific exporter
/// policy, not the framework default.
#[derive(Clone, Debug)]
pub struct RequestMetrics {
    state: Arc<Mutex<RequestMetricsState>>,
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
        let buckets = buckets.into_iter().collect::<Vec<_>>();
        validate_duration_buckets(&buckets)?;
        Ok(Self {
            state: Arc::new(Mutex::new(RequestMetricsState {
                duration_bucket_counts: vec![0; buckets.len()],
                duration_buckets: buckets,
                ..RequestMetricsState::default()
            })),
        })
    }

    /// Returns a point-in-time snapshot suitable for an exporter or readiness diagnostic.
    ///
    /// # Panics
    ///
    /// Panics only if a concurrent metrics update poisoned the internal mutex.
    #[must_use]
    pub fn snapshot(&self) -> RequestMetricsSnapshot {
        let state = self
            .state
            .lock()
            .expect("request metrics lock must not be poisoned");
        RequestMetricsSnapshot {
            in_flight: state.in_flight,
            completed: state.completed,
            status_classes: state.status_classes.clone(),
            route_classification_status_classes: state.route_classification_status_classes.clone(),
            duration_bucket_counts: state
                .duration_buckets
                .iter()
                .copied()
                .zip(state.duration_bucket_counts.iter().copied())
                .collect(),
            total_duration: state.total_duration,
        }
    }

    fn started(&self) -> InFlightRequest {
        let mut state = self
            .state
            .lock()
            .expect("request metrics lock must not be poisoned");
        state.in_flight += 1;
        InFlightRequest {
            metrics: self.clone(),
            completed: false,
        }
    }
    fn finished(&self, response: &Response, duration: Duration) {
        let mut state = self
            .state
            .lock()
            .expect("request metrics lock must not be poisoned");
        state.in_flight = state.in_flight.saturating_sub(1);
        state.completed += 1;
        let status = response.status();
        *state
            .status_classes
            .entry(status.as_u16() / 100)
            .or_default() += 1;
        if let Some(route) = response.extensions().get::<RouteClassification>() {
            *state
                .route_classification_status_classes
                .entry((route.as_str().to_owned(), status.as_u16() / 100))
                .or_default() += 1;
        }
        let RequestMetricsState {
            duration_buckets,
            duration_bucket_counts,
            ..
        } = &mut *state;
        for (upper_bound, count) in duration_buckets.iter().zip(duration_bucket_counts) {
            if duration <= *upper_bound {
                *count += 1;
            }
        }
        state.total_duration = state.total_duration.saturating_add(duration);
    }
    fn cancelled(&self) {
        let mut state = self
            .state
            .lock()
            .expect("request metrics lock must not be poisoned");
        state.in_flight = state.in_flight.saturating_sub(1);
    }
}

#[derive(Debug, Default)]
struct RequestMetricsState {
    in_flight: u64,
    completed: u64,
    status_classes: BTreeMap<u16, u64>,
    route_classification_status_classes: BTreeMap<(String, u16), u64>,
    duration_buckets: Vec<Duration>,
    duration_bucket_counts: Vec<u64>,
    total_duration: Duration,
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

fn validate_duration_buckets(buckets: &[Duration]) -> Result<(), RequestMetricsConfigError> {
    if buckets.is_empty() {
        return Err(RequestMetricsConfigError::EmptyDurationBuckets);
    }
    if buckets.len() > MAX_REQUEST_DURATION_BUCKETS {
        return Err(RequestMetricsConfigError::TooManyDurationBuckets);
    }
    if buckets.iter().any(Duration::is_zero) {
        return Err(RequestMetricsConfigError::ZeroDurationBucket);
    }
    if buckets.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(RequestMetricsConfigError::UnorderedDurationBuckets);
    }
    Ok(())
}
struct InFlightRequest {
    metrics: RequestMetrics,
    completed: bool,
}
impl InFlightRequest {
    fn finish(mut self, response: &Response, duration: Duration) {
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

/// Immutable view of request metrics collected by [`RequestMetrics`].
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
    /// A request only contributes when the Rustee router attached a [`RouteClassification`] to
    /// its response. Raw request paths are never collected.
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

/// Tower layer that records bounded request lifecycle metrics into [`RequestMetrics`].
#[derive(Clone, Debug)]
#[must_use = "a metrics layer must be applied to a service to have an effect"]
pub struct MetricsLayer {
    metrics: RequestMetrics,
}
impl MetricsLayer {
    pub fn new(metrics: RequestMetrics) -> Self {
        Self { metrics }
    }
}
#[derive(Clone, Debug)]
pub struct MetricsService {
    inner: BoxCloneService<Request, Response, Infallible>,
    metrics: RequestMetrics,
}
impl<S> Layer<S> for MetricsLayer
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = MetricsService;
    fn layer(&self, inner: S) -> Self::Service {
        MetricsService {
            inner: BoxCloneService::new(inner),
            metrics: self.metrics.clone(),
        }
    }
}
impl Service<Request> for MetricsService {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;
    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }
    fn call(&mut self, request: Request) -> Self::Future {
        let mut inner = self.inner.clone();
        let lease = self.metrics.started();
        async move {
            let started = Instant::now();
            let response = inner.call(request).await?;
            lease.finish(&response, started.elapsed());
            Ok(response)
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, time::Duration};

    use http::{Request as HttpRequest, StatusCode, header::HeaderName};
    use rustee_core::empty_body;
    use rustee_router::App;
    use tower::{Layer, ServiceExt, service_fn};

    use super::{
        MetricsLayer, RequestId, RequestIdLayer, RequestMetrics, RequestMetricsConfigError,
    };

    #[tokio::test]
    async fn generates_and_returns_a_request_id_without_trusting_client_input() {
        let service = RequestIdLayer::new().layer(
            App::new().get("/health", |request_id: RequestId| async move {
                request_id.to_string()
            }),
        );
        let request = HttpRequest::builder()
            .method("GET")
            .uri("/health")
            .header(HeaderName::from_static("x-request-id"), "client-controlled")
            .body(empty_body())
            .expect("test request must build");

        let response = service
            .oneshot(request)
            .await
            .expect("service is infallible");
        assert_eq!(response.status(), StatusCode::OK);
        let request_id = response
            .headers()
            .get("x-request-id")
            .expect("response must include a request ID")
            .to_str()
            .expect("generated ID must be ASCII");
        assert_ne!(request_id, "client-controlled");
        assert_eq!(request_id.len(), 32);
        assert!(request_id.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }

    #[tokio::test]
    async fn metrics_collect_bounded_completion_data() {
        let metrics = RequestMetrics::new();
        let service = MetricsLayer::new(metrics.clone()).layer(
            App::new()
                .get("/ok", || async { "ok" })
                .get("/missing", || async { (StatusCode::NOT_FOUND, "missing") }),
        );
        let ok = HttpRequest::builder()
            .uri("/ok")
            .body(empty_body())
            .unwrap();
        let missing = HttpRequest::builder()
            .uri("/missing")
            .body(empty_body())
            .unwrap();
        let unmatched = HttpRequest::builder()
            .uri("/not-in-the-route-table")
            .body(empty_body())
            .unwrap();
        let method_mismatch = HttpRequest::builder()
            .method("POST")
            .uri("/ok")
            .body(empty_body())
            .unwrap();

        assert_eq!(
            service.clone().oneshot(ok).await.unwrap().status(),
            StatusCode::OK
        );
        assert_eq!(
            service.clone().oneshot(missing).await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            service.clone().oneshot(unmatched).await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            service.oneshot(method_mismatch).await.unwrap().status(),
            StatusCode::METHOD_NOT_ALLOWED
        );
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.in_flight(), 0);
        assert_eq!(snapshot.completed(), 4);
        assert_eq!(snapshot.status_class(2), 1);
        assert_eq!(snapshot.status_class(4), 3);
        assert_eq!(snapshot.route_classification_status_class("/ok", 2), 1);
        assert_eq!(snapshot.route_classification_status_class("/missing", 4), 1);
        assert_eq!(
            snapshot.route_classification_status_class("<not-found>", 4),
            1
        );
        assert_eq!(
            snapshot.route_classification_status_class("<method-not-allowed>", 4),
            1
        );
        assert_eq!(
            snapshot.route_classification_status_class("/not-in-the-route-table", 4),
            0
        );
    }

    #[tokio::test]
    async fn cancelled_request_does_not_leak_in_flight_or_count_as_completed() {
        let metrics = RequestMetrics::new();
        let service = MetricsLayer::new(metrics.clone()).layer(service_fn(|_| async {
            futures_util::future::pending::<Result<rustee_core::Response, Infallible>>().await
        }));
        let request = HttpRequest::builder().uri("/").body(empty_body()).unwrap();
        let mut request_future = Box::pin(service.oneshot(request));
        tokio::select! {
            _ = request_future.as_mut() => panic!("pending test request must not complete"),
            () = tokio::task::yield_now() => {}
        }
        assert_eq!(metrics.snapshot().in_flight(), 1);
        drop(request_future);
        assert_eq!(metrics.snapshot().in_flight(), 0);
        assert_eq!(metrics.snapshot().completed(), 0);
    }

    #[test]
    fn duration_histogram_is_bounded_validated_and_cumulative() {
        assert!(matches!(
            RequestMetrics::with_duration_buckets([]),
            Err(RequestMetricsConfigError::EmptyDurationBuckets)
        ));
        assert!(matches!(
            RequestMetrics::with_duration_buckets([Duration::ZERO]),
            Err(RequestMetricsConfigError::ZeroDurationBucket)
        ));
        assert!(matches!(
            RequestMetrics::with_duration_buckets([
                Duration::from_millis(20),
                Duration::from_millis(10),
            ]),
            Err(RequestMetricsConfigError::UnorderedDurationBuckets)
        ));

        let metrics = RequestMetrics::with_duration_buckets([
            Duration::from_millis(10),
            Duration::from_millis(100),
        ])
        .unwrap();
        let response = rustee_core::response(http::StatusCode::OK, empty_body());
        metrics.finished(&response, Duration::from_millis(5));
        metrics.finished(&response, Duration::from_millis(25));
        metrics.finished(&response, Duration::from_millis(250));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.completed(), 3);
        assert_eq!(
            snapshot.duration_bucket_count(Duration::from_millis(10)),
            Some(1)
        );
        assert_eq!(
            snapshot.duration_bucket_count(Duration::from_millis(100)),
            Some(2)
        );
        assert_eq!(snapshot.total_duration(), Duration::from_millis(280));
    }
}
