use std::{convert::Infallible, sync::Arc, time::Duration};

use http::{Request as HttpRequest, StatusCode};
use rustee_core::{RouteClassification, empty_body, response};
use rustee_router::App;
use tower::{Layer, ServiceExt, service_fn};

use super::{MetricsLayer, RequestMetrics, RequestMetricsConfigError};

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

#[test]
fn metrics_saturate_domain_counters_while_observing_histograms() {
    let metrics = RequestMetrics::new();
    {
        let mut state = metrics.lock_state();
        state.in_flight = u64::MAX;
    }
    let lease = metrics.started();
    assert_eq!(metrics.snapshot().in_flight(), u64::MAX);
    drop(lease);

    {
        let mut state = metrics.lock_state();
        state.in_flight = 1;
        state.completed = u64::MAX;
        state.status_classes.insert(2, u64::MAX);
        state
            .route_classification_status_classes
            .insert(("<not-found>".to_owned(), 2), u64::MAX);
    }
    let mut completed = response(StatusCode::OK, empty_body());
    completed
        .extensions_mut()
        .insert(RouteClassification::not_found());
    metrics.finished(&completed, Duration::ZERO);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.completed(), u64::MAX);
    assert_eq!(snapshot.status_class(2), u64::MAX);
    assert_eq!(
        snapshot.route_classification_status_class("<not-found>", 2),
        u64::MAX
    );
    assert!(
        snapshot
            .duration_bucket_counts()
            .all(|(_, count)| count == 1)
    );
}

#[tokio::test]
async fn poisoned_metrics_state_does_not_interrupt_requests() {
    let metrics = RequestMetrics::new();
    let state = Arc::clone(&metrics.state);
    let poisoned = std::thread::spawn(move || {
        let _guard = state.lock().expect("test state lock must be available");
        panic!("poison request metrics state");
    });
    assert!(poisoned.join().is_err());

    let service = MetricsLayer::new(metrics.clone()).layer(service_fn(|_| async {
        Ok::<_, Infallible>(rustee_core::response(StatusCode::OK, empty_body()))
    }));
    let request = HttpRequest::builder().uri("/").body(empty_body()).unwrap();
    let response = service.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.in_flight(), 0);
    assert_eq!(snapshot.completed(), 1);
    assert_eq!(snapshot.status_class(2), 1);
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
