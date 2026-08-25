use std::{num::NonZeroU16, sync::Arc, time::Duration};

use rustee_events::{EventDeliveryObservation, EventDeliveryOutcome};

use crate::{
    EventMetrics, EventMetricsConfigError,
    collector::{MAX_PROVIDER_LABELS, OTHER_PROVIDER},
};

#[test]
fn collector_tracks_settlement_and_drop_as_unsettled() {
    let metrics = EventMetrics::new();
    let observer = Arc::new(metrics.clone());
    EventDeliveryObservation::start(observer.clone(), "apache_kafka")
        .finish(NonZeroU16::new(1), EventDeliveryOutcome::Acknowledged);
    drop(EventDeliveryObservation::start(observer, "apache_kafka"));

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.in_flight(), 0);
    assert_eq!(snapshot.started(), 2);
    assert_eq!(snapshot.completed(), 2);
    assert_eq!(
        snapshot.outcome("apache_kafka", EventDeliveryOutcome::Acknowledged),
        1
    );
    assert_eq!(
        snapshot.outcome("apache_kafka", EventDeliveryOutcome::Unsettled),
        1
    );
    assert_eq!(snapshot.duration_bucket_counts().count(), 12);
}

#[test]
fn collector_keeps_provider_outcomes_within_the_label_bound() {
    let metrics = EventMetrics::new();
    for provider in [
        "provider-01",
        "provider-02",
        "provider-03",
        "provider-04",
        "provider-05",
        "provider-06",
        "provider-07",
        "provider-08",
        "provider-09",
        "provider-10",
        "provider-11",
        "provider-12",
        "provider-13",
        "provider-14",
        "provider-15",
        "provider-16",
        "provider-17",
    ] {
        EventDeliveryObservation::start(Arc::new(metrics.clone()), provider)
            .finish(NonZeroU16::new(1), EventDeliveryOutcome::Acknowledged);
    }

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.outcome_counts().count(), MAX_PROVIDER_LABELS);
    assert_eq!(
        snapshot.outcome("provider-15", EventDeliveryOutcome::Acknowledged),
        1
    );
    assert_eq!(
        snapshot.outcome("provider-16", EventDeliveryOutcome::Acknowledged),
        0
    );
    assert_eq!(
        snapshot.outcome(OTHER_PROVIDER, EventDeliveryOutcome::Acknowledged),
        2
    );
}

#[test]
fn collector_recovers_after_metrics_state_poisoning() {
    let metrics = EventMetrics::new();
    let state = Arc::clone(&metrics.state);
    let poisoned = std::thread::spawn(move || {
        let _guard = state.lock().expect("test state lock must be available");
        panic!("poison event metrics state");
    });
    assert!(poisoned.join().is_err());

    EventDeliveryObservation::start(Arc::new(metrics.clone()), "apache_kafka")
        .finish(NonZeroU16::new(1), EventDeliveryOutcome::Acknowledged);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.in_flight(), 0);
    assert_eq!(snapshot.started(), 1);
    assert_eq!(snapshot.completed(), 1);
    assert_eq!(
        snapshot.outcome("apache_kafka", EventDeliveryOutcome::Acknowledged),
        1
    );
}

#[test]
fn collector_rejects_unbounded_histogram_configuration() {
    assert_eq!(
        EventMetrics::with_duration_buckets(std::iter::empty::<Duration>()).unwrap_err(),
        EventMetricsConfigError::EmptyDurationBuckets
    );
    assert_eq!(
        EventMetrics::with_duration_buckets([Duration::from_secs(2), Duration::from_secs(1)])
            .unwrap_err(),
        EventMetricsConfigError::UnorderedDurationBuckets
    );
}
