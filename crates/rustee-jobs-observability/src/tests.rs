use std::{num::NonZeroU16, sync::Arc, time::Duration};

use rustee_jobs::{JobDeliveryObservation, JobDeliveryOutcome};

use crate::{
    JobMetrics, JobMetricsConfigError,
    collector::{MAX_PROVIDER_LABELS, OTHER_PROVIDER},
};

#[test]
fn collector_tracks_settlement_and_drop_as_unsettled() {
    let metrics = JobMetrics::new();
    let observer = Arc::new(metrics.clone());
    JobDeliveryObservation::start(observer.clone(), "nats_jetstream")
        .finish(NonZeroU16::new(1), JobDeliveryOutcome::Acknowledged);
    drop(JobDeliveryObservation::start(observer, "nats_jetstream"));

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.in_flight(), 0);
    assert_eq!(snapshot.started(), 2);
    assert_eq!(snapshot.completed(), 2);
    assert_eq!(
        snapshot.outcome("nats_jetstream", JobDeliveryOutcome::Acknowledged),
        1
    );
    assert_eq!(
        snapshot.outcome("nats_jetstream", JobDeliveryOutcome::Unsettled),
        1
    );
    assert_eq!(snapshot.duration_bucket_counts().count(), 12);
}

#[test]
fn collector_keeps_provider_outcomes_within_the_label_bound() {
    let metrics = JobMetrics::new();
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
        JobDeliveryObservation::start(Arc::new(metrics.clone()), provider)
            .finish(NonZeroU16::new(1), JobDeliveryOutcome::Acknowledged);
    }

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.outcome_counts().count(), MAX_PROVIDER_LABELS);
    assert_eq!(
        snapshot.outcome("provider-15", JobDeliveryOutcome::Acknowledged),
        1
    );
    assert_eq!(
        snapshot.outcome("provider-16", JobDeliveryOutcome::Acknowledged),
        0
    );
    assert_eq!(
        snapshot.outcome(OTHER_PROVIDER, JobDeliveryOutcome::Acknowledged),
        2
    );
}

#[test]
fn collector_recovers_after_metrics_state_poisoning() {
    let metrics = JobMetrics::new();
    let state = Arc::clone(&metrics.state);
    let poisoned = std::thread::spawn(move || {
        let _guard = state.lock().expect("test state lock must be available");
        panic!("poison job metrics state");
    });
    assert!(poisoned.join().is_err());

    JobDeliveryObservation::start(Arc::new(metrics.clone()), "nats_jetstream")
        .finish(NonZeroU16::new(1), JobDeliveryOutcome::Acknowledged);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.in_flight(), 0);
    assert_eq!(snapshot.started(), 1);
    assert_eq!(snapshot.completed(), 1);
    assert_eq!(
        snapshot.outcome("nats_jetstream", JobDeliveryOutcome::Acknowledged),
        1
    );
}

#[test]
fn collector_rejects_unbounded_histogram_configuration() {
    assert_eq!(
        JobMetrics::with_duration_buckets(std::iter::empty::<Duration>()).unwrap_err(),
        JobMetricsConfigError::EmptyDurationBuckets
    );
    assert_eq!(
        JobMetrics::with_duration_buckets([Duration::from_secs(2), Duration::from_secs(1)])
            .unwrap_err(),
        JobMetricsConfigError::UnorderedDurationBuckets
    );
}
