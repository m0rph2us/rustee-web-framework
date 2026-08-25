use std::{sync::Arc, time::Duration};

use rustee_jobs_cron_sqlx::{
    RecurringJobFireLimit, RecurringJobFireObservation, RecurringJobFireOutcome,
};

use crate::{
    RecurringJobFireMetrics, RecurringJobFireMetricsConfigError, RecurringJobFireRowCount,
};

#[test]
fn collector_tracks_reportable_and_abandoned_scheduler_passes() {
    let metrics = RecurringJobFireMetrics::new();
    let observer = Arc::new(metrics.clone());
    RecurringJobFireObservation::start(observer.clone(), RecurringJobFireLimit::default()).finish(
        RecurringJobFireOutcome::Succeeded,
        Some(rustee_jobs_cron_sqlx::RecurringJobFireReport::default()),
    );
    drop(RecurringJobFireObservation::start(
        observer,
        RecurringJobFireLimit::default(),
    ));

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.in_flight(), 0);
    assert_eq!(snapshot.started(), 2);
    assert_eq!(snapshot.completed(), 2);
    assert_eq!(snapshot.outcome(RecurringJobFireOutcome::Succeeded), 1);
    assert_eq!(snapshot.outcome(RecurringJobFireOutcome::Abandoned), 1);
    assert_eq!(snapshot.rows(RecurringJobFireRowCount::Claimed), 0);
    assert_eq!(snapshot.duration_bucket_counts().count(), 12);
}

#[test]
fn collector_recovers_after_metrics_state_poisoning() {
    let metrics = RecurringJobFireMetrics::new();
    let state = Arc::clone(&metrics.state);
    let poisoned = std::thread::spawn(move || {
        let _guard = state.lock().expect("test state lock must be available");
        panic!("poison recurring job fire metrics state");
    });
    assert!(poisoned.join().is_err());

    RecurringJobFireObservation::start(Arc::new(metrics.clone()), RecurringJobFireLimit::default())
        .finish(RecurringJobFireOutcome::Succeeded, None);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.in_flight(), 0);
    assert_eq!(snapshot.started(), 1);
    assert_eq!(snapshot.completed(), 1);
    assert_eq!(snapshot.outcome(RecurringJobFireOutcome::Succeeded), 1);
}

#[test]
fn collector_rejects_unbounded_histogram_configuration() {
    assert_eq!(
        RecurringJobFireMetrics::with_duration_buckets(std::iter::empty::<Duration>()).unwrap_err(),
        RecurringJobFireMetricsConfigError::EmptyDurationBuckets
    );
    assert_eq!(
        RecurringJobFireMetrics::with_duration_buckets([
            Duration::from_secs(2),
            Duration::from_secs(1)
        ])
        .unwrap_err(),
        RecurringJobFireMetricsConfigError::UnorderedDurationBuckets
    );
}
