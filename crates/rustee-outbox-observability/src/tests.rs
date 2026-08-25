use std::{sync::Arc, time::Duration};

use rustee_outbox_sqlx::{RelayPassKind, RelayPassObservation, RelayPassOutcome, RelayReport};

use crate::{OutboxRelayMetrics, OutboxRelayMetricsConfigError, RelayRowCount};

#[test]
fn collector_tracks_reportable_and_abandoned_passes() {
    let metrics = OutboxRelayMetrics::new();
    let observer = Arc::new(metrics.clone());
    RelayPassObservation::start(observer.clone(), RelayPassKind::Job).finish(
        RelayPassOutcome::Succeeded,
        Some(RelayReport {
            claimed: 3,
            published: 2,
            retry_scheduled: 1,
            lease_lost: 0,
        }),
    );
    drop(RelayPassObservation::start(observer, RelayPassKind::Event));

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.in_flight(), 0);
    assert_eq!(snapshot.started(), 2);
    assert_eq!(snapshot.completed(), 2);
    assert_eq!(
        snapshot.outcome(RelayPassKind::Job, RelayPassOutcome::Succeeded),
        1
    );
    assert_eq!(
        snapshot.outcome(RelayPassKind::Event, RelayPassOutcome::Abandoned),
        1
    );
    assert_eq!(snapshot.rows(RelayPassKind::Job, RelayRowCount::Claimed), 3);
    assert_eq!(
        snapshot.rows(RelayPassKind::Job, RelayRowCount::Published),
        2
    );
    assert_eq!(
        snapshot.rows(RelayPassKind::Job, RelayRowCount::RetryScheduled),
        1
    );
    assert_eq!(snapshot.duration_bucket_counts().count(), 12);
}

#[test]
fn collector_recovers_after_metrics_state_poisoning() {
    let metrics = OutboxRelayMetrics::new();
    let state = Arc::clone(&metrics.state);
    let poisoned = std::thread::spawn(move || {
        let _guard = state.lock().expect("test state lock must be available");
        panic!("poison outbox relay metrics state");
    });
    assert!(poisoned.join().is_err());

    RelayPassObservation::start(Arc::new(metrics.clone()), RelayPassKind::Job)
        .finish(RelayPassOutcome::Succeeded, None);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.in_flight(), 0);
    assert_eq!(snapshot.started(), 1);
    assert_eq!(snapshot.completed(), 1);
    assert_eq!(
        snapshot.outcome(RelayPassKind::Job, RelayPassOutcome::Succeeded),
        1
    );
}

#[test]
fn collector_saturates_outcome_and_row_counts() {
    let metrics = OutboxRelayMetrics::new();
    {
        let mut state = metrics.lock_state();
        state
            .outcome_counts
            .insert((RelayPassKind::Job, RelayPassOutcome::Succeeded), u64::MAX);
        state
            .row_counts
            .insert((RelayPassKind::Job, RelayRowCount::Claimed), u64::MAX);
    }

    RelayPassObservation::start(Arc::new(metrics.clone()), RelayPassKind::Job).finish(
        RelayPassOutcome::Succeeded,
        Some(RelayReport {
            claimed: 1,
            published: 0,
            retry_scheduled: 0,
            lease_lost: 0,
        }),
    );

    let snapshot = metrics.snapshot();
    assert_eq!(
        snapshot.outcome(RelayPassKind::Job, RelayPassOutcome::Succeeded),
        u64::MAX
    );
    assert_eq!(
        snapshot.rows(RelayPassKind::Job, RelayRowCount::Claimed),
        u64::MAX
    );
}

#[test]
fn collector_rejects_unbounded_histogram_configuration() {
    assert_eq!(
        OutboxRelayMetrics::with_duration_buckets(std::iter::empty::<Duration>()).unwrap_err(),
        OutboxRelayMetricsConfigError::EmptyDurationBuckets
    );
    assert_eq!(
        OutboxRelayMetrics::with_duration_buckets([Duration::from_secs(2), Duration::from_secs(1)])
            .unwrap_err(),
        OutboxRelayMetricsConfigError::UnorderedDurationBuckets
    );
}
