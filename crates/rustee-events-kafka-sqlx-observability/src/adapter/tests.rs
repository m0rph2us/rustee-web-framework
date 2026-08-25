use std::{sync::Arc, time::Duration};

use rustee_events_kafka_sqlx::{
    KafkaDelayedRetryBacklog, KafkaDelayedRetryRelayObserver, KafkaDelayedRetryRelayOutcome,
    KafkaDelayedRetryRelayPassObservation,
};

use crate::{KafkaDelayedRetryRelayMetrics, KafkaDelayedRetryRelayMetricsConfigError};

#[test]
fn collector_tracks_success_abandonment_and_latest_backlog() {
    let metrics = KafkaDelayedRetryRelayMetrics::new();
    let observer: Arc<dyn KafkaDelayedRetryRelayObserver> = Arc::new(metrics.clone());
    KafkaDelayedRetryRelayPassObservation::start(Arc::clone(&observer))
        .finish(KafkaDelayedRetryRelayOutcome::Succeeded, Some(3));
    drop(KafkaDelayedRetryRelayPassObservation::start(observer));
    metrics.record_backlog(KafkaDelayedRetryBacklog {
        unpublished: 8,
        due: 5,
        leased: 2,
        oldest_due_age: Some(Duration::from_secs(7)),
    });

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.started(), 2);
    assert_eq!(snapshot.in_flight(), 0);
    assert_eq!(snapshot.completed(), 2);
    assert_eq!(
        snapshot.outcome(KafkaDelayedRetryRelayOutcome::Succeeded),
        1
    );
    assert_eq!(
        snapshot.outcome(KafkaDelayedRetryRelayOutcome::Abandoned),
        1
    );
    assert_eq!(snapshot.published(), 3);
    assert_eq!(snapshot.backlog().unwrap().due, 5);
    assert!(snapshot.total_duration() < Duration::from_secs(1));
}

#[test]
fn collector_recovers_after_metrics_state_poisoning() {
    let metrics = KafkaDelayedRetryRelayMetrics::new();
    let state = Arc::clone(&metrics.state);
    let poisoned = std::thread::spawn(move || {
        let _guard = state.lock().expect("test state lock must be available");
        panic!("poison Kafka delayed-retry relay metrics state");
    });
    assert!(poisoned.join().is_err());

    let observer: Arc<dyn KafkaDelayedRetryRelayObserver> = Arc::new(metrics.clone());
    KafkaDelayedRetryRelayPassObservation::start(observer)
        .finish(KafkaDelayedRetryRelayOutcome::Succeeded, Some(3));
    metrics.record_backlog(KafkaDelayedRetryBacklog {
        unpublished: 8,
        due: 5,
        leased: 2,
        oldest_due_age: None,
    });

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.in_flight(), 0);
    assert_eq!(snapshot.started(), 1);
    assert_eq!(snapshot.completed(), 1);
    assert_eq!(snapshot.published(), 3);
    assert_eq!(snapshot.backlog().map(|backlog| backlog.due), Some(5));
}

#[test]
fn collector_saturates_terminal_outcome_counts() {
    let metrics = KafkaDelayedRetryRelayMetrics::new();
    {
        let mut state = metrics.lock_state();
        state
            .outcome_counts
            .insert(KafkaDelayedRetryRelayOutcome::Succeeded, u64::MAX);
    }

    let observer: Arc<dyn KafkaDelayedRetryRelayObserver> = Arc::new(metrics.clone());
    KafkaDelayedRetryRelayPassObservation::start(observer)
        .finish(KafkaDelayedRetryRelayOutcome::Succeeded, None);

    assert_eq!(
        metrics
            .snapshot()
            .outcome(KafkaDelayedRetryRelayOutcome::Succeeded),
        u64::MAX
    );
}

#[test]
fn collector_rejects_unbounded_duration_configuration() {
    assert!(matches!(
        KafkaDelayedRetryRelayMetrics::with_duration_buckets([]),
        Err(KafkaDelayedRetryRelayMetricsConfigError::EmptyDurationBuckets)
    ));
    assert!(matches!(
        KafkaDelayedRetryRelayMetrics::with_duration_buckets([Duration::ZERO]),
        Err(KafkaDelayedRetryRelayMetricsConfigError::ZeroDurationBucket)
    ));
    assert!(matches!(
        KafkaDelayedRetryRelayMetrics::with_duration_buckets([
            Duration::from_secs(2),
            Duration::from_secs(1)
        ]),
        Err(KafkaDelayedRetryRelayMetricsConfigError::UnorderedDurationBuckets)
    ));
}
