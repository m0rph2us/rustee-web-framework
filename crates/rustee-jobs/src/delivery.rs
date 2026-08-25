//! Provider-neutral retry decisions and delivery-lifecycle observation.

mod observation;
mod retry;

pub use observation::{
    JobDeliveryFinished, JobDeliveryObservation, JobDeliveryObserver, JobDeliveryOutcome,
    JobDeliveryStarted, NoopJobDeliveryObserver,
};
pub use retry::{DeliveryAction, RetryPolicy};

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroU16,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::{
        DeliveryAction, JobDeliveryFinished, JobDeliveryObservation, JobDeliveryObserver,
        JobDeliveryOutcome, JobDeliveryStarted, RetryPolicy,
    };

    #[derive(Default)]
    struct CapturingObserver {
        started: Mutex<Vec<&'static str>>,
        finished: Mutex<Vec<JobDeliveryFinished>>,
    }

    impl JobDeliveryObserver for CapturingObserver {
        fn on_delivery_started(&self, delivery: JobDeliveryStarted) {
            self.started.lock().unwrap().push(delivery.provider());
        }

        fn on_delivery_finished(&self, delivery: JobDeliveryFinished) {
            self.finished.lock().unwrap().push(delivery);
        }
    }

    struct PanickingObserver;

    impl JobDeliveryObserver for PanickingObserver {
        fn on_delivery_started(&self, _delivery: JobDeliveryStarted) {
            panic!("observer panic must not affect job settlement");
        }

        fn on_delivery_finished(&self, _delivery: JobDeliveryFinished) {
            panic!("observer panic must not affect job settlement");
        }
    }

    #[test]
    fn retry_policy_uses_bounded_exponential_backoff_then_dead_letters() {
        let policy = RetryPolicy {
            max_deliveries: 3,
            initial_backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(3),
        };

        assert_eq!(
            policy.after_failure(1),
            DeliveryAction::Retry {
                next_attempt: 2,
                delay: Duration::from_secs(2),
            }
        );
        assert_eq!(
            policy.after_failure(2),
            DeliveryAction::Retry {
                next_attempt: 3,
                delay: Duration::from_secs(3),
            }
        );
        assert_eq!(policy.after_failure(3), DeliveryAction::DeadLetter);
    }

    #[test]
    fn retry_policy_requires_a_delivery_budget_and_ordered_nonzero_delays() {
        let valid = RetryPolicy::default();
        assert!(valid.is_valid());
        assert!(
            !RetryPolicy {
                max_deliveries: 0,
                ..valid
            }
            .is_valid()
        );
        assert!(
            !RetryPolicy {
                initial_backoff: Duration::ZERO,
                ..valid
            }
            .is_valid()
        );
        assert!(
            !RetryPolicy {
                max_backoff: Duration::from_millis(1),
                ..valid
            }
            .is_valid()
        );
    }

    #[test]
    fn invalid_retry_policy_fails_closed_when_used_directly() {
        let policy = RetryPolicy {
            initial_backoff: Duration::ZERO,
            ..RetryPolicy::default()
        };

        assert_eq!(policy.after_failure(1), DeliveryAction::DeadLetter);
    }

    #[test]
    fn explicit_settlement_is_recorded_once() {
        let observer = Arc::new(CapturingObserver::default());
        JobDeliveryObservation::start(observer.clone(), "redis_streams")
            .finish(NonZeroU16::new(2), JobDeliveryOutcome::Retried);

        assert_eq!(
            observer.started.lock().unwrap().as_slice(),
            ["redis_streams"]
        );
        let finished = observer.finished.lock().unwrap();
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].provider(), "redis_streams");
        assert_eq!(finished[0].attempt(), NonZeroU16::new(2));
        assert_eq!(finished[0].outcome(), JobDeliveryOutcome::Retried);
    }

    #[test]
    fn abandoned_delivery_is_reported_as_unsettled() {
        let observer = Arc::new(CapturingObserver::default());
        drop(JobDeliveryObservation::start(
            observer.clone(),
            "amazon_sqs",
        ));

        let finished = observer.finished.lock().unwrap();
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].provider(), "amazon_sqs");
        assert_eq!(finished[0].attempt(), None);
        assert_eq!(finished[0].outcome(), JobDeliveryOutcome::Unsettled);
    }

    #[test]
    fn observer_panics_do_not_change_delivery_flow() {
        JobDeliveryObservation::start(Arc::new(PanickingObserver), "test")
            .finish(None, JobDeliveryOutcome::Acknowledged);
    }
}
