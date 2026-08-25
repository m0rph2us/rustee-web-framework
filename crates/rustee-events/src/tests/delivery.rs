use std::{
    num::NonZeroU16,
    sync::{Arc, Mutex},
};

use crate::{
    EventDeliveryFinished, EventDeliveryObservation, EventDeliveryObserver, EventDeliveryOutcome,
    EventDeliveryStarted,
};

struct PanickingDeliveryObserver;

#[derive(Default)]
struct CapturingDeliveryObserver {
    started: Mutex<Vec<&'static str>>,
    finished: Mutex<Vec<EventDeliveryFinished>>,
}

impl EventDeliveryObserver for CapturingDeliveryObserver {
    fn on_delivery_started(&self, delivery: EventDeliveryStarted) {
        self.started.lock().unwrap().push(delivery.provider());
    }

    fn on_delivery_finished(&self, delivery: EventDeliveryFinished) {
        self.finished.lock().unwrap().push(delivery);
    }
}

impl EventDeliveryObserver for PanickingDeliveryObserver {
    fn on_delivery_started(&self, _: EventDeliveryStarted) {
        panic!("observer panic must not escape a consumer");
    }

    fn on_delivery_finished(&self, _: EventDeliveryFinished) {
        panic!("observer panic must not escape a consumer");
    }
}

#[test]
fn delivery_observation_isolates_observer_panics() {
    EventDeliveryObservation::start(Arc::new(PanickingDeliveryObserver), "test_provider")
        .finish(None, EventDeliveryOutcome::Acknowledged);
}

#[test]
fn delivery_observation_records_one_explicit_settlement() {
    let observer = Arc::new(CapturingDeliveryObserver::default());
    EventDeliveryObservation::start(observer.clone(), "apache_kafka")
        .finish(NonZeroU16::new(2), EventDeliveryOutcome::Retried);

    assert_eq!(
        observer.started.lock().unwrap().as_slice(),
        ["apache_kafka"]
    );
    let finished = observer.finished.lock().unwrap();
    assert_eq!(finished.len(), 1);
    assert_eq!(finished[0].provider(), "apache_kafka");
    assert_eq!(finished[0].attempt(), NonZeroU16::new(2));
    assert_eq!(finished[0].outcome(), EventDeliveryOutcome::Retried);
}

#[test]
fn dropped_delivery_observation_reports_unsettled() {
    let observer = Arc::new(CapturingDeliveryObserver::default());
    drop(EventDeliveryObservation::start(
        observer.clone(),
        "apache_kafka",
    ));

    let finished = observer.finished.lock().unwrap();
    assert_eq!(finished.len(), 1);
    assert_eq!(finished[0].provider(), "apache_kafka");
    assert_eq!(finished[0].attempt(), None);
    assert_eq!(finished[0].outcome(), EventDeliveryOutcome::Unsettled);
}
