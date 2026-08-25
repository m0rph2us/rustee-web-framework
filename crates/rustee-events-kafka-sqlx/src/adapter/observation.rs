use std::{
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    time::{Duration, Instant},
};

/// Terminal outcome of one Kafka delayed-retry relay pass.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum KafkaDelayedRetryRelayOutcome {
    /// The pass claimed, published, and confirmed all selected rows.
    Succeeded,
    /// A database or Kafka error ended the pass.
    Failed,
    /// The relay future was cancelled before it returned a terminal result.
    Abandoned,
}

impl KafkaDelayedRetryRelayOutcome {
    /// Returns the fixed exporter-safe outcome label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Abandoned => "abandoned",
        }
    }
}

/// Metadata emitted when a bounded Kafka delayed-retry relay pass starts.
///
/// Topic names, event identifiers, payloads, endpoints, and configuration are intentionally
/// absent so an observer can retain only aggregate operational telemetry.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct KafkaDelayedRetryRelayPassStarted;

/// Metadata emitted when a Kafka delayed-retry relay pass finishes or is abandoned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KafkaDelayedRetryRelayPassFinished {
    outcome: KafkaDelayedRetryRelayOutcome,
    published: Option<u16>,
    duration: Duration,
}

impl KafkaDelayedRetryRelayPassFinished {
    /// Returns the terminal status without exposing a database or Kafka error.
    #[must_use]
    pub const fn outcome(self) -> KafkaDelayedRetryRelayOutcome {
        self.outcome
    }

    /// Returns records confirmed in a fully successful pass.
    ///
    /// Failed and externally abandoned passes return `None`: they may have reached Kafka
    /// before a later database transition failed, so callers must not treat a missing count as
    /// proof that no duplicate was published.
    #[must_use]
    pub const fn published(self) -> Option<u16> {
        self.published
    }

    /// Returns elapsed time for the bounded database/Kafka pass.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.duration
    }
}

/// Synchronous, non-blocking observer for Kafka delayed-retry relay passes.
///
/// Implementations should aggregate locally or enqueue bounded export work. Observer panics
/// are caught, so observability cannot change source-offset or retry delivery semantics.
pub trait KafkaDelayedRetryRelayObserver: Send + Sync + 'static {
    /// Records the beginning of one bounded relay pass.
    fn on_relay_pass_started(&self, pass: KafkaDelayedRetryRelayPassStarted);

    /// Records one completed, failed, or externally abandoned pass.
    fn on_relay_pass_finished(&self, pass: KafkaDelayedRetryRelayPassFinished);
}

/// No-op observer used unless a relay explicitly opts into observability.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopKafkaDelayedRetryRelayObserver;

impl KafkaDelayedRetryRelayObserver for NoopKafkaDelayedRetryRelayObserver {
    fn on_relay_pass_started(&self, _pass: KafkaDelayedRetryRelayPassStarted) {}

    fn on_relay_pass_finished(&self, _pass: KafkaDelayedRetryRelayPassFinished) {}
}

/// In-progress observability value owned by one delayed-retry relay pass future.
///
/// Dropping this value without [`Self::finish`] records an `abandoned` pass, including task
/// cancellation while the relay is about to acquire or holds database leases.
pub struct KafkaDelayedRetryRelayPassObservation {
    observer: Arc<dyn KafkaDelayedRetryRelayObserver>,
    started_at: Instant,
    finished: bool,
}

impl KafkaDelayedRetryRelayPassObservation {
    /// Starts observing one bounded delayed-retry relay pass.
    #[must_use]
    pub fn start(observer: Arc<dyn KafkaDelayedRetryRelayObserver>) -> Self {
        notify_relay_started(&observer, KafkaDelayedRetryRelayPassStarted);
        Self {
            observer,
            started_at: Instant::now(),
            finished: false,
        }
    }

    /// Emits a terminal outcome after the relay future returns.
    pub fn finish(mut self, outcome: KafkaDelayedRetryRelayOutcome, published: Option<u16>) {
        self.finished = true;
        notify_relay_finished(
            &self.observer,
            KafkaDelayedRetryRelayPassFinished {
                outcome,
                published,
                duration: self.started_at.elapsed(),
            },
        );
    }
}

impl Drop for KafkaDelayedRetryRelayPassObservation {
    fn drop(&mut self) {
        if !self.finished {
            notify_relay_finished(
                &self.observer,
                KafkaDelayedRetryRelayPassFinished {
                    outcome: KafkaDelayedRetryRelayOutcome::Abandoned,
                    published: None,
                    duration: self.started_at.elapsed(),
                },
            );
        }
    }
}

impl fmt::Debug for KafkaDelayedRetryRelayPassObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaDelayedRetryRelayPassObservation")
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

fn notify_relay_started(
    observer: &Arc<dyn KafkaDelayedRetryRelayObserver>,
    pass: KafkaDelayedRetryRelayPassStarted,
) {
    let _ = catch_unwind(AssertUnwindSafe(|| observer.on_relay_pass_started(pass)));
}

fn notify_relay_finished(
    observer: &Arc<dyn KafkaDelayedRetryRelayObserver>,
    pass: KafkaDelayedRetryRelayPassFinished,
) {
    let _ = catch_unwind(AssertUnwindSafe(|| observer.on_relay_pass_finished(pass)));
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::{
        KafkaDelayedRetryRelayObserver, KafkaDelayedRetryRelayOutcome,
        KafkaDelayedRetryRelayPassFinished, KafkaDelayedRetryRelayPassObservation,
        KafkaDelayedRetryRelayPassStarted,
    };

    #[derive(Default)]
    struct RecordingObserver {
        finished: Mutex<Vec<(KafkaDelayedRetryRelayOutcome, Option<u16>)>>,
        started: Mutex<usize>,
    }

    impl KafkaDelayedRetryRelayObserver for RecordingObserver {
        fn on_relay_pass_started(&self, _pass: KafkaDelayedRetryRelayPassStarted) {
            *self
                .started
                .lock()
                .expect("test observer lock is available") += 1;
        }

        fn on_relay_pass_finished(&self, pass: KafkaDelayedRetryRelayPassFinished) {
            self.finished
                .lock()
                .expect("test observer lock is available")
                .push((pass.outcome(), pass.published()));
        }
    }

    #[test]
    fn completed_observation_records_the_successful_pass() {
        let observer = std::sync::Arc::new(RecordingObserver::default());
        let observation = KafkaDelayedRetryRelayPassObservation::start(observer.clone());
        observation.finish(KafkaDelayedRetryRelayOutcome::Succeeded, Some(3));

        assert_eq!(*observer.started.lock().unwrap(), 1);
        assert_eq!(
            *observer.finished.lock().unwrap(),
            vec![(KafkaDelayedRetryRelayOutcome::Succeeded, Some(3))]
        );
    }

    #[test]
    fn dropped_observation_records_an_abandoned_pass() {
        let observer = std::sync::Arc::new(RecordingObserver::default());
        drop(KafkaDelayedRetryRelayPassObservation::start(
            observer.clone(),
        ));

        assert_eq!(
            *observer.finished.lock().unwrap(),
            vec![(KafkaDelayedRetryRelayOutcome::Abandoned, None)]
        );
    }

    struct PanickingObserver;

    impl KafkaDelayedRetryRelayObserver for PanickingObserver {
        fn on_relay_pass_started(&self, _pass: KafkaDelayedRetryRelayPassStarted) {
            panic!("observer must not affect the relay");
        }

        fn on_relay_pass_finished(&self, _pass: KafkaDelayedRetryRelayPassFinished) {
            panic!("observer must not affect the relay");
        }
    }

    #[test]
    fn observer_panics_do_not_escape_the_observation_lifecycle() {
        let observer = std::sync::Arc::new(PanickingObserver);
        KafkaDelayedRetryRelayPassObservation::start(observer)
            .finish(KafkaDelayedRetryRelayOutcome::Failed, None);
    }
}
