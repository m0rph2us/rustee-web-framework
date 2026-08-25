use std::{
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    time::{Duration, Instant},
};

use super::{RecurringJobFireLimit, RecurringJobFireReport};

/// Terminal result of one bounded recurring-scheduler pass.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RecurringJobFireOutcome {
    /// The pass committed every selected schedule transition.
    Succeeded,
    /// Storage or stored-schedule validation caused the transaction to roll back.
    Failed,
    /// The caller cancelled or dropped the pass before it returned a terminal result.
    Abandoned,
}

impl RecurringJobFireOutcome {
    /// Returns the stable exporter-safe outcome label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Abandoned => "abandoned",
        }
    }
}

/// Bounded metadata emitted when one scheduler pass starts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecurringJobFireStarted {
    limit: RecurringJobFireLimit,
}

impl RecurringJobFireStarted {
    /// Returns the maximum due rows this pass may claim.
    #[must_use]
    pub const fn limit(self) -> RecurringJobFireLimit {
        self.limit
    }
}

/// Terminal metadata emitted after one scheduler pass finishes or is abandoned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecurringJobFireFinished {
    outcome: RecurringJobFireOutcome,
    report: Option<RecurringJobFireReport>,
    duration: Duration,
}

impl RecurringJobFireFinished {
    /// Returns the sanitized terminal outcome.
    #[must_use]
    pub const fn outcome(self) -> RecurringJobFireOutcome {
        self.outcome
    }

    /// Returns aggregate counts only when the pass committed successfully.
    ///
    /// A failed or externally abandoned transaction returns `None`; callers must not infer how
    /// many staged rows became durable from work that was later rolled back.
    #[must_use]
    pub const fn report(self) -> Option<RecurringJobFireReport> {
        self.report
    }

    /// Returns elapsed pass time, including transaction and outbox staging work.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.duration
    }
}

/// Synchronous, non-blocking observer for a recurring scheduler pass.
///
/// Observers should aggregate locally or hand work to a bounded exporter queue. Observer panics
/// are caught so telemetry cannot alter durable schedule or outbox semantics.
pub trait RecurringJobFireObserver: Send + Sync + 'static {
    /// Records the beginning of one bounded scheduler pass.
    fn on_fire_started(&self, pass: RecurringJobFireStarted);

    /// Records one committed, failed, or externally abandoned scheduler pass.
    fn on_fire_finished(&self, pass: RecurringJobFireFinished);
}

/// No-op observer used unless the scheduler opts into pass observability.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopRecurringJobFireObserver;

impl RecurringJobFireObserver for NoopRecurringJobFireObserver {
    fn on_fire_started(&self, _pass: RecurringJobFireStarted) {}

    fn on_fire_finished(&self, _pass: RecurringJobFireFinished) {}
}

/// In-progress observability value owned by one [`super::PostgresRecurringJobs::fire_due`] future.
///
/// Dropping this value without [`Self::finish`] records an `abandoned` pass, including task
/// cancellation while a transaction was active. It contains no schedule key, destination,
/// payload, tenant, or storage error detail.
pub struct RecurringJobFireObservation {
    observer: Arc<dyn RecurringJobFireObserver>,
    started_at: Instant,
    finished: bool,
}

impl RecurringJobFireObservation {
    /// Starts observing one bounded recurring scheduler pass.
    #[must_use]
    pub fn start(
        observer: Arc<dyn RecurringJobFireObserver>,
        limit: RecurringJobFireLimit,
    ) -> Self {
        notify_fire_started(&observer, RecurringJobFireStarted { limit });
        Self {
            observer,
            started_at: Instant::now(),
            finished: false,
        }
    }

    /// Emits a terminal outcome after the pass has returned.
    pub fn finish(
        mut self,
        outcome: RecurringJobFireOutcome,
        report: Option<RecurringJobFireReport>,
    ) {
        self.finished = true;
        notify_fire_finished(
            &self.observer,
            RecurringJobFireFinished {
                outcome,
                report,
                duration: self.started_at.elapsed(),
            },
        );
    }
}

impl Drop for RecurringJobFireObservation {
    fn drop(&mut self) {
        if !self.finished {
            notify_fire_finished(
                &self.observer,
                RecurringJobFireFinished {
                    outcome: RecurringJobFireOutcome::Abandoned,
                    report: None,
                    duration: self.started_at.elapsed(),
                },
            );
        }
    }
}

impl fmt::Debug for RecurringJobFireObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecurringJobFireObservation")
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

fn notify_fire_started(
    observer: &Arc<dyn RecurringJobFireObserver>,
    pass: RecurringJobFireStarted,
) {
    let _ = catch_unwind(AssertUnwindSafe(|| observer.on_fire_started(pass)));
}

fn notify_fire_finished(
    observer: &Arc<dyn RecurringJobFireObserver>,
    pass: RecurringJobFireFinished,
) {
    let _ = catch_unwind(AssertUnwindSafe(|| observer.on_fire_finished(pass)));
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use super::{
        RecurringJobFireFinished, RecurringJobFireObservation, RecurringJobFireObserver,
        RecurringJobFireOutcome, RecurringJobFireStarted,
    };
    use crate::{RecurringJobFireLimit, RecurringJobFireReport};

    #[derive(Default)]
    struct RecordingFireObserver {
        started: AtomicUsize,
        finished: Mutex<Vec<RecurringJobFireFinished>>,
    }

    impl RecurringJobFireObserver for RecordingFireObserver {
        fn on_fire_started(&self, pass: RecurringJobFireStarted) {
            assert_eq!(pass.limit().get().get(), 25);
            self.started.fetch_add(1, Ordering::Relaxed);
        }

        fn on_fire_finished(&self, pass: RecurringJobFireFinished) {
            self.finished.lock().unwrap().push(pass);
        }
    }

    struct PanickingFireObserver;

    impl RecurringJobFireObserver for PanickingFireObserver {
        fn on_fire_started(&self, _pass: RecurringJobFireStarted) {
            panic!("observer panic must not affect schedule materialization");
        }

        fn on_fire_finished(&self, _pass: RecurringJobFireFinished) {
            panic!("observer panic must not affect schedule materialization");
        }
    }

    #[test]
    fn records_terminal_and_abandoned_outcomes() {
        let recorder = Arc::new(RecordingFireObserver::default());
        let observer: Arc<dyn RecurringJobFireObserver> = recorder.clone();
        RecurringJobFireObservation::start(observer.clone(), RecurringJobFireLimit::default())
            .finish(
                RecurringJobFireOutcome::Succeeded,
                Some(RecurringJobFireReport::default()),
            );
        drop(RecurringJobFireObservation::start(
            observer,
            RecurringJobFireLimit::default(),
        ));

        assert_eq!(recorder.started.load(Ordering::Relaxed), 2);
        let finished = recorder.finished.lock().unwrap();
        assert_eq!(finished.len(), 2);
        assert_eq!(finished[0].outcome(), RecurringJobFireOutcome::Succeeded);
        assert_eq!(
            finished[0].report(),
            Some(RecurringJobFireReport::default())
        );
        assert_eq!(finished[1].outcome(), RecurringJobFireOutcome::Abandoned);
        assert_eq!(finished[1].report(), None);
    }

    #[test]
    fn observer_panics_do_not_change_pass_completion() {
        RecurringJobFireObservation::start(
            Arc::new(PanickingFireObserver),
            RecurringJobFireLimit::default(),
        )
        .finish(RecurringJobFireOutcome::Succeeded, None);
    }
}
