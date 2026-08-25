//! Content-free transactional-outbox relay pass observation.

use std::{
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    time::{Duration, Instant},
};

use super::RelayReport;

/// Fixed relay category used by bounded observability labels.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RelayPassKind {
    /// The relay is publishing append-only events.
    Event,
    /// The relay is publishing durable jobs.
    Job,
}

impl RelayPassKind {
    /// Returns the exporter-safe relay category label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Event => "event",
            Self::Job => "job",
        }
    }
}

/// Terminal outcome of one relay pass.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RelayPassOutcome {
    /// The pass completed its bounded claim/publish/confirm sequence.
    Succeeded,
    /// A database or publisher failure ended the pass.
    Failed,
    /// The async task was cancelled before the pass returned a result.
    Abandoned,
}

impl RelayPassOutcome {
    /// Returns the exporter-safe pass outcome label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Abandoned => "abandoned",
        }
    }
}

/// Metadata emitted when an event or job relay pass starts.
///
/// Destination, message identifiers, payloads, broker endpoint, and configuration are excluded
/// so an observer can safely use this as bounded operational telemetry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelayPassStarted {
    kind: RelayPassKind,
}

impl RelayPassStarted {
    /// Returns whether this pass relays events or durable jobs.
    #[must_use]
    pub const fn kind(self) -> RelayPassKind {
        self.kind
    }
}

/// Metadata emitted when an event or job relay pass finishes or is abandoned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelayPassFinished {
    kind: RelayPassKind,
    outcome: RelayPassOutcome,
    report: Option<RelayReport>,
    duration: Duration,
}

impl RelayPassFinished {
    /// Returns whether this pass relayed events or durable jobs.
    #[must_use]
    pub const fn kind(self) -> RelayPassKind {
        self.kind
    }

    /// Returns the terminal status without exposing provider error detail.
    #[must_use]
    pub const fn outcome(self) -> RelayPassOutcome {
        self.outcome
    }

    /// Returns bounded pass counts when the pass reached a reportable terminal state.
    ///
    /// Database failures before a stable report and externally cancelled passes return `None`.
    #[must_use]
    pub const fn report(self) -> Option<RelayReport> {
        self.report
    }

    /// Returns elapsed pass time, including broker publish and durable confirmation work.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.duration
    }
}

/// Synchronous, non-blocking observer for one transactional-outbox relay pass.
///
/// Implementations should aggregate locally or hand work to a bounded exporter queue. Observer
/// panics are caught so observability cannot alter durable publish or retry semantics.
pub trait OutboxRelayObserver: Send + Sync + 'static {
    /// Records the beginning of one bounded relay pass.
    fn on_relay_pass_started(&self, pass: RelayPassStarted);

    /// Records one completed, failed, or externally abandoned relay pass.
    fn on_relay_pass_finished(&self, pass: RelayPassFinished);
}

/// No-op observer used unless a relay opts into observability.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopOutboxRelayObserver;

impl OutboxRelayObserver for NoopOutboxRelayObserver {
    fn on_relay_pass_started(&self, _pass: RelayPassStarted) {}

    fn on_relay_pass_finished(&self, _pass: RelayPassFinished) {}
}

/// In-progress observability value owned by a single relay pass future.
///
/// Dropping this value without [`Self::finish`] records an `abandoned` pass, including a task
/// cancellation while it held or was about to obtain database leases.
pub struct RelayPassObservation {
    observer: Arc<dyn OutboxRelayObserver>,
    kind: RelayPassKind,
    started_at: Instant,
    finished: bool,
}

impl RelayPassObservation {
    /// Starts observing one bounded event or durable-job relay pass.
    #[must_use]
    pub fn start(observer: Arc<dyn OutboxRelayObserver>, kind: RelayPassKind) -> Self {
        notify_relay_started(&observer, RelayPassStarted { kind });
        Self {
            observer,
            kind,
            started_at: Instant::now(),
            finished: false,
        }
    }

    /// Emits a terminal pass outcome after the relay future has returned.
    pub fn finish(mut self, outcome: RelayPassOutcome, report: Option<RelayReport>) {
        self.finished = true;
        notify_relay_finished(
            &self.observer,
            RelayPassFinished {
                kind: self.kind,
                outcome,
                report,
                duration: self.started_at.elapsed(),
            },
        );
    }
}

impl Drop for RelayPassObservation {
    fn drop(&mut self) {
        if !self.finished {
            notify_relay_finished(
                &self.observer,
                RelayPassFinished {
                    kind: self.kind,
                    outcome: RelayPassOutcome::Abandoned,
                    report: None,
                    duration: self.started_at.elapsed(),
                },
            );
        }
    }
}

impl fmt::Debug for RelayPassObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelayPassObservation")
            .field("kind", &self.kind)
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

fn notify_relay_started(observer: &Arc<dyn OutboxRelayObserver>, pass: RelayPassStarted) {
    let _ = catch_unwind(AssertUnwindSafe(|| observer.on_relay_pass_started(pass)));
}

fn notify_relay_finished(observer: &Arc<dyn OutboxRelayObserver>, pass: RelayPassFinished) {
    let _ = catch_unwind(AssertUnwindSafe(|| observer.on_relay_pass_finished(pass)));
}
