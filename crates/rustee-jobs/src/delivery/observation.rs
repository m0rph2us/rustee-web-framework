//! Delivery lifecycle observation with panic isolation and abandoned-task reporting.

use std::{
    fmt,
    num::NonZeroU16,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    time::{Duration, Instant},
};

/// Result of one broker delivery after its source message has been durably settled.
///
/// The value intentionally excludes payload bytes, job IDs, queue routes, broker handles, and
/// handler error text. Those values either create unbounded metrics labels or can carry secrets.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum JobDeliveryOutcome {
    /// The source delivery was acknowledged or deleted after successful handling.
    Acknowledged,
    /// The source delivery was durably scheduled for a later attempt.
    Retried,
    /// The source delivery was copied to its dead-letter route and then acknowledged or deleted.
    DeadLettered,
    /// The worker could not durably settle the source delivery.
    Unsettled,
}

impl JobDeliveryOutcome {
    /// Returns the bounded, exporter-safe outcome label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Acknowledged => "acknowledged",
            Self::Retried => "retried",
            Self::DeadLettered => "dead_lettered",
            Self::Unsettled => "unsettled",
        }
    }
}

/// Metadata emitted when one provider worker starts processing a delivery.
///
/// `provider` must be a stable implementation identifier such as `nats_jetstream`; it is not a
/// broker URL, queue name, or user-configurable route.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobDeliveryStarted {
    provider: &'static str,
}

impl JobDeliveryStarted {
    /// Returns the stable provider identifier.
    #[must_use]
    pub const fn provider(self) -> &'static str {
        self.provider
    }
}

/// Metadata emitted after a provider worker completes or abandons one delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobDeliveryFinished {
    provider: &'static str,
    attempt: Option<NonZeroU16>,
    outcome: JobDeliveryOutcome,
    duration: Duration,
}

impl JobDeliveryFinished {
    /// Returns the stable provider identifier.
    #[must_use]
    pub const fn provider(self) -> &'static str {
        self.provider
    }

    /// Returns the one-based delivery attempt when the provider could recover it safely.
    #[must_use]
    pub const fn attempt(self) -> Option<NonZeroU16> {
        self.attempt
    }

    /// Returns the terminal settlement result observed by the worker.
    #[must_use]
    pub const fn outcome(self) -> JobDeliveryOutcome {
        self.outcome
    }

    /// Returns the elapsed worker processing time, including durable settlement.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.duration
    }
}

/// Synchronous, non-blocking observer for durable job delivery lifecycle events.
///
/// Implementations should aggregate locally or hand off work to a bounded exporter queue. The
/// framework catches observer panics so telemetry cannot change acknowledgement semantics.
pub trait JobDeliveryObserver: Send + Sync + 'static {
    /// Records the start of one received delivery.
    fn on_delivery_started(&self, delivery: JobDeliveryStarted);

    /// Records a completed delivery settlement or an unsettled worker failure.
    fn on_delivery_finished(&self, delivery: JobDeliveryFinished);
}

/// No-op observer used by workers that have not opted into job delivery telemetry.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopJobDeliveryObserver;

impl JobDeliveryObserver for NoopJobDeliveryObserver {
    fn on_delivery_started(&self, _delivery: JobDeliveryStarted) {}

    fn on_delivery_finished(&self, _delivery: JobDeliveryFinished) {}
}

/// In-progress delivery observation owned by one provider worker task.
///
/// Dropping this value without [`Self::finish`] records an `unsettled` result, including task
/// cancellation during a bounded worker drain.
pub struct JobDeliveryObservation {
    observer: Arc<dyn JobDeliveryObserver>,
    provider: &'static str,
    started_at: Instant,
    finished: bool,
}

impl JobDeliveryObservation {
    /// Starts observing a delivery for one stable provider implementation identifier.
    #[must_use]
    pub fn start(observer: Arc<dyn JobDeliveryObserver>, provider: &'static str) -> Self {
        notify_started(&observer, JobDeliveryStarted { provider });
        Self {
            observer,
            provider,
            started_at: Instant::now(),
            finished: false,
        }
    }

    /// Emits the final result after durable provider settlement completes.
    pub fn finish(mut self, attempt: Option<NonZeroU16>, outcome: JobDeliveryOutcome) {
        self.finished = true;
        notify_finished(
            &self.observer,
            JobDeliveryFinished {
                provider: self.provider,
                attempt,
                outcome,
                duration: self.started_at.elapsed(),
            },
        );
    }
}

impl Drop for JobDeliveryObservation {
    fn drop(&mut self) {
        if !self.finished {
            notify_finished(
                &self.observer,
                JobDeliveryFinished {
                    provider: self.provider,
                    attempt: None,
                    outcome: JobDeliveryOutcome::Unsettled,
                    duration: self.started_at.elapsed(),
                },
            );
        }
    }
}

impl fmt::Debug for JobDeliveryObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobDeliveryObservation")
            .field("provider", &self.provider)
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

fn notify_started(observer: &Arc<dyn JobDeliveryObserver>, delivery: JobDeliveryStarted) {
    let _ = catch_unwind(AssertUnwindSafe(|| observer.on_delivery_started(delivery)));
}

fn notify_finished(observer: &Arc<dyn JobDeliveryObserver>, delivery: JobDeliveryFinished) {
    let _ = catch_unwind(AssertUnwindSafe(|| observer.on_delivery_finished(delivery)));
}
