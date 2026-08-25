use std::{
    fmt,
    num::NonZeroU16,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    time::{Duration, Instant},
};

/// The terminal settlement result observed by an event-stream consumer.
///
/// Event streams remain at-least-once systems. `Acknowledged`, `Retried`, and `DeadLettered`
/// mean the provider reported the named source-position settlement; they do not prove that an
/// external handler side effect occurred exactly once.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum EventDeliveryOutcome {
    /// The source position was acknowledged after the typed handler completed successfully.
    Acknowledged,
    /// The source delivery was routed to its retry destination and then acknowledged.
    Retried,
    /// The source delivery was routed to its dead-letter destination and then acknowledged.
    DeadLettered,
    /// The consumer could not durably settle the source delivery.
    Unsettled,
}

impl EventDeliveryOutcome {
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

/// Metadata emitted when one provider consumer starts processing a delivery.
///
/// `provider` must be a stable implementation identifier such as `apache_kafka`; it is not a
/// broker endpoint, topic, consumer group, or application-configured route.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventDeliveryStarted {
    provider: &'static str,
}

impl EventDeliveryStarted {
    /// Returns the stable provider identifier.
    #[must_use]
    pub const fn provider(self) -> &'static str {
        self.provider
    }
}

/// Metadata emitted after a provider consumer settles or abandons one delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventDeliveryFinished {
    provider: &'static str,
    attempt: Option<NonZeroU16>,
    outcome: EventDeliveryOutcome,
    duration: Duration,
}

impl EventDeliveryFinished {
    /// Returns the stable provider identifier.
    #[must_use]
    pub const fn provider(self) -> &'static str {
        self.provider
    }

    /// Returns the one-based attempt when the provider could recover it without changing its
    /// ordinary delivery semantics.
    #[must_use]
    pub const fn attempt(self) -> Option<NonZeroU16> {
        self.attempt
    }

    /// Returns the terminal source-position settlement observed by the consumer.
    #[must_use]
    pub const fn outcome(self) -> EventDeliveryOutcome {
        self.outcome
    }

    /// Returns elapsed consumer processing time, including durable source settlement.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.duration
    }
}

/// Synchronous, non-blocking observer for durable event-delivery lifecycle events.
///
/// Implementations should aggregate locally or hand off work to a bounded exporter queue. The
/// framework catches observer panics so telemetry cannot change consumer settlement semantics.
pub trait EventDeliveryObserver: Send + Sync + 'static {
    /// Records the start of one received delivery.
    fn on_delivery_started(&self, delivery: EventDeliveryStarted);

    /// Records a completed source settlement or an unsettled consumer failure.
    fn on_delivery_finished(&self, delivery: EventDeliveryFinished);
}

/// No-op observer used by consumers that have not opted into event-delivery telemetry.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopEventDeliveryObserver;

impl EventDeliveryObserver for NoopEventDeliveryObserver {
    fn on_delivery_started(&self, _delivery: EventDeliveryStarted) {}

    fn on_delivery_finished(&self, _delivery: EventDeliveryFinished) {}
}

/// In-progress delivery observation owned by one provider consumer task.
///
/// Dropping this value without [`Self::finish`] records an `unsettled` result, including task
/// cancellation while the consumer is waiting for a handler, retry route, or source settlement.
pub struct EventDeliveryObservation {
    observer: Arc<dyn EventDeliveryObserver>,
    provider: &'static str,
    started_at: Instant,
    finished: bool,
}

impl EventDeliveryObservation {
    /// Starts observing a delivery for one stable provider implementation identifier.
    #[must_use]
    pub fn start(observer: Arc<dyn EventDeliveryObserver>, provider: &'static str) -> Self {
        notify_delivery_started(&observer, EventDeliveryStarted { provider });
        Self {
            observer,
            provider,
            started_at: Instant::now(),
            finished: false,
        }
    }

    /// Emits the final result after the provider settles the source position.
    pub fn finish(mut self, attempt: Option<NonZeroU16>, outcome: EventDeliveryOutcome) {
        self.finished = true;
        notify_delivery_finished(
            &self.observer,
            EventDeliveryFinished {
                provider: self.provider,
                attempt,
                outcome,
                duration: self.started_at.elapsed(),
            },
        );
    }
}

impl Drop for EventDeliveryObservation {
    fn drop(&mut self) {
        if !self.finished {
            notify_delivery_finished(
                &self.observer,
                EventDeliveryFinished {
                    provider: self.provider,
                    attempt: None,
                    outcome: EventDeliveryOutcome::Unsettled,
                    duration: self.started_at.elapsed(),
                },
            );
        }
    }
}

impl fmt::Debug for EventDeliveryObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventDeliveryObservation")
            .field("provider", &self.provider)
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

fn notify_delivery_started(
    observer: &Arc<dyn EventDeliveryObserver>,
    delivery: EventDeliveryStarted,
) {
    let _ = catch_unwind(AssertUnwindSafe(|| observer.on_delivery_started(delivery)));
}

fn notify_delivery_finished(
    observer: &Arc<dyn EventDeliveryObserver>,
    delivery: EventDeliveryFinished,
) {
    let _ = catch_unwind(AssertUnwindSafe(|| observer.on_delivery_finished(delivery)));
}
