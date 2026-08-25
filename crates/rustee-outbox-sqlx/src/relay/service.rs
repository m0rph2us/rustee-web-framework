//! Public event and job relay services plus content-free failure diagnostics.

use std::{error::Error as StdError, fmt, future::Future, sync::Arc};

use rustee_events::EventPublisher;
use rustee_jobs::JobPublisher;
use sqlx::PgPool;

use crate::{OutboxDestination, OutboxError};

use super::{
    config::{RelayConfig, RelayLoopConfig, RelayLoopReport, RelayReport},
    executor::RelayCore,
    kind::{EventRelayKind, JobRelayKind},
    observation::OutboxRelayObserver,
};

/// A single-pass event relay wired to an existing [`EventPublisher`].
#[derive(Clone)]
pub struct EventOutboxRelay<P> {
    core: RelayCore<P, EventRelayKind>,
}

impl<P> fmt::Debug for EventOutboxRelay<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventOutboxRelay")
            .field("destination", &"[REDACTED]")
            .field("config", &self.core.config)
            .finish_non_exhaustive()
    }
}

impl<P> EventOutboxRelay<P>
where
    P: EventPublisher,
{
    /// Creates a relay for exactly one event destination and publisher configuration.
    #[must_use]
    pub fn new(
        pool: PgPool,
        publisher: P,
        destination: OutboxDestination,
        config: RelayConfig,
    ) -> Self {
        Self {
            core: RelayCore::new(pool, publisher, destination, config),
        }
    }

    /// Attaches one exporter-neutral relay pass observer.
    #[must_use]
    pub fn with_relay_observer(mut self, observer: Arc<dyn OutboxRelayObserver>) -> Self {
        self.core = self.core.with_relay_observer(observer);
        self
    }

    /// Publishes one bounded batch and settles each successful lease.
    ///
    /// A publisher error schedules the failed row for retry using a constant sanitized failure
    /// category, then ends this pass with the original provider error. A caller owns loop timing,
    /// readiness, metrics, and graceful shutdown.
    ///
    /// # Errors
    ///
    /// Returns [`RelayError::Outbox`] when a lease transition cannot be persisted, or
    /// [`RelayError::Publisher`] after a failed broker append was rescheduled.
    pub async fn relay_once(&self) -> Result<RelayReport, RelayError<P::Error>> {
        self.core.relay_once().await
    }

    /// Repeatedly executes bounded passes until the supplied shutdown future resolves.
    ///
    /// A shutdown signal is observed before each new pass and while waiting after an empty pass.
    /// A pass already holding leases finishes before shutdown is returned, so the loop never drops
    /// an in-progress pass merely to stop quickly. Publisher and database errors retain
    /// [`Self::relay_once`]'s behavior and end the loop for the application supervisor to handle.
    ///
    /// # Errors
    ///
    /// Returns the first [`RelayError`] produced by one bounded pass.
    pub async fn run_until<Shutdown>(
        &self,
        loop_config: RelayLoopConfig,
        shutdown: Shutdown,
    ) -> Result<RelayLoopReport, RelayError<P::Error>>
    where
        Shutdown: Future<Output = ()> + Send,
    {
        self.core.run_until(loop_config, shutdown).await
    }
}

/// A single-pass durable-job relay wired to an existing [`JobPublisher`].
#[derive(Clone)]
pub struct JobOutboxRelay<P> {
    core: RelayCore<P, JobRelayKind>,
}

impl<P> fmt::Debug for JobOutboxRelay<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobOutboxRelay")
            .field("destination", &"[REDACTED]")
            .field("config", &self.core.config)
            .finish_non_exhaustive()
    }
}

impl<P> JobOutboxRelay<P>
where
    P: JobPublisher,
{
    /// Creates a relay for exactly one durable-job destination and publisher configuration.
    #[must_use]
    pub fn new(
        pool: PgPool,
        publisher: P,
        destination: OutboxDestination,
        config: RelayConfig,
    ) -> Self {
        Self {
            core: RelayCore::new(pool, publisher, destination, config),
        }
    }

    /// Attaches one exporter-neutral relay pass observer.
    #[must_use]
    pub fn with_relay_observer(mut self, observer: Arc<dyn OutboxRelayObserver>) -> Self {
        self.core = self.core.with_relay_observer(observer);
        self
    }

    /// Publishes one bounded batch and settles each successful lease.
    ///
    /// The retry and ownership behavior matches [`EventOutboxRelay::relay_once`].
    ///
    /// # Errors
    ///
    /// Returns [`RelayError::Outbox`] when a lease transition cannot be persisted, or
    /// [`RelayError::Publisher`] after a failed broker publish was rescheduled.
    pub async fn relay_once(&self) -> Result<RelayReport, RelayError<P::Error>> {
        self.core.relay_once().await
    }

    /// Repeatedly executes bounded passes until the supplied shutdown future resolves.
    ///
    /// The shutdown and failure behavior matches [`EventOutboxRelay::run_until`]. This remains an
    /// explicit caller-owned future rather than a background scheduler.
    ///
    /// # Errors
    ///
    /// Returns the first [`RelayError`] produced by one bounded pass.
    pub async fn run_until<Shutdown>(
        &self,
        loop_config: RelayLoopConfig,
        shutdown: Shutdown,
    ) -> Result<RelayLoopReport, RelayError<P::Error>>
    where
        Shutdown: Future<Output = ()> + Send,
    {
        self.core.run_until(loop_config, shutdown).await
    }
}

/// Database or provider failure while executing a relay pass.
#[derive(thiserror::Error)]
pub enum RelayError<E>
where
    E: StdError + Send + Sync + 'static,
{
    /// A durable outbox operation failed.
    #[error("PostgreSQL outbox relay operation failed")]
    Outbox(#[from] OutboxError),
    /// The broker publisher failed after the row had been leased and rescheduled.
    #[error("outbox publisher failed after retry was scheduled")]
    Publisher {
        /// The publisher error returned after the row was rescheduled.
        #[source]
        source: E,
        /// Counts collected before the failed pass ended.
        report: RelayReport,
    },
}

impl<E> fmt::Debug for RelayError<E>
where
    E: StdError + Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Outbox(_) => "outbox_failed",
            Self::Publisher { .. } => "publisher_failed",
        };
        formatter
            .debug_struct("RelayError")
            .field("kind", &kind)
            .finish()
    }
}
