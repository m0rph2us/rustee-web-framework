//! Shared bounded relay execution, retry settlement, polling, and observation lifecycle.

use std::{future::Future, marker::PhantomData, sync::Arc};

use sqlx::PgPool;

use crate::{LeaseOutcome, OutboxDestination, PostgresOutbox};

use super::{
    config::{RelayConfig, RelayLoopConfig, RelayLoopReport, RelayReport},
    kind::RelayKind,
    observation::{
        NoopOutboxRelayObserver, OutboxRelayObserver, RelayPassObservation, RelayPassOutcome,
    },
    service::RelayError,
};

/// Shared relay state and execution policy parameterized by event/job storage operations.
#[derive(Clone)]
pub(super) struct RelayCore<P, K> {
    pool: PgPool,
    outbox: PostgresOutbox,
    publisher: P,
    destination: OutboxDestination,
    pub(super) config: RelayConfig,
    observer: Arc<dyn OutboxRelayObserver>,
    kind: PhantomData<fn() -> K>,
}

impl<P, K> RelayCore<P, K> {
    pub(super) fn new(
        pool: PgPool,
        publisher: P,
        destination: OutboxDestination,
        config: RelayConfig,
    ) -> Self {
        Self {
            pool,
            outbox: PostgresOutbox,
            publisher,
            destination,
            config,
            observer: Arc::new(NoopOutboxRelayObserver),
            kind: PhantomData,
        }
    }
}

impl<P, K> RelayCore<P, K>
where
    K: RelayKind<P>,
{
    pub(super) fn with_relay_observer(mut self, observer: Arc<dyn OutboxRelayObserver>) -> Self {
        self.observer = observer;
        self
    }

    pub(super) async fn relay_once(&self) -> Result<RelayReport, RelayError<K::Error>> {
        let observation = RelayPassObservation::start(Arc::clone(&self.observer), K::PASS_KIND);
        match self.relay_once_inner().await {
            Ok(report) => {
                observation.finish(RelayPassOutcome::Succeeded, Some(report));
                Ok(report)
            }
            Err(error) => {
                let report = match &error {
                    RelayError::Outbox(_) => None,
                    RelayError::Publisher { report, .. } => Some(*report),
                };
                observation.finish(RelayPassOutcome::Failed, report);
                Err(error)
            }
        }
    }

    async fn relay_once_inner(&self) -> Result<RelayReport, RelayError<K::Error>> {
        let leases = K::lease(
            &self.outbox,
            &self.pool,
            &self.destination,
            self.config.lease,
        )
        .await?;
        let mut report = RelayReport {
            claimed: leases.len(),
            ..RelayReport::default()
        };
        for lease in leases {
            match K::publish(&self.publisher, &lease).await {
                Ok(()) => match K::acknowledge(&self.outbox, &self.pool, &lease).await? {
                    LeaseOutcome::Applied => report.published += 1,
                    LeaseOutcome::Lost => report.lease_lost += 1,
                },
                Err(source) => {
                    match K::retry(&self.outbox, &self.pool, &lease, self.config.retry_delay)
                        .await?
                    {
                        LeaseOutcome::Applied => report.retry_scheduled += 1,
                        LeaseOutcome::Lost => report.lease_lost += 1,
                    }
                    return Err(RelayError::Publisher { source, report });
                }
            }
        }
        Ok(report)
    }

    pub(super) async fn run_until<Shutdown>(
        &self,
        loop_config: RelayLoopConfig,
        shutdown: Shutdown,
    ) -> Result<RelayLoopReport, RelayError<K::Error>>
    where
        Shutdown: Future<Output = ()> + Send,
    {
        tokio::pin!(shutdown);
        let mut total = RelayLoopReport::default();
        loop {
            tokio::select! {
                biased;
                () = &mut shutdown => return Ok(total),
                () = tokio::task::yield_now() => {}
            }

            let report = self.relay_once().await?;
            total.record(report);
            if report.claimed == 0 {
                tokio::select! {
                    biased;
                    () = &mut shutdown => return Ok(total),
                    () = tokio::time::sleep(loop_config.idle_delay) => {}
                }
            }
        }
    }
}
