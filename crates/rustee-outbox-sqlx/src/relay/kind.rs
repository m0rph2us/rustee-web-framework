//! Event and job storage and publisher differences for the common relay executor.

use std::{error::Error as StdError, time::Duration};

use futures_util::future::BoxFuture;
use rustee_events::EventPublisher;
use rustee_jobs::JobPublisher;
use sqlx::PgPool;

use crate::{
    LeaseConfig, LeaseOutcome, LeasedEvent, LeasedJob, OutboxDestination, OutboxError,
    PostgresOutbox,
};

use super::observation::RelayPassKind;

/// Private event/job differences supplied to the shared relay execution policy.
pub(super) trait RelayKind<P> {
    type Lease;
    type Error: StdError + Send + Sync + 'static;

    const PASS_KIND: RelayPassKind;

    fn lease<'a>(
        outbox: &'a PostgresOutbox,
        pool: &'a PgPool,
        destination: &'a OutboxDestination,
        config: LeaseConfig,
    ) -> BoxFuture<'a, Result<Vec<Self::Lease>, OutboxError>>;

    fn publish<'a>(
        publisher: &'a P,
        lease: &'a Self::Lease,
    ) -> BoxFuture<'a, Result<(), Self::Error>>;

    fn acknowledge<'a>(
        outbox: &'a PostgresOutbox,
        pool: &'a PgPool,
        lease: &'a Self::Lease,
    ) -> BoxFuture<'a, Result<LeaseOutcome, OutboxError>>;

    fn retry<'a>(
        outbox: &'a PostgresOutbox,
        pool: &'a PgPool,
        lease: &'a Self::Lease,
        delay: Duration,
    ) -> BoxFuture<'a, Result<LeaseOutcome, OutboxError>>;
}

#[derive(Clone, Copy)]
pub(super) struct EventRelayKind;

impl<P> RelayKind<P> for EventRelayKind
where
    P: EventPublisher,
{
    type Lease = LeasedEvent;
    type Error = P::Error;

    const PASS_KIND: RelayPassKind = RelayPassKind::Event;

    fn lease<'a>(
        outbox: &'a PostgresOutbox,
        pool: &'a PgPool,
        destination: &'a OutboxDestination,
        config: LeaseConfig,
    ) -> BoxFuture<'a, Result<Vec<Self::Lease>, OutboxError>> {
        Box::pin(outbox.lease_events(pool, destination, config))
    }

    fn publish<'a>(
        publisher: &'a P,
        lease: &'a Self::Lease,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        publisher.publish(lease.message().clone())
    }

    fn acknowledge<'a>(
        outbox: &'a PostgresOutbox,
        pool: &'a PgPool,
        lease: &'a Self::Lease,
    ) -> BoxFuture<'a, Result<LeaseOutcome, OutboxError>> {
        Box::pin(outbox.acknowledge_event(pool, lease))
    }

    fn retry<'a>(
        outbox: &'a PostgresOutbox,
        pool: &'a PgPool,
        lease: &'a Self::Lease,
        delay: Duration,
    ) -> BoxFuture<'a, Result<LeaseOutcome, OutboxError>> {
        Box::pin(outbox.retry_event(pool, lease, delay))
    }
}

#[derive(Clone, Copy)]
pub(super) struct JobRelayKind;

impl<P> RelayKind<P> for JobRelayKind
where
    P: JobPublisher,
{
    type Lease = LeasedJob;
    type Error = P::Error;

    const PASS_KIND: RelayPassKind = RelayPassKind::Job;

    fn lease<'a>(
        outbox: &'a PostgresOutbox,
        pool: &'a PgPool,
        destination: &'a OutboxDestination,
        config: LeaseConfig,
    ) -> BoxFuture<'a, Result<Vec<Self::Lease>, OutboxError>> {
        Box::pin(outbox.lease_jobs(pool, destination, config))
    }

    fn publish<'a>(
        publisher: &'a P,
        lease: &'a Self::Lease,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        publisher.publish(lease.message().clone())
    }

    fn acknowledge<'a>(
        outbox: &'a PostgresOutbox,
        pool: &'a PgPool,
        lease: &'a Self::Lease,
    ) -> BoxFuture<'a, Result<LeaseOutcome, OutboxError>> {
        Box::pin(outbox.acknowledge_job(pool, lease))
    }

    fn retry<'a>(
        outbox: &'a PostgresOutbox,
        pool: &'a PgPool,
        lease: &'a Self::Lease,
        delay: Duration,
    ) -> BoxFuture<'a, Result<LeaseOutcome, OutboxError>> {
        Box::pin(outbox.retry_job(pool, lease, delay))
    }
}
