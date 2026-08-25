//! Stable public facade for explicit event and job outbox relays.

mod config;
mod executor;
mod kind;
mod observation;
mod service;

pub use config::{
    RelayConfig, RelayConfigError, RelayLoopConfig, RelayLoopConfigError, RelayLoopReport,
    RelayReport,
};
pub use observation::{
    NoopOutboxRelayObserver, OutboxRelayObserver, RelayPassFinished, RelayPassKind,
    RelayPassObservation, RelayPassOutcome, RelayPassStarted,
};
pub use service::{EventOutboxRelay, JobOutboxRelay, RelayError};

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use futures_util::{future, future::BoxFuture};
    use rustee_events::{EventMessage, EventPublisher};
    use rustee_jobs::{JobMessage, JobPublisher};
    use sqlx::postgres::PgPoolOptions;

    use crate::OutboxDestination;

    use super::{EventOutboxRelay, JobOutboxRelay, RelayConfig};

    #[derive(Clone)]
    struct TestEventPublisher;

    impl EventPublisher for TestEventPublisher {
        type Error = Infallible;

        fn publish(&self, _: EventMessage) -> BoxFuture<'static, Result<(), Self::Error>> {
            Box::pin(future::ready(Ok(())))
        }
    }

    #[derive(Clone)]
    struct TestJobPublisher;

    impl JobPublisher for TestJobPublisher {
        type Error = Infallible;

        fn publish(&self, _: JobMessage) -> BoxFuture<'static, Result<(), Self::Error>> {
            Box::pin(future::ready(Ok(())))
        }
    }

    fn pool() -> sqlx::PgPool {
        PgPoolOptions::new()
            .connect_lazy("postgres://test:test@localhost/rustee")
            .expect("test PostgreSQL URL is valid")
    }

    #[tokio::test]
    async fn relay_debug_output_redacts_the_destination() {
        let event = EventOutboxRelay::new(
            pool(),
            TestEventPublisher,
            OutboxDestination::new("private-event-destination").unwrap(),
            RelayConfig::default(),
        );
        let job = JobOutboxRelay::new(
            pool(),
            TestJobPublisher,
            OutboxDestination::new("private-job-destination").unwrap(),
            RelayConfig::default(),
        );
        let output = format!("{event:?} {job:?}");

        assert!(!output.contains("private-event-destination"));
        assert!(!output.contains("private-job-destination"));
        assert!(output.contains("[REDACTED]"));
    }
}
