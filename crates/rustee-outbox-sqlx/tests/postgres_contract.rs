//! Opt-in `PostgreSQL` transactional-outbox contract tests.

use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::future::BoxFuture;
use rustee_events::{Event, EventEnvelope, EventId, EventMessage, EventPublisher};
use rustee_jobs::{Job, JobEnvelope, JobMessage, JobPublisher};
use rustee_outbox_sqlx::{
    EventOutboxRelay, EventSchedule, INBOX_MIGRATION_SQL, InboxConsumer, InboxDecision,
    InboxMessageId, JobOutboxRelay, JobSchedule, LeaseConfig, LeaseOutcome, OUTBOX_MIGRATION_SQL,
    OUTBOX_PRIORITY_MIGRATION_SQL, OutboxDestination, OutboxMessage, OutboxPriority,
    OutboxRelayObserver, PostgresInbox, PostgresOutbox, RelayConfig, RelayLoopConfig,
    RelayPassFinished, RelayPassKind, RelayPassOutcome, RelayReport, StageOutcome,
};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::{sync::Notify, time::timeout};

fn database_url() -> String {
    std::env::var("RUSTEE_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rustee:rustee@127.0.0.1:5432/rustee".to_owned())
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct InvoicePaid {
    invoice_id: u64,
}

impl Event for InvoicePaid {
    const TYPE: &'static str = "invoices.paid";
    const VERSION: u16 = 1;
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct SendInvoiceReminder {
    invoice_id: u64,
}

impl Job for SendInvoiceReminder {
    const NAME: &'static str = "invoices.send_reminder";
    const VERSION: u16 = 1;
}

#[derive(Clone, Debug)]
struct RecordingPublisher {
    published: Arc<Notify>,
}

impl EventPublisher for RecordingPublisher {
    type Error = io::Error;

    fn publish(&self, _message: EventMessage) -> BoxFuture<'static, Result<(), Self::Error>> {
        let published = Arc::clone(&self.published);
        Box::pin(async move {
            published.notify_one();
            Ok(())
        })
    }
}

impl JobPublisher for RecordingPublisher {
    type Error = io::Error;

    fn publish(&self, _message: JobMessage) -> BoxFuture<'static, Result<(), Self::Error>> {
        let published = Arc::clone(&self.published);
        Box::pin(async move {
            published.notify_one();
            Ok(())
        })
    }
}

#[derive(Clone, Debug, Default)]
struct RecordingRelayObserver {
    finished: Arc<Mutex<Vec<RelayPassFinished>>>,
}

impl OutboxRelayObserver for RecordingRelayObserver {
    fn on_relay_pass_started(&self, _pass: rustee_outbox_sqlx::RelayPassStarted) {}

    fn on_relay_pass_finished(&self, pass: RelayPassFinished) {
        self.finished.lock().unwrap().push(pass);
    }
}

async fn pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url())
        .await
        .unwrap()
}

async fn reset_schema(pool: &PgPool) {
    sqlx::raw_sql(OUTBOX_MIGRATION_SQL)
        .execute(pool)
        .await
        .unwrap();
    sqlx::raw_sql(OUTBOX_PRIORITY_MIGRATION_SQL)
        .execute(pool)
        .await
        .unwrap();
    sqlx::raw_sql(INBOX_MIGRATION_SQL)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("TRUNCATE rustee_inbox")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("TRUNCATE rustee_outbox")
        .execute(pool)
        .await
        .unwrap();
}

fn event(destination: &OutboxDestination) -> OutboxMessage {
    let envelope =
        EventEnvelope::with_metadata(EventId::new(), InvoicePaid { invoice_id: 7 }, "acct-7", 123)
            .unwrap();
    OutboxMessage::event(destination.clone(), &envelope).unwrap()
}

fn job(destination: &OutboxDestination) -> OutboxMessage {
    let envelope = JobEnvelope::with_metadata(
        rustee_jobs::JobId::new(),
        SendInvoiceReminder { invoice_id: 7 },
        123,
    );
    OutboxMessage::job(destination.clone(), &envelope).unwrap()
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn business_transaction_controls_visibility_and_duplicate_staging() {
    let pool = pool().await;
    reset_schema(&pool).await;
    let outbox = PostgresOutbox;
    let destination = OutboxDestination::new("events.invoices").unwrap();
    let message = event(&destination);

    let mut transaction = pool.begin().await.unwrap();
    assert_eq!(
        outbox.stage(&mut transaction, &message).await.unwrap(),
        StageOutcome::Inserted(message.id())
    );
    assert!(
        outbox
            .lease_events(&pool, &destination, LeaseConfig::default())
            .await
            .unwrap()
            .is_empty()
    );
    transaction.commit().await.unwrap();

    let leases = outbox
        .lease_events(&pool, &destination, LeaseConfig::default())
        .await
        .unwrap();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].message().key(), "acct-7");
    assert_eq!(leases[0].relay_attempt(), 1);
    assert_eq!(
        outbox.acknowledge_event(&pool, &leases[0]).await.unwrap(),
        LeaseOutcome::Applied
    );
    assert!(
        outbox
            .lease_events(&pool, &destination, LeaseConfig::default())
            .await
            .unwrap()
            .is_empty()
    );

    let mut transaction = pool.begin().await.unwrap();
    assert_eq!(
        outbox.stage(&mut transaction, &message).await.unwrap(),
        StageOutcome::AlreadyPresent
    );
    transaction.commit().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn expired_or_released_delivery_is_redelivered_with_a_new_lease() {
    let pool = pool().await;
    reset_schema(&pool).await;
    let outbox = PostgresOutbox;
    let destination = OutboxDestination::new("events.invoices").unwrap();
    let message = event(&destination);
    let mut transaction = pool.begin().await.unwrap();
    outbox.stage(&mut transaction, &message).await.unwrap();
    transaction.commit().await.unwrap();

    let first = outbox
        .lease_events(&pool, &destination, LeaseConfig::default())
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(
        outbox
            .retry_event(&pool, &first, Duration::ZERO)
            .await
            .unwrap(),
        LeaseOutcome::Applied
    );

    let second = outbox
        .lease_events(&pool, &destination, LeaseConfig::default())
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(second.relay_attempt(), 2);
    assert_eq!(
        outbox.acknowledge_event(&pool, &second).await.unwrap(),
        LeaseOutcome::Applied
    );
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn delayed_job_staging_hides_the_job_until_its_database_schedule_is_due() {
    let pool = pool().await;
    reset_schema(&pool).await;
    let outbox = PostgresOutbox;
    let destination = OutboxDestination::new("jobs.invoices").unwrap();
    let message = job(&destination);
    let schedule = JobSchedule::after(Duration::from_mins(1)).unwrap();

    let mut transaction = pool.begin().await.unwrap();
    assert_eq!(
        outbox
            .stage_job_after(&mut transaction, &message, schedule)
            .await
            .unwrap(),
        StageOutcome::Inserted(message.id())
    );
    transaction.commit().await.unwrap();

    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT available_at > clock_timestamp() \
             FROM rustee_outbox WHERE kind = 'job' AND destination = $1",
        )
        .bind(destination.as_str())
        .fetch_one(&pool)
        .await
        .unwrap()
    );
    assert!(
        outbox
            .lease_jobs(&pool, &destination, LeaseConfig::default())
            .await
            .unwrap()
            .is_empty()
    );

    let mut transaction = pool.begin().await.unwrap();
    assert_eq!(
        outbox.stage(&mut transaction, &message).await.unwrap(),
        StageOutcome::AlreadyPresent
    );
    transaction.commit().await.unwrap();
    assert!(
        outbox
            .lease_jobs(&pool, &destination, LeaseConfig::default())
            .await
            .unwrap()
            .is_empty()
    );

    sqlx::query(
        "UPDATE rustee_outbox SET available_at = clock_timestamp() \
         WHERE kind = 'job' AND destination = $1",
    )
    .bind(destination.as_str())
    .execute(&pool)
    .await
    .unwrap();
    let due = outbox
        .lease_jobs(&pool, &destination, LeaseConfig::default())
        .await
        .unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].message().name(), SendInvoiceReminder::NAME);
    assert_eq!(
        outbox.acknowledge_job(&pool, &due[0]).await.unwrap(),
        LeaseOutcome::Applied
    );
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn delayed_event_staging_hides_the_event_until_its_database_schedule_is_due() {
    let pool = pool().await;
    reset_schema(&pool).await;
    let outbox = PostgresOutbox;
    let destination = OutboxDestination::new("events.invoices.delayed").unwrap();
    let message = event(&destination);
    let schedule = EventSchedule::after(Duration::from_mins(1)).unwrap();

    let mut transaction = pool.begin().await.unwrap();
    assert_eq!(
        outbox
            .stage_event_after(&mut transaction, &message, schedule)
            .await
            .unwrap(),
        StageOutcome::Inserted(message.id())
    );
    transaction.commit().await.unwrap();

    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT available_at > clock_timestamp() \
             FROM rustee_outbox WHERE kind = 'event' AND destination = $1",
        )
        .bind(destination.as_str())
        .fetch_one(&pool)
        .await
        .unwrap()
    );
    assert!(
        outbox
            .lease_events(&pool, &destination, LeaseConfig::default())
            .await
            .unwrap()
            .is_empty()
    );

    let mut transaction = pool.begin().await.unwrap();
    assert_eq!(
        outbox.stage(&mut transaction, &message).await.unwrap(),
        StageOutcome::AlreadyPresent
    );
    transaction.commit().await.unwrap();

    sqlx::query(
        "UPDATE rustee_outbox SET available_at = clock_timestamp() \
         WHERE kind = 'event' AND destination = $1",
    )
    .bind(destination.as_str())
    .execute(&pool)
    .await
    .unwrap();
    let due = outbox
        .lease_events(&pool, &destination, LeaseConfig::default())
        .await
        .unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].message().event_type(), InvoicePaid::TYPE);
    assert_eq!(
        outbox.acknowledge_event(&pool, &due[0]).await.unwrap(),
        LeaseOutcome::Applied
    );
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn durable_outbox_priority_claims_high_priority_jobs_first() {
    let pool = pool().await;
    reset_schema(&pool).await;
    let outbox = PostgresOutbox;
    let destination = OutboxDestination::new("jobs.invoices").unwrap();
    let low = job(&destination);
    let high = job(&destination).with_priority(OutboxPriority::new(200));

    let mut transaction = pool.begin().await.unwrap();
    assert_eq!(
        outbox.stage(&mut transaction, &low).await.unwrap(),
        StageOutcome::Inserted(low.id())
    );
    assert_eq!(
        outbox.stage(&mut transaction, &high).await.unwrap(),
        StageOutcome::Inserted(high.id())
    );
    transaction.commit().await.unwrap();

    let leases = outbox
        .lease_jobs(&pool, &destination, LeaseConfig::default())
        .await
        .unwrap();
    assert_eq!(leases.len(), 2);
    assert_eq!(leases[0].id(), high.id());
    assert_eq!(leases[1].id(), low.id());
    assert_eq!(
        outbox.acknowledge_job(&pool, &leases[0]).await.unwrap(),
        LeaseOutcome::Applied
    );
    assert_eq!(
        outbox.acknowledge_job(&pool, &leases[1]).await.unwrap(),
        LeaseOutcome::Applied
    );
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn explicit_relay_loop_publishes_a_due_job_then_stops_between_passes() {
    let pool = pool().await;
    reset_schema(&pool).await;
    let destination = OutboxDestination::new("jobs.invoices").unwrap();
    let message = job(&destination);
    let mut transaction = pool.begin().await.unwrap();
    PostgresOutbox
        .stage(&mut transaction, &message)
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    let published = Arc::new(Notify::new());
    let relay_observer = RecordingRelayObserver::default();
    let relay = JobOutboxRelay::new(
        pool.clone(),
        RecordingPublisher {
            published: Arc::clone(&published),
        },
        destination.clone(),
        RelayConfig::default(),
    )
    .with_relay_observer(Arc::new(relay_observer.clone()));
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        relay
            .run_until(
                RelayLoopConfig::new(Duration::from_secs(30)).unwrap(),
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
    });

    timeout(Duration::from_secs(5), published.notified())
        .await
        .expect("relay should publish the due job");
    shutdown_tx.send(()).unwrap();
    let report = timeout(Duration::from_secs(5), task)
        .await
        .expect("relay should observe shutdown between passes")
        .unwrap()
        .unwrap();
    assert!(report.passes >= 1);
    assert_eq!(report.claimed, 1);
    assert_eq!(report.published, 1);
    assert_eq!(report.retry_scheduled, 0);
    assert_eq!(report.lease_lost, 0);
    assert!(relay_observer.finished.lock().unwrap().iter().any(|pass| {
        pass.kind() == RelayPassKind::Job
            && pass.outcome() == RelayPassOutcome::Succeeded
            && pass.report()
                == Some(RelayReport {
                    claimed: 1,
                    published: 1,
                    retry_scheduled: 0,
                    lease_lost: 0,
                })
    }));
    assert!(
        PostgresOutbox
            .lease_jobs(&pool, &destination, LeaseConfig::default())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn explicit_relay_loop_publishes_a_due_event_then_stops_between_passes() {
    let pool = pool().await;
    reset_schema(&pool).await;
    let destination = OutboxDestination::new("events.invoices").unwrap();
    let message = event(&destination);
    let mut transaction = pool.begin().await.unwrap();
    PostgresOutbox
        .stage(&mut transaction, &message)
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    let published = Arc::new(Notify::new());
    let relay_observer = RecordingRelayObserver::default();
    let relay = EventOutboxRelay::new(
        pool.clone(),
        RecordingPublisher {
            published: Arc::clone(&published),
        },
        destination.clone(),
        RelayConfig::default(),
    )
    .with_relay_observer(Arc::new(relay_observer.clone()));
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        relay
            .run_until(
                RelayLoopConfig::new(Duration::from_secs(30)).unwrap(),
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
    });

    timeout(Duration::from_secs(5), published.notified())
        .await
        .expect("relay should publish the due event");
    shutdown_tx.send(()).unwrap();
    let report = timeout(Duration::from_secs(5), task)
        .await
        .expect("relay should observe shutdown between passes")
        .unwrap()
        .unwrap();
    assert!(report.passes >= 1);
    assert_eq!(report.claimed, 1);
    assert_eq!(report.published, 1);
    assert_eq!(report.retry_scheduled, 0);
    assert_eq!(report.lease_lost, 0);
    assert!(relay_observer.finished.lock().unwrap().iter().any(|pass| {
        pass.kind() == RelayPassKind::Event
            && pass.outcome() == RelayPassOutcome::Succeeded
            && pass.report()
                == Some(RelayReport {
                    claimed: 1,
                    published: 1,
                    retry_scheduled: 0,
                    lease_lost: 0,
                })
    }));
    assert!(
        PostgresOutbox
            .lease_events(&pool, &destination, LeaseConfig::default())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn inbox_receipt_rolls_back_or_suppresses_the_duplicate_delivery() {
    let pool = pool().await;
    reset_schema(&pool).await;
    let inbox = PostgresInbox;
    let consumer = InboxConsumer::new("billing-projection.v1").unwrap();
    let message_id = InboxMessageId::event(EventId::new());

    let mut transaction = pool.begin().await.unwrap();
    assert_eq!(
        inbox
            .register(&mut transaction, &consumer, &message_id)
            .await
            .unwrap(),
        InboxDecision::FirstDelivery
    );
    transaction.rollback().await.unwrap();

    let mut transaction = pool.begin().await.unwrap();
    assert_eq!(
        inbox
            .register(&mut transaction, &consumer, &message_id)
            .await
            .unwrap(),
        InboxDecision::FirstDelivery
    );
    transaction.commit().await.unwrap();

    let mut transaction = pool.begin().await.unwrap();
    assert_eq!(
        inbox
            .register(&mut transaction, &consumer, &message_id)
            .await
            .unwrap(),
        InboxDecision::Duplicate
    );
    transaction.commit().await.unwrap();
}
