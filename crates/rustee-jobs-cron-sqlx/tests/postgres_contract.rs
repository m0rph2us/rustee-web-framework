//! Opt-in `PostgreSQL` contract tests for durable recurring Rustee jobs.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use rustee_jobs::{Job, JobEnvelope};
use rustee_jobs_cron_sqlx::{
    CronExpression, PostgresRecurringJobs, RECURRING_JOB_MIGRATION_SQL,
    RECURRING_JOB_RATE_GOVERNOR_MIGRATION_SQL, RECURRING_JOB_TIME_ZONE_MIGRATION_SQL, RecurringJob,
    RecurringJobError, RecurringJobFireFinished, RecurringJobFireLimit, RecurringJobFireObserver,
    RecurringJobFireOutcome, RecurringJobFireStarted, RecurringJobKey, RecurringJobPauseOutcome,
    RecurringJobRateLimit, RecurringJobRateLimitKey, RecurringJobRegistration,
    RecurringJobResumeOutcome, RecurringJobTimeZone,
};
use rustee_outbox_sqlx::{
    LeaseConfig, OUTBOX_MIGRATION_SQL, OUTBOX_PRIORITY_MIGRATION_SQL, OutboxDestination,
    PostgresOutbox,
};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions};

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct SendInvoiceReminder {
    invoice_id: u64,
}

impl Job for SendInvoiceReminder {
    const NAME: &'static str = "billing.send-invoice-reminder";
    const VERSION: u16 = 1;
}

#[derive(Default)]
struct RecordingFireObserver {
    started: AtomicUsize,
    finished: Mutex<Vec<RecurringJobFireFinished>>,
}

impl RecurringJobFireObserver for RecordingFireObserver {
    fn on_fire_started(&self, _pass: RecurringJobFireStarted) {
        self.started.fetch_add(1, Ordering::Relaxed);
    }

    fn on_fire_finished(&self, pass: RecurringJobFireFinished) {
        self.finished.lock().unwrap().push(pass);
    }
}

fn database_url() -> String {
    std::env::var("RUSTEE_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rustee:rustee@127.0.0.1:5432/rustee".to_owned())
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
    sqlx::raw_sql(RECURRING_JOB_MIGRATION_SQL)
        .execute(pool)
        .await
        .unwrap();
    sqlx::raw_sql(RECURRING_JOB_RATE_GOVERNOR_MIGRATION_SQL)
        .execute(pool)
        .await
        .unwrap();
    sqlx::raw_sql(RECURRING_JOB_TIME_ZONE_MIGRATION_SQL)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("TRUNCATE rustee_outbox, rustee_recurring_job_rate_windows, rustee_recurring_jobs")
        .execute(pool)
        .await
        .unwrap();
}

fn rate_limit(key: &str, capacity: u32) -> RecurringJobRateLimit {
    RecurringJobRateLimit::new(
        RecurringJobRateLimitKey::new(key).unwrap(),
        NonZeroU32::new(capacity).unwrap(),
        Duration::from_secs(60),
    )
    .unwrap()
}

fn recurring(
    key: &str,
    destination: OutboxDestination,
    invoice_id: u64,
) -> RecurringJob<SendInvoiceReminder> {
    RecurringJob::new(
        RecurringJobKey::new(key).unwrap(),
        destination,
        SendInvoiceReminder { invoice_id },
        CronExpression::new("* * * * * * *").unwrap(),
    )
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn recurring_schedule_atomically_stages_one_fresh_job_and_advances() {
    let pool = pool().await;
    reset_schema(&pool).await;
    let observer = Arc::new(RecordingFireObserver::default());
    let scheduler = PostgresRecurringJobs::new(pool.clone()).with_fire_observer(observer.clone());
    let destination = OutboxDestination::new("jobs.billing").unwrap();
    let definition = recurring("billing.invoice-reminder", destination.clone(), 7)
        .with_time_zone(RecurringJobTimeZone::new("America/New_York").unwrap());
    let registration = scheduler.register(&definition).await.unwrap();
    assert!(matches!(
        registration,
        RecurringJobRegistration::Registered(_)
    ));
    assert!(matches!(
        scheduler.register(&definition).await.unwrap(),
        RecurringJobRegistration::AlreadyPresent(_)
    ));

    sqlx::query(
        "UPDATE rustee_recurring_jobs SET next_run_at = clock_timestamp() - INTERVAL '1 second' \
         WHERE schedule_key = $1",
    )
    .bind(definition.key().as_str())
    .execute(&pool)
    .await
    .unwrap();
    let report = scheduler
        .fire_due(RecurringJobFireLimit::new(NonZeroUsize::new(10).unwrap()).unwrap())
        .await
        .unwrap();
    assert_eq!(report.claimed(), 1);
    assert_eq!(report.staged(), 1);
    assert_eq!(observer.started.load(Ordering::Relaxed), 1);
    {
        let finished = observer.finished.lock().unwrap();
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].outcome(), RecurringJobFireOutcome::Succeeded);
        assert_eq!(finished[0].report(), Some(report));
    }

    let leases = PostgresOutbox
        .lease_jobs(&pool, &destination, LeaseConfig::default())
        .await
        .unwrap();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].message().name(), SendInvoiceReminder::NAME);
    let envelope =
        JobEnvelope::<SendInvoiceReminder>::decode(&leases[0].message().clone().into_payload())
            .unwrap();
    assert_eq!(
        envelope.into_payload(),
        SendInvoiceReminder { invoice_id: 7 }
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT next_run_at > clock_timestamp() FROM rustee_recurring_jobs WHERE schedule_key = $1",
        )
        .bind(definition.key().as_str())
        .fetch_one(&pool)
        .await
        .unwrap()
    );
    assert_eq!(
        scheduler
            .fire_due(RecurringJobFireLimit::default())
            .await
            .unwrap()
            .staged(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT time_zone FROM rustee_recurring_jobs WHERE schedule_key = $1",
        )
        .bind(definition.key().as_str())
        .fetch_one(&pool)
        .await
        .unwrap(),
        "America/New_York"
    );
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn recurring_schedule_rejects_definition_drift_and_can_be_paused_and_resumed() {
    let pool = pool().await;
    reset_schema(&pool).await;
    let scheduler = PostgresRecurringJobs::new(pool.clone());
    let destination = OutboxDestination::new("jobs.billing").unwrap();
    let definition = recurring("billing.invoice-reminder", destination.clone(), 7);
    scheduler.register(&definition).await.unwrap();
    let changed = recurring("billing.invoice-reminder", destination, 7)
        .with_time_zone(RecurringJobTimeZone::new("Asia/Seoul").unwrap());
    assert!(matches!(
        scheduler.register(&changed).await.unwrap_err(),
        RecurringJobError::RegistrationConflict
    ));
    assert_eq!(
        scheduler.pause(definition.key()).await.unwrap(),
        RecurringJobPauseOutcome::Paused
    );
    assert_eq!(
        scheduler.pause(definition.key()).await.unwrap(),
        RecurringJobPauseOutcome::NotFoundOrAlreadyPaused
    );
    assert_eq!(
        scheduler.resume(definition.key()).await.unwrap(),
        RecurringJobResumeOutcome::Resumed
    );
    assert_eq!(
        scheduler.resume(definition.key()).await.unwrap(),
        RecurringJobResumeOutcome::NotFoundOrAlreadyEnabled
    );
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn recurring_rate_governor_atomically_stages_or_defers_shared_capacity() {
    let pool = pool().await;
    reset_schema(&pool).await;
    let scheduler = PostgresRecurringJobs::new(pool.clone());
    let destination = OutboxDestination::new("jobs.billing").unwrap();
    let first = recurring("billing.rate-governed-a", destination.clone(), 7)
        .with_rate_limit(rate_limit("provider.billing", 1));
    let second = recurring("billing.rate-governed-b", destination.clone(), 8)
        .with_rate_limit(rate_limit("provider.billing", 1));
    scheduler.register(&first).await.unwrap();
    scheduler.register(&second).await.unwrap();

    sqlx::query(
        "UPDATE rustee_recurring_jobs SET next_run_at = clock_timestamp() - INTERVAL '1 second' \
         WHERE schedule_key IN ($1, $2)",
    )
    .bind(first.key().as_str())
    .bind(second.key().as_str())
    .execute(&pool)
    .await
    .unwrap();
    let report = scheduler
        .fire_due(RecurringJobFireLimit::new(NonZeroUsize::new(10).unwrap()).unwrap())
        .await
        .unwrap();
    assert_eq!(report.claimed(), 2);
    assert_eq!(report.staged(), 1);
    assert_eq!(report.rate_limited(), 1);
    assert_eq!(
        PostgresOutbox
            .lease_jobs(&pool, &destination, LeaseConfig::default())
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT consumed::bigint FROM rustee_recurring_job_rate_windows \
             WHERE rate_limit_key = $1",
        )
        .bind("provider.billing")
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM rustee_recurring_jobs \
             WHERE schedule_key IN ($1, $2) AND next_run_at > clock_timestamp()",
        )
        .bind(first.key().as_str())
        .bind(second.key().as_str())
        .fetch_one(&pool)
        .await
        .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM rustee_recurring_jobs \
             WHERE schedule_key IN ($1, $2) AND last_fired_at IS NULL",
        )
        .bind(first.key().as_str())
        .bind(second.key().as_str())
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn recurring_rate_governor_rejects_shared_key_policy_drift_at_registration() {
    let pool = pool().await;
    reset_schema(&pool).await;
    let scheduler = PostgresRecurringJobs::new(pool);
    let destination = OutboxDestination::new("jobs.billing").unwrap();
    let first = recurring("billing.rate-governed-a", destination.clone(), 7)
        .with_rate_limit(rate_limit("provider.billing", 1));
    let changed_policy = recurring("billing.rate-governed-b", destination, 8)
        .with_rate_limit(rate_limit("provider.billing", 2));
    scheduler.register(&first).await.unwrap();
    assert!(matches!(
        scheduler.register(&changed_policy).await.unwrap_err(),
        RecurringJobError::RateLimitPolicyConflict
    ));
}
