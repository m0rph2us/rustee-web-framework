//! Opt-in `PostgreSQL` contract for durable `MongoDB` change-stream checkpoints.

use std::time::{Duration, Instant};

use rustee_mongodb::{
    ChangeStreamCheckpointStore, ChangeStreamConsumer,
    mongodb::{bson, change_stream::event::ResumeToken},
};
use rustee_mongodb_checkpoint_sqlx::{
    CHANGE_STREAM_CHECKPOINT_MIGRATION_SQL,
    CHANGE_STREAM_CHECKPOINT_RESUME_TOKEN_BOUND_MIGRATION_SQL, ChangeStreamLeaseAcquire,
    ChangeStreamLeaseDuration, ChangeStreamLeaseOwner, MAX_CHANGE_STREAM_RESUME_TOKEN_BYTES,
    PostgresChangeStreamCheckpointError, PostgresChangeStreamCheckpointStore,
};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::time::sleep;
use uuid::Uuid;

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

fn resume_token(value: &str) -> ResumeToken {
    bson::from_document(bson::doc! { "_data": value }).unwrap()
}

async fn ensure_schema(pool: &PgPool) {
    sqlx::raw_sql(CHANGE_STREAM_CHECKPOINT_MIGRATION_SQL)
        .execute(pool)
        .await
        .unwrap();
    sqlx::raw_sql(CHANGE_STREAM_CHECKPOINT_RESUME_TOKEN_BOUND_MIGRATION_SQL)
        .execute(pool)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn checkpoints_are_durable_per_consumer_and_reject_corrupt_bytes() {
    let pool = pool().await;
    ensure_schema(&pool).await;

    let store = PostgresChangeStreamCheckpointStore::new(pool.clone());
    store.readiness(Duration::from_secs(1)).await.unwrap();
    let consumer =
        ChangeStreamConsumer::new(format!("orders-projection-v1-{}", Uuid::new_v4())).unwrap();
    assert!(store.load(consumer.clone()).await.unwrap().is_none());

    let first = resume_token("checkpoint-1");
    store.save(consumer.clone(), first.clone()).await.unwrap();
    assert_eq!(store.load(consumer.clone()).await.unwrap(), Some(first));

    let second = resume_token("checkpoint-2");
    store.save(consumer.clone(), second.clone()).await.unwrap();
    assert_eq!(store.load(consumer.clone()).await.unwrap(), Some(second));

    sqlx::query(
        "UPDATE rustee_mongodb_change_stream_checkpoint SET resume_token = $1 WHERE consumer = $2",
    )
    .bind(vec![0_u8, 1, 2])
    .bind(consumer.as_str())
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        store.load(consumer.clone()).await.unwrap_err(),
        PostgresChangeStreamCheckpointError::InvalidCheckpoint
    ));

    let oversized = sqlx::query(
        "UPDATE rustee_mongodb_change_stream_checkpoint SET resume_token = $1 WHERE consumer = $2",
    )
    .bind(vec![0_u8; MAX_CHANGE_STREAM_RESUME_TOKEN_BYTES + 1])
    .bind(consumer.as_str())
    .execute(&pool)
    .await;
    assert!(oversized.is_err());
}

#[tokio::test]
#[ignore = "requires CI to stop its PostgreSQL container before this contract"]
async fn checkpoint_store_readiness_fails_within_the_deadline_during_an_outage() {
    if std::env::var("RUSTEE_CHECKPOINT_EXPECT_OUTAGE").as_deref() != Ok("1") {
        return;
    }
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(200))
        .connect_lazy(&database_url())
        .unwrap();
    let store = PostgresChangeStreamCheckpointStore::new(pool);
    let started = Instant::now();
    let error = store
        .readiness(Duration::from_millis(500))
        .await
        .unwrap_err();

    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(matches!(
        &error,
        PostgresChangeStreamCheckpointError::Storage(_)
    ));
    assert_eq!(
        error.to_string(),
        "PostgreSQL change-stream checkpoint storage failed"
    );
    assert!(!error.to_string().contains("127.0.0.1"));
    assert!(!error.to_string().contains("postgres://"));
}

#[tokio::test]
#[ignore = "requires a PostgreSQL server; CI provisions one"]
async fn lease_blocks_contenders_and_rejects_a_stale_checkpoint_writer() {
    let pool = pool().await;
    ensure_schema(&pool).await;
    let store = PostgresChangeStreamCheckpointStore::new(pool);
    let consumer =
        ChangeStreamConsumer::new(format!("orders-projection-v2-{}", Uuid::new_v4())).unwrap();
    let first_owner = ChangeStreamLeaseOwner::new("pod-a-start-1").unwrap();
    let second_owner = ChangeStreamLeaseOwner::new("pod-b-start-1").unwrap();
    let short_lease = ChangeStreamLeaseDuration::new(Duration::from_millis(100)).unwrap();

    let first_lease = match store
        .try_acquire_lease(consumer.clone(), first_owner, short_lease)
        .await
        .unwrap()
    {
        ChangeStreamLeaseAcquire::Acquired(lease) => lease,
        ChangeStreamLeaseAcquire::Contended => panic!("first owner must acquire an empty lease"),
    };
    assert!(matches!(
        store
            .try_acquire_lease(consumer.clone(), second_owner.clone(), short_lease)
            .await
            .unwrap(),
        ChangeStreamLeaseAcquire::Contended
    ));
    store
        .save_while_leased(&first_lease, resume_token("checkpoint-1"))
        .await
        .unwrap();
    store
        .renew_lease(
            &first_lease,
            ChangeStreamLeaseDuration::new(Duration::from_millis(200)).unwrap(),
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(300)).await;
    let second_lease = match store
        .try_acquire_lease(consumer.clone(), second_owner, short_lease)
        .await
        .unwrap()
    {
        ChangeStreamLeaseAcquire::Acquired(lease) => lease,
        ChangeStreamLeaseAcquire::Contended => panic!("expired lease must be acquirable"),
    };
    assert!(matches!(
        store
            .save_while_leased(&first_lease, resume_token("stale-checkpoint"))
            .await
            .unwrap_err(),
        PostgresChangeStreamCheckpointError::LeaseLost
    ));
    assert!(matches!(
        store
            .renew_lease(&first_lease, short_lease)
            .await
            .unwrap_err(),
        PostgresChangeStreamCheckpointError::LeaseLost
    ));
    store
        .save_while_leased(&second_lease, resume_token("checkpoint-2"))
        .await
        .unwrap();
    assert_eq!(
        store.load(consumer.clone()).await.unwrap(),
        Some(resume_token("checkpoint-2"))
    );
    store.release_lease(second_lease).await.unwrap();
    assert!(matches!(
        store
            .try_acquire_lease(
                consumer,
                ChangeStreamLeaseOwner::new("pod-a-start-2").unwrap(),
                short_lease,
            )
            .await
            .unwrap(),
        ChangeStreamLeaseAcquire::Acquired(_)
    ));
}
