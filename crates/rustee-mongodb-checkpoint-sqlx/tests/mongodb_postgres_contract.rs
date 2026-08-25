//! Opt-in combined `MongoDB` and `PostgreSQL` durable change-stream restart contract.

use rustee_mongodb::{
    ChangeStreamCheckpointStore, ChangeStreamConsumer, ChangeStreamNext, MongoConfig, connect,
    database,
    mongodb::bson::{Document, doc},
    next_change_until, readiness, shutdown,
};
use rustee_mongodb_checkpoint_sqlx::{
    CHANGE_STREAM_CHECKPOINT_MIGRATION_SQL,
    CHANGE_STREAM_CHECKPOINT_RESUME_TOKEN_BOUND_MIGRATION_SQL, PostgresChangeStreamCheckpointStore,
};
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

fn mongo_uri() -> String {
    std::env::var("RUSTEE_MONGODB_URI")
        .unwrap_or_else(|_| "mongodb://127.0.0.1:27017/?replicaSet=rs0".to_owned())
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

async fn reset_checkpoints(pool: &PgPool) {
    sqlx::raw_sql(CHANGE_STREAM_CHECKPOINT_MIGRATION_SQL)
        .execute(pool)
        .await
        .unwrap();
    sqlx::raw_sql(CHANGE_STREAM_CHECKPOINT_RESUME_TOKEN_BOUND_MIGRATION_SQL)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("TRUNCATE rustee_mongodb_change_stream_checkpoint")
        .execute(pool)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires MongoDB replica set and PostgreSQL; CI provisions both"]
async fn durable_checkpoint_resumes_a_new_client_at_the_next_change() {
    let pool = pool().await;
    reset_checkpoints(&pool).await;
    let first_store = PostgresChangeStreamCheckpointStore::new(pool.clone());
    let consumer = ChangeStreamConsumer::new("orders-projection-v1").unwrap();
    let collection_name = format!("checkpoint_{}", Uuid::new_v4().simple());
    let config = MongoConfig::new(mongo_uri(), "rustee_contract")
        .unwrap()
        .with_app_name("rustee-mongodb-checkpoint-contract");
    let client = connect(&config).await.unwrap();
    readiness(&client, &config).await.unwrap();
    let collection = database(&client, &config).collection::<Document>(&collection_name);
    let mut stream = collection.watch().await.unwrap();
    collection
        .insert_one(doc! { "revision": 1_i32 })
        .await
        .unwrap();

    let first_token = match next_change_until(&mut stream, std::future::pending())
        .await
        .unwrap()
    {
        ChangeStreamNext::Event {
            event,
            resume_token: Some(token),
        } => {
            assert_eq!(event.full_document.unwrap().get_i32("revision").unwrap(), 1);
            token
        }
        ChangeStreamNext::Event {
            resume_token: None, ..
        }
        | ChangeStreamNext::Ended { .. }
        | ChangeStreamNext::Shutdown { .. } => panic!("expected a checkpointable first change"),
    };
    first_store
        .save(consumer.clone(), first_token)
        .await
        .unwrap();
    drop(stream);
    drop(collection);
    shutdown(client).await;

    let restarted_store = PostgresChangeStreamCheckpointStore::new(pool);
    let checkpoint = restarted_store.load(consumer).await.unwrap().unwrap();
    let restarted_client = connect(&config).await.unwrap();
    readiness(&restarted_client, &config).await.unwrap();
    let restarted_collection =
        database(&restarted_client, &config).collection::<Document>(&collection_name);
    let mut restarted_stream = restarted_collection
        .watch()
        .resume_after(checkpoint)
        .await
        .unwrap();
    restarted_collection
        .insert_one(doc! { "revision": 2_i32 })
        .await
        .unwrap();

    match next_change_until(&mut restarted_stream, std::future::pending())
        .await
        .unwrap()
    {
        ChangeStreamNext::Event { event, .. } => {
            assert_eq!(event.full_document.unwrap().get_i32("revision").unwrap(), 2);
        }
        ChangeStreamNext::Ended { .. } | ChangeStreamNext::Shutdown { .. } => {
            panic!("expected the next change after a durable resume")
        }
    }

    drop(restarted_stream);
    restarted_collection.drop().await.unwrap();
    shutdown(restarted_client).await;
}
