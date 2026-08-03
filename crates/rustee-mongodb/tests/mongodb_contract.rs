//! Opt-in `MongoDB` contract tests. Run with a disposable server at `RUSTEE_MONGODB_URI`.

use futures_util::TryStreamExt;
use rustee_mongodb::{
    ChangeStreamNext, MongoConfig, MongoTenantScope, TenantContext, begin_transaction,
    begin_transaction_with_options, connect, database,
    mongodb::{
        bson::{Document, doc},
        options::TransactionOptions,
    },
    next_change_until, readiness, shutdown,
};
use tokio::{
    sync::oneshot,
    time::{Duration, Instant, sleep, timeout},
};
use uuid::Uuid;

fn mongo_uri() -> String {
    std::env::var("RUSTEE_MONGODB_URI")
        .unwrap_or_else(|_| "mongodb://127.0.0.1:27017/?replicaSet=rs0".to_owned())
}

#[tokio::test]
#[ignore = "requires a MongoDB server; CI provisions one"]
async fn client_is_ready_and_performs_isolated_collection_io() {
    let config = MongoConfig::new(mongo_uri(), "rustee_contract")
        .unwrap()
        .with_app_name("rustee-mongodb-contract");
    let client = connect(&config).await.unwrap();
    readiness(&client, &config).await.unwrap();

    let collection = database(&client, &config)
        .collection::<Document>(&format!("cache_{}", Uuid::new_v4().simple()));
    collection
        .insert_one(doc! { "owner": "mongodb-contract", "revision": 7_i32 })
        .await
        .unwrap();
    let document = collection
        .find_one(doc! { "owner": "mongodb-contract" })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(document.get_i32("revision").unwrap(), 7);
    collection.drop().await.unwrap();
    shutdown(client).await;
}

#[tokio::test]
#[ignore = "requires a MongoDB replica set; CI provisions one"]
async fn transaction_commits_and_aborts_across_collections() {
    let config = MongoConfig::new(mongo_uri(), "rustee_contract")
        .unwrap()
        .with_app_name("rustee-mongodb-transaction-contract");
    let client = connect(&config).await.unwrap();
    readiness(&client, &config).await.unwrap();

    let database = database(&client, &config);
    let suffix = Uuid::new_v4().simple().to_string();
    let orders = database.collection::<Document>(&format!("orders_{suffix}"));
    let audit = database.collection::<Document>(&format!("audit_{suffix}"));
    let committed_request = Uuid::new_v4().to_string();
    let mut session = begin_transaction_with_options(
        &client,
        TransactionOptions::builder()
            .max_commit_time(Duration::from_secs(2))
            .build(),
    )
    .await
    .unwrap();
    orders
        .insert_one(doc! { "request_id": &committed_request, "state": "created" })
        .session(&mut session)
        .await
        .unwrap();
    audit
        .insert_one(doc! { "request_id": &committed_request, "event": "order_created" })
        .session(&mut session)
        .await
        .unwrap();
    session.commit_transaction().await.unwrap();

    assert!(
        orders
            .find_one(doc! { "request_id": &committed_request })
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        audit
            .find_one(doc! { "request_id": &committed_request })
            .await
            .unwrap()
            .is_some()
    );

    let aborted_request = Uuid::new_v4().to_string();
    let mut session = begin_transaction(&client).await.unwrap();
    orders
        .insert_one(doc! { "request_id": &aborted_request, "state": "discarded" })
        .session(&mut session)
        .await
        .unwrap();
    audit
        .insert_one(doc! { "request_id": &aborted_request, "event": "discarded" })
        .session(&mut session)
        .await
        .unwrap();
    session.abort_transaction().await.unwrap();

    assert!(
        orders
            .find_one(doc! { "request_id": &aborted_request })
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        audit
            .find_one(doc! { "request_id": &aborted_request })
            .await
            .unwrap()
            .is_none()
    );

    orders.drop().await.unwrap();
    audit.drop().await.unwrap();
    shutdown(client).await;
}

#[tokio::test]
#[ignore = "requires a MongoDB replica set; CI provisions one"]
async fn change_stream_exposes_a_checkpoint_and_stops_at_shutdown() {
    let config = MongoConfig::new(mongo_uri(), "rustee_contract")
        .unwrap()
        .with_app_name("rustee-mongodb-change-stream-contract");
    let client = connect(&config).await.unwrap();
    readiness(&client, &config).await.unwrap();

    let collection = database(&client, &config)
        .collection::<Document>(&format!("changes_{}", Uuid::new_v4().simple()));
    let mut stream = collection.watch().await.unwrap();
    collection
        .insert_one(doc! { "owner": "mongodb-change-stream-contract", "revision": 8_i32 })
        .await
        .unwrap();

    let next = next_change_until(&mut stream, std::future::pending())
        .await
        .unwrap();
    match next {
        ChangeStreamNext::Event {
            event,
            resume_token,
        } => {
            assert!(resume_token.is_some());
            let document = event
                .full_document
                .expect("insert change must contain its document");
            assert_eq!(document.get_i32("revision").unwrap(), 8);
        }
        ChangeStreamNext::Ended { .. } | ChangeStreamNext::Shutdown { .. } => {
            panic!("expected an inserted change event")
        }
    }

    drop(stream);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let mut shutdown_stream = collection.watch().await.unwrap();
    let worker = tokio::spawn(async move {
        next_change_until(&mut shutdown_stream, async move {
            let _ = shutdown_rx.await;
        })
        .await
    });
    sleep(Duration::from_millis(50)).await;
    shutdown_tx.send(()).unwrap();
    let stopped = timeout(Duration::from_secs(1), worker)
        .await
        .expect("change-stream worker did not honor shutdown")
        .unwrap()
        .unwrap();
    assert!(matches!(stopped, ChangeStreamNext::Shutdown { .. }));

    collection.drop().await.unwrap();
    shutdown(client).await;
}

#[tokio::test]
#[ignore = "requires a MongoDB replica set; CI provisions one"]
async fn tenant_scope_keeps_reads_and_mutations_inside_the_trusted_tenant() {
    let config = MongoConfig::new(mongo_uri(), "rustee_contract")
        .unwrap()
        .with_app_name("rustee-mongodb-tenant-contract");
    let client = connect(&config).await.unwrap();
    readiness(&client, &config).await.unwrap();

    let collection = database(&client, &config)
        .collection::<Document>(&format!("tenants_{}", Uuid::new_v4().simple()));
    let tenant_a = MongoTenantScope::new(TenantContext::new("tenant-a").unwrap());
    let tenant_b = MongoTenantScope::new(TenantContext::new("tenant-b").unwrap());
    let owned_order_id = Uuid::new_v4().to_string();
    let foreign_order_id = Uuid::new_v4().to_string();
    collection
        .insert_many([
            tenant_a
                .document(doc! { "request_id": &owned_order_id, "state": "open" })
                .unwrap(),
            tenant_b
                .document(doc! { "request_id": &foreign_order_id, "state": "open" })
                .unwrap(),
        ])
        .await
        .unwrap();

    assert!(
        collection
            .find_one(tenant_a.filter(doc! { "request_id": &owned_order_id }))
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        collection
            .find_one(tenant_a.filter(doc! { "request_id": &foreign_order_id }))
            .await
            .unwrap()
            .is_none()
    );

    let aggregate = collection
        .aggregate(
            tenant_a.aggregation_pipeline(doc! { "state": "open" }, [doc! { "$count": "orders" }]),
        )
        .await
        .unwrap()
        .try_next()
        .await
        .unwrap()
        .expect("tenant A aggregate must return its document");
    assert_eq!(aggregate.get_i32("orders").unwrap(), 1);

    let update = collection
        .update_many(
            tenant_a.filter(doc! { "state": "open" }),
            tenant_a
                .update(doc! { "$set": { "state": "closed" } })
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(update.modified_count, 1);
    assert_eq!(
        collection
            .find_one(doc! { "request_id": &owned_order_id })
            .await
            .unwrap()
            .unwrap()
            .get_str("state")
            .unwrap(),
        "closed"
    );
    assert_eq!(
        collection
            .find_one(doc! { "request_id": &foreign_order_id })
            .await
            .unwrap()
            .unwrap()
            .get_str("state")
            .unwrap(),
        "open"
    );

    let delete = collection
        .delete_many(tenant_a.filter(doc! { "state": "closed" }))
        .await
        .unwrap();
    assert_eq!(delete.deleted_count, 1);
    assert!(
        collection
            .find_one(doc! { "request_id": &owned_order_id })
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        collection
            .find_one(doc! { "request_id": &foreign_order_id })
            .await
            .unwrap()
            .is_some()
    );

    collection.drop().await.unwrap();
    shutdown(client).await;
}

#[tokio::test]
#[ignore = "requires a MongoDB replica set; CI provisions one"]
async fn tenant_scope_filters_lookup_and_union_foreign_collections() {
    let config = MongoConfig::new(mongo_uri(), "rustee_contract")
        .unwrap()
        .with_app_name("rustee-mongodb-foreign-aggregation-contract");
    let client = connect(&config).await.unwrap();
    readiness(&client, &config).await.unwrap();

    let database = database(&client, &config);
    let suffix = Uuid::new_v4().simple().to_string();
    let orders = database.collection::<Document>(&format!("orders_{suffix}"));
    let items = database.collection::<Document>(&format!("items_{suffix}"));
    let archived = database.collection::<Document>(&format!("archived_{suffix}"));
    let tenant_a = MongoTenantScope::new(TenantContext::new("tenant-a").unwrap());
    let tenant_b = MongoTenantScope::new(TenantContext::new("tenant-b").unwrap());
    let shared_order_id = Uuid::new_v4().to_string();

    orders
        .insert_many([
            tenant_a
                .document(doc! { "order_id": &shared_order_id, "state": "open" })
                .unwrap(),
            tenant_b
                .document(doc! { "order_id": Uuid::new_v4().to_string(), "state": "open" })
                .unwrap(),
        ])
        .await
        .unwrap();
    items
        .insert_many([
            tenant_a
                .document(doc! { "order_id": &shared_order_id, "sku": "tenant-a-item" })
                .unwrap(),
            tenant_b
                .document(doc! { "order_id": &shared_order_id, "sku": "tenant-b-item" })
                .unwrap(),
        ])
        .await
        .unwrap();
    archived
        .insert_many([
            tenant_a
                .document(doc! { "order_id": "archived-a", "state": "closed" })
                .unwrap(),
            tenant_b
                .document(doc! { "order_id": "archived-b", "state": "closed" })
                .unwrap(),
        ])
        .await
        .unwrap();

    let lookup = tenant_a
        .lookup_stage(
            items.name(),
            "items",
            doc! { "lookup_order_id": "$order_id" },
            doc! { "$expr": { "$eq": ["$order_id", "$$lookup_order_id"] } },
            [],
        )
        .unwrap();
    let lookup_results: Vec<Document> = orders
        .aggregate(tenant_a.aggregation_pipeline(doc! { "order_id": &shared_order_id }, [lookup]))
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(lookup_results.len(), 1);
    let joined_items = lookup_results[0].get_array("items").unwrap();
    assert_eq!(joined_items.len(), 1);
    let joined_item = joined_items[0].as_document().unwrap();
    assert_eq!(joined_item.get_str("tenant_id").unwrap(), "tenant-a");
    assert_eq!(joined_item.get_str("sku").unwrap(), "tenant-a-item");

    let union = tenant_a
        .union_with_stage(archived.name(), doc! {}, [])
        .unwrap();
    let union_results: Vec<Document> = orders
        .aggregate(tenant_a.aggregation_pipeline(doc! {}, [union]))
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(union_results.len(), 2);
    assert!(union_results.iter().all(|document| {
        document
            .get_str("tenant_id")
            .is_ok_and(|tenant| tenant == "tenant-a")
    }));

    orders.drop().await.unwrap();
    items.drop().await.unwrap();
    archived.drop().await.unwrap();
    shutdown(client).await;
}

#[tokio::test]
#[ignore = "requires CI to stop a MongoDB replica set during the contract"]
async fn readiness_fails_within_the_server_selection_deadline_during_an_outage() {
    let config = MongoConfig::new(mongo_uri(), "rustee_contract")
        .unwrap()
        .with_app_name("rustee-mongodb-outage-contract")
        .with_server_selection_timeout(Duration::from_millis(100));
    let client = connect(&config).await.unwrap();
    let started_at = Instant::now();
    assert!(readiness(&client, &config).await.is_err());
    assert!(started_at.elapsed() < Duration::from_secs(2));
    shutdown(client).await;
}
