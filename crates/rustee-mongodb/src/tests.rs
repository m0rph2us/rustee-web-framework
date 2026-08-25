use std::time::Duration;

use mongodb::bson::{Document, doc};

use super::{
    ChangeStreamConsumer, ChangeStreamConsumerError, ChangeStreamNext, ConfigError,
    MONGO_TENANT_FIELD, MongoConfig, MongoConnectError, MongoReadinessError, MongoTenantScope,
    TenantContext, TenantScopeError,
};

#[test]
fn connection_and_routing_values_are_not_exposed_in_debug_output() {
    let config = MongoConfig::new(
        "mongodb://user:password@localhost:27017",
        "private-tenant-database",
    )
    .unwrap()
    .with_app_name("private-app-name");
    let debug = format!("{config:?}");

    assert!(!debug.contains("password"));
    assert!(!debug.contains("private-tenant-database"));
    assert!(!debug.contains("private-app-name"));
    assert!(debug.contains("database: \"[REDACTED]\""));
    assert!(debug.contains("app_name: Some(\"[REDACTED]\")"));
}

#[test]
fn blank_database_is_rejected() {
    let error = MongoConfig::new("mongodb://localhost:27017", " ").unwrap_err();
    assert!(matches!(error, ConfigError::EmptyDatabase));
}

#[test]
fn finite_driver_timeouts_are_shown_in_debug_output() {
    let config = MongoConfig::new("mongodb://localhost:27017", "app")
        .unwrap()
        .with_connect_timeout(Duration::from_secs(2))
        .unwrap()
        .with_server_selection_timeout(Duration::from_secs(3))
        .unwrap();
    let debug = format!("{config:?}");
    assert!(debug.contains("2s"));
    assert!(debug.contains("3s"));
}

#[test]
fn zero_driver_timeouts_are_rejected() {
    let config = MongoConfig::new("mongodb://localhost:27017", "app").unwrap();
    assert_eq!(
        config
            .clone()
            .with_connect_timeout(Duration::ZERO)
            .unwrap_err(),
        ConfigError::ZeroConnectTimeout
    );
    assert_eq!(
        config
            .with_server_selection_timeout(Duration::ZERO)
            .unwrap_err(),
        ConfigError::ZeroServerSelectionTimeout
    );
}

#[test]
fn mongo_driver_errors_are_content_free_in_display_and_debug() {
    let source = mongodb::error::Error::custom("mongodb://user:secret@cluster.example");
    let connect_error = MongoConnectError::Driver(source);
    let source = mongodb::error::Error::custom("mongodb://user:secret@cluster.example");
    let readiness_error = MongoReadinessError::Driver(source);

    for error in [&connect_error as &dyn std::error::Error, &readiness_error] {
        let display = error.to_string();
        assert!(!display.contains("secret"));
        assert!(error.source().is_some());
    }
    assert!(!format!("{connect_error:?}").contains("secret"));
    assert!(!format!("{readiness_error:?}").contains("secret"));
}

#[test]
fn tenant_scope_composes_an_authoritative_outer_filter() {
    let scope = MongoTenantScope::new(TenantContext::new("tenant-a").unwrap());

    assert_eq!(
        scope.filter(doc! { "$or": [{ "status": "open" }, { "owner": "alice" }] }),
        doc! {
            "$and": [
                { MONGO_TENANT_FIELD: "tenant-a" },
                { "$or": [{ "status": "open" }, { "owner": "alice" }] },
            ],
        }
    );
}

#[test]
fn tenant_scope_starts_aggregation_with_an_authoritative_match() {
    let scope = MongoTenantScope::new(TenantContext::new("tenant-a").unwrap());

    assert_eq!(
        scope.aggregation_pipeline(
            doc! { "$or": [{ "status": "open" }, { "owner": "alice" }] },
            [doc! { "$group": { "_id": "$owner", "count": { "$sum": 1_i32 } } }],
        ),
        vec![
            doc! {
                "$match": {
                    "$and": [
                        { MONGO_TENANT_FIELD: "tenant-a" },
                        { "$or": [{ "status": "open" }, { "owner": "alice" }] },
                    ],
                },
            },
            doc! { "$group": { "_id": "$owner", "count": { "$sum": 1_i32 } } },
        ]
    );
}

#[test]
fn tenant_scope_builds_scoped_lookup_and_union_stages() {
    let scope = MongoTenantScope::new(TenantContext::new("tenant-a").unwrap());

    assert_eq!(
        scope
            .lookup_stage(
                "order_items",
                "items",
                doc! { "order_id": "$_id" },
                doc! { "$expr": { "$eq": ["$order_id", "$$order_id"] } },
                [doc! { "$project": { "sku": 1_i32 } }],
            )
            .unwrap(),
        doc! {
            "$lookup": {
                "from": "order_items",
                "let": { "order_id": "$_id" },
                "pipeline": [
                    {
                        "$match": {
                            "$and": [
                                { MONGO_TENANT_FIELD: "tenant-a" },
                                { "$expr": { "$eq": ["$order_id", "$$order_id"] } },
                            ],
                        },
                    },
                    { "$project": { "sku": 1_i32 } },
                ],
                "as": "items",
            },
        }
    );
    assert_eq!(
        scope
            .union_with_stage("archived_orders", doc! { "state": "open" }, [])
            .unwrap(),
        doc! {
            "$unionWith": {
                "coll": "archived_orders",
                "pipeline": [
                    {
                        "$match": {
                            "$and": [
                                { MONGO_TENANT_FIELD: "tenant-a" },
                                { "state": "open" },
                            ],
                        },
                    },
                ],
            },
        }
    );
}

#[test]
fn tenant_scope_rejects_unsafe_foreign_aggregation_identifiers() {
    let scope = MongoTenantScope::new(TenantContext::new("tenant-a").unwrap());

    assert_eq!(
        scope
            .lookup_stage(" ", "items", Document::new(), Document::new(), [])
            .unwrap_err(),
        TenantScopeError::InvalidAggregationIdentifier
    );
    assert_eq!(
        scope
            .lookup_stage("items", "$items", Document::new(), Document::new(), [])
            .unwrap_err(),
        TenantScopeError::InvalidAggregationIdentifier
    );
    assert_eq!(
        scope
            .union_with_stage("archive\0orders", Document::new(), [])
            .unwrap_err(),
        TenantScopeError::InvalidAggregationIdentifier
    );
}

#[test]
fn tenant_scope_adds_or_validates_the_document_tenant() {
    let scope = MongoTenantScope::new(TenantContext::new("tenant-a").unwrap());

    assert_eq!(
        scope.document(doc! { "status": "open" }).unwrap(),
        doc! { "status": "open", MONGO_TENANT_FIELD: "tenant-a" }
    );
    assert!(
        scope
            .document(doc! { MONGO_TENANT_FIELD: "tenant-a" })
            .is_ok()
    );
    assert_eq!(
        scope
            .document(doc! { MONGO_TENANT_FIELD: "tenant-b" })
            .unwrap_err(),
        TenantScopeError::TenantMismatch
    );
    assert_eq!(
        scope
            .document(doc! { MONGO_TENANT_FIELD: 7_i32 })
            .unwrap_err(),
        TenantScopeError::TenantMismatch
    );
}

#[test]
fn tenant_scope_rejects_updates_that_can_change_the_tenant() {
    let scope = MongoTenantScope::new(TenantContext::new("tenant-a").unwrap());

    assert!(
        scope
            .update(doc! { "$set": { "status": "closed" } })
            .is_ok()
    );
    assert_eq!(
        scope
            .update(doc! { "$set": { MONGO_TENANT_FIELD: "tenant-b" } })
            .unwrap_err(),
        TenantScopeError::TenantFieldMutation
    );
    assert_eq!(
        scope
            .update(doc! { "$setOnInsert": { MONGO_TENANT_FIELD: "tenant-a" } })
            .unwrap_err(),
        TenantScopeError::TenantFieldMutation
    );
    assert_eq!(
        scope
            .update(doc! { "$unset": { "tenant_id.profile": "" } })
            .unwrap_err(),
        TenantScopeError::TenantFieldMutation
    );
    assert_eq!(
        scope
            .update(doc! { "$rename": { "name": "renamed" } })
            .unwrap_err(),
        TenantScopeError::TenantFieldMutation
    );
    assert_eq!(
        scope.update(doc! { "status": "closed" }).unwrap_err(),
        TenantScopeError::ReplacementRequiresDocument
    );
}

#[test]
fn change_stream_consumer_is_bounded_and_redacted() {
    assert_eq!(
        ChangeStreamConsumer::new(" ").unwrap_err(),
        ChangeStreamConsumerError::InvalidConsumer
    );
    assert_eq!(
        ChangeStreamConsumer::new("source\0consumer").unwrap_err(),
        ChangeStreamConsumerError::InvalidConsumer
    );
    assert_eq!(
        ChangeStreamConsumer::new("a".repeat(256)).unwrap_err(),
        ChangeStreamConsumerError::InvalidConsumer
    );

    let consumer = ChangeStreamConsumer::new("orders-projection-v1").unwrap();
    assert_eq!(consumer.as_str(), "orders-projection-v1");
    assert!(!format!("{consumer:?}").contains("orders-projection-v1"));
}

#[test]
fn change_stream_next_debug_redacts_event_content_and_tokens() {
    let event = ChangeStreamNext::Event {
        event: "private-change-document".to_owned(),
        resume_token: None,
    };
    let ended = ChangeStreamNext::<String>::Ended { resume_token: None };

    let event_debug = format!("{event:?}");
    assert!(event_debug.contains("ChangeStreamNext::Event"));
    assert!(event_debug.contains("has_resume_token: false"));
    assert!(!event_debug.contains("private-change-document"));
    assert_eq!(
        format!("{ended:?}"),
        "ChangeStreamNext::Ended { has_resume_token: false }"
    );
}
