//! Explicit live qualification against an application-owned Confluent Schema Registry.
//!
//! This test creates one uniquely named JSON Schema subject and deliberately does not delete it.
//! Run it only with an approved non-production registry and an account allowed to read the
//! effective compatibility policy and write the generated subject.

use std::{env, time::Duration};

use rustee_events::Event;
use rustee_events_schema::{EventSchema, EventSchemaCatalog, SchemaCompatibility, SchemaSubject};
use rustee_events_schema_confluent::{
    ConfluentSchemaRegistry, ConfluentSchemaRegistryAuth, ConfluentSchemaRegistryConfig,
};
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ConfluentLiveQualificationV1 {
    id: String,
}

impl Event for ConfluentLiveQualificationV1 {
    const TYPE: &'static str = "rustee.schema-registry-qualification";
    const VERSION: u16 = 1;
}

#[tokio::test]
#[ignore = "requires RUSTEE_CONFLUENT_SCHEMA_REGISTRY_LIVE=1, an approved non-production registry URL, and Basic or bearer credentials; creates a retained schema subject"]
async fn verifies_a_new_json_schema_subject_against_a_live_confluent_registry() {
    assert_eq!(
        env::var("RUSTEE_CONFLUENT_SCHEMA_REGISTRY_LIVE").as_deref(),
        Ok("1"),
        "set RUSTEE_CONFLUENT_SCHEMA_REGISTRY_LIVE=1 only after approving the registry account, retained subject, and compatibility policy"
    );
    let base_url = env::var("RUSTEE_CONFLUENT_SCHEMA_REGISTRY_URL")
        .expect("RUSTEE_CONFLUENT_SCHEMA_REGISTRY_URL is required");
    let auth = live_auth();
    let subject = SchemaSubject::new(format!(
        "rustee-live-qualification-{}-value",
        Uuid::new_v4().simple()
    ))
    .unwrap();
    let catalog = EventSchemaCatalog::new([EventSchema::json::<ConfluentLiveQualificationV1>(
        subject,
        SchemaCompatibility::Backward,
        r#"{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}"#,
    )
    .unwrap()])
    .unwrap();
    let config = ConfluentSchemaRegistryConfig::new(Url::parse(&base_url).unwrap(), auth)
        .unwrap()
        .with_request_timeout(Duration::from_secs(15))
        .unwrap();
    let registry = ConfluentSchemaRegistry::new(config).unwrap();

    registry.verify_catalog(&catalog).await.unwrap();
}

fn live_auth() -> ConfluentSchemaRegistryAuth {
    let api_key = env::var("RUSTEE_CONFLUENT_SCHEMA_REGISTRY_API_KEY").ok();
    let api_secret = env::var("RUSTEE_CONFLUENT_SCHEMA_REGISTRY_API_SECRET").ok();
    let bearer = env::var("RUSTEE_CONFLUENT_SCHEMA_REGISTRY_BEARER_TOKEN").ok();
    match (api_key, api_secret, bearer) {
        (Some(api_key), Some(api_secret), None) => ConfluentSchemaRegistryAuth::Basic {
            api_key,
            api_secret,
        },
        (None, None, Some(token)) => ConfluentSchemaRegistryAuth::Bearer(token),
        _ => panic!(
            "set either RUSTEE_CONFLUENT_SCHEMA_REGISTRY_API_KEY and RUSTEE_CONFLUENT_SCHEMA_REGISTRY_API_SECRET, or RUSTEE_CONFLUENT_SCHEMA_REGISTRY_BEARER_TOKEN"
        ),
    }
}
