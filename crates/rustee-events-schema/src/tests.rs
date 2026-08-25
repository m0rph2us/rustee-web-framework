use std::{convert::Infallible, fmt, sync::Arc};

use futures_util::future::BoxFuture;
use rustee_events::Event;
use serde::{Deserialize, Serialize};

use crate::{
    EventSchema, EventSchemaCatalog, EventSchemaRegistry, RegisteredEventSchema,
    SchemaCatalogError, SchemaCompatibility, SchemaConfigError, SchemaFormat, SchemaSubject,
    SchemaVerificationError,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
struct AccountOpenedV1 {
    account_id: String,
}

impl Event for AccountOpenedV1 {
    const TYPE: &'static str = "account.opened";
    const VERSION: u16 = 1;
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct AccountOpenedV2 {
    account_id: String,
    email: String,
}

impl Event for AccountOpenedV2 {
    const TYPE: &'static str = "account.opened";
    const VERSION: u16 = 2;
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DifferentEventV2 {
    account_id: String,
}

struct LeakyRegistryError;

impl fmt::Debug for LeakyRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LeakyRegistryError(private-schema-registry-configuration)")
    }
}

impl fmt::Display for LeakyRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("private-schema-registry-configuration")
    }
}

impl std::error::Error for LeakyRegistryError {}

impl Event for DifferentEventV2 {
    const TYPE: &'static str = "account.closed";
    const VERSION: u16 = 2;
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct NulTypeEvent;

impl Event for NulTypeEvent {
    const TYPE: &'static str = "account\0opened";
    const VERSION: u16 = 1;
}

fn subject() -> SchemaSubject {
    SchemaSubject::new("account.opened-value").unwrap()
}

fn schema_v1() -> EventSchema {
    EventSchema::json::<AccountOpenedV1>(
        subject(),
        SchemaCompatibility::Backward,
        r#"{"type":"object","required":["account_id"]}"#,
    )
    .unwrap()
}

#[test]
fn schema_declaration_is_bounded_typed_and_definition_redacted_from_debug() {
    let schema = schema_v1();
    assert_eq!(schema.subject().as_str(), "account.opened-value");
    assert_eq!(schema.event_type(), AccountOpenedV1::TYPE);
    assert_eq!(schema.version(), 1);
    assert_eq!(schema.format(), SchemaFormat::JsonSchema);
    assert_eq!(schema.compatibility(), SchemaCompatibility::Backward);
    assert_eq!(schema.fingerprint().as_hex().len(), 64);
    assert!(!format!("{schema:?}").contains("account_id"));
    assert!(matches!(
        EventSchema::json::<AccountOpenedV1>(subject(), SchemaCompatibility::Backward, "[]"),
        Err(SchemaConfigError::JsonSchemaMustBeObject)
    ));
    assert!(matches!(
        EventSchema::json::<NulTypeEvent>(
            subject(),
            SchemaCompatibility::Backward,
            r#"{"type":"object"}"#,
        ),
        Err(SchemaConfigError::InvalidEventType)
    ));
    for invalid_subject in ["account opened", ".", ".."] {
        assert!(SchemaSubject::new(invalid_subject).is_err());
    }
}

#[test]
fn catalog_rejects_duplicate_versions_and_subject_drift() {
    assert!(matches!(
        EventSchemaCatalog::new([schema_v1(), schema_v1()]),
        Err(SchemaCatalogError::DuplicateSchemaVersion)
    ));
    let second_version = EventSchema::json::<AccountOpenedV2>(
        subject(),
        SchemaCompatibility::Backward,
        r#"{"type":"object","required":["account_id","email"]}"#,
    )
    .unwrap();
    let catalog = EventSchemaCatalog::new([schema_v1(), second_version]).unwrap();
    assert_eq!(catalog.schemas().count(), 2);
    let different_event = EventSchema::json::<DifferentEventV2>(
        subject(),
        SchemaCompatibility::Backward,
        r#"{"type":"object","required":["account_id"]}"#,
    )
    .unwrap();
    assert!(matches!(
        EventSchemaCatalog::new([schema_v1(), different_event]),
        Err(SchemaCatalogError::SubjectEventTypeDrift)
    ));
}

#[test]
fn schema_verification_error_redacts_registry_diagnostics_and_preserves_the_source() {
    let error = SchemaVerificationError::Registry(LeakyRegistryError);

    assert_eq!(format!("{error:?}"), "SchemaVerificationError::Registry");
    assert!(
        !error
            .to_string()
            .contains("private-schema-registry-configuration")
    );
    assert!(std::error::Error::source(&error).is_some());
}

#[derive(Clone, Debug)]
struct MatchingRegistry;

impl EventSchemaRegistry for MatchingRegistry {
    type Error = Infallible;

    fn register_or_verify<'a>(
        &'a self,
        schema: &'a EventSchema,
    ) -> BoxFuture<'a, Result<RegisteredEventSchema, Self::Error>> {
        Box::pin(async move {
            Ok(RegisteredEventSchema::new(
                schema.subject().clone(),
                schema.version(),
                schema.fingerprint(),
            ))
        })
    }
}

#[derive(Clone, Debug)]
struct DriftedRegistry;

impl EventSchemaRegistry for DriftedRegistry {
    type Error = Infallible;

    fn register_or_verify<'a>(
        &'a self,
        schema: &'a EventSchema,
    ) -> BoxFuture<'a, Result<RegisteredEventSchema, Self::Error>> {
        let subject = SchemaSubject::new("different.subject").unwrap();
        Box::pin(async move {
            Ok(RegisteredEventSchema::new(
                subject,
                schema.version(),
                schema.fingerprint(),
            ))
        })
    }
}

#[tokio::test]
async fn catalog_requires_exact_registry_acknowledgement() {
    let catalog = EventSchemaCatalog::new([schema_v1()]).unwrap();
    catalog.verify(&MatchingRegistry).await.unwrap();
    assert!(matches!(
        catalog.verify(&DriftedRegistry).await,
        Err(SchemaVerificationError::MismatchedRegistration)
    ));
    let registry: Arc<dyn EventSchemaRegistry<Error = Infallible>> = Arc::new(MatchingRegistry);
    catalog.verify(registry.as_ref()).await.unwrap();
}
