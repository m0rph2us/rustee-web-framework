//! Provider-neutral `JSON Schema` declarations for Rustee versioned events.
//!
//! A schema catalog is an application startup/release artifact, not a Kafka publish path. The
//! application chooses the registry implementation, compatibility check, topic mapping, rollout
//! order, and consumer fleet evidence. This crate deliberately does not publish schemas, inject
//! broker headers, mutate topics, or infer JSON compatibility from two schema documents.

use std::{
    collections::BTreeMap,
    error::Error as StdError,
    fmt::{self, Write},
};

use futures_util::future::BoxFuture;
use rustee_events::Event;
use serde_json::Value;
use sha2::{Digest, Sha256};

const MAX_SUBJECT_LEN: usize = 255;
const MAX_EVENT_TYPE_LEN: usize = 255;
const MAX_SCHEMA_LEN: usize = 1_048_576;

/// Schema artifact format supported by the provider-neutral catalog.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SchemaFormat {
    /// A source-controlled `JSON Schema` document.
    JsonSchema,
}

impl SchemaFormat {
    /// Returns the stable registry-facing format identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::JsonSchema => "json_schema",
        }
    }
}

/// Explicit compatibility policy a registry adapter must enforce for one subject.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SchemaCompatibility {
    /// New readers must accept records written by older schemas.
    Backward,
    /// Older readers must accept records written by newer schemas.
    Forward,
    /// Both old and new readers must accept each other's records.
    Full,
    /// The subject has no declared compatibility guarantee.
    None,
}

impl SchemaCompatibility {
    /// Returns the stable registry-facing compatibility identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Backward => "backward",
            Self::Forward => "forward",
            Self::Full => "full",
            Self::None => "none",
        }
    }
}

/// Stable registry subject owned by one event type.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SchemaSubject(String);

impl SchemaSubject {
    /// Creates a bounded ASCII subject containing only letters, digits, `.`, `_`, and `-`.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaConfigError::InvalidSubject`] for blank, oversized, or unsafe values.
    pub fn new(subject: impl Into<String>) -> Result<Self, SchemaConfigError> {
        let subject = subject.into();
        if subject.is_empty()
            || subject.len() > MAX_SUBJECT_LEN
            || !subject
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(SchemaConfigError::InvalidSubject);
        }
        Ok(Self(subject))
    }

    /// Returns the stable subject string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SchemaSubject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// SHA-256 fingerprint of one exact schema artifact and its format.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SchemaFingerprint([u8; 32]);

impl SchemaFingerprint {
    /// Returns the lowercase hexadecimal SHA-256 fingerprint.
    #[must_use]
    pub fn as_hex(self) -> String {
        let mut value = String::with_capacity(self.0.len() * 2);
        for byte in self.0 {
            write!(value, "{byte:02x}").expect("writing to an owned String must not fail");
        }
        value
    }
}

impl fmt::Debug for SchemaFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("SchemaFingerprint")
            .field(&self.as_hex())
            .finish()
    }
}

impl fmt::Display for SchemaFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.as_hex())
    }
}

/// Source-controlled schema declaration for one typed Rustee event version.
#[derive(Clone, Eq, PartialEq)]
pub struct EventSchema {
    subject: SchemaSubject,
    event_type: String,
    version: u16,
    compatibility: SchemaCompatibility,
    definition: String,
    fingerprint: SchemaFingerprint,
}

impl EventSchema {
    /// Declares a `JSON Schema` document for a typed event's current version.
    ///
    /// The schema document must be a JSON object no larger than one MiB. Its exact source bytes,
    /// prefixed by the format identifier, produce [`Self::fingerprint`]; whitespace-only changes
    /// intentionally require a reviewed artifact update.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaConfigError`] when the event type/version or schema document is invalid.
    pub fn json<E>(
        subject: SchemaSubject,
        compatibility: SchemaCompatibility,
        definition: impl Into<String>,
    ) -> Result<Self, SchemaConfigError>
    where
        E: Event,
    {
        let event_type = E::TYPE.to_owned();
        let version = E::VERSION;
        Self::new(
            subject,
            event_type,
            version,
            compatibility,
            definition.into(),
        )
    }

    fn new(
        subject: SchemaSubject,
        event_type: String,
        version: u16,
        compatibility: SchemaCompatibility,
        definition: String,
    ) -> Result<Self, SchemaConfigError> {
        if event_type.is_empty()
            || event_type.len() > MAX_EVENT_TYPE_LEN
            || !event_type.is_ascii()
            || event_type.bytes().any(|byte| byte.is_ascii_whitespace())
        {
            return Err(SchemaConfigError::InvalidEventType);
        }
        if version == 0 {
            return Err(SchemaConfigError::ZeroVersion);
        }
        if definition.is_empty() || definition.len() > MAX_SCHEMA_LEN {
            return Err(SchemaConfigError::InvalidSchemaLength);
        }
        let value: Value =
            serde_json::from_str(&definition).map_err(|_| SchemaConfigError::InvalidJsonSchema)?;
        if !value.is_object() {
            return Err(SchemaConfigError::JsonSchemaMustBeObject);
        }
        let fingerprint = schema_fingerprint(SchemaFormat::JsonSchema, &definition);
        Ok(Self {
            subject,
            event_type,
            version,
            compatibility,
            definition,
            fingerprint,
        })
    }

    /// Returns this schema's registry subject.
    #[must_use]
    pub fn subject(&self) -> &SchemaSubject {
        &self.subject
    }

    /// Returns the typed event name associated with this subject.
    #[must_use]
    pub fn event_type(&self) -> &str {
        &self.event_type
    }

    /// Returns the typed event version associated with this declaration.
    #[must_use]
    pub const fn version(&self) -> u16 {
        self.version
    }

    /// Returns the schema format.
    #[must_use]
    pub const fn format(&self) -> SchemaFormat {
        SchemaFormat::JsonSchema
    }

    /// Returns the registry compatibility policy that must remain stable for this subject.
    #[must_use]
    pub const fn compatibility(&self) -> SchemaCompatibility {
        self.compatibility
    }

    /// Returns the exact source-controlled `JSON Schema` document.
    ///
    /// Schema definitions can reveal product fields, so callers should treat this as an explicit
    /// release artifact and avoid copying it into unbounded logs or metric labels.
    #[must_use]
    pub fn definition(&self) -> &str {
        &self.definition
    }

    /// Returns the exact format-plus-document fingerprint expected from a registry adapter.
    #[must_use]
    pub const fn fingerprint(&self) -> SchemaFingerprint {
        self.fingerprint
    }
}

impl fmt::Debug for EventSchema {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventSchema")
            .field("subject", &self.subject)
            .field("event_type", &self.event_type)
            .field("version", &self.version)
            .field("format", &self.format())
            .field("compatibility", &self.compatibility)
            .field("fingerprint", &self.fingerprint)
            .finish_non_exhaustive()
    }
}

fn schema_fingerprint(format: SchemaFormat, definition: &str) -> SchemaFingerprint {
    let mut hasher = Sha256::new();
    hasher.update(format.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(definition.as_bytes());
    SchemaFingerprint(hasher.finalize().into())
}

/// Invalid local schema declaration configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SchemaConfigError {
    /// The registry subject was blank, too long, or contained unsafe characters.
    #[error(
        "event schema subject must be 1-255 ASCII letters, digits, dots, underscores, or hyphens"
    )]
    InvalidSubject,
    /// The typed event name was blank, too long, non-ASCII, or contained whitespace.
    #[error("event schema type must be a non-blank ASCII identifier without whitespace")]
    InvalidEventType,
    /// Version zero cannot represent a declared schema evolution point.
    #[error("event schema version must be greater than zero")]
    ZeroVersion,
    /// The schema document was blank or larger than the bounded catalog artifact size.
    #[error("event JSON Schema must be 1 byte to 1 MiB")]
    InvalidSchemaLength,
    /// The schema document was not valid JSON.
    #[error("event JSON Schema must be valid JSON")]
    InvalidJsonSchema,
    /// The schema document was valid JSON but not a top-level object.
    #[error("event JSON Schema must be a top-level object")]
    JsonSchemaMustBeObject,
}

/// Validated, stable-order collection of schemas that share rollout policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventSchemaCatalog {
    schemas: BTreeMap<SchemaSubject, BTreeMap<u16, EventSchema>>,
}

impl EventSchemaCatalog {
    /// Creates a catalog with one event type and one compatibility policy per subject.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaCatalogError`] when a subject repeats a version, changes its event type,
    /// or changes its declared compatibility policy.
    pub fn new(schemas: impl IntoIterator<Item = EventSchema>) -> Result<Self, SchemaCatalogError> {
        let mut catalog = Self {
            schemas: BTreeMap::new(),
        };
        for schema in schemas {
            let versions = catalog.schemas.entry(schema.subject.clone()).or_default();
            if let Some(existing) = versions.values().next() {
                if existing.event_type != schema.event_type {
                    return Err(SchemaCatalogError::SubjectEventTypeDrift);
                }
                if existing.compatibility != schema.compatibility {
                    return Err(SchemaCatalogError::SubjectCompatibilityDrift);
                }
            }
            if versions.insert(schema.version, schema).is_some() {
                return Err(SchemaCatalogError::DuplicateSchemaVersion);
            }
        }
        Ok(catalog)
    }

    /// Returns a schema by subject and typed event version.
    #[must_use]
    pub fn schema(&self, subject: &SchemaSubject, version: u16) -> Option<&EventSchema> {
        self.schemas.get(subject)?.get(&version)
    }

    /// Iterates schemas in deterministic subject then version order.
    pub fn schemas(&self) -> impl Iterator<Item = &EventSchema> {
        self.schemas.values().flat_map(BTreeMap::values)
    }

    /// Verifies every local declaration against an application-owned registry adapter.
    ///
    /// The adapter must perform the remote compatibility behavior appropriate for its provider.
    /// This method only rejects a response whose subject, version, or fingerprint differs from the
    /// immutable local artifact. It performs no Kafka network operation and has no retry policy.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaVerificationError::Registry`] when the adapter fails, or
    /// [`SchemaVerificationError::MismatchedRegistration`] when it acknowledges a different
    /// local declaration.
    pub async fn verify<R>(&self, registry: &R) -> Result<(), SchemaVerificationError<R::Error>>
    where
        R: EventSchemaRegistry + ?Sized,
    {
        for schema in self.schemas() {
            let registration = registry
                .register_or_verify(schema)
                .await
                .map_err(SchemaVerificationError::Registry)?;
            if registration.subject != schema.subject
                || registration.version != schema.version
                || registration.fingerprint != schema.fingerprint
            {
                return Err(SchemaVerificationError::MismatchedRegistration);
            }
        }
        Ok(())
    }
}

/// Catalog-level consistency failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SchemaCatalogError {
    /// A subject declared the same schema version more than once.
    #[error("event schema catalog contains a duplicate subject version")]
    DuplicateSchemaVersion,
    /// A subject was declared for more than one event type.
    #[error("event schema catalog subject cannot change its event type")]
    SubjectEventTypeDrift,
    /// A subject was declared with conflicting compatibility policies.
    #[error("event schema catalog subject cannot change its compatibility policy")]
    SubjectCompatibilityDrift,
}

/// Registry acknowledgement for one local schema declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisteredEventSchema {
    subject: SchemaSubject,
    version: u16,
    fingerprint: SchemaFingerprint,
}

impl RegisteredEventSchema {
    /// Creates a registry acknowledgement from the identity it verified or registered.
    #[must_use]
    pub const fn new(subject: SchemaSubject, version: u16, fingerprint: SchemaFingerprint) -> Self {
        Self {
            subject,
            version,
            fingerprint,
        }
    }

    /// Returns the registry subject.
    #[must_use]
    pub fn subject(&self) -> &SchemaSubject {
        &self.subject
    }

    /// Returns the event version.
    #[must_use]
    pub const fn version(&self) -> u16 {
        self.version
    }

    /// Returns the exact schema artifact fingerprint.
    #[must_use]
    pub const fn fingerprint(&self) -> SchemaFingerprint {
        self.fingerprint
    }
}

/// Application-owned remote schema registry adapter.
///
/// Implementations decide whether registration is allowed during startup, how credentials and
/// endpoints are supplied, and which remote compatibility operation is required. The future must
/// not contain payload records or alter a live consumer group's offsets.
pub trait EventSchemaRegistry: Send + Sync + 'static {
    /// Registry-specific error returned without framework-side stringification.
    type Error: StdError + Send + Sync + 'static;

    /// Registers or verifies one local schema artifact and returns the exact identity observed.
    fn register_or_verify<'a>(
        &'a self,
        schema: &'a EventSchema,
    ) -> BoxFuture<'a, Result<RegisteredEventSchema, Self::Error>>;
}

/// Sanitized failure from verifying a schema catalog against a registry adapter.
#[derive(Debug)]
pub enum SchemaVerificationError<E> {
    /// The application-owned registry adapter failed.
    Registry(E),
    /// The adapter acknowledged a different subject, version, or schema fingerprint.
    MismatchedRegistration,
}

impl<E> fmt::Display for SchemaVerificationError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Registry(_) => formatter.write_str("event schema registry verification failed"),
            Self::MismatchedRegistration => formatter
                .write_str("event schema registry acknowledged a different schema artifact"),
        }
    }
}

impl<E> StdError for SchemaVerificationError<E>
where
    E: StdError + 'static,
{
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Registry(error) => Some(error),
            Self::MismatchedRegistration => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, sync::Arc};

    use serde::{Deserialize, Serialize};

    use super::*;

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

    impl Event for DifferentEventV2 {
        const TYPE: &'static str = "account.closed";
        const VERSION: u16 = 2;
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
        assert!(SchemaSubject::new("account opened").is_err());
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
                    schema.subject.clone(),
                    schema.version,
                    schema.fingerprint,
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
                    schema.version,
                    schema.fingerprint,
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
}
