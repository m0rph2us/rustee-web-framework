//! Deterministic schema catalog assembly and registry-verification contracts.

use std::{collections::BTreeMap, error::Error as StdError, fmt};

use futures_util::future::BoxFuture;

use super::{EventSchema, SchemaFingerprint, SchemaSubject};

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
            let versions = catalog.schemas.entry(schema.subject().clone()).or_default();
            if let Some(existing) = versions.values().next() {
                if existing.event_type() != schema.event_type() {
                    return Err(SchemaCatalogError::SubjectEventTypeDrift);
                }
                if existing.compatibility() != schema.compatibility() {
                    return Err(SchemaCatalogError::SubjectCompatibilityDrift);
                }
            }
            if versions.insert(schema.version(), schema).is_some() {
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
            if registration.subject != *schema.subject()
                || registration.version != schema.version()
                || registration.fingerprint != schema.fingerprint()
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
pub enum SchemaVerificationError<E> {
    /// The application-owned registry adapter failed.
    Registry(E),
    /// The adapter acknowledged a different subject, version, or schema fingerprint.
    MismatchedRegistration,
}

impl<E> fmt::Debug for SchemaVerificationError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Registry(_) => formatter.write_str("SchemaVerificationError::Registry"),
            Self::MismatchedRegistration => {
                formatter.write_str("SchemaVerificationError::MismatchedRegistration")
            }
        }
    }
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
