use std::fmt::{self, Write};

use rustee_events::{Event, is_valid_event_type};
use serde_json::Value;
use sha2::{Digest, Sha256};

const MAX_SUBJECT_LEN: usize = 255;
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
    /// URL dot segments (`.` and `..`) are excluded because registry adapters use subjects as
    /// path segments.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaConfigError::InvalidSubject`] for blank, oversized, or unsafe values.
    pub fn new(subject: impl Into<String>) -> Result<Self, SchemaConfigError> {
        let subject = subject.into();
        if subject.is_empty()
            || subject.len() > MAX_SUBJECT_LEN
            || matches!(subject.as_str(), "." | "..")
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
        if !is_valid_event_type(&event_type)
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
    /// The typed event name was blank, too long, contained NUL, non-ASCII, or contained whitespace.
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
