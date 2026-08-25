//! Bounded reusable JSON Schema values for `OpenAPI` documents.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, Write},
};

use serde_json::{Map, Value, json};

use super::{MAX_SCHEMA_BYTES, OpenApiError, validate_identifier, validate_metadata};

/// A bounded JSON Schema object used in an `OpenAPI` request or response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenApiSchema(Value);

impl OpenApiSchema {
    /// Creates a schema from one explicitly supplied JSON object.
    ///
    /// # Errors
    ///
    /// Returns [`OpenApiError::InvalidSchema`] when the schema is not an object or exceeds the
    /// documented size bound.
    pub fn from_value(value: Value) -> std::result::Result<Self, OpenApiError> {
        if !value.is_object() || !json_within_limit(&value, MAX_SCHEMA_BYTES) {
            return Err(OpenApiError::InvalidSchema);
        }
        Ok(Self(value))
    }

    /// Creates a string schema.
    #[must_use]
    pub fn string() -> Self {
        Self(json!({ "type": "string" }))
    }

    /// Creates an integer schema.
    #[must_use]
    pub fn integer() -> Self {
        Self(json!({ "type": "integer" }))
    }

    /// Creates a boolean schema.
    #[must_use]
    pub fn boolean() -> Self {
        Self(json!({ "type": "boolean" }))
    }

    /// Creates an array schema with one item schema.
    #[must_use]
    pub fn array(items: Self) -> Self {
        let items = items.into_value();
        Self(json!({ "type": "array", "items": items }))
    }

    /// Creates an object schema from named properties and a required-property set.
    ///
    /// # Errors
    ///
    /// Returns [`OpenApiError::InvalidMetadata`] for an unsafe property name or
    /// [`OpenApiError::UnknownRequiredProperty`] when a required property was not declared.
    pub fn object(
        properties: BTreeMap<String, Self>,
        required: impl IntoIterator<Item = String>,
    ) -> std::result::Result<Self, OpenApiError> {
        let mut required_names = BTreeSet::new();
        for name in required {
            validate_metadata(&name, "required property")?;
            if !properties.contains_key(&name) {
                return Err(OpenApiError::UnknownRequiredProperty);
            }
            required_names.insert(name);
        }
        let mut rendered_properties = Map::new();
        for (name, schema) in properties {
            validate_metadata(&name, "property name")?;
            rendered_properties.insert(name, schema.into_value());
        }
        let rendered_required = required_names
            .into_iter()
            .map(Value::String)
            .collect::<Vec<_>>();

        let mut schema = Map::new();
        schema.insert("type".to_owned(), Value::String("object".to_owned()));
        schema.insert("properties".to_owned(), Value::Object(rendered_properties));
        if !rendered_required.is_empty() {
            schema.insert("required".to_owned(), Value::Array(rendered_required));
        }
        Self::from_value(Value::Object(schema))
    }

    /// References one component schema by validated name.
    ///
    /// # Errors
    ///
    /// Returns [`OpenApiError::InvalidIdentifier`] when `component` cannot safely identify a
    /// component schema.
    pub fn component_reference(
        component: impl AsRef<str>,
    ) -> std::result::Result<Self, OpenApiError> {
        validate_identifier(component.as_ref(), "component name")?;
        Self::from_value(json!({ "$ref": format!("#/components/schemas/{}", component.as_ref()) }))
    }

    pub(crate) fn as_value(&self) -> &Value {
        &self.0
    }

    fn into_value(self) -> Value {
        self.0
    }
}

fn json_within_limit(value: &Value, max_bytes: usize) -> bool {
    let mut counter = BoundedJsonCounter::new(max_bytes);
    serde_json::to_writer(&mut counter, value).is_ok() && !counter.exceeded
}

struct BoundedJsonCounter {
    bytes_written: usize,
    max_bytes: usize,
    exceeded: bool,
}

impl BoundedJsonCounter {
    fn new(max_bytes: usize) -> Self {
        Self {
            bytes_written: 0,
            max_bytes,
            exceeded: false,
        }
    }
}

impl Write for BoundedJsonCounter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.len() > self.max_bytes.saturating_sub(self.bytes_written) {
            self.exceeded = true;
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "OpenAPI schema limit exceeded",
            ));
        }

        self.bytes_written += buffer.len();
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
