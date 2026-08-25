use rustee_events_schema::{EventSchema, SchemaCompatibility};
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
pub(crate) struct CompatibilityResponse {
    #[serde(rename = "compatibilityLevel")]
    pub(crate) compatibility_level: String,
}

#[derive(Deserialize)]
pub(crate) struct RemoteSchema {
    pub(crate) subject: String,
    pub(crate) version: u16,
    #[serde(rename = "schemaType")]
    pub(crate) schema_type: Option<String>,
    pub(crate) schema: String,
}

#[derive(Deserialize)]
pub(crate) struct RegistrationResponse {
    #[serde(rename = "id")]
    _id: i64,
}

pub(crate) fn schema_request(schema: &EventSchema) -> serde_json::Value {
    json!({
        "schemaType": "JSON",
        "schema": schema.definition(),
    })
}

pub(crate) const fn confluent_compatibility(compatibility: SchemaCompatibility) -> &'static str {
    match compatibility {
        SchemaCompatibility::Backward => "BACKWARD",
        SchemaCompatibility::Forward => "FORWARD",
        SchemaCompatibility::Full => "FULL",
        SchemaCompatibility::None => "NONE",
    }
}
