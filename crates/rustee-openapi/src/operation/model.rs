//! Validated operation declarations and deterministic `OpenAPI` rendering.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value, json};

use crate::{OpenApiSchema, OpenApiSecurityRequirement};

/// The location of an operation parameter.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum OpenApiParameterLocation {
    /// A templated route parameter.
    Path,
    /// A URI query parameter.
    Query,
    /// A request header parameter.
    Header,
}

impl OpenApiParameterLocation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Path => "path",
            Self::Query => "query",
            Self::Header => "header",
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct OpenApiParameter {
    pub(super) name: String,
    pub(super) location: OpenApiParameterLocation,
    pub(super) required: bool,
    pub(super) schema: OpenApiSchema,
}

#[derive(Clone, Debug)]
pub(super) struct OpenApiRequestBody {
    pub(super) required: bool,
    pub(super) schema: OpenApiSchema,
}

#[derive(Clone, Debug)]
pub(super) struct OpenApiResponse {
    pub(super) description: String,
    pub(super) schema: Option<OpenApiSchema>,
}

/// A validated, explicit `OpenAPI` operation.
#[derive(Clone, Debug)]
pub struct OpenApiOperation {
    pub(super) operation_id: String,
    pub(super) summary: Option<String>,
    pub(super) tags: Vec<String>,
    pub(super) parameters: Vec<OpenApiParameter>,
    pub(super) request_body: Option<OpenApiRequestBody>,
    pub(super) responses: BTreeMap<u16, OpenApiResponse>,
    pub(crate) security: Option<Vec<OpenApiSecurityRequirement>>,
}

impl OpenApiOperation {
    pub(crate) fn path_parameter_names(&self) -> BTreeSet<String> {
        self.parameters
            .iter()
            .filter(|parameter| parameter.location == OpenApiParameterLocation::Path)
            .map(|parameter| parameter.name.clone())
            .collect()
    }

    pub(crate) fn operation_id(&self) -> &str {
        &self.operation_id
    }

    pub(crate) fn to_value(&self) -> Value {
        let mut operation = Map::new();
        operation.insert(
            "operationId".to_owned(),
            Value::String(self.operation_id.clone()),
        );
        if let Some(summary) = &self.summary {
            operation.insert("summary".to_owned(), Value::String(summary.clone()));
        }
        if !self.tags.is_empty() {
            operation.insert(
                "tags".to_owned(),
                Value::Array(self.tags.iter().cloned().map(Value::String).collect()),
            );
        }
        if !self.parameters.is_empty() {
            operation.insert(
                "parameters".to_owned(),
                Value::Array(
                    self.parameters
                        .iter()
                        .map(OpenApiParameter::to_value)
                        .collect(),
                ),
            );
        }
        if let Some(request_body) = &self.request_body {
            operation.insert("requestBody".to_owned(), request_body.to_value());
        }
        if let Some(security) = &self.security {
            operation.insert(
                "security".to_owned(),
                Value::Array(
                    security
                        .iter()
                        .map(OpenApiSecurityRequirement::to_value)
                        .collect(),
                ),
            );
        }
        operation.insert(
            "responses".to_owned(),
            Value::Object(
                self.responses
                    .iter()
                    .map(|(status, response)| (status.to_string(), response.to_value()))
                    .collect(),
            ),
        );
        Value::Object(operation)
    }
}

impl OpenApiParameter {
    fn to_value(&self) -> Value {
        json!({
            "name": self.name,
            "in": self.location.as_str(),
            "required": self.required || self.location == OpenApiParameterLocation::Path,
            "schema": self.schema.as_value(),
        })
    }
}

impl OpenApiRequestBody {
    fn to_value(&self) -> Value {
        json!({
            "required": self.required,
            "content": {
                "application/json": {
                    "schema": self.schema.as_value(),
                }
            }
        })
    }
}

impl OpenApiResponse {
    fn to_value(&self) -> Value {
        let mut response = Map::new();
        response.insert(
            "description".to_owned(),
            Value::String(self.description.clone()),
        );
        if let Some(schema) = &self.schema {
            response.insert(
                "content".to_owned(),
                json!({ "application/json": { "schema": schema.as_value() } }),
            );
        }
        Value::Object(response)
    }
}
