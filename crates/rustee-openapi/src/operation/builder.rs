//! Operation declaration construction and admission validation.

use std::collections::{BTreeMap, BTreeSet};

use http::{StatusCode, header::HeaderName};

use super::model::{
    OpenApiOperation, OpenApiParameter, OpenApiParameterLocation, OpenApiRequestBody,
    OpenApiResponse,
};
use crate::{
    OpenApiError, OpenApiSchema, OpenApiSecurityRequirement, validate_identifier, validate_metadata,
};

/// Builder for a content-free `OpenAPI` operation declaration.
#[derive(Clone, Debug)]
#[must_use = "call build to validate and create the operation"]
pub struct OpenApiOperationBuilder {
    operation_id: String,
    summary: Option<String>,
    tags: Vec<String>,
    parameters: Vec<OpenApiParameter>,
    request_body: Option<OpenApiRequestBody>,
    responses: BTreeMap<u16, OpenApiResponse>,
    duplicate_response: bool,
    security: Option<Vec<OpenApiSecurityRequirement>>,
}

impl OpenApiOperationBuilder {
    pub(super) fn new(operation_id: String) -> Self {
        Self {
            operation_id,
            summary: None,
            tags: Vec::new(),
            parameters: Vec::new(),
            request_body: None,
            responses: BTreeMap::new(),
            duplicate_response: false,
            security: None,
        }
    }

    /// Adds a short operation summary.
    pub fn summary(mut self, summary: impl Into<String>) -> Self {
        self.summary = Some(summary.into());
        self
    }

    /// Adds an operation tag.
    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Adds a required templated route parameter.
    pub fn path_parameter(mut self, name: impl Into<String>, schema: OpenApiSchema) -> Self {
        self.parameters.push(OpenApiParameter {
            name: name.into(),
            location: OpenApiParameterLocation::Path,
            required: true,
            schema,
        });
        self
    }

    /// Adds a query parameter with an explicit required flag.
    pub fn query_parameter(
        mut self,
        name: impl Into<String>,
        required: bool,
        schema: OpenApiSchema,
    ) -> Self {
        self.parameters.push(OpenApiParameter {
            name: name.into(),
            location: OpenApiParameterLocation::Query,
            required,
            schema,
        });
        self
    }

    /// Adds a request-header parameter with an explicit required flag.
    pub fn header_parameter(
        mut self,
        name: impl Into<String>,
        required: bool,
        schema: OpenApiSchema,
    ) -> Self {
        self.parameters.push(OpenApiParameter {
            name: name.into(),
            location: OpenApiParameterLocation::Header,
            required,
            schema,
        });
        self
    }

    /// Declares one JSON request body.
    pub fn json_request(mut self, required: bool, schema: OpenApiSchema) -> Self {
        self.request_body = Some(OpenApiRequestBody { required, schema });
        self
    }

    /// Adds one alternative authentication requirement to this operation.
    ///
    /// Requirements are rendered as `OpenAPI` logical OR alternatives. Schemes inside one
    /// [`OpenApiSecurityRequirement`] remain a logical AND set. Scheme existence and scope
    /// compatibility are checked by [`crate::OpenApiDocument::operation`]. Calling this method creates
    /// an operation-local security array, overriding a document-wide default when present.
    pub fn security_requirement(mut self, requirement: OpenApiSecurityRequirement) -> Self {
        self.security.get_or_insert_default().push(requirement);
        self
    }

    /// Adds explicit anonymous access as one operation-local security alternative.
    ///
    /// This renders one empty security requirement object (`{}`); other requirements added with
    /// [`Self::security_requirement`] remain alternatives in the same operation-local array.
    pub fn anonymous_access(mut self) -> Self {
        self.security
            .get_or_insert_default()
            .push(OpenApiSecurityRequirement::anonymous());
        self
    }

    /// Removes any inherited document-wide security requirement for this operation.
    ///
    /// This renders an empty `security` array, which is distinct from [`Self::anonymous_access`]
    /// and from omitting the operation `security` field.
    pub fn clear_security_requirements(mut self) -> Self {
        self.security = Some(Vec::new());
        self
    }

    /// Declares one JSON response for a status code.
    pub fn json_response(
        mut self,
        status: StatusCode,
        description: impl Into<String>,
        schema: OpenApiSchema,
    ) -> Self {
        self.duplicate_response |= self
            .responses
            .insert(
                status.as_u16(),
                OpenApiResponse {
                    description: description.into(),
                    schema: Some(schema),
                },
            )
            .is_some();
        self
    }

    /// Declares a response with no documented body.
    pub fn empty_response(mut self, status: StatusCode, description: impl Into<String>) -> Self {
        self.duplicate_response |= self
            .responses
            .insert(
                status.as_u16(),
                OpenApiResponse {
                    description: description.into(),
                    schema: None,
                },
            )
            .is_some();
        self
    }

    /// Validates and creates the operation.
    ///
    /// # Errors
    ///
    /// Returns an [`OpenApiError`] when metadata is invalid, parameters conflict, a status-code
    /// response repeats, or no response is declared.
    pub fn build(mut self) -> std::result::Result<OpenApiOperation, OpenApiError> {
        validate_identifier(&self.operation_id, "operation ID")?;
        if let Some(summary) = &self.summary {
            validate_metadata(summary, "operation summary")?;
        }
        let mut tags = BTreeSet::new();
        self.tags.retain(|tag| tags.insert(tag.clone()));
        for tag in &self.tags {
            validate_metadata(tag, "operation tag")?;
        }
        let mut parameter_names = BTreeSet::new();
        for parameter in &self.parameters {
            validate_metadata(&parameter.name, "parameter name")?;
            let normalized_name = if parameter.location == OpenApiParameterLocation::Header {
                HeaderName::from_bytes(parameter.name.as_bytes()).map_err(|_| {
                    OpenApiError::InvalidMetadata {
                        field: "header parameter name",
                    }
                })?;
                parameter.name.to_ascii_lowercase()
            } else {
                parameter.name.clone()
            };
            if !parameter_names.insert((parameter.location, normalized_name)) {
                return Err(OpenApiError::DuplicateParameter);
            }
        }
        if self.duplicate_response {
            return Err(OpenApiError::DuplicateResponse);
        }
        if self.responses.is_empty() {
            return Err(OpenApiError::MissingResponse);
        }
        for response in self.responses.values() {
            validate_metadata(&response.description, "response description")?;
        }
        Ok(OpenApiOperation {
            operation_id: self.operation_id,
            summary: self.summary,
            tags: self.tags,
            parameters: self.parameters,
            request_body: self.request_body,
            responses: self.responses,
            security: self.security,
        })
    }
}
