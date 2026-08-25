use std::collections::{BTreeMap, BTreeSet};

use http::StatusCode;
use rustee_core::{IntoResponse, Response, Result, json_response};
use serde_json::{Map, Value, json};

use crate::{
    OpenApiError, OpenApiMethod, OpenApiOperation, OpenApiRoute, OpenApiSchema,
    OpenApiSecurityRequirement, OpenApiSecurityScheme, validate_identifier, validate_metadata,
    validation::OPENAPI_VERSION,
};

/// An explicit `OpenAPI` 3.1 document for an application.
#[derive(Clone, Debug)]
pub struct OpenApiDocument {
    title: String,
    version: String,
    paths: BTreeMap<String, BTreeMap<OpenApiMethod, OpenApiOperation>>,
    operation_ids: BTreeSet<String>,
    components: BTreeMap<String, OpenApiSchema>,
    security_schemes: BTreeMap<String, OpenApiSecurityScheme>,
    security: Vec<OpenApiSecurityRequirement>,
}

impl OpenApiDocument {
    /// Starts an `OpenAPI` 3.1 document with required API title and version.
    ///
    /// # Errors
    ///
    /// Returns [`OpenApiError::InvalidMetadata`] when either value is blank or too large.
    pub fn new(
        title: impl Into<String>,
        version: impl Into<String>,
    ) -> std::result::Result<Self, OpenApiError> {
        let title = title.into();
        let version = version.into();
        validate_metadata(&title, "title")?;
        validate_metadata(&version, "version")?;
        Ok(Self {
            title,
            version,
            paths: BTreeMap::new(),
            operation_ids: BTreeSet::new(),
            components: BTreeMap::new(),
            security_schemes: BTreeMap::new(),
            security: Vec::new(),
        })
    }

    /// Adds a named reusable JSON Schema component.
    ///
    /// # Errors
    ///
    /// Returns [`OpenApiError::InvalidIdentifier`] when the component name is unsafe or
    /// [`OpenApiError::DuplicateComponent`] when the name was already registered.
    pub fn component(
        mut self,
        name: impl AsRef<str>,
        schema: OpenApiSchema,
    ) -> std::result::Result<Self, OpenApiError> {
        let name = name.as_ref();
        validate_identifier(name, "component name")?;
        if self.components.contains_key(name) {
            return Err(OpenApiError::DuplicateComponent);
        }
        self.components.insert(name.to_owned(), schema);
        Ok(self)
    }

    /// Adds one named security-scheme component.
    ///
    /// Add schemes before operations that reference them so the document can fail closed on
    /// unknown references or incompatible scope requirements.
    ///
    /// # Errors
    ///
    /// Returns [`OpenApiError::InvalidIdentifier`] for an unsafe component name or
    /// [`OpenApiError::DuplicateSecurityScheme`] when the name was already registered.
    pub fn security_scheme(
        mut self,
        name: impl AsRef<str>,
        scheme: OpenApiSecurityScheme,
    ) -> std::result::Result<Self, OpenApiError> {
        let name = name.as_ref();
        validate_identifier(name, "security scheme name")?;
        if self.security_schemes.contains_key(name) {
            return Err(OpenApiError::DuplicateSecurityScheme);
        }
        self.security_schemes.insert(name.to_owned(), scheme);
        Ok(self)
    }

    /// Adds one document-wide security requirement alternative.
    ///
    /// Operations without their own security declaration inherit this array. Register referenced
    /// security schemes before calling this method so invalid names and scopes fail closed.
    ///
    /// # Errors
    ///
    /// Returns [`OpenApiError::UnknownSecurityScheme`], [`OpenApiError::SecurityScopesNotAllowed`]
    /// or [`OpenApiError::UnknownOAuthScope`] when `requirement` is incompatible with declared
    /// schemes.
    pub fn global_security_requirement(
        mut self,
        requirement: OpenApiSecurityRequirement,
    ) -> std::result::Result<Self, OpenApiError> {
        self.validate_security_requirements(std::slice::from_ref(&requirement))?;
        self.security.push(requirement);
        Ok(self)
    }

    /// Adds one documented operation for a validated Rustee route.
    ///
    /// # Errors
    ///
    /// Returns an [`OpenApiError`] when the operation duplicates one method/path pair or
    /// operation ID, or when the path parameter declaration differs from the route template.
    pub fn operation(
        mut self,
        route: OpenApiRoute,
        method: OpenApiMethod,
        operation: OpenApiOperation,
    ) -> std::result::Result<Self, OpenApiError> {
        let declared_parameters = operation.path_parameter_names();
        if route.parameters != declared_parameters {
            return if route.parameters.is_subset(&declared_parameters) {
                Err(OpenApiError::ExtraneousPathParameter)
            } else {
                Err(OpenApiError::MissingPathParameter)
            };
        }
        if let Some(requirements) = &operation.security {
            self.validate_security_requirements(requirements)?;
        }

        if self
            .paths
            .get(&route.path)
            .is_some_and(|operations| operations.contains_key(&method))
        {
            return Err(OpenApiError::DuplicateOperation);
        }
        if !self
            .operation_ids
            .insert(operation.operation_id().to_owned())
        {
            return Err(OpenApiError::DuplicateOperationId);
        }
        self.paths
            .entry(route.path)
            .or_default()
            .insert(method, operation);
        Ok(self)
    }

    fn validate_security_requirements(
        &self,
        requirements: &[OpenApiSecurityRequirement],
    ) -> std::result::Result<(), OpenApiError> {
        for requirement in requirements {
            for (name, scopes) in requirement.schemes() {
                let Some(scheme) = self.security_schemes.get(name) else {
                    return Err(OpenApiError::UnknownSecurityScheme);
                };
                scheme.validate_required_scopes(scopes)?;
            }
        }
        Ok(())
    }

    /// Returns the document as a deterministic JSON value.
    #[must_use]
    pub fn to_value(&self) -> Value {
        let mut document = Map::new();
        document.insert(
            "openapi".to_owned(),
            Value::String(OPENAPI_VERSION.to_owned()),
        );
        document.insert(
            "info".to_owned(),
            json!({ "title": self.title, "version": self.version }),
        );
        document.insert(
            "paths".to_owned(),
            Value::Object(
                self.paths
                    .iter()
                    .map(|(path, operations)| {
                        (
                            path.clone(),
                            Value::Object(
                                operations
                                    .iter()
                                    .map(|(method, operation)| {
                                        (method.as_str().to_owned(), operation.to_value())
                                    })
                                    .collect(),
                            ),
                        )
                    })
                    .collect(),
            ),
        );
        if !self.security.is_empty() {
            document.insert(
                "security".to_owned(),
                Value::Array(
                    self.security
                        .iter()
                        .map(OpenApiSecurityRequirement::to_value)
                        .collect(),
                ),
            );
        }
        let mut components = Map::new();
        if !self.components.is_empty() {
            components.insert(
                "schemas".to_owned(),
                Value::Object(
                    self.components
                        .iter()
                        .map(|(name, schema)| (name.clone(), schema.as_value().clone()))
                        .collect(),
                ),
            );
        }
        if !self.security_schemes.is_empty() {
            components.insert(
                "securitySchemes".to_owned(),
                Value::Object(
                    self.security_schemes
                        .iter()
                        .map(|(name, scheme)| (name.clone(), scheme.to_value()))
                        .collect(),
                ),
            );
        }
        if !components.is_empty() {
            document.insert("components".to_owned(), Value::Object(components));
        }
        Value::Object(document)
    }

    /// Returns the document as an <code>application/json</code> Rustee response.
    ///
    /// # Errors
    ///
    /// Returns a sanitized internal error only if JSON serialization unexpectedly fails.
    pub fn json_response(&self) -> Result<Response> {
        json_response(StatusCode::OK, &self.to_value())
    }
}

impl IntoResponse for OpenApiDocument {
    fn into_response(self) -> Response {
        self.json_response()
            .unwrap_or_else(IntoResponse::into_response)
    }
}
