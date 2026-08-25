use std::collections::BTreeSet;

use thiserror::Error;

use crate::{MAX_METADATA_CHARS, MAX_SCHEMA_BYTES};

/// Errors reported while building an explicit `OpenAPI` description.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum OpenApiError {
    /// A required metadata field was blank or too large.
    #[error("OpenAPI {field} must be non-empty and at most {MAX_METADATA_CHARS} characters")]
    InvalidMetadata {
        /// The invalid field name.
        field: &'static str,
    },
    /// A stable `OpenAPI` identifier was not safe to render.
    #[error("OpenAPI {field} must contain only ASCII letters, digits, '.', '-', or '_'")]
    InvalidIdentifier {
        /// The invalid field name.
        field: &'static str,
    },
    /// A Rustee route could not be translated into an `OpenAPI` path.
    #[error(
        "OpenAPI routes must be absolute Rustee route templates without query, fragment, or braces"
    )]
    InvalidRoute,
    /// A raw schema was not a bounded JSON object.
    #[error("OpenAPI schemas must be JSON objects no larger than {MAX_SCHEMA_BYTES} bytes")]
    InvalidSchema,
    /// A required property did not exist in an object schema.
    #[error("OpenAPI object schema required property was not declared")]
    UnknownRequiredProperty,
    /// An operation must document at least one response.
    #[error("OpenAPI operations must declare at least one response")]
    MissingResponse,
    /// A parameter was repeated in the same location.
    #[error("OpenAPI operation repeated one parameter name in the same location")]
    DuplicateParameter,
    /// A path template parameter has no matching path parameter declaration.
    #[error("OpenAPI route parameter has no matching required path parameter declaration")]
    MissingPathParameter,
    /// An operation declared a path parameter that the route does not contain.
    #[error("OpenAPI operation declared a path parameter that the route does not contain")]
    ExtraneousPathParameter,
    /// An operation for this method was already added to the path.
    #[error("OpenAPI document already has an operation for this method and path")]
    DuplicateOperation,
    /// An operation ID was already used by another operation.
    #[error("OpenAPI document already has an operation with this operation ID")]
    DuplicateOperationId,
    /// A reusable schema component name was registered twice.
    #[error("OpenAPI document already has a schema component with this name")]
    DuplicateComponent,
    /// A security scheme component name was registered twice.
    #[error("OpenAPI document already has a security scheme with this name")]
    DuplicateSecurityScheme,
    /// An operation declared one status-code response more than once.
    #[error("OpenAPI operation already has a response for this status code")]
    DuplicateResponse,
    /// An operation referenced a security scheme that the document does not declare.
    #[error("OpenAPI operation referenced an unknown security scheme")]
    UnknownSecurityScheme,
    /// One security requirement repeated a scheme name.
    #[error("OpenAPI security requirement repeated one security scheme")]
    DuplicateSecurityRequirement,
    /// Scopes were supplied for a scheme that does not support scopes.
    #[error("OpenAPI security requirement supplied scopes for a non-OAuth/OIDC scheme")]
    SecurityScopesNotAllowed,
    /// A security-scheme URL was not safe public metadata.
    #[error(
        "OpenAPI security scheme URLs must be HTTPS or loopback HTTP without credentials, query, or fragment"
    )]
    InvalidSecuritySchemeUrl,
    /// An `OAuth2` scheme did not declare any supported flow.
    #[error("OpenAPI OAuth2 security schemes must declare at least one supported flow")]
    MissingOAuthFlow,
    /// An `OAuth2` scheme repeated one flow kind.
    #[error("OpenAPI OAuth2 security schemes cannot repeat one flow kind")]
    DuplicateOAuthFlow,
    /// One `OAuth2` flow repeated a scope name.
    #[error("OpenAPI OAuth2 flow repeated one scope name")]
    DuplicateOAuthScope,
    /// An `OAuth2` security requirement requested an undeclared scope.
    #[error("OpenAPI security requirement requested an OAuth2 scope not declared by the scheme")]
    UnknownOAuthScope,
}

/// A validated Rustee route template translated to an `OpenAPI` path template.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenApiRoute {
    pub(crate) path: String,
    pub(crate) parameters: BTreeSet<String>,
}

impl OpenApiRoute {
    /// Translates a Rustee-style route such as <code>/todos/:id</code> into
    /// <code>/todos/{id}</code>.
    ///
    /// # Errors
    ///
    /// Returns [`OpenApiError::InvalidRoute`] when `route` is not compatible with Rustee's route
    /// parameter grammar or would be ambiguous as an `OpenAPI` path.
    pub fn from_rustee(route: &str) -> std::result::Result<Self, OpenApiError> {
        if !route.starts_with('/') || route.contains(['?', '#', '{', '}']) || route.contains("//") {
            return Err(OpenApiError::InvalidRoute);
        }

        let mut parameters = BTreeSet::new();
        let mut segments = Vec::new();
        for segment in route
            .trim_matches('/')
            .split('/')
            .filter(|segment| !segment.is_empty())
        {
            if let Some(parameter) = segment.strip_prefix(':') {
                if !valid_route_parameter(parameter) || !parameters.insert(parameter.to_owned()) {
                    return Err(OpenApiError::InvalidRoute);
                }
                segments.push(format!("{{{parameter}}}"));
            } else {
                segments.push(segment.to_owned());
            }
        }
        let path = if segments.is_empty() {
            "/".to_owned()
        } else {
            format!("/{}", segments.join("/"))
        };
        Ok(Self { path, parameters })
    }

    /// Returns the rendered `OpenAPI` path template.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.path
    }
}

/// An `OpenAPI` operation method supported by Rustee's router.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum OpenApiMethod {
    /// GET.
    Get,
    /// POST.
    Post,
    /// PUT.
    Put,
    /// PATCH.
    Patch,
    /// DELETE.
    Delete,
    /// HEAD.
    Head,
    /// OPTIONS.
    Options,
}

impl OpenApiMethod {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Post => "post",
            Self::Put => "put",
            Self::Patch => "patch",
            Self::Delete => "delete",
            Self::Head => "head",
            Self::Options => "options",
        }
    }
}

pub(crate) fn valid_route_parameter(parameter: &str) -> bool {
    !parameter.is_empty()
        && parameter
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
}
