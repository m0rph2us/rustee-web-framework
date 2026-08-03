//! Explicit `OpenAPI` 3.1 descriptions for Rustee applications.
//!
//! This crate does not inspect route handlers, extractors, state, authorization, or response
//! values. Applications declare an [`OpenApiRoute`] and [`OpenApiOperation`] beside the Rustee
//! route they mount. The document checks that path parameters agree with the Rustee-style route
//! template and can be returned directly from a normal handler.

use std::collections::{BTreeMap, BTreeSet};

use http::StatusCode;
use rustee_core::{IntoResponse, Response, Result, json_response};
use serde_json::{Map, Value, json};
use thiserror::Error;
use url::{Host, Url};

const OPENAPI_VERSION: &str = "3.1.1";
const MAX_METADATA_CHARS: usize = 1_024;
const MAX_SCHEMA_BYTES: usize = 128 * 1_024;

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
    /// A security scheme component name was registered twice.
    #[error("OpenAPI document already has a security scheme with this name")]
    DuplicateSecurityScheme,
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
    path: String,
    parameters: BTreeSet<String>,
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
        if !route.starts_with('/') || route.contains(['?', '#', '{', '}']) {
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
    const fn as_str(self) -> &'static str {
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
        if !value.is_object()
            || serde_json::to_vec(&value).map_or(true, |encoded| encoded.len() > MAX_SCHEMA_BYTES)
        {
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

    fn as_value(&self) -> &Value {
        &self.0
    }

    fn into_value(self) -> Value {
        self.0
    }
}

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

/// The location of an API key security credential.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenApiApiKeyLocation {
    /// An HTTP request header.
    Header,
    /// A URI query parameter.
    Query,
    /// An HTTP cookie.
    Cookie,
}

impl OpenApiApiKeyLocation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Header => "header",
            Self::Query => "query",
            Self::Cookie => "cookie",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum OpenApiOAuthFlowKind {
    AuthorizationCode,
    ClientCredentials,
}

impl OpenApiOAuthFlowKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::AuthorizationCode => "authorizationCode",
            Self::ClientCredentials => "clientCredentials",
        }
    }
}

/// One explicit OAuth 2.0 flow documented by an [`OpenApiSecurityScheme`].
///
/// Rustee deliberately models the recommended authorization-code and client-credentials flows
/// only. This is static `OpenAPI` metadata; it does not run a token exchange, hold a client
/// credential, or attach an authentication middleware.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenApiOAuthFlow {
    kind: OpenApiOAuthFlowKind,
    authorization_url: Option<String>,
    token_url: String,
    refresh_url: Option<String>,
    scopes: BTreeMap<String, String>,
}

impl OpenApiOAuthFlow {
    /// Creates an authorization-code flow with declared public scopes.
    ///
    /// # Errors
    ///
    /// Returns [`OpenApiError::InvalidSecuritySchemeUrl`] for an unsafe authorization or token
    /// URL, [`OpenApiError::InvalidMetadata`] for an invalid scope description, or
    /// [`OpenApiError::DuplicateOAuthScope`] for a repeated scope name.
    pub fn authorization_code<I, S, D>(
        authorization_url: impl Into<String>,
        token_url: impl Into<String>,
        scopes: I,
    ) -> std::result::Result<Self, OpenApiError>
    where
        I: IntoIterator<Item = (S, D)>,
        S: AsRef<str>,
        D: AsRef<str>,
    {
        let authorization_url = authorization_url.into();
        let token_url = token_url.into();
        validate_security_scheme_url(&authorization_url)?;
        validate_security_scheme_url(&token_url)?;
        Ok(Self {
            kind: OpenApiOAuthFlowKind::AuthorizationCode,
            authorization_url: Some(authorization_url),
            token_url,
            refresh_url: None,
            scopes: collect_oauth_scopes(scopes)?,
        })
    }

    /// Creates a client-credentials flow with declared public scopes.
    ///
    /// # Errors
    ///
    /// Returns [`OpenApiError::InvalidSecuritySchemeUrl`] for an unsafe token URL,
    /// [`OpenApiError::InvalidMetadata`] for an invalid scope description, or
    /// [`OpenApiError::DuplicateOAuthScope`] for a repeated scope name.
    pub fn client_credentials<I, S, D>(
        token_url: impl Into<String>,
        scopes: I,
    ) -> std::result::Result<Self, OpenApiError>
    where
        I: IntoIterator<Item = (S, D)>,
        S: AsRef<str>,
        D: AsRef<str>,
    {
        let token_url = token_url.into();
        validate_security_scheme_url(&token_url)?;
        Ok(Self {
            kind: OpenApiOAuthFlowKind::ClientCredentials,
            authorization_url: None,
            token_url,
            refresh_url: None,
            scopes: collect_oauth_scopes(scopes)?,
        })
    }

    /// Adds one public refresh-token endpoint URL to this flow.
    ///
    /// # Errors
    ///
    /// Returns [`OpenApiError::InvalidSecuritySchemeUrl`] when `refresh_url` is unsafe public
    /// metadata.
    pub fn with_refresh_url(
        mut self,
        refresh_url: impl Into<String>,
    ) -> std::result::Result<Self, OpenApiError> {
        let refresh_url = refresh_url.into();
        validate_security_scheme_url(&refresh_url)?;
        self.refresh_url = Some(refresh_url);
        Ok(self)
    }

    fn kind(&self) -> OpenApiOAuthFlowKind {
        self.kind
    }

    fn supports_scope(&self, scope: &str) -> bool {
        self.scopes.contains_key(scope)
    }

    fn to_value(&self) -> Value {
        let mut flow = Map::from_iter([
            ("tokenUrl".to_owned(), Value::String(self.token_url.clone())),
            (
                "scopes".to_owned(),
                Value::Object(
                    self.scopes
                        .iter()
                        .map(|(scope, description)| {
                            (scope.clone(), Value::String(description.clone()))
                        })
                        .collect(),
                ),
            ),
        ]);
        if let Some(authorization_url) = &self.authorization_url {
            flow.insert(
                "authorizationUrl".to_owned(),
                Value::String(authorization_url.clone()),
            );
        }
        if let Some(refresh_url) = &self.refresh_url {
            flow.insert("refreshUrl".to_owned(), Value::String(refresh_url.clone()));
        }
        Value::Object(flow)
    }
}

/// An explicit `OpenAPI` security-scheme component.
///
/// This declaration describes a public API contract only. It does not attach authentication to a
/// Rustee route, validate credentials, or infer authorization from handler signatures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OpenApiSecurityScheme {
    /// HTTP Basic authentication.
    HttpBasic,
    /// HTTP Bearer authentication, optionally with one public bearer-format label.
    HttpBearer {
        /// Optional public format label such as `JWT`.
        bearer_format: Option<String>,
    },
    /// An API key supplied at one explicit request location.
    ApiKey {
        /// Parameter or header name carrying the key.
        name: String,
        /// Request location carrying the key.
        location: OpenApiApiKeyLocation,
    },
    /// `OpenID` Connect discovery metadata at a validated public URL.
    OpenIdConnect {
        /// Discovery document URL.
        discovery_url: String,
    },
    /// OAuth 2.0 authorization-code and/or client-credentials flow metadata.
    OAuth2 {
        /// Validated supported OAuth flow declarations.
        flows: Vec<OpenApiOAuthFlow>,
    },
    /// Mutual TLS authentication performed by the deployment transport.
    ///
    /// This documents the `OpenAPI` `mutualTLS` scheme only. Rustee does not terminate TLS,
    /// validate a client certificate, or create a principal from a certificate.
    MutualTls,
}

impl OpenApiSecurityScheme {
    /// Creates an HTTP Basic security-scheme component.
    #[must_use]
    pub const fn http_basic() -> Self {
        Self::HttpBasic
    }

    /// Creates an HTTP Bearer security-scheme component without a format label.
    #[must_use]
    pub const fn http_bearer() -> Self {
        Self::HttpBearer {
            bearer_format: None,
        }
    }

    /// Creates an HTTP Bearer security-scheme component with one public format label.
    ///
    /// # Errors
    ///
    /// Returns [`OpenApiError::InvalidMetadata`] when `bearer_format` is blank or unbounded.
    pub fn http_bearer_with_format(
        bearer_format: impl Into<String>,
    ) -> std::result::Result<Self, OpenApiError> {
        let bearer_format = bearer_format.into();
        validate_metadata(&bearer_format, "bearer format")?;
        Ok(Self::HttpBearer {
            bearer_format: Some(bearer_format),
        })
    }

    /// Creates an API-key security-scheme component.
    ///
    /// # Errors
    ///
    /// Returns [`OpenApiError::InvalidMetadata`] when `name` is blank or unbounded, or when a
    /// header-located key does not use a valid HTTP field name.
    pub fn api_key(
        name: impl Into<String>,
        location: OpenApiApiKeyLocation,
    ) -> std::result::Result<Self, OpenApiError> {
        let name = name.into();
        validate_metadata(&name, "API key name")?;
        if matches!(location, OpenApiApiKeyLocation::Header)
            && http::header::HeaderName::from_bytes(name.as_bytes()).is_err()
        {
            return Err(OpenApiError::InvalidMetadata {
                field: "API key header name",
            });
        }
        Ok(Self::ApiKey { name, location })
    }

    /// Creates an `OpenID` Connect security-scheme component.
    ///
    /// # Errors
    ///
    /// Returns [`OpenApiError::InvalidSecuritySchemeUrl`] when `discovery_url` has an unsafe
    /// scheme, embedded credential, query, or fragment.
    pub fn open_id_connect(
        discovery_url: impl Into<String>,
    ) -> std::result::Result<Self, OpenApiError> {
        let discovery_url = discovery_url.into();
        validate_security_scheme_url(&discovery_url)?;
        Ok(Self::OpenIdConnect { discovery_url })
    }

    /// Creates an OAuth 2.0 security-scheme component from authorization-code and/or
    /// client-credentials flow metadata.
    ///
    /// # Errors
    ///
    /// Returns [`OpenApiError::MissingOAuthFlow`] when `flows` is empty or
    /// [`OpenApiError::DuplicateOAuthFlow`] when the same flow kind appears twice.
    pub fn oauth2<I>(flows: I) -> std::result::Result<Self, OpenApiError>
    where
        I: IntoIterator<Item = OpenApiOAuthFlow>,
    {
        let mut kinds = BTreeSet::new();
        let mut declared_flows = Vec::new();
        for flow in flows {
            if !kinds.insert(flow.kind()) {
                return Err(OpenApiError::DuplicateOAuthFlow);
            }
            declared_flows.push(flow);
        }
        if declared_flows.is_empty() {
            return Err(OpenApiError::MissingOAuthFlow);
        }
        declared_flows.sort_by_key(OpenApiOAuthFlow::kind);
        Ok(Self::OAuth2 {
            flows: declared_flows,
        })
    }

    /// Creates an `OpenAPI` mutual-TLS security-scheme component.
    ///
    /// This is static API metadata. TLS termination, client-certificate validation, trust-store
    /// rotation, and mapping a verified certificate to application identity remain deployment
    /// responsibilities.
    #[must_use]
    pub const fn mutual_tls() -> Self {
        Self::MutualTls
    }

    fn validate_required_scopes(&self, scopes: &[String]) -> std::result::Result<(), OpenApiError> {
        if scopes.is_empty() || matches!(self, Self::OpenIdConnect { .. }) {
            return Ok(());
        }
        let Self::OAuth2 { flows } = self else {
            return Err(OpenApiError::SecurityScopesNotAllowed);
        };
        if scopes
            .iter()
            .all(|scope| flows.iter().any(|flow| flow.supports_scope(scope)))
        {
            Ok(())
        } else {
            Err(OpenApiError::UnknownOAuthScope)
        }
    }

    fn to_value(&self) -> Value {
        match self {
            Self::HttpBasic => json!({ "type": "http", "scheme": "basic" }),
            Self::HttpBearer { bearer_format } => {
                let mut scheme = Map::from_iter([
                    ("type".to_owned(), Value::String("http".to_owned())),
                    ("scheme".to_owned(), Value::String("bearer".to_owned())),
                ]);
                if let Some(bearer_format) = bearer_format {
                    scheme.insert(
                        "bearerFormat".to_owned(),
                        Value::String(bearer_format.clone()),
                    );
                }
                Value::Object(scheme)
            }
            Self::ApiKey { name, location } => json!({
                "type": "apiKey",
                "name": name,
                "in": location.as_str(),
            }),
            Self::OpenIdConnect { discovery_url } => json!({
                "type": "openIdConnect",
                "openIdConnectUrl": discovery_url,
            }),
            Self::OAuth2 { flows } => json!({
                "type": "oauth2",
                "flows": flows
                    .iter()
                    .map(|flow| (flow.kind().as_str().to_owned(), flow.to_value()))
                    .collect::<Map<String, Value>>(),
            }),
            Self::MutualTls => json!({ "type": "mutualTLS" }),
        }
    }
}

/// One explicit `OpenAPI` security-requirement alternative.
///
/// Schemes in one value are combined with logical AND. Repeated requirements on an operation are
/// alternatives (logical OR). An empty value permits anonymous access as one alternative. The
/// document validates scheme references and rejects scopes for schemes other than `OAuth2` or
/// `OpenID` Connect, including mutual TLS.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenApiSecurityRequirement {
    schemes: BTreeMap<String, Vec<String>>,
}

impl OpenApiSecurityRequirement {
    /// Creates an empty requirement that explicitly permits anonymous access.
    #[must_use]
    pub fn anonymous() -> Self {
        Self {
            schemes: BTreeMap::new(),
        }
    }

    /// Starts one requirement with a scheme that has no scopes.
    ///
    /// # Errors
    ///
    /// Returns [`OpenApiError::InvalidIdentifier`] when `scheme` is not a safe component name.
    pub fn scheme(scheme: impl AsRef<str>) -> std::result::Result<Self, OpenApiError> {
        Self::scoped(scheme, std::iter::empty::<String>())
    }

    /// Starts one requirement with an `OAuth2` or `OpenID` Connect scheme and explicit scopes.
    ///
    /// The operation is not connected to runtime authentication. [`OpenApiDocument::operation`]
    /// checks that the scheme exists, supports scopes, and declares requested `OAuth2` scopes.
    ///
    /// # Errors
    ///
    /// Returns [`OpenApiError::InvalidIdentifier`] for an unsafe scheme name or
    /// [`OpenApiError::InvalidMetadata`] for an unsafe scope token.
    pub fn scoped<I, S>(
        scheme: impl AsRef<str>,
        scopes: I,
    ) -> std::result::Result<Self, OpenApiError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            schemes: BTreeMap::new(),
        }
        .and_scoped(scheme, scopes)
    }

    /// Adds a no-scope scheme to the same requirement alternative.
    ///
    /// # Errors
    ///
    /// Returns [`OpenApiError::DuplicateSecurityRequirement`] when the scheme is already present.
    pub fn and_scheme(self, scheme: impl AsRef<str>) -> std::result::Result<Self, OpenApiError> {
        self.and_scoped(scheme, std::iter::empty::<String>())
    }

    /// Adds one scoped scheme to the same requirement alternative.
    ///
    /// # Errors
    ///
    /// Returns [`OpenApiError::DuplicateSecurityRequirement`] when the scheme is already present.
    pub fn and_scoped<I, S>(
        mut self,
        scheme: impl AsRef<str>,
        scopes: I,
    ) -> std::result::Result<Self, OpenApiError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let scheme = scheme.as_ref();
        validate_identifier(scheme, "security scheme name")?;
        let mut unique_scopes = BTreeSet::new();
        for scope in scopes {
            let scope = scope.as_ref();
            validate_scope(scope)?;
            unique_scopes.insert(scope.to_owned());
        }
        if self
            .schemes
            .insert(scheme.to_owned(), unique_scopes.into_iter().collect())
            .is_some()
        {
            return Err(OpenApiError::DuplicateSecurityRequirement);
        }
        Ok(self)
    }

    fn to_value(&self) -> Value {
        Value::Object(
            self.schemes
                .iter()
                .map(|(scheme, scopes)| {
                    (
                        scheme.clone(),
                        Value::Array(scopes.iter().cloned().map(Value::String).collect()),
                    )
                })
                .collect(),
        )
    }
}

#[derive(Clone, Debug)]
struct OpenApiParameter {
    name: String,
    location: OpenApiParameterLocation,
    required: bool,
    schema: OpenApiSchema,
}

#[derive(Clone, Debug)]
struct OpenApiRequestBody {
    required: bool,
    schema: OpenApiSchema,
}

#[derive(Clone, Debug)]
struct OpenApiResponse {
    description: String,
    schema: Option<OpenApiSchema>,
}

/// A validated, explicit `OpenAPI` operation.
#[derive(Clone, Debug)]
pub struct OpenApiOperation {
    operation_id: String,
    summary: Option<String>,
    tags: Vec<String>,
    parameters: Vec<OpenApiParameter>,
    request_body: Option<OpenApiRequestBody>,
    responses: BTreeMap<u16, OpenApiResponse>,
    security: Option<Vec<OpenApiSecurityRequirement>>,
}

impl OpenApiOperation {
    /// Starts a builder for one operation ID.
    pub fn builder(operation_id: impl Into<String>) -> OpenApiOperationBuilder {
        OpenApiOperationBuilder {
            operation_id: operation_id.into(),
            summary: None,
            tags: Vec::new(),
            parameters: Vec::new(),
            request_body: None,
            responses: BTreeMap::new(),
            security: None,
        }
    }

    fn path_parameter_names(&self) -> BTreeSet<String> {
        self.parameters
            .iter()
            .filter(|parameter| parameter.location == OpenApiParameterLocation::Path)
            .map(|parameter| parameter.name.clone())
            .collect()
    }

    fn to_value(&self) -> Value {
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
    security: Option<Vec<OpenApiSecurityRequirement>>,
}

impl OpenApiOperationBuilder {
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
    /// compatibility are checked by [`OpenApiDocument::operation`]. Calling this method creates
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
        self.responses.insert(
            status.as_u16(),
            OpenApiResponse {
                description: description.into(),
                schema: Some(schema),
            },
        );
        self
    }

    /// Declares a response with no documented body.
    pub fn empty_response(mut self, status: StatusCode, description: impl Into<String>) -> Self {
        self.responses.insert(
            status.as_u16(),
            OpenApiResponse {
                description: description.into(),
                schema: None,
            },
        );
        self
    }

    /// Validates and creates the operation.
    ///
    /// # Errors
    ///
    /// Returns an [`OpenApiError`] when metadata is invalid, parameters conflict, or no response
    /// is declared.
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
            if !parameter_names.insert((parameter.location, parameter.name.clone())) {
                return Err(OpenApiError::DuplicateParameter);
            }
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

/// An explicit `OpenAPI` 3.1 document for an application.
#[derive(Clone, Debug)]
pub struct OpenApiDocument {
    title: String,
    version: String,
    paths: BTreeMap<String, BTreeMap<OpenApiMethod, OpenApiOperation>>,
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
            components: BTreeMap::new(),
            security_schemes: BTreeMap::new(),
            security: Vec::new(),
        })
    }

    /// Adds a named reusable JSON Schema component.
    ///
    /// # Errors
    ///
    /// Returns [`OpenApiError::InvalidIdentifier`] when the component name is unsafe.
    pub fn component(
        mut self,
        name: impl AsRef<str>,
        schema: OpenApiSchema,
    ) -> std::result::Result<Self, OpenApiError> {
        validate_identifier(name.as_ref(), "component name")?;
        self.components.insert(name.as_ref().to_owned(), schema);
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
    /// Returns an [`OpenApiError`] when the operation duplicates one method/path pair or the path
    /// parameter declaration differs from the route template.
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

        let operations = self.paths.entry(route.path).or_default();
        if operations.insert(method, operation).is_some() {
            return Err(OpenApiError::DuplicateOperation);
        }
        Ok(self)
    }

    fn validate_security_requirements(
        &self,
        requirements: &[OpenApiSecurityRequirement],
    ) -> std::result::Result<(), OpenApiError> {
        for requirement in requirements {
            for (name, scopes) in &requirement.schemes {
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

fn validate_metadata(value: &str, field: &'static str) -> std::result::Result<(), OpenApiError> {
    if value.trim().is_empty() || value.chars().count() > MAX_METADATA_CHARS || value.contains('\0')
    {
        return Err(OpenApiError::InvalidMetadata { field });
    }
    Ok(())
}

fn validate_identifier(value: &str, field: &'static str) -> std::result::Result<(), OpenApiError> {
    if value.is_empty()
        || value.chars().count() > MAX_METADATA_CHARS
        || !value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_')
        })
    {
        return Err(OpenApiError::InvalidIdentifier { field });
    }
    Ok(())
}

fn validate_scope(scope: &str) -> std::result::Result<(), OpenApiError> {
    if scope.is_empty()
        || scope.chars().count() > MAX_METADATA_CHARS
        || !scope.bytes().all(|byte| {
            byte == b'!' || (b'#'..=b'[').contains(&byte) || (b']'..=b'~').contains(&byte)
        })
    {
        return Err(OpenApiError::InvalidMetadata {
            field: "security scope",
        });
    }
    Ok(())
}

fn collect_oauth_scopes<I, S, D>(
    scopes: I,
) -> std::result::Result<BTreeMap<String, String>, OpenApiError>
where
    I: IntoIterator<Item = (S, D)>,
    S: AsRef<str>,
    D: AsRef<str>,
{
    let mut values = BTreeMap::new();
    for (scope, description) in scopes {
        let scope = scope.as_ref();
        let description = description.as_ref();
        validate_scope(scope)?;
        validate_metadata(description, "OAuth scope description")?;
        if values
            .insert(scope.to_owned(), description.to_owned())
            .is_some()
        {
            return Err(OpenApiError::DuplicateOAuthScope);
        }
    }
    Ok(values)
}

fn validate_security_scheme_url(value: &str) -> std::result::Result<(), OpenApiError> {
    if value.chars().count() > MAX_METADATA_CHARS || value.contains('\0') {
        return Err(OpenApiError::InvalidSecuritySchemeUrl);
    }
    let parsed = Url::parse(value).map_err(|_| OpenApiError::InvalidSecuritySchemeUrl)?;
    let loopback_host = match parsed.host() {
        Some(Host::Domain("localhost")) => true,
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        _ => false,
    };
    let loopback_http = parsed.scheme() == "http" && loopback_host;
    if parsed.host().is_none()
        || !(parsed.scheme() == "https" || loopback_http)
        || parsed.username() != ""
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(OpenApiError::InvalidSecuritySchemeUrl);
    }
    Ok(())
}

fn valid_route_parameter(parameter: &str) -> bool {
    !parameter.is_empty()
        && parameter
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use http::header::CONTENT_TYPE;
    use http_body_util::BodyExt;
    use proptest::prelude::*;

    use super::*;

    fn todo_schema() -> OpenApiSchema {
        OpenApiSchema::object(
            BTreeMap::from([
                ("id".to_owned(), OpenApiSchema::integer()),
                ("title".to_owned(), OpenApiSchema::string()),
            ]),
            ["id".to_owned(), "title".to_owned()],
        )
        .unwrap()
    }

    #[test]
    fn document_translates_routes_and_renders_json_contracts() {
        let document = OpenApiDocument::new("Todo API", "0.1.0")
            .unwrap()
            .component("Todo", todo_schema())
            .unwrap()
            .operation(
                OpenApiRoute::from_rustee("/todos/:todo_id").unwrap(),
                OpenApiMethod::Get,
                OpenApiOperation::builder("get_todo")
                    .summary("Gets one todo")
                    .tag("todos")
                    .path_parameter("todo_id", OpenApiSchema::integer())
                    .json_response(
                        StatusCode::OK,
                        "The requested todo",
                        OpenApiSchema::component_reference("Todo").unwrap(),
                    )
                    .empty_response(StatusCode::NOT_FOUND, "The todo was not found")
                    .build()
                    .unwrap(),
            )
            .unwrap();

        let document = document.to_value();
        assert_eq!(document["openapi"], "3.1.1");
        assert_eq!(
            document["paths"]["/todos/{todo_id}"]["get"]["operationId"],
            "get_todo"
        );
        assert_eq!(
            document["paths"]["/todos/{todo_id}"]["get"]["parameters"][0]["in"],
            "path"
        );
        assert_eq!(
            document["paths"]["/todos/{todo_id}"]["get"]["responses"]["200"]["content"]["application/json"]
                ["schema"]["$ref"],
            "#/components/schemas/Todo"
        );
    }

    #[test]
    fn document_rejects_missing_or_extraneous_path_parameters() {
        let operation = OpenApiOperation::builder("get_todo")
            .empty_response(StatusCode::NO_CONTENT, "No content")
            .build()
            .unwrap();
        assert_eq!(
            OpenApiDocument::new("Todo API", "0.1.0")
                .unwrap()
                .operation(
                    OpenApiRoute::from_rustee("/todos/:todo_id").unwrap(),
                    OpenApiMethod::Get,
                    operation,
                )
                .unwrap_err(),
            OpenApiError::MissingPathParameter
        );

        let operation = OpenApiOperation::builder("list_todos")
            .path_parameter("todo_id", OpenApiSchema::integer())
            .empty_response(StatusCode::OK, "No content")
            .build()
            .unwrap();
        assert_eq!(
            OpenApiDocument::new("Todo API", "0.1.0")
                .unwrap()
                .operation(
                    OpenApiRoute::from_rustee("/todos").unwrap(),
                    OpenApiMethod::Get,
                    operation,
                )
                .unwrap_err(),
            OpenApiError::ExtraneousPathParameter
        );
    }

    #[test]
    fn operation_rejects_duplicate_parameters_and_missing_responses() {
        assert_eq!(
            OpenApiOperation::builder("search_todos")
                .query_parameter("cursor", false, OpenApiSchema::string())
                .query_parameter("cursor", false, OpenApiSchema::string())
                .empty_response(StatusCode::OK, "Results")
                .build()
                .unwrap_err(),
            OpenApiError::DuplicateParameter
        );
        assert_eq!(
            OpenApiOperation::builder("search_todos")
                .build()
                .unwrap_err(),
            OpenApiError::MissingResponse
        );
    }

    #[tokio::test]
    async fn document_is_a_json_handler_response() {
        let document = OpenApiDocument::new("Todo API", "0.1.0").unwrap();
        let response = document.into_response();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[CONTENT_TYPE],
            "application/json; charset=utf-8"
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap()["info"]["title"],
            "Todo API"
        );
    }

    #[test]
    fn route_and_schema_validation_are_fail_closed() {
        assert_eq!(
            OpenApiRoute::from_rustee("/todos/{todo_id}").unwrap_err(),
            OpenApiError::InvalidRoute
        );
        assert_eq!(
            OpenApiSchema::from_value(Value::String("not a schema".to_owned())).unwrap_err(),
            OpenApiError::InvalidSchema
        );
        assert_eq!(
            OpenApiSchema::object(BTreeMap::new(), ["missing".to_owned()]).unwrap_err(),
            OpenApiError::UnknownRequiredProperty
        );
    }

    #[test]
    fn document_renders_explicit_security_schemes_and_requirements() {
        let document = OpenApiDocument::new("Todo API", "0.1.0")
            .unwrap()
            .security_scheme(
                "bearerAuth",
                OpenApiSecurityScheme::http_bearer_with_format("JWT").unwrap(),
            )
            .unwrap()
            .security_scheme(
                "oidc",
                OpenApiSecurityScheme::open_id_connect(
                    "https://issuer.example/.well-known/openid-configuration",
                )
                .unwrap(),
            )
            .unwrap()
            .security_scheme(
                "tenantKey",
                OpenApiSecurityScheme::api_key("X-Tenant-Key", OpenApiApiKeyLocation::Header)
                    .unwrap(),
            )
            .unwrap()
            .security_scheme("serviceTls", OpenApiSecurityScheme::mutual_tls())
            .unwrap()
            .operation(
                OpenApiRoute::from_rustee("/todos").unwrap(),
                OpenApiMethod::Get,
                OpenApiOperation::builder("list_todos")
                    .security_requirement(OpenApiSecurityRequirement::scheme("bearerAuth").unwrap())
                    .security_requirement(
                        OpenApiSecurityRequirement::scoped("oidc", ["todos.read"])
                            .unwrap()
                            .and_scheme("tenantKey")
                            .unwrap(),
                    )
                    .empty_response(StatusCode::OK, "Todos")
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .to_value();

        assert_eq!(
            document["components"]["securitySchemes"]["bearerAuth"],
            json!({ "type": "http", "scheme": "bearer", "bearerFormat": "JWT" })
        );
        assert_eq!(
            document["components"]["securitySchemes"]["tenantKey"],
            json!({ "type": "apiKey", "name": "X-Tenant-Key", "in": "header" })
        );
        assert_eq!(
            document["components"]["securitySchemes"]["serviceTls"],
            json!({ "type": "mutualTLS" })
        );
        assert_eq!(
            document["paths"]["/todos"]["get"]["security"],
            json!([
                { "bearerAuth": [] },
                { "oidc": ["todos.read"], "tenantKey": [] },
            ])
        );
    }

    #[test]
    fn security_requirements_fail_closed_on_unknown_or_incompatible_schemes() {
        let unknown = OpenApiOperation::builder("list_todos")
            .security_requirement(OpenApiSecurityRequirement::scheme("unknown").unwrap())
            .empty_response(StatusCode::OK, "Todos")
            .build()
            .unwrap();
        assert_eq!(
            OpenApiDocument::new("Todo API", "0.1.0")
                .unwrap()
                .operation(
                    OpenApiRoute::from_rustee("/todos").unwrap(),
                    OpenApiMethod::Get,
                    unknown,
                )
                .unwrap_err(),
            OpenApiError::UnknownSecurityScheme
        );

        let scoped_bearer = OpenApiOperation::builder("list_todos")
            .security_requirement(
                OpenApiSecurityRequirement::scoped("bearerAuth", ["todos.read"]).unwrap(),
            )
            .empty_response(StatusCode::OK, "Todos")
            .build()
            .unwrap();
        assert_eq!(
            OpenApiDocument::new("Todo API", "0.1.0")
                .unwrap()
                .security_scheme("bearerAuth", OpenApiSecurityScheme::http_bearer())
                .unwrap()
                .operation(
                    OpenApiRoute::from_rustee("/todos").unwrap(),
                    OpenApiMethod::Get,
                    scoped_bearer,
                )
                .unwrap_err(),
            OpenApiError::SecurityScopesNotAllowed
        );

        let scoped_mutual_tls = OpenApiOperation::builder("list_todos")
            .security_requirement(
                OpenApiSecurityRequirement::scoped("serviceTls", ["todos.read"]).unwrap(),
            )
            .empty_response(StatusCode::OK, "Todos")
            .build()
            .unwrap();
        assert_eq!(
            OpenApiDocument::new("Todo API", "0.1.0")
                .unwrap()
                .security_scheme("serviceTls", OpenApiSecurityScheme::mutual_tls())
                .unwrap()
                .operation(
                    OpenApiRoute::from_rustee("/todos").unwrap(),
                    OpenApiMethod::Get,
                    scoped_mutual_tls,
                )
                .unwrap_err(),
            OpenApiError::SecurityScopesNotAllowed
        );
    }

    #[test]
    fn security_scheme_validation_rejects_duplicate_or_unsafe_metadata() {
        assert!(
            OpenApiSecurityScheme::api_key("X-Tenant-Key", OpenApiApiKeyLocation::Header).is_ok()
        );
        assert_eq!(
            OpenApiSecurityScheme::api_key("x tenant key", OpenApiApiKeyLocation::Header)
                .unwrap_err(),
            OpenApiError::InvalidMetadata {
                field: "API key header name",
            }
        );
        let query_document = OpenApiDocument::new("Todo API", "0.1.0")
            .unwrap()
            .security_scheme(
                "queryKey",
                OpenApiSecurityScheme::api_key("tenant key", OpenApiApiKeyLocation::Query).unwrap(),
            )
            .unwrap()
            .to_value();
        assert_eq!(
            query_document["components"]["securitySchemes"]["queryKey"],
            json!({ "type": "apiKey", "name": "tenant key", "in": "query" })
        );
        assert!(
            OpenApiSecurityScheme::open_id_connect(
                "http://127.0.0.1:8080/.well-known/openid-configuration"
            )
            .is_ok()
        );
        assert_eq!(
            OpenApiSecurityRequirement::scheme("bearerAuth")
                .unwrap()
                .and_scheme("bearerAuth")
                .unwrap_err(),
            OpenApiError::DuplicateSecurityRequirement
        );
        assert_eq!(
            OpenApiSecurityScheme::open_id_connect("http://issuer.example/discovery").unwrap_err(),
            OpenApiError::InvalidSecuritySchemeUrl
        );
        assert_eq!(
            OpenApiSecurityScheme::open_id_connect("https://user:secret@issuer.example/discovery")
                .unwrap_err(),
            OpenApiError::InvalidSecuritySchemeUrl
        );
        assert_eq!(
            OpenApiSecurityScheme::open_id_connect("https://issuer.example/discovery?debug=1")
                .unwrap_err(),
            OpenApiError::InvalidSecuritySchemeUrl
        );
        assert_eq!(
            OpenApiSecurityScheme::open_id_connect("https://issuer.example/discovery#fragment")
                .unwrap_err(),
            OpenApiError::InvalidSecuritySchemeUrl
        );
        assert_eq!(
            OpenApiSecurityScheme::open_id_connect("https://").unwrap_err(),
            OpenApiError::InvalidSecuritySchemeUrl
        );
        assert_eq!(
            OpenApiDocument::new("Todo API", "0.1.0")
                .unwrap()
                .security_scheme("bearerAuth", OpenApiSecurityScheme::http_bearer())
                .unwrap()
                .security_scheme("bearerAuth", OpenApiSecurityScheme::http_basic())
                .unwrap_err(),
            OpenApiError::DuplicateSecurityScheme
        );
    }

    #[test]
    fn document_renders_oauth2_flows_and_global_operation_security_semantics() {
        let authorization_code = OpenApiOAuthFlow::authorization_code(
            "https://issuer.example/authorize",
            "https://issuer.example/token",
            [("todos.read", "Read todos"), ("todos.write", "Write todos")],
        )
        .unwrap()
        .with_refresh_url("https://issuer.example/refresh")
        .unwrap();
        let client_credentials = OpenApiOAuthFlow::client_credentials(
            "https://issuer.example/token",
            [("todos.read", "Read todos")],
        )
        .unwrap();
        let document = OpenApiDocument::new("Todo API", "0.1.0")
            .unwrap()
            .security_scheme(
                "oauth",
                OpenApiSecurityScheme::oauth2([authorization_code, client_credentials]).unwrap(),
            )
            .unwrap()
            .global_security_requirement(
                OpenApiSecurityRequirement::scoped("oauth", ["todos.read"]).unwrap(),
            )
            .unwrap()
            .operation(
                OpenApiRoute::from_rustee("/todos").unwrap(),
                OpenApiMethod::Get,
                OpenApiOperation::builder("list_todos")
                    .empty_response(StatusCode::OK, "Todos")
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .operation(
                OpenApiRoute::from_rustee("/session").unwrap(),
                OpenApiMethod::Post,
                OpenApiOperation::builder("start_session")
                    .clear_security_requirements()
                    .empty_response(StatusCode::NO_CONTENT, "Session started")
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .operation(
                OpenApiRoute::from_rustee("/invite").unwrap(),
                OpenApiMethod::Get,
                OpenApiOperation::builder("read_invite")
                    .anonymous_access()
                    .security_requirement(
                        OpenApiSecurityRequirement::scoped("oauth", ["todos.read"]).unwrap(),
                    )
                    .empty_response(StatusCode::OK, "Invite")
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .to_value();

        assert_eq!(
            document["components"]["securitySchemes"]["oauth"],
            json!({
                "type": "oauth2",
                "flows": {
                    "authorizationCode": {
                        "authorizationUrl": "https://issuer.example/authorize",
                        "tokenUrl": "https://issuer.example/token",
                        "refreshUrl": "https://issuer.example/refresh",
                        "scopes": {
                            "todos.read": "Read todos",
                            "todos.write": "Write todos",
                        },
                    },
                    "clientCredentials": {
                        "tokenUrl": "https://issuer.example/token",
                        "scopes": { "todos.read": "Read todos" },
                    },
                },
            })
        );
        assert_eq!(document["security"], json!([{ "oauth": ["todos.read"] }]));
        assert!(document["paths"]["/todos"]["get"].get("security").is_none());
        assert_eq!(document["paths"]["/session"]["post"]["security"], json!([]));
        assert_eq!(
            document["paths"]["/invite"]["get"]["security"],
            json!([{}, { "oauth": ["todos.read"] }])
        );
    }

    #[test]
    fn oauth2_security_metadata_rejects_unsafe_or_incompatible_flow_details() {
        assert_eq!(
            OpenApiSecurityScheme::oauth2(std::iter::empty::<OpenApiOAuthFlow>()).unwrap_err(),
            OpenApiError::MissingOAuthFlow
        );
        let flow = OpenApiOAuthFlow::client_credentials(
            "https://issuer.example/token",
            [("todos.read", "Read todos")],
        )
        .unwrap();
        assert_eq!(
            OpenApiSecurityScheme::oauth2([flow.clone(), flow.clone()]).unwrap_err(),
            OpenApiError::DuplicateOAuthFlow
        );
        assert_eq!(
            OpenApiOAuthFlow::client_credentials(
                "https://issuer.example/token",
                [("todos.read", "Read todos"), ("todos.read", "Read again")],
            )
            .unwrap_err(),
            OpenApiError::DuplicateOAuthScope
        );
        assert_eq!(
            OpenApiOAuthFlow::authorization_code(
                "http://issuer.example/authorize",
                "https://issuer.example/token",
                [("todos.read", "Read todos")],
            )
            .unwrap_err(),
            OpenApiError::InvalidSecuritySchemeUrl
        );

        let operation = OpenApiOperation::builder("list_todos")
            .security_requirement(
                OpenApiSecurityRequirement::scoped("oauth", ["todos.write"]).unwrap(),
            )
            .empty_response(StatusCode::OK, "Todos")
            .build()
            .unwrap();
        assert_eq!(
            OpenApiDocument::new("Todo API", "0.1.0")
                .unwrap()
                .security_scheme("oauth", OpenApiSecurityScheme::oauth2([flow]).unwrap())
                .unwrap()
                .operation(
                    OpenApiRoute::from_rustee("/todos").unwrap(),
                    OpenApiMethod::Get,
                    operation,
                )
                .unwrap_err(),
            OpenApiError::UnknownOAuthScope
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(96))]

        #[test]
        fn route_conversion_preserves_the_accepted_rustee_template_grammar(
            route in prop::collection::vec(any::<char>(), 0..128)
                .prop_map(|characters| characters.into_iter().collect::<String>()),
        ) {
            let result = OpenApiRoute::from_rustee(&route);
            if let Ok(openapi_route) = result {
                prop_assert!(route.starts_with('/'));
                let has_forbidden_character = route.contains('?')
                    || route.contains('#')
                    || route.contains('{')
                    || route.contains('}');
                prop_assert!(!has_forbidden_character);

                let mut parameter_names = BTreeSet::new();
                let mut rendered_segments = Vec::new();
                for segment in route
                    .trim_matches('/')
                    .split('/')
                    .filter(|segment| !segment.is_empty())
                {
                    if let Some(parameter) = segment.strip_prefix(':') {
                        prop_assert!(valid_route_parameter(parameter));
                        prop_assert!(parameter_names.insert(parameter.to_owned()));
                        rendered_segments.push(format!("{{{parameter}}}"));
                    } else {
                        rendered_segments.push(segment.to_owned());
                    }
                }
                let expected_path = if rendered_segments.is_empty() {
                    "/".to_owned()
                } else {
                    format!("/{}", rendered_segments.join("/"))
                };

                prop_assert_eq!(openapi_route.as_str(), expected_path);
                prop_assert_eq!(openapi_route.parameters, parameter_names);
            }
        }
    }
}
