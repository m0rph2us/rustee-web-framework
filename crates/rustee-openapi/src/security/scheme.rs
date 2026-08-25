//! Security-scheme declarations and `OpenAPI` rendering.

use std::collections::BTreeSet;

use serde_json::{Map, Value, json};
use url::{Host, Url};

use super::super::{MAX_METADATA_CHARS, OpenApiError, validate_metadata};

mod oauth;

pub use oauth::OpenApiOAuthFlow;

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

    pub(crate) fn validate_required_scopes(
        &self,
        scopes: &[String],
    ) -> std::result::Result<(), OpenApiError> {
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

    pub(crate) fn to_value(&self) -> Value {
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

pub(super) fn validate_scope(scope: &str) -> std::result::Result<(), OpenApiError> {
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
