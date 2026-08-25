//! OAuth flow declarations, metadata validation, and JSON rendering.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use super::super::super::{OpenApiError, validate_metadata};
use super::{validate_scope, validate_security_scheme_url};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum OpenApiOAuthFlowKind {
    AuthorizationCode,
    ClientCredentials,
}

impl OpenApiOAuthFlowKind {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::AuthorizationCode => "authorizationCode",
            Self::ClientCredentials => "clientCredentials",
        }
    }
}

/// One explicit OAuth 2.0 flow documented by an [`crate::OpenApiSecurityScheme`].
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

    pub(super) const fn kind(&self) -> OpenApiOAuthFlowKind {
        self.kind
    }

    pub(super) fn supports_scope(&self, scope: &str) -> bool {
        self.scopes.contains_key(scope)
    }

    pub(super) fn to_value(&self) -> Value {
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
