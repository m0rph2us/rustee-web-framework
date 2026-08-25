//! `OpenAPI` security metadata, validation, and rendering.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use super::{OpenApiError, validate_identifier};

mod scheme;

pub use scheme::{OpenApiApiKeyLocation, OpenApiOAuthFlow, OpenApiSecurityScheme};

use scheme::validate_scope;

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
    /// The operation is not connected to runtime authentication. [`crate::OpenApiDocument::operation`]
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

    pub(crate) fn schemes(&self) -> &BTreeMap<String, Vec<String>> {
        &self.schemes
    }

    pub(crate) fn to_value(&self) -> Value {
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
