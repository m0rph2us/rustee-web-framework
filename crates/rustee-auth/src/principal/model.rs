//! Provider-neutral principal data, serde restoration, and claim normalization.

use std::{collections::BTreeSet, fmt};

use super::admission::{PrincipalError, insert_authorization_value, validate_identifier};

/// A validated identity made available to application handlers.
///
/// [`fmt::Debug`] redacts identity values while retaining only authorization-set cardinalities.
#[derive(Clone, Eq, PartialEq, serde::Serialize)]
pub struct Principal {
    subject: String,
    issuer: Option<String>,
    tenant: Option<String>,
    scopes: BTreeSet<String>,
    #[serde(default)]
    roles: BTreeSet<String>,
    #[serde(default)]
    permissions: BTreeSet<String>,
}

#[derive(serde::Deserialize)]
struct SerializedPrincipal {
    subject: String,
    #[serde(default)]
    issuer: Option<String>,
    #[serde(default)]
    tenant: Option<String>,
    scopes: BTreeSet<String>,
    #[serde(default)]
    roles: BTreeSet<String>,
    #[serde(default)]
    permissions: BTreeSet<String>,
}

impl fmt::Debug for Principal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Principal")
            .field("subject", &"[REDACTED]")
            .field("issuer", &self.issuer.as_ref().map(|_| "[REDACTED]"))
            .field("tenant", &self.tenant.as_ref().map(|_| "[REDACTED]"))
            .field("scope_count", &self.scopes.len())
            .field("role_count", &self.roles.len())
            .field("permission_count", &self.permissions.len())
            .finish()
    }
}

impl<'de> serde::Deserialize<'de> for Principal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serialized = SerializedPrincipal::deserialize(deserializer)?;
        Self::from_serialized(serialized).map_err(serde::de::Error::custom)
    }
}

impl Principal {
    /// Creates a principal with a non-blank subject identifier.
    ///
    /// # Errors
    ///
    /// Returns [`PrincipalError::BlankField`] when `subject` is blank or
    /// [`PrincipalError::ValueTooLong`] when it exceeds
    /// [`MAX_PRINCIPAL_IDENTIFIER_BYTES`](crate::MAX_PRINCIPAL_IDENTIFIER_BYTES).
    pub fn new(subject: impl Into<String>) -> Result<Self, PrincipalError> {
        let subject = subject.into();
        validate_identifier(&subject, "subject")?;
        Ok(Self {
            subject,
            issuer: None,
            tenant: None,
            scopes: BTreeSet::new(),
            roles: BTreeSet::new(),
            permissions: BTreeSet::new(),
        })
    }

    /// Adds the issuer that validated this principal.
    ///
    /// # Errors
    ///
    /// Returns [`PrincipalError::BlankField`] when `issuer` is blank or
    /// [`PrincipalError::ValueTooLong`] when it exceeds
    /// [`MAX_PRINCIPAL_IDENTIFIER_BYTES`](crate::MAX_PRINCIPAL_IDENTIFIER_BYTES).
    pub fn with_issuer(mut self, issuer: impl Into<String>) -> Result<Self, PrincipalError> {
        let issuer = issuer.into();
        validate_identifier(&issuer, "issuer")?;
        self.issuer = Some(issuer);
        Ok(self)
    }

    /// Adds the verified tenant context for this principal.
    ///
    /// # Errors
    ///
    /// Returns [`PrincipalError::BlankField`] when `tenant` is blank or
    /// [`PrincipalError::ValueTooLong`] when it exceeds
    /// [`MAX_PRINCIPAL_IDENTIFIER_BYTES`](crate::MAX_PRINCIPAL_IDENTIFIER_BYTES).
    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Result<Self, PrincipalError> {
        let tenant = tenant.into();
        validate_identifier(&tenant, "tenant")?;
        self.tenant = Some(tenant);
        Ok(self)
    }

    /// Adds a verified scope.
    ///
    /// # Errors
    ///
    /// Returns [`PrincipalError::BlankField`] when `scope` is blank,
    /// [`PrincipalError::ValueTooLong`] when it exceeds
    /// [`MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES`](crate::MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES), or
    /// [`PrincipalError::TooManyValues`] when it would exceed
    /// [`MAX_PRINCIPAL_AUTHORIZATION_VALUES`](crate::MAX_PRINCIPAL_AUTHORIZATION_VALUES) distinct
    /// scopes.
    pub fn with_scope(mut self, scope: impl Into<String>) -> Result<Self, PrincipalError> {
        let scope = scope.into();
        insert_authorization_value(&mut self.scopes, scope, "scopes")?;
        Ok(self)
    }

    /// Adds a role supplied by a trusted identity verifier or server-side identity mapping.
    ///
    /// # Errors
    ///
    /// Returns [`PrincipalError::BlankField`] when `role` is blank,
    /// [`PrincipalError::ValueTooLong`] when it exceeds
    /// [`MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES`](crate::MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES), or
    /// [`PrincipalError::TooManyValues`] when it would exceed
    /// [`MAX_PRINCIPAL_AUTHORIZATION_VALUES`](crate::MAX_PRINCIPAL_AUTHORIZATION_VALUES) distinct
    /// roles.
    pub fn with_role(mut self, role: impl Into<String>) -> Result<Self, PrincipalError> {
        let role = role.into();
        insert_authorization_value(&mut self.roles, role, "roles")?;
        Ok(self)
    }

    /// Adds a direct permission supplied by a trusted identity verifier or server-side mapping.
    ///
    /// # Errors
    ///
    /// Returns [`PrincipalError::BlankField`] when `permission` is blank,
    /// [`PrincipalError::ValueTooLong`] when it exceeds
    /// [`MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES`](crate::MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES), or
    /// [`PrincipalError::TooManyValues`] when it would exceed
    /// [`MAX_PRINCIPAL_AUTHORIZATION_VALUES`](crate::MAX_PRINCIPAL_AUTHORIZATION_VALUES) distinct
    /// permissions.
    pub fn with_permission(
        mut self,
        permission: impl Into<String>,
    ) -> Result<Self, PrincipalError> {
        let permission = permission.into();
        insert_authorization_value(&mut self.permissions, permission, "permissions")?;
        Ok(self)
    }

    /// Normalizes provider-verified identity and authorization claims into a bounded principal.
    ///
    /// Callers must validate the credential, issuer, audience, and time constraints before using
    /// this helper. It validates the identity values and applies the same limits to every supplied
    /// authorization value as the individual builder methods.
    ///
    /// # Errors
    ///
    /// Returns [`PrincipalError`] when any supplied identity or authorization value is invalid or
    /// exceeds its fixed limit.
    pub fn from_verified_claims(
        subject: String,
        issuer: String,
        tenant: Option<String>,
        scopes: impl IntoIterator<Item = String>,
        roles: impl IntoIterator<Item = String>,
        permissions: impl IntoIterator<Item = String>,
    ) -> Result<Self, PrincipalError> {
        let mut principal = Self::new(subject)?.with_issuer(issuer)?;
        if let Some(tenant) = tenant {
            principal = principal.with_tenant(tenant)?;
        }
        for scope in scopes {
            principal = principal.with_scope(scope)?;
        }
        for role in roles {
            principal = principal.with_role(role)?;
        }
        for permission in permissions {
            principal = principal.with_permission(permission)?;
        }
        Ok(principal)
    }

    /// Returns the authenticated subject identifier.
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Returns the issuer when the verifier provided one.
    #[must_use]
    pub fn issuer(&self) -> Option<&str> {
        self.issuer.as_deref()
    }

    /// Returns the verified tenant when the verifier provided one.
    #[must_use]
    pub fn tenant(&self) -> Option<&str> {
        self.tenant.as_deref()
    }

    /// Returns the verified scopes in deterministic order.
    pub fn scopes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.scopes.iter().map(String::as_str)
    }

    /// Returns whether this principal includes the supplied scope.
    #[must_use]
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.contains(scope)
    }

    /// Returns verified roles in deterministic order.
    pub fn roles(&self) -> impl ExactSizeIterator<Item = &str> {
        self.roles.iter().map(String::as_str)
    }

    /// Returns whether the principal has one verified role.
    #[must_use]
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.contains(role)
    }

    /// Returns direct verified permissions in deterministic order.
    pub fn permissions(&self) -> impl ExactSizeIterator<Item = &str> {
        self.permissions.iter().map(String::as_str)
    }

    /// Returns whether the principal has one direct verified permission.
    #[must_use]
    pub fn has_permission(&self, permission: &str) -> bool {
        self.permissions.contains(permission)
    }

    fn from_serialized(serialized: SerializedPrincipal) -> Result<Self, PrincipalError> {
        let mut principal = Self::new(serialized.subject)?;
        if let Some(issuer) = serialized.issuer {
            principal = principal.with_issuer(issuer)?;
        }
        if let Some(tenant) = serialized.tenant {
            principal = principal.with_tenant(tenant)?;
        }
        for scope in serialized.scopes {
            principal = principal.with_scope(scope)?;
        }
        for role in serialized.roles {
            principal = principal.with_role(role)?;
        }
        for permission in serialized.permissions {
            principal = principal.with_permission(permission)?;
        }
        Ok(principal)
    }
}
