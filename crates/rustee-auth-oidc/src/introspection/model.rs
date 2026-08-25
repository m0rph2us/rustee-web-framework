//! Opaque-token provider responses and claim-to-principal validation.

use std::{fmt, time::Duration};

use rustee_auth::{AuthError, Principal, constant_time_eq};
use serde::Deserialize;

use crate::claims::{ScopeClaim, StringSetClaim, normalize_verified_principal};

use super::{OpaqueIntrospectionConfig, unix_seconds};

/// A provider response for one opaque bearer credential.
///
/// The deserializer defaults absent `active` to `false`, so malformed successful HTTP responses
/// never become authenticated identities. Its [`Debug`] output intentionally reports only claim
/// presence, never identity or authorization values.
#[derive(Clone, Deserialize)]
pub struct OpaqueTokenIntrospection {
    #[serde(default)]
    active: bool,
    #[serde(default)]
    sub: Option<String>,
    #[serde(default)]
    iss: Option<String>,
    #[serde(default)]
    aud: Option<AudienceClaim>,
    #[serde(default)]
    exp: Option<u64>,
    #[serde(default)]
    nbf: Option<u64>,
    #[serde(default)]
    tenant: Option<String>,
    #[serde(default)]
    scope: Option<ScopeClaim>,
    #[serde(default)]
    roles: Option<StringSetClaim>,
    #[serde(default)]
    permissions: Option<StringSetClaim>,
}

impl fmt::Debug for OpaqueTokenIntrospection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpaqueTokenIntrospection")
            .field("active", &self.active)
            .field("has_subject", &self.sub.is_some())
            .field("has_issuer", &self.iss.is_some())
            .field("has_audience", &self.aud.is_some())
            .field("has_expiration", &self.exp.is_some())
            .field("has_not_before", &self.nbf.is_some())
            .field("has_tenant", &self.tenant.is_some())
            .field("has_scope", &self.scope.is_some())
            .field("has_roles", &self.roles.is_some())
            .field("has_permissions", &self.permissions.is_some())
            .finish()
    }
}

impl OpaqueTokenIntrospection {
    /// Creates an inactive response for custom-adapter tests.
    #[must_use]
    pub const fn inactive() -> Self {
        Self {
            active: false,
            sub: None,
            iss: None,
            aud: None,
            exp: None,
            nbf: None,
            tenant: None,
            scope: None,
            roles: None,
            permissions: None,
        }
    }

    /// Creates an active response with the three identity claims Rustee requires.
    #[must_use]
    pub fn active(
        subject: impl Into<String>,
        issuer: impl Into<String>,
        audience: impl Into<String>,
    ) -> Self {
        Self {
            active: true,
            sub: Some(subject.into()),
            iss: Some(issuer.into()),
            aud: Some(AudienceClaim::One(audience.into())),
            exp: None,
            nbf: None,
            tenant: None,
            scope: None,
            roles: None,
            permissions: None,
        }
    }

    /// Adds a provider expiration time expressed as Unix seconds.
    #[must_use]
    pub const fn with_expiration(mut self, expiration_unix_seconds: u64) -> Self {
        self.exp = Some(expiration_unix_seconds);
        self
    }

    /// Adds a provider not-before time expressed as Unix seconds.
    #[must_use]
    pub const fn with_not_before(mut self, not_before_unix_seconds: u64) -> Self {
        self.nbf = Some(not_before_unix_seconds);
        self
    }

    /// Adds a provider-confirmed tenant context.
    #[must_use]
    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    /// Adds OAuth scopes as a space-delimited string.
    #[must_use]
    pub fn with_scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(ScopeClaim::SpaceDelimited(scope.into()));
        self
    }

    /// Adds one provider-confirmed role.
    #[must_use]
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.roles = Some(StringSetClaim::One(role.into()));
        self
    }

    /// Adds one provider-confirmed direct permission.
    #[must_use]
    pub fn with_permission(mut self, permission: impl Into<String>) -> Self {
        self.permissions = Some(StringSetClaim::One(permission.into()));
        self
    }

    pub(super) fn validated_principal(
        self,
        config: &OpaqueIntrospectionConfig,
    ) -> Result<(Principal, Option<Duration>), AuthError> {
        if !self.active
            || self.sub.as_deref().is_none_or(str::is_empty)
            || !self
                .iss
                .as_deref()
                .is_some_and(|issuer| constant_time_eq(issuer.as_bytes(), config.issuer.as_bytes()))
            || !self
                .aud
                .as_ref()
                .is_some_and(|audience| audience.contains(&config.audience))
        {
            return Err(AuthError::RejectedBearerToken);
        }

        let now = unix_seconds();
        if self
            .exp
            .is_some_and(|expiration| expiration.saturating_add(config.leeway_seconds) <= now)
            || self
                .nbf
                .is_some_and(|not_before| not_before > now.saturating_add(config.leeway_seconds))
        {
            return Err(AuthError::RejectedBearerToken);
        }

        let (Some(subject), Some(issuer)) = (self.sub, self.iss) else {
            return Err(AuthError::RejectedBearerToken);
        };
        let principal = normalize_verified_principal(
            subject,
            issuer,
            self.tenant,
            self.scope,
            self.roles,
            self.permissions,
        )?;

        let cache_ttl = self.exp.and_then(|expiration| {
            let remaining = expiration.saturating_sub(now);
            let bounded = config.cache_ttl.min(Duration::from_secs(remaining));
            (!bounded.is_zero()).then_some(bounded)
        });
        Ok((principal, cache_ttl))
    }
}

#[derive(Clone, Deserialize)]
#[serde(untagged)]
enum AudienceClaim {
    One(String),
    Many(Vec<String>),
}

impl AudienceClaim {
    fn contains(&self, expected: &str) -> bool {
        match self {
            Self::One(audience) => constant_time_eq(audience.as_bytes(), expected.as_bytes()),
            Self::Many(audiences) => audiences
                .iter()
                .any(|audience| constant_time_eq(audience.as_bytes(), expected.as_bytes())),
        }
    }
}
