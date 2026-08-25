//! Verified OIDC claim decoding and Rustee principal normalization.

use std::fmt;

use rustee_auth::{
    AuthError, MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES, Principal, constant_time_eq,
};
use rustee_core::are_valid_oauth_scope_tokens;
use serde::Deserialize;

/// Access-token claims accepted only after signature verification.
#[derive(Deserialize)]
pub(crate) struct Claims {
    sub: String,
    iss: String,
    #[serde(rename = "aud")]
    audience: serde_json::Value,
    #[serde(rename = "exp")]
    expiration: serde_json::Value,
    #[serde(rename = "nbf")]
    not_before: serde_json::Value,
    #[serde(default)]
    tenant: Option<String>,
    #[serde(default)]
    scope: Option<ScopeClaim>,
    #[serde(default)]
    roles: Option<StringSetClaim>,
    #[serde(default)]
    permissions: Option<StringSetClaim>,
}

impl fmt::Debug for Claims {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Claims")
            .field("has_subject", &!self.sub.is_empty())
            .field("has_issuer", &!self.iss.is_empty())
            .field("has_audience", &!self.audience.is_null())
            .field("has_expiration", &!self.expiration.is_null())
            .field("has_not_before", &!self.not_before.is_null())
            .field("has_tenant", &self.tenant.is_some())
            .field("has_scope", &self.scope.is_some())
            .field("has_roles", &self.roles.is_some())
            .field("has_permissions", &self.permissions.is_some())
            .finish()
    }
}

impl Claims {
    pub(crate) fn into_principal(self) -> Result<Principal, AuthError> {
        normalize_verified_principal(
            self.sub,
            self.iss,
            self.tenant,
            self.scope,
            self.roles,
            self.permissions,
        )
    }
}

/// ID-token claims accepted only after signature verification.
#[derive(Deserialize)]
pub(crate) struct IdTokenClaims {
    sub: String,
    iss: String,
    aud: serde_json::Value,
    #[serde(rename = "exp")]
    expiration: serde_json::Value,
    #[serde(default, rename = "nbf")]
    not_before: Option<serde_json::Value>,
    #[serde(rename = "iat")]
    issued_at: u64,
    nonce: String,
    #[serde(default, rename = "azp")]
    authorized_party: Option<String>,
    #[serde(default)]
    tenant: Option<String>,
    #[serde(default)]
    scope: Option<ScopeClaim>,
    #[serde(default)]
    roles: Option<StringSetClaim>,
    #[serde(default)]
    permissions: Option<StringSetClaim>,
}

impl fmt::Debug for IdTokenClaims {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IdTokenClaims")
            .field("has_subject", &!self.sub.is_empty())
            .field("has_issuer", &!self.iss.is_empty())
            .field("has_audience", &!self.aud.is_null())
            .field("has_expiration", &!self.expiration.is_null())
            .field("has_not_before", &self.not_before.is_some())
            .field("has_issued_at", &(self.issued_at != 0))
            .field("has_nonce", &!self.nonce.is_empty())
            .field("has_authorized_party", &self.authorized_party.is_some())
            .field("has_tenant", &self.tenant.is_some())
            .field("has_scope", &self.scope.is_some())
            .field("has_roles", &self.roles.is_some())
            .field("has_permissions", &self.permissions.is_some())
            .finish()
    }
}

impl IdTokenClaims {
    pub(crate) fn matches_browser_login_binding(
        &self,
        audience: &str,
        expected_nonce: &str,
        latest_issued_at: u64,
    ) -> bool {
        let has_multiple_audiences = self
            .aud
            .as_array()
            .is_some_and(|audiences| audiences.len() > 1);
        self.issued_at <= latest_issued_at
            && (!has_multiple_audiences
                || self
                    .authorized_party
                    .as_deref()
                    .is_some_and(|party| constant_time_eq(party.as_bytes(), audience.as_bytes())))
            && constant_time_eq(self.nonce.as_bytes(), expected_nonce.as_bytes())
    }

    pub(crate) fn into_principal(self) -> Result<Principal, AuthError> {
        normalize_verified_principal(
            self.sub,
            self.iss,
            self.tenant,
            self.scope,
            self.roles,
            self.permissions,
        )
    }
}

/// Applies the shared bounded authorization-claim contract to a verified identity.
pub(crate) fn normalize_verified_principal(
    subject: String,
    issuer: String,
    tenant: Option<String>,
    scope: Option<ScopeClaim>,
    roles: Option<StringSetClaim>,
    permissions: Option<StringSetClaim>,
) -> Result<Principal, AuthError> {
    let scopes = scope
        .map(ScopeClaim::into_scopes)
        .transpose()?
        .unwrap_or_default();
    Principal::from_verified_claims(
        subject,
        issuer,
        tenant,
        scopes,
        roles.into_iter().flat_map(StringSetClaim::into_values),
        permissions
            .into_iter()
            .flat_map(StringSetClaim::into_values),
    )
    .map_err(|_| AuthError::RejectedBearerToken)
}

#[derive(Clone, Deserialize)]
#[serde(untagged)]
pub(crate) enum ScopeClaim {
    SpaceDelimited(String),
    Values(Vec<String>),
}

impl ScopeClaim {
    pub(crate) fn into_scopes(self) -> Result<Vec<String>, AuthError> {
        let scopes = match self {
            Self::SpaceDelimited(scopes) => scopes.split(' ').map(ToOwned::to_owned).collect(),
            Self::Values(scopes) => scopes,
        };
        if !are_valid_oauth_scope_tokens(
            scopes.iter().map(String::as_str),
            MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES,
        ) {
            return Err(AuthError::RejectedBearerToken);
        }
        Ok(scopes)
    }
}

#[derive(Clone, Deserialize)]
#[serde(untagged)]
pub(crate) enum StringSetClaim {
    One(String),
    Values(Vec<String>),
}

impl StringSetClaim {
    pub(crate) fn into_values(self) -> Vec<String> {
        match self {
            Self::One(value) => vec![value],
            Self::Values(values) => values,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthError, ScopeClaim};

    #[test]
    fn scope_claims_require_rfc_6749_tokens_and_space_delimiting() {
        assert_eq!(
            ScopeClaim::SpaceDelimited("profile:read mcp:tools".to_owned())
                .into_scopes()
                .expect("space-delimited scope tokens must be accepted"),
            vec!["profile:read", "mcp:tools"]
        );
        assert_eq!(
            ScopeClaim::Values(vec!["profile:read".to_owned(), "mcp:tools".to_owned()])
                .into_scopes()
                .expect("array scope tokens must be accepted"),
            vec!["profile:read", "mcp:tools"]
        );

        for claim in [
            ScopeClaim::SpaceDelimited(String::new()),
            ScopeClaim::SpaceDelimited("profile:read\tmcp:tools".to_owned()),
            ScopeClaim::SpaceDelimited("profile:read  mcp:tools".to_owned()),
            ScopeClaim::Values(vec!["profile\"read".to_owned()]),
            ScopeClaim::Values(vec!["profile\\read".to_owned()]),
            ScopeClaim::Values(vec!["profile:\u{00e9}".to_owned()]),
            ScopeClaim::Values(Vec::new()),
        ] {
            assert_eq!(claim.into_scopes(), Err(AuthError::RejectedBearerToken));
        }
    }
}
