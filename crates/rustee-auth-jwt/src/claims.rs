//! Verified JWT claim decoding and normalization into Rustee principals.

use std::fmt;

use rustee_auth::{AuthError, MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES, Principal};
use rustee_core::are_valid_oauth_scope_tokens;
use serde::Deserialize;

/// Verified JWT claims before they are normalized into a [`Principal`].
///
/// Debug output reports only which claim categories were present, never claim values.
#[derive(Deserialize)]
pub(crate) struct VerifiedClaims {
    pub(crate) sub: String,
    pub(crate) iss: String,
    #[serde(rename = "aud")]
    pub(crate) audience_claim: serde_json::Value,
    #[serde(rename = "exp")]
    pub(crate) expiration_claim: serde_json::Value,
    #[serde(rename = "nbf")]
    pub(crate) not_before_claim: serde_json::Value,
    #[serde(default)]
    pub(crate) tenant: Option<String>,
    #[serde(default)]
    pub(crate) scope: Option<ScopeClaim>,
    #[serde(default)]
    pub(crate) roles: Option<StringSetClaim>,
    #[serde(default)]
    pub(crate) permissions: Option<StringSetClaim>,
}

impl fmt::Debug for VerifiedClaims {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedClaims")
            .field("has_subject", &!self.sub.is_empty())
            .field("has_issuer", &!self.iss.is_empty())
            .field("has_audience", &!self.audience_claim.is_null())
            .field("has_expiration", &!self.expiration_claim.is_null())
            .field("has_not_before", &!self.not_before_claim.is_null())
            .field("has_tenant", &self.tenant.is_some())
            .field("has_scope", &self.scope.is_some())
            .field("has_roles", &self.roles.is_some())
            .field("has_permissions", &self.permissions.is_some())
            .finish()
    }
}

impl VerifiedClaims {
    pub(crate) fn into_principal(self) -> Result<Principal, AuthError> {
        let scopes = self
            .scope
            .map(ScopeClaim::into_scopes)
            .transpose()?
            .unwrap_or_default();
        Principal::from_verified_claims(
            self.sub,
            self.iss,
            self.tenant,
            scopes,
            self.roles.into_iter().flat_map(StringSetClaim::into_values),
            self.permissions
                .into_iter()
                .flat_map(StringSetClaim::into_values),
        )
        .map_err(|_| AuthError::RejectedBearerToken)
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
pub(crate) enum ScopeClaim {
    SpaceDelimited(String),
    Values(Vec<String>),
}

impl ScopeClaim {
    fn into_scopes(self) -> Result<Vec<String>, AuthError> {
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

#[derive(Deserialize)]
#[serde(untagged)]
pub(crate) enum StringSetClaim {
    One(String),
    Values(Vec<String>),
}

impl StringSetClaim {
    fn into_values(self) -> Vec<String> {
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
