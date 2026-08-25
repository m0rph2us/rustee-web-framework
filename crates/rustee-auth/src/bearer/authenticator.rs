//! Bearer verifier contracts and a duplicate-rejecting local test authenticator.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    fmt,
};

use futures_util::future::BoxFuture;

use crate::Principal;

use super::token::{MAX_BEARER_TOKEN_BYTES, is_valid_bearer_token};

/// A failure that is safe to render as an authentication rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AuthError {
    /// No bearer credential was supplied.
    #[error("missing bearer token")]
    MissingBearerToken,
    /// The authorization header did not contain one well-formed bearer credential.
    #[error("invalid bearer token")]
    InvalidBearerToken,
    /// A provider rejected a syntactically valid bearer credential.
    #[error("bearer token was rejected")]
    RejectedBearerToken,
    /// Required authentication infrastructure could not be reached safely.
    #[error("authentication provider is unavailable")]
    ProviderUnavailable,
}

/// A provider-specific verifier of a syntactically valid, bounded bearer credential.
pub trait BearerAuthenticator: Clone + Send + Sync + 'static {
    /// Verifies a raw bearer credential and returns only a validated principal.
    fn authenticate(&self, token: &str) -> BoxFuture<'static, Result<Principal, AuthError>>;
}

/// A deliberately simple static authenticator intended for tests and local examples only.
#[derive(Clone, Default)]
pub struct StaticTokenAuthenticator {
    tokens: BTreeMap<String, Principal>,
}

impl StaticTokenAuthenticator {
    /// Creates an empty authenticator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one local token-to-principal mapping.
    ///
    /// # Errors
    ///
    /// Returns [`StaticTokenError::BlankToken`] when `token` is blank,
    /// [`StaticTokenError::InvalidToken`] when it cannot be presented as a valid RFC 6750 Bearer
    /// credential, or [`StaticTokenError::DuplicateToken`] when `token` is already registered.
    pub fn insert(
        &mut self,
        token: impl Into<String>,
        principal: Principal,
    ) -> Result<(), StaticTokenError> {
        let token = token.into();
        if token.trim().is_empty() {
            return Err(StaticTokenError::BlankToken);
        }
        if !is_valid_bearer_token(&token) {
            return Err(StaticTokenError::InvalidToken);
        }
        match self.tokens.entry(token) {
            Entry::Vacant(entry) => {
                entry.insert(principal);
                Ok(())
            }
            Entry::Occupied(_) => Err(StaticTokenError::DuplicateToken),
        }
    }
}

impl fmt::Debug for StaticTokenAuthenticator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StaticTokenAuthenticator")
            .field("registered_tokens", &self.tokens.len())
            .finish()
    }
}

impl BearerAuthenticator for StaticTokenAuthenticator {
    fn authenticate(&self, token: &str) -> BoxFuture<'static, Result<Principal, AuthError>> {
        let principal = self.tokens.get(token).cloned();
        Box::pin(async move { principal.ok_or(AuthError::RejectedBearerToken) })
    }
}

/// Invalid static authenticator configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StaticTokenError {
    /// A local static token was blank.
    #[error("static bearer token must not be blank")]
    BlankToken,
    /// A local static token cannot be presented as a bounded Bearer credential.
    #[error(
        "static bearer token must be an RFC 6750 b64token and at most {MAX_BEARER_TOKEN_BYTES} bytes"
    )]
    InvalidToken,
    /// A token already maps to one local principal.
    #[error("static bearer token is already registered")]
    DuplicateToken,
}
