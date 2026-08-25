//! OIDC Authorization Code + PKCE browser-login protocol support.
//!
//! This module deliberately stops at a verified [`Principal`]. Applications establish their own
//! Rustee server-side session afterwards, so OAuth tokens never need to be stored in browser
//! cookies or passed to ordinary handlers.

use std::{
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use http::StatusCode;
use rustee_auth::{MAX_BEARER_TOKEN_BYTES, Principal};
use rustee_core::{Error as HttpError, IntoResponse, Response};
use sha2::{Digest, Sha256};

const MAX_AUTHORIZATION_CODE_BYTES: usize = 16 * 1024;
const MAX_AUTHORIZATION_REDIRECT_BYTES: usize = 8 * 1024;
const MAX_ID_TOKEN_BYTES: usize = MAX_BEARER_TOKEN_BYTES;
const MAX_PROVIDER_ERROR_BYTES: usize = 256;

mod config;
mod flow;
mod transaction;
mod transport;

#[cfg(test)]
pub(crate) use config::{MAX_SCOPE_BYTES, MAX_SCOPES};
pub use config::{OidcBrowserConfig, OidcBrowserConfigError};
pub use transaction::{
    AuthorizationTransactionStore, AuthorizationValueGenerator,
    InMemoryAuthorizationTransactionStore, InMemoryAuthorizationTransactionStoreError,
    PendingAuthorization, UuidAuthorizationValueGenerator,
};
pub use transport::{
    AuthorizationCallback, AuthorizationRedirect, HttpOidcDiscovery, HttpOidcTokenExchanger,
    OidcDiscovery, OidcProviderMetadata, OidcTokenExchangeRequest, OidcTokenExchanger,
    OidcTokenResponse,
};

/// Sanitized browser-login failures that are safe to map to application HTTP responses.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OidcLoginError {
    /// The transaction store failed.
    #[error("OIDC authorization transaction service is unavailable")]
    TransactionStoreUnavailable,
    /// Discovery could not be retrieved.
    #[error("OIDC discovery service is unavailable")]
    DiscoveryUnavailable,
    /// Discovery metadata did not match the configured trusted provider.
    #[error("OIDC discovery metadata was rejected")]
    InvalidProviderMetadata,
    /// No matching transaction existed for the returned state.
    #[error("OIDC authorization state was rejected")]
    StateRejected,
    /// The authorization callback arrived after its server-side transaction expired.
    #[error("OIDC authorization transaction expired")]
    TransactionExpired,
    /// Callback content was incomplete or malformed.
    #[error("OIDC authorization callback was rejected")]
    CallbackRejected,
    /// The identity provider denied authorization.
    #[error("OIDC authorization was rejected by the provider")]
    ProviderRejected,
    /// The token endpoint could not be reached or accepted no request.
    #[error("OIDC token endpoint is unavailable")]
    TokenExchangeUnavailable,
    /// The token response was missing an ID token.
    #[error("OIDC token response was rejected")]
    MissingIdToken,
    /// The ID-token verifier could not reach trusted key infrastructure.
    #[error("OIDC identity verification service is unavailable")]
    IdentityProviderUnavailable,
    /// The durable browser session could not be created after successful OIDC verification.
    #[error("OIDC browser session service is unavailable")]
    SessionUnavailable,
    /// The ID token or its transaction-bound nonce was rejected.
    #[error("OIDC identity token was rejected")]
    IdentityTokenRejected,
}

impl OidcLoginError {
    /// Returns whether the failed dependency can be retried by starting a new login transaction.
    #[must_use]
    pub const fn is_provider_unavailable(self) -> bool {
        matches!(
            self,
            Self::TransactionStoreUnavailable
                | Self::DiscoveryUnavailable
                | Self::TokenExchangeUnavailable
                | Self::IdentityProviderUnavailable
                | Self::SessionUnavailable
        )
    }
}

impl IntoResponse for OidcLoginError {
    fn into_response(self) -> Response {
        let (status, code, message) = if self.is_provider_unavailable() {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "oidc_unavailable",
                "OIDC login service is unavailable",
            )
        } else if matches!(self, Self::InvalidProviderMetadata) {
            (
                StatusCode::BAD_GATEWAY,
                "oidc_provider_rejected",
                "OIDC provider metadata was rejected",
            )
        } else {
            (
                StatusCode::BAD_REQUEST,
                "oidc_login_rejected",
                "OIDC login request was rejected",
            )
        };
        HttpError::new(status, code, message).into_response()
    }
}

/// A completed OIDC login containing only a verified application principal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OidcLoginResult {
    principal: Principal,
}

impl OidcLoginResult {
    /// Returns the verified principal suitable for `SessionManager::establish`.
    #[must_use]
    pub fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Consumes the result and returns its verified principal.
    #[must_use]
    pub fn into_principal(self) -> Principal {
        self.principal
    }
}

/// OIDC Authorization Code + PKCE browser-login orchestrator.
#[derive(Clone)]
pub struct OidcBrowserLogin<S, D, E, V, G> {
    config: OidcBrowserConfig,
    transactions: S,
    discovery: D,
    exchanger: E,
    verifier: V,
    generator: G,
}

impl<S, D, E, V, G> fmt::Debug for OidcBrowserLogin<S, D, E, V, G> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OidcBrowserLogin")
            .field("config", &self.config)
            .field("transaction_store", &std::any::type_name::<S>())
            .field("discovery", &std::any::type_name::<D>())
            .field("exchanger", &std::any::type_name::<E>())
            .field("verifier", &std::any::type_name::<V>())
            .finish_non_exhaustive()
    }
}

fn pkce_challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
#[path = "browser_login/tests.rs"]
mod tests;
