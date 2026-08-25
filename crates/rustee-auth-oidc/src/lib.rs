//! Remote JWKS-backed OIDC resource-server authentication.
//!
//! Tokens must carry a `kid`. Only signature-verification JWKs that explicitly declare the
//! configured asymmetric algorithm are cached. The verifier refreshes for a missing key and on
//! cache expiry, while a small refresh interval prevents untrusted `kid` values from causing a
//! fetch storm.

mod browser_login;
mod claims;
mod client_auth;
mod http;
mod introspection;
mod jwks;
mod resource_server_config;
mod trust;

pub use http::OidcHttpError;

pub use browser_login::{
    AuthorizationCallback, AuthorizationRedirect, AuthorizationTransactionStore,
    AuthorizationValueGenerator, HttpOidcDiscovery, HttpOidcTokenExchanger,
    InMemoryAuthorizationTransactionStore, InMemoryAuthorizationTransactionStoreError,
    OidcBrowserConfig, OidcBrowserConfigError, OidcBrowserLogin, OidcDiscovery, OidcLoginError,
    OidcLoginResult, OidcProviderMetadata, OidcTokenExchangeRequest, OidcTokenExchanger,
    OidcTokenResponse, PendingAuthorization, UuidAuthorizationValueGenerator,
};
pub use client_auth::{OidcClientAuthentication, OidcClientAuthenticationError, OidcClientSecret};
pub use introspection::{
    HttpOpaqueTokenIntrospector, OpaqueIntrospectionConfig, OpaqueIntrospectionConfigError,
    OpaqueTokenAuthenticator, OpaqueTokenIntrospection, OpaqueTokenIntrospectionRequest,
    OpaqueTokenIntrospector,
};
pub use jwks::{HttpJwksFetcher, IdTokenVerifier, JwksAuthenticator, JwksFetcher};
pub use resource_server_config::{OidcConfigError, OidcResourceServerConfig};

#[cfg(test)]
mod tests;
