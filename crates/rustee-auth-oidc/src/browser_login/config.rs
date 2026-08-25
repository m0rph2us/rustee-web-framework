//! Trusted browser-login configuration.

use std::{collections::BTreeSet, fmt, time::Duration};

use rustee_core::is_valid_oauth_scope_token;
use url::Url;

use crate::{
    OidcClientAuthentication,
    client_auth::{basic_authorization_is_within_limit, valid_client_id},
    trust::valid_https_url as valid_trusted_https_url,
};

const DEFAULT_TRANSACTION_TTL: Duration = Duration::from_mins(10);
pub(crate) const MAX_TRANSACTION_TTL: Duration = Duration::from_hours(1);
pub(crate) const MAX_SCOPE_BYTES: usize = 256;
pub(crate) const MAX_SCOPES: usize = 32;

/// Configuration for one server-side OIDC browser client.
#[derive(Clone, Eq, PartialEq)]
pub struct OidcBrowserConfig {
    issuer: Url,
    client_id: String,
    redirect_uri: Url,
    jwks_url: Url,
    authentication: OidcClientAuthentication,
    scopes: BTreeSet<String>,
    transaction_ttl: Duration,
}

impl OidcBrowserConfig {
    /// Creates a browser-client configuration with the required `openid` scope.
    ///
    /// The configured issuer and endpoints are later compared exactly with discovery metadata.
    /// `jwks_url` binds the browser flow to the same endpoint configured for its ID-token
    /// verifier, rather than accepting a token-selected key endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`OidcBrowserConfigError`] for invalid client IDs or invalid HTTPS URLs.
    pub fn new(
        issuer: Url,
        client_id: impl Into<String>,
        redirect_uri: Url,
        jwks_url: Url,
        authentication: OidcClientAuthentication,
    ) -> Result<Self, OidcBrowserConfigError> {
        let client_id = client_id.into();
        if client_id.trim().is_empty() {
            return Err(OidcBrowserConfigError::BlankClientId);
        }
        if !valid_client_id(&client_id) {
            return Err(OidcBrowserConfigError::InvalidClientId);
        }
        if !basic_authorization_is_within_limit(&client_id, &authentication) {
            return Err(OidcBrowserConfigError::ClientAuthenticationTooLarge);
        }
        if !is_valid_issuer_url(&issuer) {
            return Err(OidcBrowserConfigError::InvalidIssuerUrl);
        }
        if !is_valid_https_url(&redirect_uri) {
            return Err(OidcBrowserConfigError::InvalidRedirectUri);
        }
        if !is_valid_https_url(&jwks_url) {
            return Err(OidcBrowserConfigError::InvalidJwksUrl);
        }
        Ok(Self {
            issuer,
            client_id,
            redirect_uri,
            jwks_url,
            authentication,
            scopes: BTreeSet::from(["openid".to_owned()]),
            transaction_ttl: DEFAULT_TRANSACTION_TTL,
        })
    }

    /// Adds one OIDC scope to the authorization request.
    ///
    /// # Errors
    ///
    /// Returns [`OidcBrowserConfigError::InvalidScope`] when a scope is blank, oversized, or not
    /// an RFC 6749 scope token, and [`OidcBrowserConfigError::TooManyScopes`] when the policy
    /// already contains the maximum number of distinct scopes. Call this separately for every
    /// requested scope.
    pub fn with_scope(mut self, scope: impl Into<String>) -> Result<Self, OidcBrowserConfigError> {
        let scope = scope.into();
        if !is_valid_oauth_scope_token(&scope, MAX_SCOPE_BYTES) {
            return Err(OidcBrowserConfigError::InvalidScope);
        }
        if !self.scopes.contains(&scope) && self.scopes.len() == MAX_SCOPES {
            return Err(OidcBrowserConfigError::TooManyScopes);
        }
        self.scopes.insert(scope);
        Ok(self)
    }

    /// Sets the maximum lifetime of a state/nonce/PKCE authorization transaction.
    ///
    /// # Errors
    ///
    /// Returns [`OidcBrowserConfigError::ZeroTransactionTtl`] for a sub-second or zero TTL,
    /// [`OidcBrowserConfigError::FractionalTransactionTtl`] when the value cannot be stored as an
    /// exact Unix-second expiry, or [`OidcBrowserConfigError::TransactionTtlTooLong`] when it
    /// would retain a one-time capability for more than one hour.
    pub fn with_transaction_ttl(
        mut self,
        transaction_ttl: Duration,
    ) -> Result<Self, OidcBrowserConfigError> {
        if transaction_ttl.as_secs() == 0 {
            return Err(OidcBrowserConfigError::ZeroTransactionTtl);
        }
        if transaction_ttl.subsec_nanos() != 0 {
            return Err(OidcBrowserConfigError::FractionalTransactionTtl);
        }
        if transaction_ttl > MAX_TRANSACTION_TTL {
            return Err(OidcBrowserConfigError::TransactionTtlTooLong);
        }
        self.transaction_ttl = transaction_ttl;
        Ok(self)
    }

    /// Returns the trusted issuer URL.
    #[must_use]
    pub const fn issuer(&self) -> &Url {
        &self.issuer
    }

    /// Returns the registered OAuth client ID.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the exact registered browser callback URL.
    #[must_use]
    pub const fn redirect_uri(&self) -> &Url {
        &self.redirect_uri
    }

    /// Returns the JWKS URL expected from the OIDC discovery response.
    #[must_use]
    pub const fn jwks_url(&self) -> &Url {
        &self.jwks_url
    }

    /// Returns client authentication used only at the trusted token endpoint.
    #[must_use]
    pub const fn authentication(&self) -> &OidcClientAuthentication {
        &self.authentication
    }

    /// Returns requested scopes in deterministic order.
    pub fn scopes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.scopes.iter().map(String::as_str)
    }

    /// Returns the maximum duration of a pending browser authorization transaction.
    #[must_use]
    pub const fn transaction_ttl(&self) -> Duration {
        self.transaction_ttl
    }
}

impl fmt::Debug for OidcBrowserConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OidcBrowserConfig")
            .field("issuer", &"[REDACTED]")
            .field("client_id", &"[REDACTED]")
            .field("redirect_uri", &"[REDACTED]")
            .field("jwks_url", &"[REDACTED]")
            .field("authentication", &self.authentication)
            .field("scope_count", &self.scopes.len())
            .field("transaction_ttl", &self.transaction_ttl)
            .finish()
    }
}

/// Invalid browser-client configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OidcBrowserConfigError {
    /// The client ID was blank.
    #[error("OIDC client ID must not be blank")]
    BlankClientId,
    /// The client ID exceeded the configured HTTP-safe size or contained control characters.
    #[error("OIDC client ID must be bounded and free of control characters")]
    InvalidClientId,
    /// Basic client credentials would exceed the supported HTTP header size.
    #[error("OIDC HTTP Basic client credentials exceed the supported header size")]
    ClientAuthenticationTooLarge,
    /// The configured issuer was not an absolute HTTPS issuer URL.
    #[error("OIDC issuer must be an absolute HTTPS URL without credentials, query, or fragment")]
    InvalidIssuerUrl,
    /// The registered redirect URI was invalid for a server-side browser flow.
    #[error("OIDC redirect URI must be an absolute HTTPS URL without credentials or a fragment")]
    InvalidRedirectUri,
    /// The expected JWKS URL was invalid.
    #[error("OIDC JWKS URL must be an absolute HTTPS URL without credentials or a fragment")]
    InvalidJwksUrl,
    /// A requested scope was not a bounded RFC 6749 scope token.
    #[error("OIDC scopes must be bounded RFC 6749 scope tokens")]
    InvalidScope,
    /// The authorization request would carry too many distinct scopes.
    #[error("OIDC browser login supports at most {MAX_SCOPES} distinct scopes")]
    TooManyScopes,
    /// A pending authorization transaction would expire immediately.
    #[error("OIDC authorization transaction TTL must be at least one second")]
    ZeroTransactionTtl,
    /// A pending authorization transaction must have an exact Unix-second lifetime.
    #[error("OIDC authorization transaction TTL must be a whole number of seconds")]
    FractionalTransactionTtl,
    /// A one-time authorization capability must not remain valid for more than one hour.
    #[error("OIDC authorization transaction TTL must not exceed one hour")]
    TransactionTtlTooLong,
    /// An HTTP timeout was zero.
    #[error("OIDC HTTP timeout must be greater than zero")]
    ZeroHttpTimeout,
    /// A HTTP client could not be constructed.
    #[error("OIDC HTTP client could not be initialized")]
    HttpClientInitialization,
}

pub(super) fn is_valid_issuer_url(url: &Url) -> bool {
    is_valid_https_url(url) && url.query().is_none()
}

pub(super) fn is_valid_https_url(url: &Url) -> bool {
    valid_trusted_https_url(url)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use url::Url;

    use crate::{
        OidcClientAuthentication, OidcClientAuthenticationError, OidcClientSecret,
        client_auth::{MAX_CLIENT_ID_BYTES, MAX_CLIENT_SECRET_BYTES},
    };

    use super::{
        MAX_SCOPE_BYTES, MAX_SCOPES, MAX_TRANSACTION_TTL, OidcBrowserConfig, OidcBrowserConfigError,
    };

    #[test]
    fn browser_configuration_bounds_redirect_contributors() {
        let issuer = Url::parse("https://issuer.example.test").expect("test issuer URL must parse");
        let redirect = Url::parse("https://app.example.test/auth/callback")
            .expect("test redirect URL must parse");
        let jwks =
            Url::parse("https://issuer.example.test/keys").expect("test JWKS URL must parse");

        assert_eq!(
            OidcBrowserConfig::new(
                issuer.clone(),
                "c".repeat(MAX_CLIENT_ID_BYTES + 1),
                redirect.clone(),
                jwks.clone(),
                OidcClientAuthentication::None,
            ),
            Err(OidcBrowserConfigError::InvalidClientId)
        );
        assert_eq!(
            OidcBrowserConfig::new(
                issuer.clone(),
                "client",
                Url::parse(&format!(
                    "https://app.example.test/{}",
                    "p".repeat(2 * 1024)
                ))
                .expect("oversized redirect URL must parse"),
                jwks,
                OidcClientAuthentication::None,
            ),
            Err(OidcBrowserConfigError::InvalidRedirectUri)
        );

        let config = OidcBrowserConfig::new(
            issuer,
            "client",
            redirect,
            Url::parse("https://issuer.example.test/keys").expect("test JWKS URL must parse"),
            OidcClientAuthentication::None,
        )
        .expect("baseline configuration must be valid");
        assert_eq!(
            config
                .clone()
                .with_transaction_ttl(Duration::from_secs(1) + Duration::from_nanos(1)),
            Err(OidcBrowserConfigError::FractionalTransactionTtl)
        );
        assert_eq!(
            config
                .clone()
                .with_transaction_ttl(MAX_TRANSACTION_TTL + Duration::from_secs(1)),
            Err(OidcBrowserConfigError::TransactionTtlTooLong)
        );
        assert_eq!(
            config.clone().with_scope("s".repeat(MAX_SCOPE_BYTES + 1)),
            Err(OidcBrowserConfigError::InvalidScope)
        );
        for scope in ["profile\"read", "profile\\read", "profile:\u{00e9}"] {
            assert_eq!(
                config.clone().with_scope(scope),
                Err(OidcBrowserConfigError::InvalidScope)
            );
        }

        let mut full = config;
        for index in 0..(MAX_SCOPES - 1) {
            full = full
                .with_scope(format!("scope-{index}"))
                .expect("bounded distinct scope must be accepted");
        }
        assert_eq!(
            full.with_scope("one-too-many"),
            Err(OidcBrowserConfigError::TooManyScopes)
        );
    }

    #[test]
    fn confidential_client_credentials_are_bounded_before_token_exchange() {
        assert_eq!(
            OidcClientSecret::new("s".repeat(MAX_CLIENT_SECRET_BYTES + 1)),
            Err(OidcClientAuthenticationError::InvalidSecret)
        );

        let secret = OidcClientSecret::new("!".repeat(MAX_CLIENT_SECRET_BYTES))
            .expect("bounded secret must be accepted independently");
        assert_eq!(
            OidcBrowserConfig::new(
                Url::parse("https://issuer.example.test").expect("test issuer URL must parse"),
                "client",
                Url::parse("https://app.example.test/auth/callback")
                    .expect("test redirect URL must parse"),
                Url::parse("https://issuer.example.test/keys").expect("test JWKS URL must parse"),
                OidcClientAuthentication::ClientSecretBasic(secret.clone()),
            ),
            Err(OidcBrowserConfigError::ClientAuthenticationTooLarge)
        );
        assert!(
            OidcBrowserConfig::new(
                Url::parse("https://issuer.example.test").expect("test issuer URL must parse"),
                "client",
                Url::parse("https://app.example.test/auth/callback")
                    .expect("test redirect URL must parse"),
                Url::parse("https://issuer.example.test/keys").expect("test JWKS URL must parse"),
                OidcClientAuthentication::ClientSecretPost(secret),
            )
            .is_ok()
        );
    }
}
