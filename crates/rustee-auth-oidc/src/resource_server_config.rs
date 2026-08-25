//! Trusted resource-server settings for JWKS-backed OIDC token verification.

use std::{fmt, time::Duration};

use jsonwebtoken::{Algorithm, Validation};
use url::Url;

use crate::trust::{
    MAX_AUDIENCE_BYTES, MAX_ISSUER_BYTES, MAX_LEEWAY_SECONDS,
    valid_https_url as valid_trusted_https_url, valid_text,
};

const DEFAULT_CACHE_TTL: Duration = Duration::from_mins(5);
const DEFAULT_MINIMUM_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// OIDC resource-server settings for one trusted issuer and JWKS endpoint.
#[derive(Clone, Eq, PartialEq)]
pub struct OidcResourceServerConfig {
    algorithm: Algorithm,
    issuer: String,
    audience: String,
    jwks_url: Url,
    leeway_seconds: u64,
    cache_ttl: Duration,
    minimum_refresh_interval: Duration,
}

impl fmt::Debug for OidcResourceServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OidcResourceServerConfig")
            .field("algorithm", &self.algorithm)
            .field("issuer", &"[REDACTED]")
            .field("audience", &"[REDACTED]")
            .field("jwks_url", &"[REDACTED]")
            .field("leeway_seconds", &self.leeway_seconds)
            .field("cache_ttl", &self.cache_ttl)
            .field("minimum_refresh_interval", &self.minimum_refresh_interval)
            .finish()
    }
}

impl OidcResourceServerConfig {
    /// Creates settings that accept one asymmetric JWT algorithm from an HTTPS JWKS endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`OidcConfigError`] for invalid issuer/audience values, invalid JWKS URLs, or
    /// symmetric algorithms that must not be trusted from a remote OIDC key set.
    pub fn new(
        algorithm: Algorithm,
        issuer: impl Into<String>,
        audience: impl Into<String>,
        jwks_url: Url,
    ) -> Result<Self, OidcConfigError> {
        let issuer = issuer.into();
        let audience = audience.into();
        if issuer.trim().is_empty() || audience.trim().is_empty() {
            return Err(OidcConfigError::BlankField);
        }
        if !valid_text(&issuer, MAX_ISSUER_BYTES) || !valid_text(&audience, MAX_AUDIENCE_BYTES) {
            return Err(OidcConfigError::InvalidField);
        }
        if !is_asymmetric(algorithm) {
            return Err(OidcConfigError::SymmetricAlgorithm);
        }
        if !is_valid_jwks_url(&jwks_url) {
            return Err(OidcConfigError::InvalidJwksUrl);
        }
        Ok(Self {
            algorithm,
            issuer,
            audience,
            jwks_url,
            leeway_seconds: 0,
            cache_ttl: DEFAULT_CACHE_TTL,
            minimum_refresh_interval: DEFAULT_MINIMUM_REFRESH_INTERVAL,
        })
    }

    /// Allows a bounded clock skew for expiry and not-before validation.
    ///
    /// # Errors
    ///
    /// Returns [`OidcConfigError::LeewayTooLarge`] when the requested leeway exceeds five
    /// minutes.
    pub fn with_leeway_seconds(mut self, leeway_seconds: u64) -> Result<Self, OidcConfigError> {
        if leeway_seconds > MAX_LEEWAY_SECONDS {
            return Err(OidcConfigError::LeewayTooLarge);
        }
        self.leeway_seconds = leeway_seconds;
        Ok(self)
    }

    /// Sets how long a successful JWKS response can be used before it must be refreshed.
    ///
    /// A zero duration is useful for deterministic tests, but production callers should retain a
    /// bounded cache duration to avoid making every token validation depend on the identity
    /// provider.
    #[must_use]
    pub const fn with_cache_ttl(mut self, cache_ttl: Duration) -> Self {
        self.cache_ttl = cache_ttl;
        self
    }

    /// Sets the minimum time between remote refresh attempts.
    ///
    /// This also bounds refreshes caused by arbitrary unknown `kid` values. Set it to zero only
    /// when deterministic tests or an explicitly managed upstream cache make that appropriate.
    #[must_use]
    pub const fn with_minimum_refresh_interval(mut self, interval: Duration) -> Self {
        self.minimum_refresh_interval = interval;
        self
    }

    /// Returns the only token algorithm accepted by this verifier.
    #[must_use]
    pub const fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    /// Returns the required token issuer.
    #[must_use]
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Returns the required token audience.
    #[must_use]
    pub fn audience(&self) -> &str {
        &self.audience
    }

    /// Returns the configured HTTPS JWKS endpoint.
    #[must_use]
    pub const fn jwks_url(&self) -> &Url {
        &self.jwks_url
    }

    /// Returns the successful JWKS cache lifetime.
    #[must_use]
    pub const fn cache_ttl(&self) -> Duration {
        self.cache_ttl
    }

    /// Returns the minimum interval between refresh attempts.
    #[must_use]
    pub const fn minimum_refresh_interval(&self) -> Duration {
        self.minimum_refresh_interval
    }

    pub(super) fn validation(&self) -> Validation {
        let mut validation = Validation::new(self.algorithm);
        validation.leeway = self.leeway_seconds;
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.validate_aud = true;
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.set_audience(&[self.audience.as_str()]);
        validation.set_required_spec_claims(&["sub", "iss", "aud", "exp", "nbf"]);
        validation
    }

    pub(super) fn id_token_validation(&self) -> Validation {
        let mut validation = Validation::new(self.algorithm);
        validation.leeway = self.leeway_seconds;
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.validate_aud = true;
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.set_audience(&[self.audience.as_str()]);
        validation.set_required_spec_claims(&["sub", "iss", "aud", "exp"]);
        validation
    }

    pub(super) const fn leeway_seconds(&self) -> u64 {
        self.leeway_seconds
    }
}

/// Invalid OIDC resource-server settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OidcConfigError {
    /// An issuer or audience was blank.
    #[error("OIDC issuer and audience must not be blank")]
    BlankField,
    /// An issuer or audience was oversized or contained control characters.
    #[error("OIDC issuer and audience must be bounded and free of control characters")]
    InvalidField,
    /// The configured clock skew exceeded the fixed validation bound.
    #[error("OIDC clock-skew leeway must not exceed five minutes")]
    LeewayTooLarge,
    /// The configured algorithm was symmetric.
    #[error("remote OIDC JWKS verification requires an asymmetric algorithm")]
    SymmetricAlgorithm,
    /// The configured JWKS endpoint was not a valid HTTPS endpoint.
    #[error("OIDC JWKS URL must be an absolute HTTPS URL without credentials or a fragment")]
    InvalidJwksUrl,
    /// The HTTP client timeout was zero.
    #[error("OIDC JWKS HTTP timeout must be greater than zero")]
    ZeroFetchTimeout,
    /// The HTTP client could not be initialized.
    #[error("OIDC JWKS HTTP client could not be initialized")]
    HttpClientInitialization,
}

pub(super) fn is_valid_jwks_url(url: &Url) -> bool {
    valid_trusted_https_url(url)
}

const fn is_asymmetric(algorithm: Algorithm) -> bool {
    matches!(
        algorithm,
        Algorithm::RS256
            | Algorithm::RS384
            | Algorithm::RS512
            | Algorithm::PS256
            | Algorithm::PS384
            | Algorithm::PS512
            | Algorithm::ES256
            | Algorithm::ES384
            | Algorithm::EdDSA
    )
}

#[cfg(test)]
mod tests {
    use jsonwebtoken::Algorithm;
    use url::Url;

    use super::{OidcConfigError, OidcResourceServerConfig};

    #[test]
    fn debug_redacts_resource_server_identity_metadata() {
        let config = OidcResourceServerConfig::new(
            Algorithm::RS256,
            "https://private-resource-server-issuer.example.test",
            "private-resource-server-audience",
            Url::parse("https://private-jwks.example.test/keys").expect("valid JWKS URL"),
        )
        .expect("valid resource-server configuration");

        let debug = format!("{config:?}");
        assert!(debug.contains("jwks_url: \"[REDACTED]\""));
        for value in [
            "private-resource-server-issuer.example.test",
            "private-resource-server-audience",
            "private-jwks.example.test",
        ] {
            assert!(
                !debug.contains(value),
                "Debug output must not include {value:?}"
            );
        }
    }

    #[test]
    fn configuration_bounds_trusted_values_and_clock_skew() {
        let jwks_url =
            Url::parse("https://issuer.example.test/keys").expect("test JWKS URL must parse");
        assert_eq!(
            OidcResourceServerConfig::new(
                Algorithm::RS256,
                "i".repeat(2 * 1024 + 1),
                "rustee-api",
                jwks_url,
            ),
            Err(OidcConfigError::InvalidField)
        );

        let config = OidcResourceServerConfig::new(
            Algorithm::RS256,
            "https://issuer.example.test",
            "rustee-api",
            Url::parse("https://issuer.example.test/keys").expect("test JWKS URL must parse"),
        )
        .expect("baseline resource-server configuration must be valid");
        assert_eq!(
            config.with_leeway_seconds(301),
            Err(OidcConfigError::LeewayTooLarge)
        );
    }
}
