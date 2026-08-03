//! Static-key JWT resource-server authentication for Rustee.
//!
//! The verifier has a deliberately narrow secure default: one configured algorithm, a required
//! signature, and required `sub`, `iss`, `aud`, `exp`, and `nbf` claims. For remote OIDC/JWKS key
//! discovery and rotation, use the future OIDC adapter rather than weakening this verifier.

use std::{fmt, sync::Arc};

use futures_util::future::BoxFuture;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use rustee_auth::{AuthError, BearerAuthenticator, Principal};
use serde::Deserialize;

/// Non-secret JWT validation settings for a resource server.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JwtConfig {
    algorithm: Algorithm,
    issuer: String,
    audience: String,
    leeway_seconds: u64,
}

impl JwtConfig {
    /// Creates a validation configuration with no clock-skew leeway.
    ///
    /// # Errors
    ///
    /// Returns [`JwtConfigurationError::BlankField`] when `issuer` or `audience` is blank.
    pub fn new(
        algorithm: Algorithm,
        issuer: impl Into<String>,
        audience: impl Into<String>,
    ) -> Result<Self, JwtConfigurationError> {
        let issuer = issuer.into();
        let audience = audience.into();
        ensure_not_blank(&issuer, "issuer")?;
        ensure_not_blank(&audience, "audience")?;
        Ok(Self {
            algorithm,
            issuer,
            audience,
            leeway_seconds: 0,
        })
    }

    /// Allows a bounded clock skew when validating `exp` and `nbf` timestamps.
    #[must_use]
    pub const fn with_leeway_seconds(mut self, leeway_seconds: u64) -> Self {
        self.leeway_seconds = leeway_seconds;
        self
    }

    /// Returns the sole algorithm accepted by this verifier.
    #[must_use]
    pub const fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    /// Returns the required issuer.
    #[must_use]
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Returns the required audience.
    #[must_use]
    pub fn audience(&self) -> &str {
        &self.audience
    }

    /// Returns the configured timestamp leeway in seconds.
    #[must_use]
    pub const fn leeway_seconds(&self) -> u64 {
        self.leeway_seconds
    }

    fn into_validation(self) -> Validation {
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
}

/// A static-key JWT verifier that implements Rustee bearer authentication.
#[derive(Clone)]
pub struct JwtAuthenticator {
    key: Arc<DecodingKey>,
    validation: Validation,
}

impl JwtAuthenticator {
    /// Creates an HMAC JWT verifier.
    ///
    /// Only HS256, HS384, and HS512 configurations are accepted. The secret is consumed by the
    /// decoding key and is never exposed through this type's debug output.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured algorithm is incompatible or the secret is blank.
    pub fn from_hmac_secret(
        config: JwtConfig,
        secret: impl AsRef<[u8]>,
    ) -> Result<Self, JwtConfigurationError> {
        require_algorithm(config.algorithm, "an HMAC secret", is_hmac_algorithm)?;
        let secret = secret.as_ref();
        if secret.is_empty() {
            return Err(JwtConfigurationError::BlankVerificationKey);
        }
        Ok(Self::with_key(config, DecodingKey::from_secret(secret)))
    }

    /// Creates an RSA or RSASSA-PSS JWT verifier from a PEM-encoded public key.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured algorithm is incompatible or the PEM is invalid.
    pub fn from_rsa_pem(
        config: JwtConfig,
        public_key_pem: &[u8],
    ) -> Result<Self, JwtConfigurationError> {
        require_algorithm(config.algorithm, "an RSA public key", is_rsa_algorithm)?;
        let key = DecodingKey::from_rsa_pem(public_key_pem)
            .map_err(|_| JwtConfigurationError::InvalidVerificationKey)?;
        Ok(Self::with_key(config, key))
    }

    /// Creates an ECDSA JWT verifier from a PEM-encoded public key.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured algorithm is incompatible or the PEM is invalid.
    pub fn from_ec_pem(
        config: JwtConfig,
        public_key_pem: &[u8],
    ) -> Result<Self, JwtConfigurationError> {
        require_algorithm(config.algorithm, "an ECDSA public key", is_ec_algorithm)?;
        let key = DecodingKey::from_ec_pem(public_key_pem)
            .map_err(|_| JwtConfigurationError::InvalidVerificationKey)?;
        Ok(Self::with_key(config, key))
    }

    /// Creates an `EdDSA` JWT verifier from a PEM-encoded public key.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured algorithm is incompatible or the PEM is invalid.
    pub fn from_ed_pem(
        config: JwtConfig,
        public_key_pem: &[u8],
    ) -> Result<Self, JwtConfigurationError> {
        require_algorithm(config.algorithm, "an EdDSA public key", is_eddsa_algorithm)?;
        let key = DecodingKey::from_ed_pem(public_key_pem)
            .map_err(|_| JwtConfigurationError::InvalidVerificationKey)?;
        Ok(Self::with_key(config, key))
    }

    fn with_key(config: JwtConfig, key: DecodingKey) -> Self {
        Self {
            key: Arc::new(key),
            validation: config.into_validation(),
        }
    }
}

impl fmt::Debug for JwtAuthenticator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JwtAuthenticator")
            .field("algorithms", &self.validation.algorithms)
            .field("issuer_configured", &self.validation.iss.is_some())
            .field("audience_configured", &self.validation.aud.is_some())
            .finish_non_exhaustive()
    }
}

impl BearerAuthenticator for JwtAuthenticator {
    fn authenticate(&self, token: &str) -> BoxFuture<'static, Result<Principal, AuthError>> {
        let key = Arc::clone(&self.key);
        let validation = self.validation.clone();
        let token = token.to_owned();
        Box::pin(async move {
            let claims = decode::<VerifiedClaims>(&token, key.as_ref(), &validation)
                .map_err(|_| AuthError::RejectedBearerToken)?
                .claims;
            claims.into_principal()
        })
    }
}

/// Invalid static JWT verifier configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum JwtConfigurationError {
    /// An issuer or audience value was blank.
    #[error("JWT {field} must not be blank")]
    BlankField {
        /// The invalid field name.
        field: &'static str,
    },
    /// An HMAC verifier was given an empty secret.
    #[error("JWT verification key must not be blank")]
    BlankVerificationKey,
    /// The configured algorithm cannot use the requested verification-key kind.
    #[error("JWT algorithm {algorithm:?} is incompatible with {key_kind}")]
    AlgorithmKeyMismatch {
        /// The configured JWT algorithm.
        algorithm: Algorithm,
        /// The human-readable verification-key kind.
        key_kind: &'static str,
    },
    /// A PEM-encoded verification key was invalid.
    #[error("JWT verification key is invalid")]
    InvalidVerificationKey,
}

fn ensure_not_blank(value: &str, field: &'static str) -> Result<(), JwtConfigurationError> {
    if value.trim().is_empty() {
        return Err(JwtConfigurationError::BlankField { field });
    }
    Ok(())
}

fn require_algorithm(
    algorithm: Algorithm,
    key_kind: &'static str,
    accepts_algorithm: fn(Algorithm) -> bool,
) -> Result<(), JwtConfigurationError> {
    if accepts_algorithm(algorithm) {
        Ok(())
    } else {
        Err(JwtConfigurationError::AlgorithmKeyMismatch {
            algorithm,
            key_kind,
        })
    }
}

const fn is_hmac_algorithm(algorithm: Algorithm) -> bool {
    matches!(
        algorithm,
        Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512
    )
}

const fn is_rsa_algorithm(algorithm: Algorithm) -> bool {
    matches!(
        algorithm,
        Algorithm::RS256
            | Algorithm::RS384
            | Algorithm::RS512
            | Algorithm::PS256
            | Algorithm::PS384
            | Algorithm::PS512
    )
}

const fn is_ec_algorithm(algorithm: Algorithm) -> bool {
    matches!(algorithm, Algorithm::ES256 | Algorithm::ES384)
}

const fn is_eddsa_algorithm(algorithm: Algorithm) -> bool {
    matches!(algorithm, Algorithm::EdDSA)
}

#[derive(Debug, Deserialize)]
struct VerifiedClaims {
    sub: String,
    iss: String,
    #[serde(rename = "aud")]
    _audience: serde_json::Value,
    #[serde(rename = "exp")]
    _expiration: serde_json::Value,
    #[serde(rename = "nbf")]
    _not_before: serde_json::Value,
    #[serde(default)]
    tenant: Option<String>,
    #[serde(default)]
    scope: Option<ScopeClaim>,
    #[serde(default)]
    roles: Option<StringSetClaim>,
    #[serde(default)]
    permissions: Option<StringSetClaim>,
}

impl VerifiedClaims {
    fn into_principal(self) -> Result<Principal, AuthError> {
        let mut principal = Principal::new(self.sub)
            .and_then(|principal| principal.with_issuer(self.iss))
            .map_err(|_| AuthError::RejectedBearerToken)?;
        if let Some(tenant) = self.tenant {
            principal = principal
                .with_tenant(tenant)
                .map_err(|_| AuthError::RejectedBearerToken)?;
        }
        for scope in self.scope.into_iter().flat_map(ScopeClaim::into_scopes) {
            principal = principal
                .with_scope(scope)
                .map_err(|_| AuthError::RejectedBearerToken)?;
        }
        for role in self.roles.into_iter().flat_map(StringSetClaim::into_values) {
            principal = principal
                .with_role(role)
                .map_err(|_| AuthError::RejectedBearerToken)?;
        }
        for permission in self
            .permissions
            .into_iter()
            .flat_map(StringSetClaim::into_values)
        {
            principal = principal
                .with_permission(permission)
                .map_err(|_| AuthError::RejectedBearerToken)?;
        }
        Ok(principal)
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ScopeClaim {
    SpaceDelimited(String),
    Values(Vec<String>),
}

impl ScopeClaim {
    fn into_scopes(self) -> Vec<String> {
        match self {
            Self::SpaceDelimited(scopes) => scopes
                .split_ascii_whitespace()
                .map(ToOwned::to_owned)
                .collect(),
            Self::Values(scopes) => scopes,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum StringSetClaim {
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
    use std::time::{SystemTime, UNIX_EPOCH};

    use http::{Request as HttpRequest, StatusCode};
    use jsonwebtoken::{EncodingKey, Header, encode};
    use rustee_auth::{AuthLayer, AuthUser, BearerAuthenticator};
    use rustee_core::empty_body;
    use rustee_router::App;
    use serde::Serialize;
    use tower::{Layer, ServiceExt};

    use super::{Algorithm, AuthError, JwtAuthenticator, JwtConfig, JwtConfigurationError};

    const SECRET: &[u8] = b"unit-test-secret-with-sufficient-length";
    const ISSUER: &str = "https://issuer.example.test";
    const AUDIENCE: &str = "rustee-api";

    #[derive(Serialize)]
    struct Claims<'a> {
        sub: &'a str,
        iss: &'a str,
        aud: &'a str,
        exp: u64,
        nbf: u64,
        tenant: &'a str,
        scope: &'a str,
        roles: &'a [&'a str],
        permissions: &'a [&'a str],
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn token(algorithm: Algorithm, issuer: &str, audience: &str, exp: u64, nbf: u64) -> String {
        encode(
            &Header::new(algorithm),
            &Claims {
                sub: "alice",
                iss: issuer,
                aud: audience,
                exp,
                nbf,
                tenant: "acme",
                scope: "profile:read profile:write",
                roles: &["project-viewer"],
                permissions: &["project:read"],
            },
            &EncodingKey::from_secret(SECRET),
        )
        .unwrap()
    }

    fn authenticator() -> JwtAuthenticator {
        JwtAuthenticator::from_hmac_secret(
            JwtConfig::new(Algorithm::HS256, ISSUER, AUDIENCE).unwrap(),
            SECRET,
        )
        .unwrap()
    }

    fn request(token: &str) -> rustee_core::Request {
        HttpRequest::builder()
            .method("GET")
            .uri("/me")
            .header("authorization", format!("Bearer {token}"))
            .body(empty_body())
            .unwrap()
    }

    #[tokio::test]
    async fn authenticates_a_verified_jwt_into_a_principal() {
        let current = now();
        let principal = authenticator()
            .authenticate(&token(
                Algorithm::HS256,
                ISSUER,
                AUDIENCE,
                current + 300,
                current - 1,
            ))
            .await
            .unwrap();

        assert_eq!(principal.subject(), "alice");
        assert_eq!(principal.issuer(), Some(ISSUER));
        assert_eq!(principal.tenant(), Some("acme"));
        assert!(principal.has_scope("profile:read"));
        assert!(principal.has_scope("profile:write"));
        assert!(principal.has_role("project-viewer"));
        assert!(principal.has_permission("project:read"));
    }

    #[tokio::test]
    async fn resource_server_layer_allows_only_a_verified_jwt() {
        let service = AuthLayer::bearer(authenticator()).layer(
            App::new().get("/me", |AuthUser(principal): AuthUser| async move {
                principal.subject().to_owned()
            }),
        );
        let current = now();
        let valid = service
            .clone()
            .oneshot(request(&token(
                Algorithm::HS256,
                ISSUER,
                AUDIENCE,
                current + 300,
                current - 1,
            )))
            .await
            .unwrap();
        let expired = service
            .oneshot(request(&token(
                Algorithm::HS256,
                ISSUER,
                AUDIENCE,
                current - 1,
                current - 300,
            )))
            .await
            .unwrap();

        assert_eq!(valid.status(), StatusCode::OK);
        assert_eq!(expired.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn rejects_a_signed_token_with_an_algorithm_outside_the_allowlist() {
        let current = now();
        let result = authenticator()
            .authenticate(&token(
                Algorithm::HS384,
                ISSUER,
                AUDIENCE,
                current + 300,
                current - 1,
            ))
            .await;

        assert_eq!(result.unwrap_err(), AuthError::RejectedBearerToken);
    }

    #[tokio::test]
    async fn rejects_expired_or_wrong_audience_tokens() {
        let current = now();
        let expired = authenticator()
            .authenticate(&token(
                Algorithm::HS256,
                ISSUER,
                AUDIENCE,
                current - 1,
                current - 300,
            ))
            .await;
        let wrong_audience = authenticator()
            .authenticate(&token(
                Algorithm::HS256,
                ISSUER,
                "another-api",
                current + 300,
                current - 1,
            ))
            .await;

        assert_eq!(expired.unwrap_err(), AuthError::RejectedBearerToken);
        assert_eq!(wrong_audience.unwrap_err(), AuthError::RejectedBearerToken);
    }

    #[tokio::test]
    async fn rejects_wrong_issuer_and_not_yet_valid_tokens() {
        let current = now();
        let wrong_issuer = authenticator()
            .authenticate(&token(
                Algorithm::HS256,
                "https://another-issuer.example.test",
                AUDIENCE,
                current + 300,
                current - 1,
            ))
            .await;
        let not_yet_valid = authenticator()
            .authenticate(&token(
                Algorithm::HS256,
                ISSUER,
                AUDIENCE,
                current + 300,
                current + 60,
            ))
            .await;

        assert_eq!(wrong_issuer.unwrap_err(), AuthError::RejectedBearerToken);
        assert_eq!(not_yet_valid.unwrap_err(), AuthError::RejectedBearerToken);
    }

    #[test]
    fn rejects_a_key_type_that_does_not_match_the_algorithm() {
        let error = JwtAuthenticator::from_hmac_secret(
            JwtConfig::new(Algorithm::RS256, ISSUER, AUDIENCE).unwrap(),
            SECRET,
        )
        .unwrap_err();

        assert_eq!(
            error,
            JwtConfigurationError::AlgorithmKeyMismatch {
                algorithm: Algorithm::RS256,
                key_kind: "an HMAC secret",
            }
        );
    }
}
