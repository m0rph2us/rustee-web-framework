use std::fmt;

use jsonwebtoken::{Algorithm, Validation};

const MAX_AUDIENCE_BYTES: usize = 1024;
const MAX_ISSUER_BYTES: usize = 2 * 1024;
const MAX_LEEWAY_SECONDS: u64 = 300;

/// Non-secret JWT validation settings for a resource server.
#[derive(Clone, Eq, PartialEq)]
pub struct JwtConfig {
    algorithm: Algorithm,
    issuer: String,
    audience: String,
    leeway_seconds: u64,
}

impl fmt::Debug for JwtConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JwtConfig")
            .field("algorithm", &self.algorithm)
            .field("issuer_configured", &!self.issuer.is_empty())
            .field("audience_configured", &!self.audience.is_empty())
            .field("leeway_seconds", &self.leeway_seconds)
            .finish()
    }
}

impl JwtConfig {
    /// Creates a validation configuration with no clock-skew leeway.
    ///
    /// # Errors
    ///
    /// Returns [`JwtConfigurationError::BlankField`] when `issuer` or `audience` is blank, and
    /// [`JwtConfigurationError::InvalidField`] when either value is oversized or includes control
    /// characters.
    pub fn new(
        algorithm: Algorithm,
        issuer: impl Into<String>,
        audience: impl Into<String>,
    ) -> Result<Self, JwtConfigurationError> {
        let issuer = issuer.into();
        let audience = audience.into();
        ensure_valid_field(&issuer, "issuer", MAX_ISSUER_BYTES)?;
        ensure_valid_field(&audience, "audience", MAX_AUDIENCE_BYTES)?;
        Ok(Self {
            algorithm,
            issuer,
            audience,
            leeway_seconds: 0,
        })
    }

    /// Allows a finite clock skew when validating `exp` and `nbf` timestamps.
    ///
    /// # Errors
    ///
    /// Returns [`JwtConfigurationError::LeewayTooLarge`] when the requested leeway exceeds five
    /// minutes.
    pub fn with_leeway_seconds(
        mut self,
        leeway_seconds: u64,
    ) -> Result<Self, JwtConfigurationError> {
        if leeway_seconds > MAX_LEEWAY_SECONDS {
            return Err(JwtConfigurationError::LeewayTooLarge);
        }
        self.leeway_seconds = leeway_seconds;
        Ok(self)
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

    pub(crate) fn into_validation(self) -> Validation {
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

/// Invalid static JWT verifier configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum JwtConfigurationError {
    /// An issuer or audience value was blank.
    #[error("JWT {field} must not be blank")]
    BlankField {
        /// The invalid field name.
        field: &'static str,
    },
    /// An issuer or audience value was oversized or contained control characters.
    #[error("JWT {field} must be bounded and free of control characters")]
    InvalidField {
        /// The invalid field name.
        field: &'static str,
    },
    /// The configured clock skew exceeded the fixed validation bound.
    #[error("JWT clock-skew leeway must not exceed five minutes")]
    LeewayTooLarge,
    /// An HMAC verifier was given an empty secret.
    #[error("JWT verification key must not be blank")]
    BlankVerificationKey,
    /// A verification key exceeded the supported parser-admission bound.
    #[error("JWT verification key exceeds the supported size")]
    VerificationKeyTooLarge,
    /// An HMAC verification key was shorter than the configured algorithm requires.
    #[error("JWT HMAC verification key must be at least {minimum_bytes} bytes")]
    VerificationKeyTooShort {
        /// The minimum key length required by the selected HMAC algorithm.
        minimum_bytes: usize,
    },
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

fn ensure_valid_field(
    value: &str,
    field: &'static str,
    maximum_bytes: usize,
) -> Result<(), JwtConfigurationError> {
    if value.trim().is_empty() {
        return Err(JwtConfigurationError::BlankField { field });
    }
    if value.len() > maximum_bytes || value.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(JwtConfigurationError::InvalidField { field });
    }
    Ok(())
}

pub(crate) fn require_algorithm(
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

pub(crate) const fn is_hmac_algorithm(algorithm: Algorithm) -> bool {
    matches!(
        algorithm,
        Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512
    )
}

pub(crate) const fn hmac_secret_minimum_bytes(algorithm: Algorithm) -> Option<usize> {
    match algorithm {
        Algorithm::HS256 => Some(32),
        Algorithm::HS384 => Some(48),
        Algorithm::HS512 => Some(64),
        _ => None,
    }
}

pub(crate) const fn is_rsa_algorithm(algorithm: Algorithm) -> bool {
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

pub(crate) const fn is_ec_algorithm(algorithm: Algorithm) -> bool {
    matches!(algorithm, Algorithm::ES256 | Algorithm::ES384)
}

pub(crate) const fn is_eddsa_algorithm(algorithm: Algorithm) -> bool {
    matches!(algorithm, Algorithm::EdDSA)
}
