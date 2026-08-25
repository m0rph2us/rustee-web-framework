//! Shared confidential-client authentication values and HTTP Basic admission.

use std::fmt;

use base64::{Engine, engine::general_purpose::STANDARD};

pub(crate) const MAX_CLIENT_ID_BYTES: usize = 1024;
pub(crate) const MAX_CLIENT_SECRET_BYTES: usize = 4 * 1024;
const MAX_BASIC_AUTHORIZATION_BYTES: usize = 8 * 1024;

/// A secret used only for confidential OIDC client authentication.
#[derive(Clone, Eq, PartialEq)]
pub struct OidcClientSecret(String);

impl OidcClientSecret {
    /// Stores a bounded secret for a trusted token or introspection endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`OidcClientAuthenticationError::BlankSecret`] when `value` is blank and
    /// [`OidcClientAuthenticationError::InvalidSecret`] when it is oversized or has control
    /// characters.
    pub fn new(value: impl Into<String>) -> Result<Self, OidcClientAuthenticationError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(OidcClientAuthenticationError::BlankSecret);
        }
        if value.len() > MAX_CLIENT_SECRET_BYTES
            || value.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(OidcClientAuthenticationError::InvalidSecret);
        }
        Ok(Self(value))
    }

    /// Exposes the secret to a trusted custom OIDC endpoint adapter.
    ///
    /// Callers must not log, serialize, or return this value in an HTTP response.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for OidcClientSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OidcClientSecret([REDACTED])")
    }
}

/// Client authentication selected for one trusted OIDC endpoint.
#[derive(Clone, Eq, PartialEq)]
pub enum OidcClientAuthentication {
    /// A public client that sends only its `client_id` in the form request.
    None,
    /// HTTP Basic authentication for a confidential client.
    ClientSecretBasic(OidcClientSecret),
    /// A `client_secret` form parameter for providers that explicitly require it.
    ClientSecretPost(OidcClientSecret),
}

impl fmt::Debug for OidcClientAuthentication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => formatter.write_str("OidcClientAuthentication::None"),
            Self::ClientSecretBasic(_) => {
                formatter.write_str("OidcClientAuthentication::ClientSecretBasic([REDACTED])")
            }
            Self::ClientSecretPost(_) => {
                formatter.write_str("OidcClientAuthentication::ClientSecretPost([REDACTED])")
            }
        }
    }
}

/// Invalid shared OIDC confidential-client authentication input.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OidcClientAuthenticationError {
    /// A confidential-client secret was blank.
    #[error("OIDC client secret must not be blank")]
    BlankSecret,
    /// A confidential-client secret was oversized or contained control characters.
    #[error("OIDC client secret must be bounded and free of control characters")]
    InvalidSecret,
}

pub(crate) fn valid_client_id(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_CLIENT_ID_BYTES
        && value.bytes().all(|byte| !byte.is_ascii_control())
}

pub(crate) fn basic_authorization_is_within_limit(
    client_id: &str,
    authentication: &OidcClientAuthentication,
) -> bool {
    match authentication {
        OidcClientAuthentication::ClientSecretBasic(secret) => {
            basic_authorization_len(client_id, secret.expose())
                .is_some_and(|length| length <= MAX_BASIC_AUTHORIZATION_BYTES)
        }
        OidcClientAuthentication::None | OidcClientAuthentication::ClientSecretPost(_) => true,
    }
}

pub(crate) fn basic_authorization_header(client_id: &str, secret: &OidcClientSecret) -> String {
    let user = form_encode_component(client_id);
    let password = form_encode_component(secret.expose());
    let credentials = STANDARD.encode(format!("{user}:{password}"));
    format!("Basic {credentials}")
}

fn basic_authorization_len(client_id: &str, secret: &str) -> Option<usize> {
    let encoded_credentials = form_encoded_len(client_id)
        .checked_add(1)?
        .checked_add(form_encoded_len(secret))?;
    let encoded_base64 = encoded_credentials
        .checked_add(2)?
        .checked_div(3)?
        .checked_mul(4)?;
    b"Basic ".len().checked_add(encoded_base64)
}

fn form_encode_component(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

fn form_encoded_len(value: &str) -> usize {
    value
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'*' | b'-' | b'.' | b'_') {
                1
            } else {
                3
            }
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_CLIENT_SECRET_BYTES, OidcClientAuthentication, OidcClientAuthenticationError,
        OidcClientSecret,
    };

    #[test]
    fn confidential_client_debug_redacts_the_secret() {
        const SECRET: &str = "private-client-secret";
        let secret = OidcClientSecret::new(SECRET).expect("test client secret must be valid");
        let secret_debug = format!("{secret:?}");
        let basic = OidcClientAuthentication::ClientSecretBasic(secret.clone());
        let post = OidcClientAuthentication::ClientSecretPost(secret);
        let debug = format!("{basic:?}{post:?}");

        assert!(!secret_debug.contains(SECRET));
        assert_eq!(secret_debug, "OidcClientSecret([REDACTED])");
        assert!(!debug.contains(SECRET));
        assert!(debug.contains("ClientSecretBasic([REDACTED])"));
        assert!(debug.contains("ClientSecretPost([REDACTED])"));
    }

    #[test]
    fn client_secret_has_a_fixed_admission_bound() {
        assert_eq!(
            OidcClientSecret::new("s".repeat(MAX_CLIENT_SECRET_BYTES + 1)),
            Err(OidcClientAuthenticationError::InvalidSecret)
        );
    }
}
