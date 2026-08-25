//! Bearer authorization-header admission and RFC 6750 credential grammar.

use http::{HeaderMap, header::AUTHORIZATION};

use super::authenticator::AuthError;

/// Maximum UTF-8 byte length of a bearer credential accepted from an HTTP header.
pub const MAX_BEARER_TOKEN_BYTES: usize = 16 * 1024;

/// Extracts one bounded Bearer credential from an HTTP authorization header.
///
/// # Errors
///
/// Returns [`AuthError::MissingBearerToken`] when the header is absent, or
/// [`AuthError::InvalidBearerToken`] when it has duplicate values, invalid bytes, a non-Bearer
/// scheme, a credential outside the RFC 6750 `b64token` grammar, or exceeds
/// [`MAX_BEARER_TOKEN_BYTES`].
pub fn extract_bearer_token(headers: &HeaderMap) -> Result<&str, AuthError> {
    let mut values = headers.get_all(AUTHORIZATION).iter();
    let value = values.next().ok_or(AuthError::MissingBearerToken)?;
    if values.next().is_some() {
        return Err(AuthError::InvalidBearerToken);
    }
    let value = value.to_str().map_err(|_| AuthError::InvalidBearerToken)?;
    let (_, token) = value
        .split_once(' ')
        .filter(|(scheme, token)| {
            scheme.eq_ignore_ascii_case("Bearer") && is_valid_bearer_token(token)
        })
        .ok_or(AuthError::InvalidBearerToken)?;
    Ok(token)
}

pub(super) fn is_valid_bearer_token(token: &str) -> bool {
    let credential = token.trim_end_matches('=');
    !credential.is_empty()
        && token.len() <= MAX_BEARER_TOKEN_BYTES
        && credential.bytes().all(is_bearer_token_character)
}

fn is_bearer_token_character(byte: u8) -> bool {
    matches!(
        byte,
        b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'-'
            | b'.'
            | b'_'
            | b'~'
            | b'+'
            | b'/'
    )
}
