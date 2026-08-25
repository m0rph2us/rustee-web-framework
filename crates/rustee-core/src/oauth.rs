//! OAuth wire-value admission helpers shared by authorization boundaries.

/// Returns whether `value` is valid for an OAuth state, nonce, or PKCE verifier capability.
///
/// The accepted alphabet and length follow the shared authorization-flow contract. Callers retain
/// ownership of generation, storage, binding, and redaction for the resulting capability value.
#[must_use]
pub fn is_valid_oauth_authorization_value(value: &str) -> bool {
    (43..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-._~".contains(&byte))
}

/// Returns whether `value` is a bounded, non-control OAuth authorization code.
///
/// The caller supplies the provider-input byte limit and retains provider-specific code semantics.
#[must_use]
pub fn is_valid_oauth_authorization_code(value: &str, max_value_bytes: usize) -> bool {
    !value.trim().is_empty()
        && value.len() <= max_value_bytes
        && value.bytes().all(|byte| !byte.is_ascii_control())
}

/// Returns whether `value` is a bounded OAuth provider error identifier.
///
/// This admits only the standard token-like error alphabet; the caller supplies the byte limit.
#[must_use]
pub fn is_valid_oauth_provider_error(value: &str, max_value_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_value_bytes
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-._".contains(&byte))
}

/// Returns whether `value` is a bounded RFC 6749 OAuth scope token.
///
/// The caller supplies the policy-specific byte limit and retains ownership of scope selection
/// and authorization policy.
#[must_use]
pub fn is_valid_oauth_scope_token(value: &str, max_value_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_value_bytes
        && value
            .bytes()
            .all(|byte| matches!(byte, b'!' | b'#'..=b'[' | b']'..=b'~'))
}

/// Returns whether `values` is a non-empty sequence of bounded RFC 6749 OAuth scope tokens.
///
/// The caller retains ownership of claim-shape parsing, scope ordering, duplicate handling, and
/// authorization policy.
#[must_use]
pub fn are_valid_oauth_scope_tokens<'a>(
    values: impl IntoIterator<Item = &'a str>,
    max_value_bytes: usize,
) -> bool {
    let mut values = values.into_iter();
    let Some(first) = values.next() else {
        return false;
    };
    is_valid_oauth_scope_token(first, max_value_bytes)
        && values.all(|value| is_valid_oauth_scope_token(value, max_value_bytes))
}

#[cfg(test)]
mod tests {
    use super::{
        are_valid_oauth_scope_tokens, is_valid_oauth_authorization_code,
        is_valid_oauth_authorization_value, is_valid_oauth_provider_error,
        is_valid_oauth_scope_token,
    };

    #[test]
    fn authorization_values_have_one_shared_capability_contract() {
        assert!(is_valid_oauth_authorization_value(&"a".repeat(43)));
        assert!(is_valid_oauth_authorization_value(&format!(
            "{}-._~",
            "a".repeat(124)
        )));
        assert!(!is_valid_oauth_authorization_value(&"a".repeat(42)));
        assert!(!is_valid_oauth_authorization_value(&"a".repeat(129)));
        assert!(!is_valid_oauth_authorization_value(&format!(
            "{}*",
            "a".repeat(42)
        )));
    }

    #[test]
    fn provider_callback_values_keep_caller_supplied_bounds() {
        assert!(is_valid_oauth_authorization_code("provider-code", 13));
        assert!(!is_valid_oauth_authorization_code(" ", 13));
        assert!(!is_valid_oauth_authorization_code("code\r\n", 13));
        assert!(!is_valid_oauth_authorization_code("provider-code", 12));

        assert!(is_valid_oauth_provider_error("access_denied", 13));
        assert!(!is_valid_oauth_provider_error("access denied", 13));
        assert!(!is_valid_oauth_provider_error("access_denied", 12));
    }

    #[test]
    fn scope_tokens_follow_the_shared_rfc_6749_wire_grammar() {
        assert!(is_valid_oauth_scope_token("mcp:tools", 9));
        assert!(is_valid_oauth_scope_token("!#$[]~", 6));
        assert!(!is_valid_oauth_scope_token("", 256));
        assert!(!is_valid_oauth_scope_token("orders read", 256));
        assert!(!is_valid_oauth_scope_token("orders\"read", 256));
        assert!(!is_valid_oauth_scope_token("orders\\read", 256));
        assert!(!is_valid_oauth_scope_token("orders:\u{00e9}", 256));
        assert!(!is_valid_oauth_scope_token("mcp:tools", 8));

        assert!(are_valid_oauth_scope_tokens(
            ["profile:read", "mcp:tools"],
            256
        ));
        assert!(!are_valid_oauth_scope_tokens(
            std::iter::empty::<&str>(),
            256
        ));
        assert!(!are_valid_oauth_scope_tokens(
            ["profile:read", "orders read"],
            256
        ));
    }
}
