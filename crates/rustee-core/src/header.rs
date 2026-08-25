//! Transport-neutral HTTP header-value admission helpers.

use http::HeaderValue;

/// Returns whether `value` is non-blank, within `max_value_bytes`, and can form an HTTP Bearer
/// header field value.
///
/// This checks wire-level header admission only. Callers retain responsibility for provider or
/// application credential semantics.
#[must_use]
pub fn is_valid_http_bearer_value(value: &str, max_value_bytes: usize) -> bool {
    !value.trim().is_empty()
        && value.len() <= max_value_bytes
        && HeaderValue::from_str(&format!("Bearer {value}")).is_ok()
}

#[cfg(test)]
mod tests {
    use super::is_valid_http_bearer_value;

    #[test]
    fn bearer_header_admission_checks_wire_safety_and_the_caller_bound() {
        assert!(is_valid_http_bearer_value("provider:opaque-token", 32));
        assert!(is_valid_http_bearer_value("token", 5));
        assert!(!is_valid_http_bearer_value(" ", 32));
        assert!(!is_valid_http_bearer_value("token\r\n", 32));
        assert!(!is_valid_http_bearer_value("token", 4));
    }
}
