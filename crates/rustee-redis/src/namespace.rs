//! Shared admission rules for framework-owned Redis key namespaces.

/// Maximum byte length for one framework-owned Redis key namespace.
pub const MAX_KEY_NAMESPACE_BYTES: usize = 128;

/// Returns whether `namespace` is a bounded, Redis Cluster-safe key prefix.
///
/// Rustee namespaces are ASCII identifiers containing only letters, digits, colon, underscore,
/// hyphen, or dot. Braces are excluded so a configured prefix cannot introduce an accidental
/// Redis Cluster hash tag.
#[must_use]
pub fn is_valid_key_namespace(namespace: &str) -> bool {
    !namespace.is_empty()
        && namespace.len() <= MAX_KEY_NAMESPACE_BYTES
        && namespace
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'_' | b'-' | b'.'))
}

#[cfg(test)]
mod tests {
    use super::{MAX_KEY_NAMESPACE_BYTES, is_valid_key_namespace};

    #[test]
    fn key_namespace_admission_is_bounded_and_cluster_safe() {
        assert!(is_valid_key_namespace("tenant-a:session.v1"));
        assert!(!is_valid_key_namespace(""));
        assert!(!is_valid_key_namespace("tenant session"));
        assert!(!is_valid_key_namespace("tenant{shared-slot}"));
        assert!(!is_valid_key_namespace(
            &"x".repeat(MAX_KEY_NAMESPACE_BYTES + 1)
        ));
    }
}
