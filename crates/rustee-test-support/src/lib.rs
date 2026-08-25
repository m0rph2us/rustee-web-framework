//! Workspace-only support utilities live here to keep them out of Rustee's public API.

use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_NAMESPACE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Returns a unique test namespace prefix within the current process.
#[must_use]
pub fn namespace(test_name: &str) -> String {
    let sequence = NEXT_NAMESPACE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("rustee-test-{test_name}-{}-{sequence}", std::process::id())
}

#[cfg(test)]
mod tests {
    use super::namespace;

    #[test]
    fn namespace_binds_the_test_name_process_id_and_unique_sequence() {
        let first = namespace("cache-contract");
        let second = namespace("cache-contract");
        let prefix = format!("rustee-test-cache-contract-{}-", std::process::id());

        assert!(first.starts_with(&prefix));
        assert!(second.starts_with(&prefix));
        assert_ne!(first, second);
    }
}
