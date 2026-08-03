//! Workspace-only support utilities live here to keep them out of Rustee's public API.

/// Returns a unique test namespace prefix.
#[must_use]
pub fn namespace(test_name: &str) -> String {
    format!("rustee-test-{test_name}-{}", std::process::id())
}
