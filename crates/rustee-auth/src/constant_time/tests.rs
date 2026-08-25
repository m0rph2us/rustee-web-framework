//! Regression coverage for fixed-length authentication-value comparison.

use super::constant_time_eq;

#[test]
fn comparison_accepts_only_identical_equal_length_values() {
    let value = b"fixed-length-secret";

    assert!(constant_time_eq(value, value));
    assert!(!constant_time_eq(value, b"fixed-length-secreu"));
    assert!(!constant_time_eq(value, b"fixed-length-secret!"));
}
