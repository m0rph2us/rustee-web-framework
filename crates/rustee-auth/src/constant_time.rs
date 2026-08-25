//! Shared equality comparison for fixed-length authentication values.

/// Compares equal-length byte strings without exiting early based on their contents.
///
/// Different lengths return `false` before the byte comparison, so callers must not use this
/// function when the length of either value is sensitive.
#[must_use]
pub fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

#[cfg(test)]
mod tests;
