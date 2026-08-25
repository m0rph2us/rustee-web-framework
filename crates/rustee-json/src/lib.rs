//! Bounded JSON materialization primitives for Rustee integrations.
//!
//! This crate keeps the byte-boundary behavior shared by independent Rustee integrations in one
//! place. It has no transport, storage, or domain dependency; consumers retain their own public
//! error contracts by mapping [`BoundedJsonError`] at their boundary.

use std::{
    fmt,
    io::{self, Write},
};

use serde::Serialize;

/// JSON materialization failed before a complete bounded value was available.
///
/// Its `Debug` output retains only the failure category; callers that map this workspace helper
/// can still inspect the trusted source chain when a serialization failure needs diagnosis.
#[derive(thiserror::Error)]
pub enum BoundedJsonError {
    /// Serialization would exceed the caller-supplied byte bound.
    #[error("JSON value exceeds the configured byte limit")]
    TooLarge,
    /// The value could not be serialized as JSON.
    #[error("JSON serialization failed")]
    Serialize(#[source] serde_json::Error),
}

impl fmt::Debug for BoundedJsonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::TooLarge => "too_large",
            Self::Serialize(_) => "serialization_failed",
        };
        formatter
            .debug_struct("BoundedJsonError")
            .field("kind", &kind)
            .finish()
    }
}

/// Serializes a value as JSON without retaining more than `max_bytes`.
///
/// The writer stops as soon as a serialization chunk would exceed `max_bytes`, avoiding the
/// allocation of a complete oversized JSON value.
///
/// # Errors
///
/// Returns [`BoundedJsonError::TooLarge`] when the encoded value exceeds `max_bytes`, or
/// [`BoundedJsonError::Serialize`] when JSON serialization fails for another reason.
pub fn to_vec_bounded<T>(value: &T, max_bytes: usize) -> Result<Vec<u8>, BoundedJsonError>
where
    T: Serialize + ?Sized,
{
    let mut buffer = BoundedJsonBuffer::new(max_bytes);
    match serde_json::to_writer(&mut buffer, value) {
        Ok(()) => Ok(buffer.into_inner()),
        Err(_) if buffer.exceeded => Err(BoundedJsonError::TooLarge),
        Err(error) => Err(BoundedJsonError::Serialize(error)),
    }
}

struct BoundedJsonBuffer {
    bytes: Vec<u8>,
    max_bytes: usize,
    exceeded: bool,
}

impl BoundedJsonBuffer {
    fn new(max_bytes: usize) -> Self {
        Self {
            bytes: Vec::new(),
            max_bytes,
            exceeded: false,
        }
    }

    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for BoundedJsonBuffer {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.len() > self.max_bytes.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "JSON value exceeds configured limit",
            ));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error as StdError,
        io::{ErrorKind, Write},
    };

    use serde::{Serialize, Serializer};

    use super::{BoundedJsonBuffer, BoundedJsonError, to_vec_bounded};

    struct SerializationFails;

    impl Serialize for SerializationFails {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            Err(serde::ser::Error::custom(
                "intentional serialization failure",
            ))
        }
    }

    #[test]
    fn encodes_a_value_at_its_exact_bound() {
        assert_eq!(to_vec_bounded(&42_u8, 2).unwrap(), b"42");
    }

    #[test]
    fn rejects_a_value_before_retaining_more_than_its_bound() {
        assert!(matches!(
            to_vec_bounded(&42_u8, 1),
            Err(BoundedJsonError::TooLarge)
        ));
    }

    #[test]
    fn buffer_rejects_an_overflowing_write_without_partial_admission() {
        let mut buffer = BoundedJsonBuffer::new(3);

        assert_eq!(buffer.write(b"ok").unwrap(), 2);
        let error = buffer.write(b"more").unwrap_err();

        assert_eq!(error.kind(), ErrorKind::WriteZero);
        assert!(buffer.exceeded);
        assert_eq!(buffer.bytes, b"ok");
        assert!(buffer.bytes.len() <= buffer.max_bytes);
    }

    #[test]
    fn preserves_a_serialization_failure_when_the_byte_bound_has_not_been_hit() {
        let error = to_vec_bounded(&SerializationFails, 1024).unwrap_err();
        assert!(matches!(&error, BoundedJsonError::Serialize(_)));
        assert!(!format!("{error:?}").contains("intentional serialization failure"));
        assert!(
            !error
                .to_string()
                .contains("intentional serialization failure")
        );
        assert!(StdError::source(&error).is_some());
    }
}
