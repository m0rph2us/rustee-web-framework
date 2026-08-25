//! Bounded job-envelope JSON encoding and pre-decode byte admission.

use rustee_json::{BoundedJsonError, to_vec_bounded};
use serde::Serialize;

use super::{EnvelopeError, MAX_JOB_ENVELOPE_BYTES};

pub(super) fn encode<T>(value: &T) -> Result<Vec<u8>, EnvelopeError>
where
    T: Serialize + ?Sized,
{
    match to_vec_bounded(value, MAX_JOB_ENVELOPE_BYTES) {
        Ok(encoded) => Ok(encoded),
        Err(BoundedJsonError::TooLarge) => Err(EnvelopeError::TooLarge),
        Err(BoundedJsonError::Serialize(error)) => Err(EnvelopeError::Serialize(error)),
    }
}

pub(super) fn validate_envelope_bytes(bytes: &[u8]) -> Result<(), EnvelopeError> {
    if bytes.len() > MAX_JOB_ENVELOPE_BYTES {
        return Err(EnvelopeError::TooLarge);
    }
    Ok(())
}
