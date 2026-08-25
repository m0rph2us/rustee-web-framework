//! JSON response encoding with optional bounded materialization.

use http::{HeaderValue, StatusCode, header::CONTENT_TYPE};
use rustee_json::{BoundedJsonError, to_vec_bounded};
use serde::Serialize;

use super::{Error, Response, Result, full_body, response};

/// Encodes a value as a JSON response.
///
/// # Errors
///
/// Returns an internal error when `value` cannot be serialized as JSON.
pub fn json_response<T>(status: StatusCode, value: &T) -> Result<Response>
where
    T: Serialize,
{
    let encoded = serde_json::to_vec(value).map_err(|_| Error::internal())?;
    Ok(json_response_from_encoded(status, encoded))
}

/// Encodes a value as a JSON response within an explicit byte limit.
///
/// The encoder stops before retaining more than `max_bytes`, so an oversized value is not fully
/// materialized in memory. This is an opt-in counterpart to [`json_response`] for endpoints with
/// application-controlled response budgets.
///
/// # Errors
///
/// Returns a sanitized internal error when `value` cannot be serialized as JSON or its encoded
/// JSON representation exceeds `max_bytes`.
pub fn json_response_bounded<T>(status: StatusCode, value: &T, max_bytes: usize) -> Result<Response>
where
    T: Serialize,
{
    let encoded = encode_json_bounded(value, max_bytes)?;
    Ok(json_response_from_encoded(status, encoded))
}

fn json_response_from_encoded(status: StatusCode, encoded: Vec<u8>) -> Response {
    let mut response = response(status, full_body(encoded));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    response
}

fn encode_json_bounded<T>(value: &T, max_bytes: usize) -> Result<Vec<u8>>
where
    T: Serialize,
{
    match to_vec_bounded(value, max_bytes) {
        Ok(encoded) => Ok(encoded),
        Err(BoundedJsonError::TooLarge) => Err(Error::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "response_too_large",
            "response body exceeds configured limit",
        )),
        Err(BoundedJsonError::Serialize(_)) => Err(Error::internal()),
    }
}
