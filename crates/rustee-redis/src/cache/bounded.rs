//! Atomic Redis-side byte admission for bounded JSON reads and consumption.

use redis::{Script, aio::ConnectionManager};
use serde::de::DeserializeOwned;

use super::CacheError;

const BOUNDED_TAKE_SCRIPT: &str = r"
    local length = redis.call('STRLEN', KEYS[1])
    if length == 0 and redis.call('EXISTS', KEYS[1]) == 0 then
        return {0, ''}
    end
    if length > tonumber(ARGV[1]) then
        return {2, ''}
    end
    local value = redis.call('GET', KEYS[1])
    redis.call('DEL', KEYS[1])
    return {1, value}
";

const BOUNDED_READ_SCRIPT: &str = r"
    local length = redis.call('STRLEN', KEYS[1])
    if length == 0 and redis.call('EXISTS', KEYS[1]) == 0 then
        return {0, ''}
    end
    if length > tonumber(ARGV[1]) then
        return {2, ''}
    end
    local value = redis.call('GET', KEYS[1])
    return {1, value}
";

/// Reads JSON from Redis without receiving more than `max_bytes` serialized bytes.
///
/// The Redis-side script measures one value before reading it in a single server execution. A
/// missing key is still a cache miss; a present empty value remains invalid JSON.
///
/// # Errors
///
/// Returns an error for Redis communication, a value larger than `max_bytes`, or JSON decoding
/// failure.
pub async fn get_json_bounded<T>(
    connection: &ConnectionManager,
    key: &str,
    max_bytes: usize,
) -> Result<Option<T>, CacheError>
where
    T: DeserializeOwned,
{
    let max_bytes_i64 = i64::try_from(max_bytes).map_err(|_| CacheError::ValueTooLarge)?;
    let mut connection = connection.clone();
    let (status, value): (i64, Vec<u8>) = Script::new(BOUNDED_READ_SCRIPT)
        .key(key)
        .arg(max_bytes_i64)
        .invoke_async(&mut connection)
        .await?;
    decode_bounded_response(status, value, max_bytes)
}

/// Atomically removes and deserializes JSON without receiving more than `max_bytes` bytes.
///
/// The Redis-side script preserves the atomic `GETDEL`-style contract: it measures before reading,
/// leaves an oversized value stored, and deletes a value at or below the limit before returning it.
/// This helper is appropriate for one-time values when both bounded memory use and exactly-once
/// consumption matter.
///
/// # Errors
///
/// Returns an error for Redis communication, a value larger than `max_bytes`, an unexpected Redis
/// script reply, or JSON decoding failure.
pub async fn take_json_bounded<T>(
    connection: &ConnectionManager,
    key: &str,
    max_bytes: usize,
) -> Result<Option<T>, CacheError>
where
    T: DeserializeOwned,
{
    let max_bytes_i64 = i64::try_from(max_bytes).map_err(|_| CacheError::ValueTooLarge)?;
    let mut connection = connection.clone();
    let (status, value): (i64, Vec<u8>) = Script::new(BOUNDED_TAKE_SCRIPT)
        .key(key)
        .arg(max_bytes_i64)
        .invoke_async(&mut connection)
        .await?;
    decode_bounded_response(status, value, max_bytes)
}

fn decode_json_bounded<T>(value: Option<Vec<u8>>, max_bytes: usize) -> Result<Option<T>, CacheError>
where
    T: DeserializeOwned,
{
    value
        .map(|value| {
            if value.len() > max_bytes {
                return Err(CacheError::ValueTooLarge);
            }
            serde_json::from_slice(&value).map_err(Into::into)
        })
        .transpose()
}

fn decode_bounded_response<T>(
    status: i64,
    value: Vec<u8>,
    max_bytes: usize,
) -> Result<Option<T>, CacheError>
where
    T: DeserializeOwned,
{
    match status {
        0 => Ok(None),
        1 => decode_json_bounded(Some(value), max_bytes),
        2 => Err(CacheError::ValueTooLarge),
        _ => Err(CacheError::UnexpectedResponse),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{CacheError, decode_bounded_response, decode_json_bounded};

    #[test]
    fn bounded_json_decode_rejects_oversize_values_before_deserialization() {
        assert!(matches!(
            decode_json_bounded::<Value>(Some(b"not-json".to_vec()), 4),
            Err(CacheError::ValueTooLarge)
        ));
        assert_eq!(
            decode_json_bounded::<Value>(Some(b"null".to_vec()), 4).unwrap(),
            Some(Value::Null)
        );
        assert_eq!(decode_json_bounded::<Value>(None, 4).unwrap(), None);
    }

    #[test]
    fn bounded_response_preserves_miss_limit_and_protocol_contracts() {
        assert_eq!(
            decode_bounded_response::<Value>(0, Vec::new(), 4).unwrap(),
            None
        );
        assert_eq!(
            decode_bounded_response::<Value>(1, b"null".to_vec(), 4).unwrap(),
            Some(Value::Null)
        );
        assert!(matches!(
            decode_bounded_response::<Value>(1, b"oversized".to_vec(), 4),
            Err(CacheError::ValueTooLarge)
        ));
        assert!(matches!(
            decode_bounded_response::<Value>(2, Vec::new(), 4),
            Err(CacheError::ValueTooLarge)
        ));
        assert!(matches!(
            decode_bounded_response::<Value>(3, Vec::new(), 4),
            Err(CacheError::UnexpectedResponse)
        ));
    }

    #[test]
    fn bounded_json_decode_reports_size_before_utf8_validation() {
        assert!(matches!(
            decode_json_bounded::<Value>(Some(vec![b'"', 0xea, 0xb0]), 2),
            Err(CacheError::ValueTooLarge)
        ));
    }
}
