//! Explicit TTL JSON cache writes and bounded serialization.

use redis::{AsyncCommands, aio::ConnectionManager};
use rustee_json::{BoundedJsonError, to_vec_bounded};
use serde::Serialize;

use super::{CacheError, MAX_REDIS_TTL_SECONDS};

/// Serializes and stores a JSON value with an explicit TTL in seconds.
///
/// # Errors
///
/// Returns an error for zero TTL, JSON serialization, or Redis communication failure.
pub async fn set_json<T>(
    connection: &ConnectionManager,
    key: &str,
    value: &T,
    ttl_seconds: u64,
) -> Result<(), CacheError>
where
    T: Serialize,
{
    validate_ttl_seconds(ttl_seconds)?;
    let encoded = serde_json::to_string(value)?;
    let mut connection = connection.clone();
    connection
        .set_ex::<_, _, ()>(key, encoded, ttl_seconds)
        .await?;
    Ok(())
}

/// Serializes and stores a JSON value only when `key` is not already present.
///
/// Redis `SET NX EX` creates the value and applies its TTL atomically. This is appropriate for
/// one-time capabilities such as authorization state, not for cache refreshes or mutable records.
///
/// # Errors
///
/// Returns [`CacheError::EntryExists`] when `key` is already present, or an error for zero TTL,
/// JSON serialization, or Redis communication failure.
pub async fn set_json_if_absent<T>(
    connection: &ConnectionManager,
    key: &str,
    value: &T,
    ttl_seconds: u64,
) -> Result<(), CacheError>
where
    T: Serialize,
{
    validate_ttl_seconds(ttl_seconds)?;
    let encoded = serde_json::to_string(value)?;
    let mut connection = connection.clone();
    let created: Option<String> = redis::cmd("SET")
        .arg(key)
        .arg(encoded)
        .arg("NX")
        .arg("EX")
        .arg(ttl_seconds)
        .query_async(&mut connection)
        .await?;
    created.ok_or(CacheError::EntryExists)?;
    Ok(())
}

/// Serializes and stores a JSON value only when `key` is absent and the result fits `max_bytes`.
///
/// This is the bounded counterpart to [`set_json_if_absent`]. It materializes JSON within the
/// requested limit before issuing the atomic Redis `SET NX EX` command, so an oversized value
/// neither reaches Redis nor replaces an existing record.
///
/// # Errors
///
/// Returns [`CacheError::EntryExists`] when `key` is already present, or an error for zero TTL, a
/// value larger than `max_bytes`, JSON serialization, or Redis communication failure.
pub async fn set_json_bounded_if_absent<T>(
    connection: &ConnectionManager,
    key: &str,
    value: &T,
    ttl_seconds: u64,
    max_bytes: usize,
) -> Result<(), CacheError>
where
    T: Serialize,
{
    validate_ttl_seconds(ttl_seconds)?;
    let encoded = encode_json_bounded(value, max_bytes)?;
    let mut connection = connection.clone();
    let created: Option<String> = redis::cmd("SET")
        .arg(key)
        .arg(encoded)
        .arg("NX")
        .arg("EX")
        .arg(ttl_seconds)
        .query_async(&mut connection)
        .await?;
    created.ok_or(CacheError::EntryExists)?;
    Ok(())
}

/// Serializes and stores a JSON value without exceeding `max_bytes` before the Redis write.
///
/// This is the bounded counterpart to [`set_json`]. It avoids constructing an oversized
/// serialized value in memory and does not issue a Redis write when serialization exceeds the
/// limit.
///
/// # Errors
///
/// Returns an error for zero TTL, a value larger than `max_bytes`, JSON serialization, or Redis
/// communication failure.
pub async fn set_json_bounded<T>(
    connection: &ConnectionManager,
    key: &str,
    value: &T,
    ttl_seconds: u64,
    max_bytes: usize,
) -> Result<(), CacheError>
where
    T: Serialize,
{
    validate_ttl_seconds(ttl_seconds)?;
    let encoded = encode_json_bounded(value, max_bytes)?;
    let mut connection = connection.clone();
    connection
        .set_ex::<_, _, ()>(key, encoded, ttl_seconds)
        .await?;
    Ok(())
}

fn encode_json_bounded<T>(value: &T, max_bytes: usize) -> Result<Vec<u8>, CacheError>
where
    T: Serialize,
{
    match to_vec_bounded(value, max_bytes) {
        Ok(encoded) => Ok(encoded),
        Err(BoundedJsonError::TooLarge) => Err(CacheError::ValueTooLarge),
        Err(BoundedJsonError::Serialize(error)) => Err(CacheError::Json(error)),
    }
}

fn validate_ttl_seconds(ttl_seconds: u64) -> Result<(), CacheError> {
    if ttl_seconds == 0 {
        return Err(CacheError::ZeroTtl);
    }
    if ttl_seconds > MAX_REDIS_TTL_SECONDS {
        return Err(CacheError::TtlOutOfRange);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{CacheError, MAX_REDIS_TTL_SECONDS, encode_json_bounded, validate_ttl_seconds};

    #[test]
    fn cache_writes_reject_a_zero_ttl_as_a_local_configuration_error() {
        assert!(matches!(validate_ttl_seconds(0), Err(CacheError::ZeroTtl)));
        assert!(validate_ttl_seconds(1).is_ok());
        assert!(validate_ttl_seconds(MAX_REDIS_TTL_SECONDS).is_ok());
        assert!(matches!(
            validate_ttl_seconds(MAX_REDIS_TTL_SECONDS + 1),
            Err(CacheError::TtlOutOfRange)
        ));
    }

    #[test]
    fn bounded_json_encode_rejects_oversize_values_before_storing() {
        assert_eq!(encode_json_bounded(&Value::Null, 4).unwrap(), b"null");
        assert!(matches!(
            encode_json_bounded(&Value::String("value".repeat(8)), 16),
            Err(CacheError::ValueTooLarge)
        ));
    }
}
