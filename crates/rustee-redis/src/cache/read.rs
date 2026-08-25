//! Direct JSON cache reads, atomic consumption, and deletion.

use redis::{AsyncCommands, aio::ConnectionManager};
use serde::de::DeserializeOwned;

use super::CacheError;

/// Reads a JSON value from Redis without hiding cache-miss behavior.
///
/// # Errors
///
/// Returns an error for Redis communication or JSON decoding failure.
pub async fn get_json<T>(connection: &ConnectionManager, key: &str) -> Result<Option<T>, CacheError>
where
    T: DeserializeOwned,
{
    let mut connection = connection.clone();
    let value: Option<String> = connection.get(key).await?;
    value
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(Into::into)
}

/// Atomically removes and deserializes one JSON value without treating a cache miss as an error.
///
/// # Errors
///
/// Returns an error for Redis communication or JSON decoding failure. This requires Redis 6.2 or
/// newer because it uses `GETDEL` rather than a non-atomic read/delete pair.
pub async fn take_json<T>(
    connection: &ConnectionManager,
    key: &str,
) -> Result<Option<T>, CacheError>
where
    T: DeserializeOwned,
{
    let mut connection = connection.clone();
    let value: Option<String> = redis::cmd("GETDEL")
        .arg(key)
        .query_async(&mut connection)
        .await?;
    value
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(Into::into)
}

/// Deletes one key without treating a missing key as an error.
///
/// # Errors
///
/// Returns a content-free [`CacheError`] when the command cannot be executed.
pub async fn delete(connection: &ConnectionManager, key: &str) -> Result<(), CacheError> {
    let mut connection = connection.clone();
    connection.del::<_, ()>(key).await?;
    Ok(())
}
