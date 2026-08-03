//! Redis-backed atomic fixed-window rate limiting.
//!
//! The adapter uses one Lua script to increment a key, assign its first-window TTL, and return
//! both the counter and remaining window duration. It does not select request identities or hide
//! storage failures; those policies belong to [`rustee_rate_limit::RateLimitLayer`].

use std::{fmt, time::Duration};

use futures_util::future::BoxFuture;
use redis::{RedisError, Script, aio::ConnectionManager};
use rustee_rate_limit::{FixedWindow, RateLimitDecision, RateLimitKey, RateLimitStore};

const FIXED_WINDOW_SCRIPT: &str = r"
local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
local ttl = redis.call('PTTL', KEYS[1])
return { count, ttl }
";
const MAX_NAMESPACE_BYTES: usize = 128;

/// Redis fixed-window store with a versioned application-controlled key namespace.
#[derive(Clone)]
pub struct RedisFixedWindowStore {
    connection: ConnectionManager,
    namespace: String,
}

impl RedisFixedWindowStore {
    /// Creates a Redis rate-limit store.
    ///
    /// # Errors
    ///
    /// Returns [`RedisRateLimitConfigError::InvalidNamespace`] when `namespace` is blank,
    /// oversized, or contains a control character.
    pub fn new(
        connection: ConnectionManager,
        namespace: impl Into<String>,
    ) -> Result<Self, RedisRateLimitConfigError> {
        let namespace = namespace.into();
        validate_namespace(&namespace)?;
        Ok(Self {
            connection,
            namespace,
        })
    }

    /// Returns the non-secret key namespace.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}

impl fmt::Debug for RedisFixedWindowStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisFixedWindowStore")
            .field("connection", &"[REDACTED]")
            .field("namespace", &self.namespace)
            .finish()
    }
}

impl RateLimitStore for RedisFixedWindowStore {
    type Error = RedisError;

    fn check(
        &self,
        key: RateLimitKey,
        policy: FixedWindow,
    ) -> BoxFuture<'static, Result<RateLimitDecision, Self::Error>> {
        let mut connection = self.connection.clone();
        let redis_key = format!("{}:{}", self.namespace, key.as_str());
        Box::pin(async move {
            let (count, ttl): (u64, i64) = Script::new(FIXED_WINDOW_SCRIPT)
                .key(redis_key)
                .arg(policy.window_millis())
                .invoke_async(&mut connection)
                .await?;
            let reset_after =
                Duration::from_millis(u64::try_from(ttl).unwrap_or(policy.window_millis()));
            let accepted = count <= u64::from(policy.limit());
            Ok(if accepted {
                RateLimitDecision::allowed(
                    policy,
                    policy
                        .limit()
                        .saturating_sub(u32::try_from(count).unwrap_or(u32::MAX)),
                    reset_after,
                )
            } else {
                RateLimitDecision::denied(policy, reset_after)
            })
        })
    }
}

fn validate_namespace(namespace: &str) -> Result<(), RedisRateLimitConfigError> {
    if namespace.trim().is_empty()
        || namespace.len() > MAX_NAMESPACE_BYTES
        || namespace.chars().any(char::is_control)
    {
        return Err(RedisRateLimitConfigError::InvalidNamespace);
    }
    Ok(())
}

/// Invalid Redis rate-limit store configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RedisRateLimitConfigError {
    /// The namespace was blank, too large, or contained a control character.
    #[error(
        "Redis rate-limit namespace must be non-blank, at most 128 bytes, and contain no control characters"
    )]
    InvalidNamespace,
}

#[cfg(test)]
mod tests {
    use super::{RedisRateLimitConfigError, validate_namespace};

    #[test]
    fn invalid_namespace_is_rejected_before_connecting() {
        assert_eq!(
            validate_namespace(" \n").unwrap_err(),
            RedisRateLimitConfigError::InvalidNamespace
        );
    }
}
