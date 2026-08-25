//! Redis-backed atomic fixed-window rate limiting.
//!
//! The adapter uses one Lua script to increment a key, assign its first-window TTL, and return
//! both the counter and remaining window duration. It does not select request identities or hide
//! storage failures; those policies belong to [`rustee_rate_limit::RateLimitLayer`].

use std::{fmt, time::Duration};

use futures_util::future::BoxFuture;
use redis::{ErrorKind, RedisError, Script, aio::ConnectionManager};
use rustee_rate_limit::{FixedWindow, RateLimitDecision, RateLimitKey, RateLimitStore};
use rustee_redis::is_valid_key_namespace;

const FIXED_WINDOW_SCRIPT: &str = r"
local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
local ttl = redis.call('PTTL', KEYS[1])
return { count, ttl }
";
const STORAGE_KEY_NAMESPACE: &str = "rustee:rate-limit:v1";

/// Redis fixed-window store with a versioned application-controlled key namespace.
///
/// Its `Debug` output keeps connection and namespace values redacted.
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
    /// oversized, or uses unsafe Redis key syntax.
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
        RedisFixedWindowStoreDebug {
            namespace: &self.namespace,
        }
        .fmt(formatter)
    }
}

struct RedisFixedWindowStoreDebug<'a> {
    namespace: &'a str,
}

impl fmt::Debug for RedisFixedWindowStoreDebug<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisFixedWindowStore")
            .field("connection", &"[REDACTED]")
            .field("namespace", &"[REDACTED]")
            .field("namespace_length", &self.namespace.len())
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
        let redis_key = storage_key(&self.namespace, &key);
        Box::pin(async move {
            let (count, ttl): (u64, i64) = Script::new(FIXED_WINDOW_SCRIPT)
                .key(redis_key)
                .arg(policy.window_millis())
                .invoke_async(&mut connection)
                .await?;
            let reset_after = reset_after(ttl)?;
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

fn reset_after(ttl_millis: i64) -> Result<Duration, RedisError> {
    let ttl_millis = u64::try_from(ttl_millis).map_err(|_| invalid_script_reply())?;
    Ok(Duration::from_millis(ttl_millis))
}

fn invalid_script_reply() -> RedisError {
    RedisError::from((
        ErrorKind::UnexpectedReturnType,
        "Redis rate-limit script returned an invalid TTL",
    ))
}

fn validate_namespace(namespace: &str) -> Result<(), RedisRateLimitConfigError> {
    if !is_valid_key_namespace(namespace) {
        return Err(RedisRateLimitConfigError::InvalidNamespace);
    }
    Ok(())
}

fn storage_key(namespace: &str, key: &RateLimitKey) -> String {
    let key = key.as_str();
    format!(
        "{STORAGE_KEY_NAMESPACE}:{}:{namespace}:{}:{key}",
        namespace.len(),
        key.len(),
    )
}

/// Invalid Redis rate-limit store configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RedisRateLimitConfigError {
    /// The namespace was blank, too large, or used unsafe Redis key syntax.
    #[error(
        "Redis rate-limit namespace must use bounded ASCII letters, digits, colon, underscore, hyphen, or dot"
    )]
    InvalidNamespace,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use redis::ErrorKind;

    use rustee_rate_limit::RateLimitKey;

    use super::{
        RedisFixedWindowStoreDebug, RedisRateLimitConfigError, reset_after, storage_key,
        validate_namespace,
    };

    #[test]
    fn script_ttl_keeps_exact_milliseconds_and_rejects_negative_reply_values() {
        assert_eq!(reset_after(7).unwrap(), Duration::from_millis(7));

        let error = reset_after(-1).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UnexpectedReturnType);
    }

    #[test]
    fn invalid_namespace_is_rejected_before_connecting() {
        assert_eq!(
            validate_namespace(" \n").unwrap_err(),
            RedisRateLimitConfigError::InvalidNamespace
        );
        assert_eq!(
            validate_namespace("rate{shared-slot}").unwrap_err(),
            RedisRateLimitConfigError::InvalidNamespace
        );
    }

    #[test]
    fn storage_key_is_length_delimited_across_namespace_and_opaque_key_boundaries() {
        let first = RateLimitKey::new("tenant:client").unwrap();
        let second = RateLimitKey::new("client").unwrap();

        assert_eq!(
            storage_key("rate", &first),
            "rustee:rate-limit:v1:4:rate:13:tenant:client"
        );
        assert_ne!(
            storage_key("rate", &first),
            storage_key("rate:tenant", &second)
        );
    }

    #[test]
    fn store_debug_redacts_the_deployment_namespace() {
        let debug = format!(
            "{:?}",
            RedisFixedWindowStoreDebug {
                namespace: "tenant.acme.rate-limit.v1",
            }
        );

        assert!(!debug.contains("tenant.acme.rate-limit.v1"));
        assert!(debug.contains("[REDACTED]"));
        assert!(debug.contains("namespace_length"));
    }
}
