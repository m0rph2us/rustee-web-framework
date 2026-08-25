//! Redis integration built around redis-rs' reconnecting `ConnectionManager`.
//!
//! Cache behavior remains explicit: callers choose key namespaces, TTLs, and fallback behavior.

mod cache;
mod config;
mod namespace;
mod readiness;

pub use cache::{
    CacheError, MAX_REDIS_TTL_SECONDS, delete, get_json, get_json_bounded, set_json,
    set_json_bounded, set_json_bounded_if_absent, set_json_if_absent, take_json, take_json_bounded,
};
pub use config::{RedisConfig, RedisConfigError, RedisConnectError, connect};
pub use namespace::is_valid_key_namespace;
pub use readiness::{RedisReadinessError, readiness};
pub use redis;

#[cfg(test)]
mod tests {
    use std::{error::Error as StdError, time::Duration};

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::{
        CacheError, RedisConfig, RedisConfigError, RedisConnectError, RedisReadinessError,
        readiness, readiness::validate_readiness_timeout,
    };

    #[test]
    fn redis_url_is_not_exposed_in_debug_output() {
        let config = RedisConfig::new("redis://user:password@localhost:6379/0");
        assert!(!format!("{config:?}").contains("password"));
    }

    #[test]
    fn malformed_url_is_rejected_before_connecting() {
        let config = RedisConfig::new("not a redis URL with private-url-detail");
        let error = config.client().unwrap_err();

        assert_eq!(error, RedisConnectError::Connection);
        assert!(!format!("{error:?} {error}").contains("private-url-detail"));
    }

    #[test]
    fn configuration_requires_a_non_zero_connect_deadline() {
        let error = RedisConfig::new("redis://localhost:6379/0")
            .with_connect_timeout(Duration::ZERO)
            .unwrap_err();
        assert_eq!(error, RedisConfigError::ZeroConnectTimeout);
    }

    #[test]
    fn readiness_requires_a_non_zero_deadline() {
        let error = validate_readiness_timeout(Duration::ZERO).unwrap_err();
        assert!(matches!(error, RedisReadinessError::ZeroTimeout));
    }

    #[test]
    fn readiness_diagnostics_redact_redis_error_details_and_preserve_the_source() {
        let error = RedisReadinessError::Redis(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "private-redis-readiness-detail",
        )));

        assert!(!format!("{error:?}").contains("private-redis-readiness-detail"));
        assert!(!error.to_string().contains("private-redis-readiness-detail"));
        assert!(StdError::source(&error).is_some());
    }

    #[tokio::test]
    async fn readiness_deadline_cancels_a_nonresponsive_redis_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 1];
            stream.read_exact(&mut request).await.unwrap();
            stream.write_all(b"+OK\r\n+OK\r\n").await.unwrap();
            std::future::pending::<()>().await;
        });

        let config = RedisConfig::new(format!("redis://{address}/0"))
            .with_connect_timeout(Duration::from_millis(100))
            .unwrap();
        let connection = super::connect(&config).await.unwrap();
        let deadline = Duration::from_millis(50);
        let started = tokio::time::Instant::now();
        let error = readiness(&connection, deadline).await.unwrap_err();

        assert!(matches!(
            error,
            RedisReadinessError::Timeout(timeout) if timeout == deadline
        ));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "non-responsive Redis readiness exceeded the configured deadline"
        );

        server.abort();
        let _ = server.await;
    }

    #[test]
    fn cache_diagnostics_redact_redis_error_details_and_preserve_the_source() {
        let error = CacheError::Redis(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "private-redis-endpoint-detail",
        )));

        assert!(!format!("{error:?}").contains("private-redis-endpoint-detail"));
        assert!(!error.to_string().contains("private-redis-endpoint-detail"));
        assert!(StdError::source(&error).is_some());
    }
}
