use std::{fmt, time::Duration};

use futures_util::future::BoxFuture;
use rustee_core::Request;

const MAX_KEY_BYTES: usize = 256;

/// A bounded opaque identity used as a rate-limit storage key suffix.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct RateLimitKey(String);

impl RateLimitKey {
    /// Creates a non-blank, control-character-free key.
    ///
    /// The layer does not derive this from untrusted headers. Applications normally use a verified
    /// principal, an API-key fingerprint, or trusted proxy-normalized client address.
    ///
    /// # Errors
    ///
    /// Returns [`RateLimitConfigError::InvalidKey`] for blank, oversized, or control-character
    /// values.
    pub fn new(value: impl Into<String>) -> Result<Self, RateLimitConfigError> {
        let value = value.into();
        if value.trim().is_empty()
            || value.len() > MAX_KEY_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(RateLimitConfigError::InvalidKey);
        }
        Ok(Self(value))
    }

    /// Returns the key suffix for a storage adapter.
    ///
    /// This value is intentionally omitted from [`fmt::Debug`] output because it can identify a
    /// principal or credential fingerprint.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for RateLimitKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RateLimitKey")
            .field("byte_len", &self.0.len())
            .finish()
    }
}

/// Fixed-window rate-limit policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FixedWindow {
    limit: u32,
    window: Duration,
    window_millis: u64,
}

impl FixedWindow {
    /// Creates one policy with a positive request limit and positive window.
    ///
    /// # Errors
    ///
    /// Returns [`RateLimitConfigError`] when the limit or duration cannot be represented by an
    /// `i64` millisecond-based storage adapter.
    pub fn new(limit: u32, window: Duration) -> Result<Self, RateLimitConfigError> {
        if limit == 0 {
            return Err(RateLimitConfigError::ZeroLimit);
        }
        let window_millis =
            u64::try_from(window.as_millis()).map_err(|_| RateLimitConfigError::InvalidWindow)?;
        if window_millis == 0
            || !window.subsec_nanos().is_multiple_of(1_000_000)
            || window_millis > i64::MAX as u64
        {
            return Err(RateLimitConfigError::InvalidWindow);
        }
        Ok(Self {
            limit,
            window,
            window_millis,
        })
    }

    /// Returns the maximum accepted requests in one fixed window.
    #[must_use]
    pub const fn limit(self) -> u32 {
        self.limit
    }

    /// Returns the fixed-window duration.
    #[must_use]
    pub const fn window(self) -> Duration {
        self.window
    }

    /// Returns the window in non-zero milliseconds for storage adapters.
    #[must_use]
    pub const fn window_millis(self) -> u64 {
        self.window_millis
    }
}

/// Invalid rate-limit configuration or key material.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RateLimitConfigError {
    /// The configured request limit was zero.
    #[error("rate-limit request limit must be greater than zero")]
    ZeroLimit,
    /// The configured window was zero, fractional at millisecond precision, or cannot be
    /// represented as signed milliseconds.
    #[error("rate-limit window must be a positive whole-millisecond signed duration")]
    InvalidWindow,
    /// The key was blank, too large, or contained a control character.
    #[error(
        "rate-limit key must be non-blank, at most 256 bytes, and contain no control characters"
    )]
    InvalidKey,
}

/// The result of an atomic rate-limit storage check.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateLimitDecision {
    allowed: bool,
    limit: u32,
    remaining: u32,
    reset_after: Duration,
}

impl RateLimitDecision {
    /// Creates an allowed decision with bounded remaining capacity.
    #[must_use]
    pub fn allowed(policy: FixedWindow, remaining: u32, reset_after: Duration) -> Self {
        Self {
            allowed: true,
            limit: policy.limit(),
            remaining: remaining.min(policy.limit()),
            reset_after,
        }
    }

    /// Creates a denied decision with no remaining capacity.
    #[must_use]
    pub fn denied(policy: FixedWindow, reset_after: Duration) -> Self {
        Self {
            allowed: false,
            limit: policy.limit(),
            remaining: 0,
            reset_after,
        }
    }

    /// Returns whether the request may proceed.
    #[must_use]
    pub const fn is_allowed(self) -> bool {
        self.allowed
    }

    /// Returns the configured request limit.
    #[must_use]
    pub const fn limit(self) -> u32 {
        self.limit
    }

    /// Returns the remaining requests in this window.
    #[must_use]
    pub const fn remaining(self) -> u32 {
        self.remaining
    }

    /// Returns the time until the current window resets.
    #[must_use]
    pub const fn reset_after(self) -> Duration {
        self.reset_after
    }
}

/// Atomic storage contract for keyed fixed-window rate limiting.
pub trait RateLimitStore: Clone + Send + Sync + 'static {
    /// Storage or provider failure type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Records one request and returns the current window decision.
    fn check(
        &self,
        key: RateLimitKey,
        policy: FixedWindow,
    ) -> BoxFuture<'static, Result<RateLimitDecision, Self::Error>>;
}

/// Resolves a storage key from a request that has already passed the application's trust boundary.
pub trait RateLimitKeyResolver: Clone + Send + Sync + 'static {
    /// Resolves one trusted request key, or returns `None` when no key is available and the layer
    /// must reject the request.
    fn resolve(&self, request: &Request) -> Option<RateLimitKey>;
}

impl<F> RateLimitKeyResolver for F
where
    F: Fn(&Request) -> Option<RateLimitKey> + Clone + Send + Sync + 'static,
{
    fn resolve(&self, request: &Request) -> Option<RateLimitKey> {
        self(request)
    }
}

/// Required behavior when the rate-limit store cannot answer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreFailurePolicy {
    /// Return a sanitized 503 response and do not call the application.
    FailClosed,
    /// Call the application without rate-limit response headers.
    FailOpen,
}
