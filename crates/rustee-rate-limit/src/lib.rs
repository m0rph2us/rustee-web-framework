//! Explicit keyed rate-limit policy and Tower middleware contracts.
//!
//! Applications resolve a key from trusted request context. Storage adapters implement
//! [`RateLimitStore`], and every layer declares whether a storage outage is fail-open or
//! fail-closed.

use std::{convert::Infallible, fmt, time::Duration};

use futures_util::future::BoxFuture;
use http::{HeaderName, HeaderValue, StatusCode};
use rustee_core::{Error, IntoResponse, Request, Response};
use tower::{Layer, Service, util::BoxCloneService};

const MAX_KEY_BYTES: usize = 256;
const RATE_LIMIT_LIMIT: HeaderName = HeaderName::from_static("ratelimit-limit");
const RATE_LIMIT_REMAINING: HeaderName = HeaderName::from_static("ratelimit-remaining");
const RATE_LIMIT_RESET: HeaderName = HeaderName::from_static("ratelimit-reset");
const RETRY_AFTER: HeaderName = HeaderName::from_static("retry-after");

/// A bounded opaque identity used as a rate-limit storage key suffix.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
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
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Fixed-window rate-limit policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FixedWindow {
    limit: u32,
    window: Duration,
}

impl FixedWindow {
    /// Creates one policy with a positive request limit and positive window.
    ///
    /// # Errors
    ///
    /// Returns [`RateLimitConfigError`] when the limit or duration cannot be represented by a
    /// millisecond-based storage adapter.
    pub fn new(limit: u32, window: Duration) -> Result<Self, RateLimitConfigError> {
        if limit == 0 {
            return Err(RateLimitConfigError::ZeroLimit);
        }
        if window.is_zero() || window.as_millis() > u128::from(u64::MAX) {
            return Err(RateLimitConfigError::InvalidWindow);
        }
        Ok(Self { limit, window })
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
    pub fn window_millis(self) -> u64 {
        u64::try_from(self.window.as_millis()).unwrap_or(u64::MAX)
    }
}

/// Invalid rate-limit configuration or key material.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RateLimitConfigError {
    /// The configured request limit was zero.
    #[error("rate-limit request limit must be greater than zero")]
    ZeroLimit,
    /// The configured window was zero or cannot be represented in milliseconds.
    #[error("rate-limit window must be a positive millisecond duration")]
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

/// Tower layer that applies a keyed fixed-window rate-limit policy.
#[derive(Clone)]
#[must_use = "a rate-limit layer must be applied to a service to have an effect"]
pub struct RateLimitLayer<S, K> {
    store: S,
    policy: FixedWindow,
    resolver: K,
    failure_policy: StoreFailurePolicy,
}

impl<S, K> RateLimitLayer<S, K> {
    /// Creates a rate-limit layer with an explicit storage failure policy.
    pub const fn new(
        store: S,
        policy: FixedWindow,
        resolver: K,
        failure_policy: StoreFailurePolicy,
    ) -> Self {
        Self {
            store,
            policy,
            resolver,
            failure_policy,
        }
    }
}

impl<S, K> fmt::Debug for RateLimitLayer<S, K> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RateLimitLayer")
            .field("store", &std::any::type_name::<S>())
            .field("policy", &self.policy)
            .field("resolver", &std::any::type_name::<K>())
            .field("failure_policy", &self.failure_policy)
            .finish()
    }
}

/// Service produced by [`RateLimitLayer`].
#[derive(Clone)]
pub struct RateLimit<S, K> {
    inner: BoxCloneService<Request, Response, Infallible>,
    store: S,
    policy: FixedWindow,
    resolver: K,
    failure_policy: StoreFailurePolicy,
}

impl<S, K> fmt::Debug for RateLimit<S, K> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RateLimit")
            .field("store", &std::any::type_name::<S>())
            .field("policy", &self.policy)
            .field("resolver", &std::any::type_name::<K>())
            .field("failure_policy", &self.failure_policy)
            .finish_non_exhaustive()
    }
}

impl<I, S, K> Layer<I> for RateLimitLayer<S, K>
where
    I: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    I::Future: Send + 'static,
    S: RateLimitStore,
    K: RateLimitKeyResolver,
{
    type Service = RateLimit<S, K>;

    fn layer(&self, inner: I) -> Self::Service {
        RateLimit {
            inner: BoxCloneService::new(inner),
            store: self.store.clone(),
            policy: self.policy,
            resolver: self.resolver.clone(),
            failure_policy: self.failure_policy,
        }
    }
}

impl<S, K> Service<Request> for RateLimit<S, K>
where
    S: RateLimitStore,
    K: RateLimitKeyResolver,
{
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let key = self.resolver.resolve(&request);
        let store = self.store.clone();
        let policy = self.policy;
        let failure_policy = self.failure_policy;
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let Some(key) = key else {
                return Ok(Error::new(
                    StatusCode::BAD_REQUEST,
                    "rate_limit_key_missing",
                    "a rate-limit key is required",
                )
                .into_response());
            };
            match store.check(key, policy).await {
                Ok(decision) if decision.is_allowed() => {
                    let response = inner.call(request).await?;
                    Ok(with_rate_limit_headers(response, decision, false))
                }
                Ok(decision) => Ok(with_rate_limit_headers(
                    Error::new(
                        StatusCode::TOO_MANY_REQUESTS,
                        "rate_limit_exceeded",
                        "request rate limit exceeded",
                    )
                    .into_response(),
                    decision,
                    true,
                )),
                Err(_) if failure_policy == StoreFailurePolicy::FailOpen => {
                    inner.call(request).await
                }
                Err(_) => Ok(Error::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "rate_limit_unavailable",
                    "rate limiting is unavailable",
                )
                .into_response()),
            }
        })
    }
}

fn with_rate_limit_headers(
    mut response: Response,
    decision: RateLimitDecision,
    include_retry_after: bool,
) -> Response {
    let reset_seconds = rounded_seconds(decision.reset_after());
    response.headers_mut().insert(
        RATE_LIMIT_LIMIT,
        HeaderValue::from_str(&decision.limit().to_string()).expect("numeric header value"),
    );
    response.headers_mut().insert(
        RATE_LIMIT_REMAINING,
        HeaderValue::from_str(&decision.remaining().to_string()).expect("numeric header value"),
    );
    response.headers_mut().insert(
        RATE_LIMIT_RESET,
        HeaderValue::from_str(&reset_seconds.to_string()).expect("numeric header value"),
    );
    if include_retry_after {
        response.headers_mut().insert(
            RETRY_AFTER,
            HeaderValue::from_str(&reset_seconds.to_string()).expect("numeric header value"),
        );
    }
    response
}

fn rounded_seconds(duration: Duration) -> u64 {
    duration
        .as_secs()
        .saturating_add(u64::from(!duration.subsec_nanos().eq(&0)))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        io,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use http::{Request as HttpRequest, StatusCode};
    use http_body_util::BodyExt;
    use rustee_core::empty_body;
    use rustee_router::App;
    use tower::{Layer, ServiceExt};

    use super::{
        FixedWindow, RateLimitDecision, RateLimitKey, RateLimitLayer, RateLimitStore,
        StoreFailurePolicy,
    };

    #[derive(Clone)]
    struct ScriptedStore {
        decisions: Arc<Mutex<VecDeque<Result<RateLimitDecision, io::Error>>>>,
    }

    impl ScriptedStore {
        fn new(decisions: impl IntoIterator<Item = Result<RateLimitDecision, io::Error>>) -> Self {
            Self {
                decisions: Arc::new(Mutex::new(decisions.into_iter().collect())),
            }
        }
    }

    impl RateLimitStore for ScriptedStore {
        type Error = io::Error;

        fn check(
            &self,
            _: RateLimitKey,
            _: FixedWindow,
        ) -> futures_util::future::BoxFuture<'static, Result<RateLimitDecision, Self::Error>>
        {
            let decision = self.decisions.lock().unwrap().pop_front().unwrap();
            Box::pin(async move { decision })
        }
    }

    fn policy() -> FixedWindow {
        FixedWindow::new(3, Duration::from_secs(30)).unwrap()
    }

    fn request() -> rustee_core::Request {
        HttpRequest::builder().uri("/").body(empty_body()).unwrap()
    }

    fn verified_principal_key(_: &rustee_core::Request) -> Option<RateLimitKey> {
        RateLimitKey::new("verified-principal").ok()
    }

    #[tokio::test]
    async fn allowed_response_includes_bounded_rate_limit_headers() {
        let store = ScriptedStore::new([Ok(RateLimitDecision::allowed(
            policy(),
            2,
            Duration::from_millis(1_250),
        ))]);
        let service = RateLimitLayer::new(
            store,
            policy(),
            verified_principal_key,
            StoreFailurePolicy::FailClosed,
        )
        .layer(App::new().get("/", || async { "ok" }));

        let response = service.oneshot(request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["ratelimit-limit"], "3");
        assert_eq!(response.headers()["ratelimit-remaining"], "2");
        assert_eq!(response.headers()["ratelimit-reset"], "2");
        assert!(response.headers().get("retry-after").is_none());
    }

    #[tokio::test]
    async fn denied_request_does_not_reach_the_application() {
        let store = ScriptedStore::new([Ok(RateLimitDecision::denied(
            policy(),
            Duration::from_millis(1),
        ))]);
        let service = RateLimitLayer::new(
            store,
            policy(),
            verified_principal_key,
            StoreFailurePolicy::FailClosed,
        )
        .layer(App::new().get("/", || async { "unexpected" }));

        let response = service.oneshot(request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()["ratelimit-remaining"], "0");
        assert_eq!(response.headers()["retry-after"], "1");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert!(
            std::str::from_utf8(&body)
                .unwrap()
                .contains("rate_limit_exceeded")
        );
    }

    #[tokio::test]
    async fn store_failure_is_sanitized_when_fail_closed() {
        let store =
            ScriptedStore::new([Err(io::Error::other("redis endpoint and key are secret"))]);
        let service = RateLimitLayer::new(
            store,
            policy(),
            verified_principal_key,
            StoreFailurePolicy::FailClosed,
        )
        .layer(App::new().get("/", || async { "unexpected" }));

        let response = service.oneshot(request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("rate_limit_unavailable"));
        assert!(!body.contains("redis endpoint"));
    }

    #[tokio::test]
    async fn store_failure_passes_through_only_when_fail_open_is_explicit() {
        let store = ScriptedStore::new([Err(io::Error::other("unavailable"))]);
        let service = RateLimitLayer::new(
            store,
            policy(),
            verified_principal_key,
            StoreFailurePolicy::FailOpen,
        )
        .layer(App::new().get("/", || async { "allowed" }));

        let response = service.oneshot(request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get("ratelimit-limit").is_none());
    }
}
