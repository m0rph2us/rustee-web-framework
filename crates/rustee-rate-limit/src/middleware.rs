use std::{convert::Infallible, fmt, time::Duration};

use futures_util::future::BoxFuture;
use http::{HeaderName, HeaderValue, StatusCode};
use rustee_core::{BoxCloneServiceExt, Error, IntoResponse, Request, Response};
use tower::{Layer, Service, util::BoxCloneService};

use crate::{
    FixedWindow, RateLimitDecision, RateLimitKeyResolver, RateLimitStore, StoreFailurePolicy,
};

const RATE_LIMIT_LIMIT: HeaderName = HeaderName::from_static("ratelimit-limit");
const RATE_LIMIT_REMAINING: HeaderName = HeaderName::from_static("ratelimit-remaining");
const RATE_LIMIT_RESET: HeaderName = HeaderName::from_static("ratelimit-reset");
const RETRY_AFTER: HeaderName = HeaderName::from_static("retry-after");

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
        let inner = self.inner.clone();
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
                    let response = inner.call_ready(request).await?;
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
                    inner.call_ready(request).await
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
        .saturating_add(u64::from(duration.subsec_nanos() != 0))
}
