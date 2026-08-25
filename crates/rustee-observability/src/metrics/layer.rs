//! Tower request lifecycle instrumentation.

use std::{
    convert::Infallible,
    task::{Context, Poll},
    time::Instant,
};

use futures_util::{FutureExt, future::BoxFuture};
use rustee_core::{BoxCloneServiceExt, Request, Response};
use tower::{Layer, Service, util::BoxCloneService};

use super::RequestMetrics;

/// Tower layer that records bounded request lifecycle metrics into [`RequestMetrics`].
#[derive(Clone, Debug)]
#[must_use = "a metrics layer must be applied to a service to have an effect"]
pub struct MetricsLayer {
    metrics: RequestMetrics,
}

impl MetricsLayer {
    /// Creates a layer that records completed request metrics in the supplied accumulator.
    pub fn new(metrics: RequestMetrics) -> Self {
        Self { metrics }
    }
}

/// Service produced by [`MetricsLayer`] that records request lifecycle metrics.
#[derive(Clone, Debug)]
pub struct MetricsService {
    inner: BoxCloneService<Request, Response, Infallible>,
    metrics: RequestMetrics,
}

impl<S> Layer<S> for MetricsLayer
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = MetricsService;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsService {
            inner: BoxCloneService::new(inner),
            metrics: self.metrics.clone(),
        }
    }
}

impl Service<Request> for MetricsService {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let inner = self.inner.clone();
        let lease = self.metrics.started();
        async move {
            let started = Instant::now();
            let response = inner.call_ready(request).await?;
            lease.finish(&response, started.elapsed());
            Ok(response)
        }
        .boxed()
    }
}
