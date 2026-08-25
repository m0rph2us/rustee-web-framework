use std::{
    fmt,
    task::{Context as TaskContext, Poll},
};

use opentelemetry_sdk::propagation::TraceContextPropagator;
use rustee_core::Request;
use rustee_observability::RequestSpanParent;
use tower::{Layer, Service};

use super::carrier::remote_parent;

/// Opt-in Tower layer that makes a valid inbound W3C trace context the parent of Rustee's request
/// span.
///
/// Place this outside [`rustee_observability::RequestIdLayer`] so the request span sees the parsed
/// parent before it is entered. The layer accepts one bounded ASCII `traceparent`, combines bounded
/// `tracestate` fields in wire order, and silently starts a new root trace for invalid, duplicate,
/// or oversized input. It does not trust a remote sampled flag over the application's tracer
/// provider sampler, record raw propagation headers, or add tracing headers to HTTP responses.
#[derive(Clone, Debug, Default)]
#[must_use = "a trace context layer must be applied to a service to have an effect"]
pub struct TraceContextLayer {
    propagator: TraceContextPropagator,
}

impl TraceContextLayer {
    /// Creates a layer using the W3C `traceparent` and `tracestate` propagator only.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Service produced by [`TraceContextLayer`].
#[derive(Clone)]
pub struct TraceContextService<S> {
    inner: S,
    propagator: TraceContextPropagator,
}

impl<S> fmt::Debug for TraceContextService<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TraceContextService")
            .field("inner_type", &std::any::type_name::<S>())
            .finish_non_exhaustive()
    }
}

impl<S> Layer<S> for TraceContextLayer {
    type Service = TraceContextService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TraceContextService {
            inner,
            propagator: self.propagator.clone(),
        }
    }
}

impl<S> Service<Request> for TraceContextService<S>
where
    S: Service<Request>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, context: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, mut request: Request) -> Self::Future {
        if let Some(parent) = remote_parent(&self.propagator, request.headers()) {
            request
                .extensions_mut()
                .insert(RequestSpanParent::new(parent));
        }
        self.inner.call(request)
    }
}
