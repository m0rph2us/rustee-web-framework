//! Opt-in panic-to-HTTP response boundary for uncommitted Rustee service failures.

use std::{
    convert::Infallible,
    panic::AssertUnwindSafe,
    task::{Context, Poll},
};

use futures_util::{FutureExt, future::BoxFuture};
use rustee_core::{BoxCloneServiceExt, Error, IntoResponse, Request, Response};
use tower::{Layer, Service, util::BoxCloneService};

/// Opt-in HTTP boundary that turns an uncommitted handler panic into a redacted 500 response.
///
/// This layer catches only a panic while its inner service is producing a [`Response`]. It cannot
/// replace the process-wide panic hook, recover a `panic = "abort"` build, or change a panic from
/// a body stream after response headers have been committed. Place it outside the handlers and
/// middleware whose call futures need the same response boundary.
#[derive(Clone, Copy, Debug, Default)]
#[must_use = "a panic-catch layer must be layered onto an application to have an effect"]
pub struct PanicCatchLayer;

impl PanicCatchLayer {
    /// Creates a panic boundary with Rustee's fixed public internal-error response.
    pub const fn new() -> Self {
        Self
    }
}

/// Service produced by [`PanicCatchLayer`].
#[derive(Clone, Debug)]
pub struct PanicCatch {
    inner: BoxCloneService<Request, Response, Infallible>,
}

impl<S> Layer<S> for PanicCatchLayer
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = PanicCatch;

    fn layer(&self, inner: S) -> Self::Service {
        PanicCatch {
            inner: BoxCloneService::new(inner),
        }
    }
}

impl Service<Request> for PanicCatch {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let inner = self.inner.clone();
        Box::pin(async move {
            match AssertUnwindSafe(async { inner.call_ready(request).await })
                .catch_unwind()
                .await
            {
                Ok(Ok(response)) => Ok(response),
                Ok(Err(never)) => match never {},
                Err(_) => {
                    tracing::error!("Rustee service panicked before producing an HTTP response");
                    Ok(Error::internal().into_response())
                }
            }
        })
    }
}
