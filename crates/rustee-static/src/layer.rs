//! Tower layer and service adaptation for static-file delivery.

use std::{
    convert::Infallible,
    task::{Context, Poll},
};

use futures_util::future::BoxFuture;
use http::Method;
use rustee_core::{BoxCloneServiceExt, Request, Response};
use tower::{Layer, Service, util::BoxCloneService};

use super::{
    config::StaticFiles,
    delivery::{serve_file, static_not_found},
};

/// Tower layer produced by [`StaticFiles::layer`].
#[derive(Clone, Debug)]
#[must_use = "a static file layer must be applied to a service to have an effect"]
pub struct StaticFilesLayer {
    files: StaticFiles,
}

impl StaticFilesLayer {
    pub(super) const fn new(files: StaticFiles) -> Self {
        Self { files }
    }
}

/// Service produced by [`StaticFilesLayer`].
#[derive(Clone, Debug)]
pub struct StaticFilesService {
    inner: BoxCloneService<Request, Response, Infallible>,
    files: StaticFiles,
}

impl<S> Layer<S> for StaticFilesLayer
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = StaticFilesService;

    fn layer(&self, inner: S) -> Self::Service {
        StaticFilesService {
            inner: BoxCloneService::new(inner),
            files: self.files.clone(),
        }
    }
}

impl Service<Request> for StaticFilesService {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let inner = self.inner.clone();
        let files = self.files.clone();
        Box::pin(async move {
            if !matches!(request.method(), &Method::GET | &Method::HEAD) {
                return inner.call_ready(request).await;
            }
            let relative = match files.relative_path(request.uri().path()) {
                Ok(None) => return inner.call_ready(request).await,
                Ok(Some(relative)) => relative,
                Err(()) => return Ok(static_not_found()),
            };
            Ok(serve_file(
                files,
                relative,
                request.method() == Method::HEAD,
                request.headers(),
            )
            .await)
        })
    }
}
