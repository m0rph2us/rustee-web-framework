//! Private typed-handler adaptation for route and fallback dispatch.

use std::{future::Future, marker::PhantomData};

use futures_util::future::BoxFuture;
use rustee_core::{FromRequest, IntoResponse, Request, Response, RouteParams, StateStore};

/// A route handler with a tuple of typed request extractors.
pub trait Handler<T>: Clone + Send + Sync + 'static {
    /// Calls this handler after the router has matched a route.
    fn call(
        &self,
        request: Request,
        params: RouteParams,
        state: StateStore,
    ) -> BoxFuture<'static, Response>;
}

impl<F, FutureOutput, Output> Handler<()> for F
where
    F: Fn() -> FutureOutput + Clone + Send + Sync + 'static,
    FutureOutput: Future<Output = Output> + Send + 'static,
    Output: IntoResponse + Send + 'static,
{
    fn call(
        &self,
        _request: Request,
        _params: RouteParams,
        _state: StateStore,
    ) -> BoxFuture<'static, Response> {
        let handler = self.clone();
        Box::pin(async move { handler().await.into_response() })
    }
}

macro_rules! impl_handler {
    ($($extractor:ident : $value:ident),+ $(,)?) => {
        impl<HandlerFn, FutureOutput, Output, $($extractor,)+> Handler<($($extractor,)+)> for HandlerFn
        where
            HandlerFn: Fn($($extractor),+) -> FutureOutput + Clone + Send + Sync + 'static,
            FutureOutput: Future<Output = Output> + Send + 'static,
            Output: IntoResponse + Send + 'static,
            $($extractor: FromRequest + 'static,)+
        {
            fn call(
                &self,
                request: Request,
                params: RouteParams,
                state: StateStore,
            ) -> BoxFuture<'static, Response> {
                let handler = self.clone();
                Box::pin(async move {
                    let mut request = request;
                    $(
                        let $value = match $extractor::from_request(&mut request, &params, &state).await {
                            Ok(value) => value,
                            Err(error) => return error.into_response(),
                        };
                    )+
                    handler($($value),+).await.into_response()
                })
            }
        }
    };
}

impl_handler!(A: a);
impl_handler!(A: a, B: b);
impl_handler!(A: a, B: b, C: c);
impl_handler!(A: a, B: b, C: c, D: d);
impl_handler!(A: a, B: b, C: c, D: d, E: e);
impl_handler!(A: a, B: b, C: c, D: d, E: e, F: f);

pub(crate) trait Endpoint: Send + Sync {
    fn call(
        &self,
        request: Request,
        params: RouteParams,
        state: StateStore,
    ) -> BoxFuture<'static, Response>;
}

#[derive(Debug)]
pub(crate) struct HandlerEndpoint<H, T> {
    handler: H,
    marker: PhantomData<fn() -> T>,
}

impl<H, T> HandlerEndpoint<H, T> {
    pub(crate) fn new(handler: H) -> Self {
        Self {
            handler,
            marker: PhantomData,
        }
    }
}

impl<H, T> Endpoint for HandlerEndpoint<H, T>
where
    H: Handler<T>,
    T: Send + Sync + 'static,
{
    fn call(
        &self,
        request: Request,
        params: RouteParams,
        state: StateStore,
    ) -> BoxFuture<'static, Response> {
        self.handler.call(request, params, state)
    }
}
