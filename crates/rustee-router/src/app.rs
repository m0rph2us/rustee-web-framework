//! Application construction, route storage, and Tower service adaptation.

use std::{convert::Infallible, sync::Arc};

use futures_util::future::BoxFuture;
use http::Method;
use rustee_core::{Request, Response, RouteTemplate, StateStore};
use tower::{Layer, Service};

use super::{
    dispatch::dispatch,
    handler::{Endpoint, Handler, HandlerEndpoint},
    nesting::NestedRoute,
    pattern::{NestedPrefix, RoutePattern},
};

#[derive(Clone)]
pub(super) struct Route {
    pub(super) method: Method,
    pub(super) pattern: RoutePattern,
    pub(super) template: RouteTemplate,
    pub(super) endpoint: Arc<dyn Endpoint>,
    pub(super) order: usize,
}

impl std::fmt::Debug for Route {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Route")
            .field("method", &self.method)
            .field("pattern", &self.pattern)
            .field("template", &self.template)
            .field("order", &self.order)
            .finish_non_exhaustive()
    }
}

/// A construction-time error for an invalid route pattern.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteError {
    message: &'static str,
}

impl RouteError {
    pub(crate) const fn new(message: &'static str) -> Self {
        Self { message }
    }
}

impl std::fmt::Display for RouteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for RouteError {}

/// A cloneable application router and state container.
#[derive(Clone, Default)]
#[must_use = "an App builder must be retained to preserve its routes and state"]
pub struct App {
    pub(super) routes: Arc<Vec<Route>>,
    pub(super) nested_routes: Arc<Vec<NestedRoute>>,
    pub(super) state: StateStore,
    pub(super) fallback: Option<Arc<dyn Endpoint>>,
}

impl std::fmt::Debug for App {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("App")
            .field("routes", &self.routes)
            .field("nested_routes", &self.nested_routes)
            .field("state", &self.state)
            .field("has_fallback", &self.fallback.is_some())
            .finish()
    }
}

impl App {
    /// Creates an empty application.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds typed state that is available through [`rustee_core::State`].
    pub fn with_state<T>(mut self, value: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        self.state.insert(value);
        self
    }

    /// Adds a GET route.
    ///
    /// # Panics
    ///
    /// Panics when `path` is not a valid Rustee route pattern or an equivalent GET route is
    /// already registered. Use [`App::try_route`] when route declarations come from runtime
    /// configuration.
    pub fn get<T, H>(self, path: &str, handler: H) -> Self
    where
        T: Send + Sync + 'static,
        H: Handler<T>,
    {
        self.route(Method::GET, path, handler)
    }

    /// Adds a POST route.
    ///
    /// # Panics
    ///
    /// Panics when `path` is not a valid Rustee route pattern or an equivalent POST route is
    /// already registered. Use [`App::try_route`] when route declarations come from runtime
    /// configuration.
    pub fn post<T, H>(self, path: &str, handler: H) -> Self
    where
        T: Send + Sync + 'static,
        H: Handler<T>,
    {
        self.route(Method::POST, path, handler)
    }

    /// Adds a PUT route.
    ///
    /// # Panics
    ///
    /// Panics when `path` is not a valid Rustee route pattern or an equivalent PUT route is
    /// already registered. Use [`App::try_route`] when route declarations come from runtime
    /// configuration.
    pub fn put<T, H>(self, path: &str, handler: H) -> Self
    where
        T: Send + Sync + 'static,
        H: Handler<T>,
    {
        self.route(Method::PUT, path, handler)
    }

    /// Adds a PATCH route.
    ///
    /// # Panics
    ///
    /// Panics when `path` is not a valid Rustee route pattern or an equivalent PATCH route is
    /// already registered. Use [`App::try_route`] when route declarations come from runtime
    /// configuration.
    pub fn patch<T, H>(self, path: &str, handler: H) -> Self
    where
        T: Send + Sync + 'static,
        H: Handler<T>,
    {
        self.route(Method::PATCH, path, handler)
    }

    /// Adds a DELETE route.
    ///
    /// # Panics
    ///
    /// Panics when `path` is not a valid Rustee route pattern or an equivalent DELETE route is
    /// already registered. Use [`App::try_route`] when route declarations come from runtime
    /// configuration.
    pub fn delete<T, H>(self, path: &str, handler: H) -> Self
    where
        T: Send + Sync + 'static,
        H: Handler<T>,
    {
        self.route(Method::DELETE, path, handler)
    }

    /// Adds a route for a specific method.
    ///
    /// Invalid or duplicate route declarations are programmer errors; use [`App::try_route`] to
    /// handle them explicitly.
    ///
    /// # Panics
    ///
    /// Panics when `path` is not a valid Rustee route pattern or another route for `method`
    /// already matches the same set of request paths. The panic diagnostic reports the validation
    /// reason but does not echo the supplied path.
    pub fn route<T, H>(self, method: Method, path: &str, handler: H) -> Self
    where
        T: Send + Sync + 'static,
        H: Handler<T>,
    {
        self.try_route(method, path, handler)
            .unwrap_or_else(|error| panic!("invalid Rustee route: {error}"))
    }

    /// Tries to add a route for a specific method.
    ///
    /// # Errors
    ///
    /// Returns a [`RouteError`] when `path` is not a valid Rustee route pattern or another route
    /// for the same method already matches the same set of request paths.
    pub fn try_route<T, H>(
        mut self,
        method: Method,
        path: &str,
        handler: H,
    ) -> std::result::Result<Self, RouteError>
    where
        T: Send + Sync + 'static,
        H: Handler<T>,
    {
        let pattern = RoutePattern::parse(path)?;
        let routes = Arc::make_mut(&mut self.routes);
        if routes
            .iter()
            .any(|route| route.method == method && route.pattern.is_equivalent_to(&pattern))
        {
            return Err(RouteError::new(
                "a route for this method and path pattern is already registered",
            ));
        }
        let order = routes.len();
        routes.push(Route {
            method,
            pattern,
            template: RouteTemplate::new(path),
            endpoint: Arc::new(HandlerEndpoint::<H, T>::new(handler)),
            order,
        });
        Ok(self)
    }

    /// Mounts a Tower service below one static path prefix.
    ///
    /// The mounted service receives the request with the prefix removed while preserving its query
    /// string and request extensions. A child Rustee [`App`] keeps the externally visible full
    /// [`RouteTemplate`] for handlers and response-layer observability.
    ///
    /// # Panics
    ///
    /// Panics when `prefix` is not a valid non-root static path prefix. The panic diagnostic
    /// reports the validation reason but does not echo the supplied prefix. Use [`App::try_nest`]
    /// to handle configuration errors explicitly.
    pub fn nest<S>(self, prefix: &str, service: S) -> Self
    where
        S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
        S::Future: Send + 'static,
    {
        self.try_nest(prefix, service)
            .unwrap_or_else(|error| panic!("invalid Rustee nest prefix: {error}"))
    }

    /// Tries to mount a Tower service below one static path prefix.
    ///
    /// Prefixes must start with `/`, contain at least one static segment, and cannot contain route
    /// parameters. A direct parent route path takes priority, including its method-mismatch 405.
    /// Otherwise, the nested service owns requests below its prefix, including its fallback and
    /// method-mismatch responses. Parent and child [`StateStore`] values remain separate.
    ///
    /// # Errors
    ///
    /// Returns a [`RouteError`] when `prefix` is not a valid non-root static path prefix.
    pub fn try_nest<S>(mut self, prefix: &str, service: S) -> std::result::Result<Self, RouteError>
    where
        S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
        S::Future: Send + 'static,
    {
        let prefix = NestedPrefix::parse(prefix)?;
        let nested_routes = Arc::make_mut(&mut self.nested_routes);
        let order = nested_routes.len();
        nested_routes.push(NestedRoute::new(prefix, service, order));
        Ok(self)
    }

    /// Sets the handler used when no route matches.
    pub fn fallback<T, H>(mut self, handler: H) -> Self
    where
        T: Send + Sync + 'static,
        H: Handler<T>,
    {
        self.fallback = Some(Arc::new(HandlerEndpoint::<H, T>::new(handler)));
        self
    }

    /// Applies a Tower layer after all routes and state have been configured.
    #[must_use]
    pub fn layer<L>(self, layer: L) -> L::Service
    where
        L: Layer<Self>,
    {
        layer.layer(self)
    }

    /// Handles one already-owned request.
    pub fn call(&self, request: Request) -> BoxFuture<'static, Response> {
        dispatch(self, request)
    }
}

impl Service<Request> for App {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, std::result::Result<Response, Infallible>>;

    fn poll_ready(
        &mut self,
        _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let app = self.clone();
        Box::pin(async move { Ok(App::call(&app, request).await) })
    }
}
