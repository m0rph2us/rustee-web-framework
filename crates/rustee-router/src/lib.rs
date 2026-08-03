//! Route matching and typed handler dispatch.

use std::{
    cmp::Ordering,
    collections::BTreeSet,
    convert::Infallible,
    future::Future,
    marker::PhantomData,
    str::FromStr,
    sync::{Arc, Mutex},
};

use futures_util::future::BoxFuture;
use http::{HeaderValue, Method, StatusCode, Uri, header::ALLOW, uri::PathAndQuery};
use rustee_core::{
    Error, FromRequest, IntoResponse, Request, Response, RouteClassification, RouteParams,
    RouteTemplate, StateStore,
};
use tower::{Layer, Service, util::BoxCloneService};

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

trait Endpoint: Send + Sync {
    fn call(
        &self,
        request: Request,
        params: RouteParams,
        state: StateStore,
    ) -> BoxFuture<'static, Response>;
}

#[derive(Debug)]
struct HandlerEndpoint<H, T> {
    handler: H,
    marker: PhantomData<fn() -> T>,
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

#[derive(Clone, Debug)]
enum Segment {
    Static(String),
    Parameter(String),
}

#[derive(Clone, Debug)]
struct RoutePattern {
    segments: Vec<Segment>,
    static_segments: usize,
}

impl RoutePattern {
    fn parse(path: &str) -> std::result::Result<Self, RouteError> {
        if !path.starts_with('/') {
            return Err(RouteError::new("route paths must start with '/'"));
        }
        if path.contains('?') || path.contains('#') {
            return Err(RouteError::new(
                "route paths cannot contain a query string or fragment",
            ));
        }

        let mut static_segments = 0;
        let mut names = BTreeSet::new();
        let mut segments = Vec::new();
        for segment in path
            .trim_matches('/')
            .split('/')
            .filter(|segment| !segment.is_empty())
        {
            if let Some(name) = segment.strip_prefix(':') {
                if name.is_empty()
                    || !name
                        .chars()
                        .all(|character| character.is_ascii_alphanumeric() || character == '_')
                {
                    return Err(RouteError::new(
                        "route parameter names must contain only ASCII letters, digits, or underscores",
                    ));
                }
                if !names.insert(name.to_owned()) {
                    return Err(RouteError::new("route parameter names must be unique"));
                }
                segments.push(Segment::Parameter(name.to_owned()));
            } else {
                static_segments += 1;
                segments.push(Segment::Static(segment.to_owned()));
            }
        }

        Ok(Self {
            segments,
            static_segments,
        })
    }

    fn matches(&self, path: &str) -> Option<RouteParams> {
        let incoming = path
            .trim_matches('/')
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>();
        if incoming.len() != self.segments.len() {
            return None;
        }

        let mut params = Vec::new();
        for (segment, incoming) in self.segments.iter().zip(incoming) {
            match segment {
                Segment::Static(expected) if expected == incoming => {}
                Segment::Static(_) => return None,
                Segment::Parameter(name) => params.push((name.clone(), incoming.to_owned())),
            }
        }
        Some(RouteParams::new(params))
    }
}

#[derive(Clone)]
struct Route {
    method: Method,
    pattern: RoutePattern,
    template: RouteTemplate,
    endpoint: Arc<dyn Endpoint>,
    order: usize,
}

#[derive(Clone, Debug)]
struct NestedPrefix {
    value: String,
    segment_count: usize,
}

impl NestedPrefix {
    fn parse(path: &str) -> std::result::Result<Self, RouteError> {
        let pattern = RoutePattern::parse(path)?;
        if pattern.segments.is_empty() {
            return Err(RouteError::new(
                "nest prefixes must contain at least one static path segment",
            ));
        }
        if !pattern
            .segments
            .iter()
            .all(|segment| matches!(segment, Segment::Static(_)))
        {
            return Err(RouteError::new(
                "nest prefixes cannot contain route parameters",
            ));
        }
        let value = format!(
            "/{}",
            pattern
                .segments
                .iter()
                .map(|segment| match segment {
                    Segment::Static(value) => value.as_str(),
                    Segment::Parameter(_) => unreachable!("parameter prefixes were rejected"),
                })
                .collect::<Vec<_>>()
                .join("/")
        );
        Ok(Self {
            value,
            segment_count: pattern.segments.len(),
        })
    }

    fn strip<'a>(&self, path: &'a str) -> Option<&'a str> {
        let remaining = path.strip_prefix(&self.value)?;
        if remaining.is_empty() {
            Some("/")
        } else if remaining.starts_with('/') {
            Some(remaining)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
struct NestedRoutePrefix(String);

#[derive(Clone)]
struct NestedRoute {
    prefix: NestedPrefix,
    service: Arc<Mutex<BoxCloneService<Request, Response, Infallible>>>,
    order: usize,
}

impl std::fmt::Debug for NestedRoute {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NestedRoute")
            .field("prefix", &self.prefix)
            .field("order", &self.order)
            .finish_non_exhaustive()
    }
}

impl NestedRoute {
    fn call(&self, mut request: Request) -> BoxFuture<'static, Response> {
        let Some(path) = self.prefix.strip(request.uri().path()).map(str::to_owned) else {
            return Box::pin(async {
                Error::not_found("the requested route was not found").into_response()
            });
        };
        let visible_prefix = request.extensions().get::<NestedRoutePrefix>().map_or_else(
            || self.prefix.value.clone(),
            |parent| join_route_paths(&parent.0, &self.prefix.value),
        );
        request
            .extensions_mut()
            .insert(NestedRoutePrefix(visible_prefix));
        if !replace_request_path(&mut request, &path) {
            return Box::pin(async {
                Error::not_found("the requested route was not found").into_response()
            });
        }
        let mut service = self
            .service
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        Box::pin(async move {
            match service.call(request).await {
                Ok(response) => response,
                Err(never) => match never {},
            }
        })
    }
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
    const fn new(message: &'static str) -> Self {
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
    routes: Arc<Vec<Route>>,
    nested_routes: Arc<Vec<NestedRoute>>,
    state: StateStore,
    fallback: Option<Arc<dyn Endpoint>>,
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
    pub fn get<T, H>(self, path: &str, handler: H) -> Self
    where
        T: Send + Sync + 'static,
        H: Handler<T>,
    {
        self.route(Method::GET, path, handler)
    }

    /// Adds a POST route.
    pub fn post<T, H>(self, path: &str, handler: H) -> Self
    where
        T: Send + Sync + 'static,
        H: Handler<T>,
    {
        self.route(Method::POST, path, handler)
    }

    /// Adds a PUT route.
    pub fn put<T, H>(self, path: &str, handler: H) -> Self
    where
        T: Send + Sync + 'static,
        H: Handler<T>,
    {
        self.route(Method::PUT, path, handler)
    }

    /// Adds a PATCH route.
    pub fn patch<T, H>(self, path: &str, handler: H) -> Self
    where
        T: Send + Sync + 'static,
        H: Handler<T>,
    {
        self.route(Method::PATCH, path, handler)
    }

    /// Adds a DELETE route.
    pub fn delete<T, H>(self, path: &str, handler: H) -> Self
    where
        T: Send + Sync + 'static,
        H: Handler<T>,
    {
        self.route(Method::DELETE, path, handler)
    }

    /// Adds a route for a specific method.
    ///
    /// Invalid patterns are programmer errors; use [`App::try_route`] to handle them explicitly.
    ///
    /// # Panics
    ///
    /// Panics when `path` is not a valid Rustee route pattern.
    pub fn route<T, H>(self, method: Method, path: &str, handler: H) -> Self
    where
        T: Send + Sync + 'static,
        H: Handler<T>,
    {
        self.try_route(method, path, handler)
            .unwrap_or_else(|error| panic!("invalid Rustee route {path:?}: {error}"))
    }

    /// Tries to add a route for a specific method.
    ///
    /// # Errors
    ///
    /// Returns a [`RouteError`] when `path` is not a valid Rustee route pattern.
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
        let order = routes.len();
        routes.push(Route {
            method,
            pattern,
            template: RouteTemplate::new(path),
            endpoint: Arc::new(HandlerEndpoint::<H, T> {
                handler,
                marker: PhantomData,
            }),
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
    /// Panics when `prefix` is not a valid non-root static path prefix. Use [`App::try_nest`] to
    /// handle configuration errors explicitly.
    pub fn nest<S>(self, prefix: &str, service: S) -> Self
    where
        S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
        S::Future: Send + 'static,
    {
        self.try_nest(prefix, service)
            .unwrap_or_else(|error| panic!("invalid Rustee nest prefix {prefix:?}: {error}"))
    }

    /// Tries to mount a Tower service below one static path prefix.
    ///
    /// Prefixes must start with `/`, contain at least one static segment, and cannot contain route
    /// parameters. The nested service owns every request below its prefix, including its fallback
    /// and method-mismatch responses. Parent and child [`StateStore`] values remain separate.
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
        nested_routes.push(NestedRoute {
            prefix,
            service: Arc::new(Mutex::new(BoxCloneService::new(service))),
            order,
        });
        Ok(self)
    }

    /// Sets the handler used when no route matches.
    pub fn fallback<T, H>(mut self, handler: H) -> Self
    where
        T: Send + Sync + 'static,
        H: Handler<T>,
    {
        self.fallback = Some(Arc::new(HandlerEndpoint::<H, T> {
            handler,
            marker: PhantomData,
        }));
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
        let path = request.uri().path().to_owned();
        let method = request.method().clone();
        let mut allowed_methods = BTreeSet::new();
        let mut candidate: Option<(&Route, RouteParams)> = None;

        for route in self.routes.iter() {
            let Some(params) = route.pattern.matches(&path) else {
                continue;
            };
            allowed_methods.insert(route.method.as_str().to_owned());
            if route.method != method {
                continue;
            }
            let replace = candidate
                .as_ref()
                .is_none_or(|(current, _)| compare_routes(route, current) == Ordering::Greater);
            if replace {
                candidate = Some((route, params));
            }
        }

        if let Some((route, params)) = candidate {
            let mut request = request;
            let template = route_template_for_request(&request, &route.template);
            request.extensions_mut().insert(template.clone());
            let response = route.endpoint.call(request, params, self.state.clone());
            return Box::pin(async move {
                let mut response = response.await;
                response.extensions_mut().insert(template.clone());
                response
                    .extensions_mut()
                    .insert(RouteClassification::matched(template));
                response
            });
        }

        if !allowed_methods.is_empty() {
            let allow = allowed_methods.into_iter().collect::<Vec<_>>().join(", ");
            return Box::pin(async move {
                let mut response = Error::new(
                    StatusCode::METHOD_NOT_ALLOWED,
                    "method_not_allowed",
                    "the route does not support this HTTP method",
                )
                .into_response();
                if let Ok(value) = HeaderValue::from_str(&allow) {
                    response.headers_mut().insert(ALLOW, value);
                }
                response
                    .extensions_mut()
                    .insert(RouteClassification::method_not_allowed());
                response
            });
        }

        let mut nested_candidate: Option<&NestedRoute> = None;
        for nested_route in self.nested_routes.iter() {
            if nested_route.prefix.strip(&path).is_none() {
                continue;
            }
            let replace = nested_candidate.as_ref().is_none_or(|current| {
                compare_nested_routes(nested_route, current) == Ordering::Greater
            });
            if replace {
                nested_candidate = Some(nested_route);
            }
        }
        if let Some(nested_route) = nested_candidate {
            return nested_route.call(request);
        }

        if let Some(fallback) = &self.fallback {
            let response = fallback.call(request, RouteParams::default(), self.state.clone());
            return Box::pin(async move {
                let mut response = response.await;
                response
                    .extensions_mut()
                    .insert(RouteClassification::fallback());
                response
            });
        }

        Box::pin(async {
            let mut response =
                Error::not_found("the requested route was not found").into_response();
            response
                .extensions_mut()
                .insert(RouteClassification::not_found());
            response
        })
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

fn compare_routes(left: &Route, right: &Route) -> Ordering {
    left.pattern
        .static_segments
        .cmp(&right.pattern.static_segments)
        .then_with(|| right.order.cmp(&left.order))
}

fn compare_nested_routes(left: &NestedRoute, right: &NestedRoute) -> Ordering {
    left.prefix
        .segment_count
        .cmp(&right.prefix.segment_count)
        .then_with(|| right.order.cmp(&left.order))
}

fn route_template_for_request(request: &Request, template: &RouteTemplate) -> RouteTemplate {
    request.extensions().get::<NestedRoutePrefix>().map_or_else(
        || template.clone(),
        |prefix| RouteTemplate::new(join_route_paths(&prefix.0, template.as_str())),
    )
}

fn join_route_paths(prefix: &str, path: &str) -> String {
    if prefix == "/" {
        path.to_owned()
    } else if path == "/" {
        prefix.to_owned()
    } else {
        format!("{prefix}{path}")
    }
}

fn replace_request_path(request: &mut Request, path: &str) -> bool {
    let path_and_query = match request.uri().query() {
        Some(query) => format!("{path}?{query}"),
        None => path.to_owned(),
    };
    let Ok(path_and_query) = PathAndQuery::from_str(&path_and_query) else {
        return false;
    };
    let mut parts = request.uri().clone().into_parts();
    parts.path_and_query = Some(path_and_query);
    let Ok(uri) = Uri::from_parts(parts) else {
        return false;
    };
    *request.uri_mut() = uri;
    true
}

#[cfg(test)]
mod tests {
    use http::{HeaderValue, Request as HttpRequest, StatusCode, header::CONTENT_TYPE};
    use http_body_util::BodyExt;
    use proptest::prelude::*;
    use rustee_core::{
        FromHeader, Header, Json, Path, Query, RouteClassification, RouteTemplate, State,
        empty_body, full_body,
    };
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Deserialize)]
    struct UserPath {
        id: u64,
    }

    #[derive(Serialize)]
    struct User {
        id: u64,
    }

    #[derive(Deserialize)]
    struct CreateUser {
        name: String,
    }

    #[derive(Serialize)]
    struct CreatedUser {
        name: String,
    }

    #[derive(Deserialize)]
    struct GreetingQuery {
        name: String,
    }

    struct GreetingState {
        prefix: String,
    }

    struct RequestId(String);

    impl FromHeader for RequestId {
        const NAME: &'static str = "x-request-id";

        fn from_header(value: &HeaderValue) -> rustee_core::Result<Self> {
            value
                .to_str()
                .map(|value| Self(value.to_owned()))
                .map_err(|_| rustee_core::Error::bad_request("invalid x-request-id header"))
        }
    }

    fn request(method: Method, uri: &str) -> Request {
        HttpRequest::builder()
            .method(method)
            .uri(uri)
            .body(empty_body())
            .unwrap()
    }

    #[tokio::test]
    async fn static_routes_outrank_parameter_routes() {
        let app = App::new()
            .get("/users/:id", |_path: Path<UserPath>| async { "parameter" })
            .get("/users/me", || async { "static" });

        let response = app.call(request(Method::GET, "/users/me")).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "static"
        );
    }

    #[tokio::test]
    async fn nest_strips_its_prefix_and_preserves_the_full_route_template() {
        let api = App::new().get(
            "/users/:id",
            |template: RouteTemplate,
             Path(path): Path<UserPath>,
             Query(query): Query<GreetingQuery>| async move {
                format!("{}:{}:{}", template.as_str(), path.id, query.name)
            },
        );
        let app = App::new().nest("/api", api);

        let response = app
            .call(request(Method::GET, "/api/users/42?name=Ada"))
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .extensions()
                .get::<RouteTemplate>()
                .map(RouteTemplate::as_str),
            Some("/api/users/:id")
        );
        assert_eq!(
            response
                .extensions()
                .get::<RouteClassification>()
                .map(RouteClassification::as_str),
            Some("/api/users/:id")
        );
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "/api/users/:id:42:Ada"
        );
    }

    #[tokio::test]
    async fn nested_router_owns_its_fallback_and_method_mismatch() {
        let api = App::new()
            .get("/users", || async { "api users" })
            .fallback(|| async { "api fallback" });
        let app = App::new()
            .nest("/api", api)
            .fallback(|| async { "parent fallback" });

        let response = app.call(request(Method::POST, "/api/users")).await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(response.headers()[ALLOW], "GET");
        assert_eq!(
            response
                .extensions()
                .get::<RouteClassification>()
                .map(RouteClassification::as_str),
            Some("<method-not-allowed>")
        );

        let response = app.call(request(Method::GET, "/api/missing")).await;
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "api fallback"
        );

        let response = app.call(request(Method::GET, "/outside")).await;
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "parent fallback"
        );
    }

    #[tokio::test]
    async fn most_specific_nested_prefix_and_direct_routes_outrank_broader_nests() {
        let broad = App::new().get("/v1/users", || async { "broad" });
        let specific = App::new().get("/users", || async { "specific" });
        let app = App::new()
            .nest("/api", broad)
            .nest("/api/v1", specific)
            .get("/api/v1/health", || async { "direct" });

        let response = app.call(request(Method::GET, "/api/v1/users")).await;
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "specific"
        );

        let response = app.call(request(Method::GET, "/api/v1/health")).await;
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "direct"
        );
    }

    #[tokio::test]
    async fn recursively_nested_apps_keep_one_full_external_route_template() {
        let users = App::new().get(
            "/:id",
            |template: RouteTemplate, Path(path): Path<UserPath>| async move {
                format!("{}:{}", template.as_str(), path.id)
            },
        );
        let versioned = App::new().nest("/v1", users);
        let app = App::new().nest("/api", versioned);

        let response = app.call(request(Method::GET, "/api/v1/42")).await;
        assert_eq!(
            response
                .extensions()
                .get::<RouteClassification>()
                .map(RouteClassification::as_str),
            Some("/api/v1/:id")
        );
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "/api/v1/:id:42"
        );
    }

    #[tokio::test]
    async fn generic_nested_service_receives_a_prefix_stripped_uri() {
        let service = tower::service_fn(|request: Request| async move {
            Ok::<_, Infallible>(request.uri().to_string().into_response())
        });
        let app = App::new().nest("/api", service);

        let response = app
            .call(request(Method::GET, "/api/resources?limit=2"))
            .await;
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "/resources?limit=2"
        );
    }

    #[test]
    fn nest_rejects_root_parameterized_and_malformed_prefixes() {
        for prefix in ["/", "/api/:tenant", "api"] {
            assert!(App::new().try_nest(prefix, App::new()).is_err());
        }
    }

    #[tokio::test]
    async fn path_extractor_deserializes_named_parameters() {
        let app = App::new().get("/users/:id", |Path(path): Path<UserPath>| async move {
            Json(User { id: path.id })
        });

        let response = app.call(request(Method::GET, "/users/42")).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            r#"{"id":42}"#
        );
    }

    #[tokio::test]
    async fn matched_route_template_is_available_to_handlers_and_response_layers() {
        let app = App::new().get("/users/:id", |template: RouteTemplate| async move {
            template.as_str().to_owned()
        });

        let response = app.call(request(Method::GET, "/users/42")).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .extensions()
                .get::<RouteTemplate>()
                .map(RouteTemplate::as_str),
            Some("/users/:id")
        );
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "/users/:id"
        );
    }

    #[tokio::test]
    async fn method_mismatch_returns_allow_header() {
        let app = App::new().get("/users", || async { "users" });
        let response = app.call(request(Method::POST, "/users")).await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(response.headers()[ALLOW], "GET");
        assert_eq!(
            response
                .extensions()
                .get::<RouteClassification>()
                .map(RouteClassification::as_str),
            Some("<method-not-allowed>")
        );
    }

    #[tokio::test]
    async fn unmatched_paths_use_a_reserved_observability_classification() {
        let response = App::new()
            .call(request(Method::GET, "/not-in-the-route-table"))
            .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response
                .extensions()
                .get::<RouteClassification>()
                .map(RouteClassification::as_str),
            Some("<not-found>")
        );
    }

    #[tokio::test]
    async fn json_extractor_requires_json_and_returns_typed_response() {
        let app = App::new().post("/users", |Json(user): Json<CreateUser>| async move {
            (StatusCode::CREATED, Json(CreatedUser { name: user.name }))
        });
        let request = HttpRequest::builder()
            .method(Method::POST)
            .uri("/users")
            .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
            .body(full_body(r#"{"name":"Ada"}"#))
            .unwrap();

        let response = app.call(request).await;
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            r#"{"name":"Ada"}"#
        );
    }

    #[tokio::test]
    async fn json_extractor_rejects_missing_content_type() {
        let app = App::new().post("/users", |_user: Json<CreateUser>| async { "created" });
        let request = HttpRequest::builder()
            .method(Method::POST)
            .uri("/users")
            .body(full_body(r#"{"name":"Ada"}"#))
            .unwrap();

        assert_eq!(
            app.call(request).await.status(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE
        );
    }

    #[tokio::test]
    async fn query_and_typed_state_are_available_to_handlers() {
        let app = App::new()
            .get(
                "/greeting",
                |State(state): State<GreetingState>, Query(query): Query<GreetingQuery>| async move {
                    format!("{}, {}", state.prefix, query.name)
                },
            )
            .with_state(GreetingState {
                prefix: String::from("Hello"),
            });

        let response = app
            .call(request(Method::GET, "/greeting?name=Rustee"))
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "Hello, Rustee"
        );
    }

    #[tokio::test]
    async fn typed_header_extractor_uses_the_declared_header_name() {
        let app = App::new().get("/request-id", |Header(id): Header<RequestId>| async move {
            id.0
        });
        let request = HttpRequest::builder()
            .method(Method::GET)
            .uri("/request-id")
            .header("x-request-id", "request-123")
            .body(empty_body())
            .unwrap();

        let response = app.call(request).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "request-123"
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(96))]

        #[test]
        fn route_pattern_parser_accepts_only_its_documented_grammar(
            path in prop::collection::vec(any::<char>(), 0..128)
                .prop_map(|characters| characters.into_iter().collect::<String>()),
        ) {
            let result = RoutePattern::parse(&path);
            if let Ok(pattern) = result {
                prop_assert!(path.starts_with('/'));
                prop_assert!(!path.contains('?'));
                prop_assert!(!path.contains('#'));
                prop_assert_eq!(pattern.matches(&path).is_some(), true);

                let segments = path
                    .trim_matches('/')
                    .split('/')
                    .filter(|segment| !segment.is_empty())
                    .collect::<Vec<_>>();
                prop_assert_eq!(pattern.segments.len(), segments.len());
                prop_assert_eq!(
                    pattern.static_segments,
                    segments.iter().filter(|segment| !segment.starts_with(':')).count(),
                );

                let mut names = BTreeSet::new();
                for (parsed, original) in pattern.segments.iter().zip(segments) {
                    match (parsed, original.strip_prefix(':')) {
                        (Segment::Static(value), None) => prop_assert_eq!(value, original),
                        (Segment::Parameter(parsed_name), Some(original_name)) => {
                            prop_assert!(!original_name.is_empty());
                            let valid_name = original_name.chars().all(|character| {
                                character.is_ascii_alphanumeric() || character == '_'
                            });
                            prop_assert!(valid_name);
                            prop_assert!(names.insert(original_name));
                            prop_assert_eq!(parsed_name, original_name);
                        }
                        _ => prop_assert!(false, "parser changed the route segment kind"),
                    }
                }
            }
        }

        #[test]
        fn parameter_matches_preserve_each_input_segment(
            user_id in "[^/]{1,64}",
            post_id in "[^/]{1,64}",
        ) {
            let pattern = RoutePattern::parse("/users/:user_id/posts/:post_id").unwrap();
            let path = format!("/users/{user_id}/posts/{post_id}");
            let params = pattern.matches(&path).expect("the generated path matches its template");
            let missing_post_id = format!("/users/{user_id}/posts");
            let extra_segment = format!("/users/{user_id}/posts/{post_id}/extra");

            prop_assert_eq!(params.get("user_id"), Some(user_id.as_str()));
            prop_assert_eq!(params.get("post_id"), Some(post_id.as_str()));
            prop_assert!(pattern.matches(&missing_post_id).is_none());
            prop_assert!(pattern.matches(&extra_segment).is_none());
        }

        #[test]
        fn static_routes_outrank_parameter_routes_for_every_valid_static_segment(
            segment in "[a-z0-9_]{1,48}",
        ) {
            let static_path = format!("/items/{segment}");
            let app = App::new()
                .get("/items/:id", || async { "parameter" })
                .get(&static_path, || async { "static" });
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime builds");
            let response = runtime.block_on(app.call(request(Method::GET, &static_path)));
            let classification = response
                .extensions()
                .get::<RouteClassification>()
                .map(|value| value.as_str().to_owned());

            prop_assert_eq!(classification, Some(static_path));
        }
    }
}
