use std::{
    convert::Infallible,
    future::{Ready, ready},
    task::{Context, Poll},
};

use http::{Method, StatusCode, header::ALLOW};
use http_body_util::BodyExt;
use rustee_core::{
    IntoResponse, Path, Query, Request, Response, RouteClassification, RouteTemplate,
};
use serde::Deserialize;
use tower::Service;

use crate::App;

use super::request;

#[derive(Deserialize)]
struct UserPath {
    id: u64,
}

#[derive(Deserialize)]
struct GreetingQuery {
    name: String,
}

#[derive(Default)]
struct CloneRequiresReadiness {
    ready: bool,
}

impl Clone for CloneRequiresReadiness {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl Service<Request> for CloneRequiresReadiness {
    type Response = Response;
    type Error = Infallible;
    type Future = Ready<Result<Response, Infallible>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.ready = true;
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: Request) -> Self::Future {
        assert!(self.ready, "clone-local poll_ready must precede call");
        self.ready = false;
        ready(Ok("ready".into_response()))
    }
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
async fn direct_parent_method_mismatch_outranks_a_nested_service() {
    let api = App::new().get("/users", || async { "child users" });
    let app = App::new()
        .post("/api/users", || async { "parent users" })
        .nest("/api", api);

    let response = app.call(request(Method::GET, "/api/users")).await;
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(response.headers()[ALLOW], "POST");
    assert_eq!(
        response
            .extensions()
            .get::<RouteClassification>()
            .map(RouteClassification::as_str),
        Some("<method-not-allowed>")
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

#[tokio::test]
async fn generic_nested_service_readies_a_clone_before_call() {
    let app = App::new().nest("/api", CloneRequiresReadiness::default());

    let response = app.call(request(Method::GET, "/api/ready")).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "ready"
    );
}

#[tokio::test]
async fn poisoned_nested_service_state_fails_closed() {
    let app = App::new().nest("/api", App::new().get("/health", || async { "healthy" }));
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        app.nested_routes[0].poison_for_test();
    }));
    assert!(poisoned.is_err());

    let response = app.call(request(Method::GET, "/api/health")).await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert!(!std::str::from_utf8(&body).unwrap().contains("poison"));
}

#[test]
fn nest_rejects_root_parameterized_and_malformed_prefixes() {
    for prefix in ["/", "/api/:tenant", "/api//v1", "api"] {
        assert!(App::new().try_nest(prefix, App::new()).is_err());
    }
}
