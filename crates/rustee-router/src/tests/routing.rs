use std::collections::BTreeSet;

use http::{Method, StatusCode, header::ALLOW};
use http_body_util::BodyExt;
use proptest::prelude::*;
use rustee_core::{RouteClassification, RouteTemplate};

use crate::pattern::Segment;
use crate::{App, RoutePattern};

use super::request;

#[tokio::test]
async fn static_routes_outrank_parameter_routes() {
    let app = App::new()
        .get("/users/:id", || async { "parameter" })
        .get("/users/me", || async { "static" });

    let response = app.call(request(Method::GET, "/users/me")).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "static"
    );
}

#[tokio::test]
async fn routes_do_not_match_repeated_path_separators() {
    let app = App::new().get("/users/:id", || async { "matched" });

    let response = app.call(request(Method::GET, "/users//42")).await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response
            .extensions()
            .get::<RouteClassification>()
            .map(RouteClassification::as_str),
        Some("<not-found>")
    );
}

#[test]
fn routes_reject_repeated_path_separators() {
    assert!(
        App::new()
            .try_route(Method::GET, "/users//:id", || async { "unreachable" })
            .is_err()
    );
}

#[test]
fn routes_reject_equivalent_patterns_for_the_same_method() {
    let error = App::new()
        .try_route(Method::GET, "/users/:id", || async { "first" })
        .expect("first route is valid")
        .try_route(Method::GET, "/users/:user_id", || async { "unreachable" })
        .expect_err("equivalent patterns must not silently shadow a handler");

    assert_eq!(
        error.to_string(),
        "a route for this method and path pattern is already registered"
    );
}

#[test]
fn convenience_builder_panics_for_an_equivalent_route() {
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = App::new()
            .get("/users/:id", || async { "first" })
            .get("/users/:user_id", || async { "unreachable" });
    }))
    .expect_err("equivalent convenience route must panic");

    assert_eq!(
        panic_message(panic.as_ref()),
        "invalid Rustee route: a route for this method and path pattern is already registered"
    );
}

#[test]
fn equivalent_patterns_remain_available_for_distinct_methods() {
    let app = App::new()
        .try_route(Method::GET, "/users/:id", || async { "get" })
        .expect("GET route is valid")
        .try_route(Method::POST, "/users/:user_id", || async { "post" });

    assert!(app.is_ok());
}

#[test]
fn convenience_builders_do_not_echo_invalid_route_or_prefix_values() {
    let route = "private-route-secret";
    let route_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = App::new().route(Method::GET, route, || async { "unreachable" });
    }))
    .expect_err("invalid route must panic");
    let route_message = panic_message(route_panic.as_ref());
    assert_eq!(
        route_message,
        "invalid Rustee route: route paths must start with '/'"
    );
    assert!(!route_message.contains(route));

    let prefix = "/private-nest-secret/:tenant";
    let prefix_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = App::new().nest(prefix, App::new());
    }))
    .expect_err("invalid nest prefix must panic");
    let prefix_message = panic_message(prefix_panic.as_ref());
    assert_eq!(
        prefix_message,
        "invalid Rustee nest prefix: nest prefixes cannot contain route parameters"
    );
    assert!(!prefix_message.contains(prefix));
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        return (*message).to_owned();
    }
    "non-string panic payload".to_owned()
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
            prop_assert!(!path.contains("//"));
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
