//! Route selection and response classification for an already-built application.

use std::{cmp::Ordering, collections::BTreeSet};

use futures_util::future::BoxFuture;
use http::{HeaderValue, Method, StatusCode, header::ALLOW};
use rustee_core::{Error, IntoResponse, Request, Response, RouteClassification, RouteParams};

use super::{
    app::{App, Route},
    nesting::{NestedRoute, route_template_for_request},
};

enum DirectRouteSelection<'a> {
    Matched(&'a Route, RouteParams),
    MethodNotAllowed(BTreeSet<&'a str>),
    NoMatch,
}

pub(super) fn dispatch(app: &App, request: Request) -> BoxFuture<'static, Response> {
    let path = request.uri().path().to_owned();
    let method = request.method().clone();
    match select_direct_route(&app.routes, &path, &method) {
        DirectRouteSelection::Matched(route, params) => {
            let mut request = request;
            let template = route_template_for_request(&request, &route.template);
            request.extensions_mut().insert(template.clone());
            let response = route.endpoint.call(request, params, app.state.clone());
            return Box::pin(async move {
                let mut response = response.await;
                response.extensions_mut().insert(template.clone());
                response
                    .extensions_mut()
                    .insert(RouteClassification::matched(template));
                response
            });
        }
        DirectRouteSelection::MethodNotAllowed(allowed_methods) => {
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
        DirectRouteSelection::NoMatch => {}
    }

    if let Some(nested_route) = select_nested_route(&app.nested_routes, &path) {
        return nested_route.call(request);
    }

    if let Some(fallback) = &app.fallback {
        let response = fallback.call(request, RouteParams::default(), app.state.clone());
        return Box::pin(async move {
            let mut response = response.await;
            response
                .extensions_mut()
                .insert(RouteClassification::fallback());
            response
        });
    }

    Box::pin(async {
        let mut response = Error::not_found("the requested route was not found").into_response();
        response
            .extensions_mut()
            .insert(RouteClassification::not_found());
        response
    })
}

fn select_nested_route<'a>(routes: &'a [NestedRoute], path: &str) -> Option<&'a NestedRoute> {
    let mut candidate = None;
    for route in routes {
        if !route.matches(path) {
            continue;
        }
        if candidate.is_none_or(|current| route.is_more_specific_than(current)) {
            candidate = Some(route);
        }
    }
    candidate
}

fn compare_routes(left: &Route, right: &Route) -> Ordering {
    left.pattern
        .static_segments
        .cmp(&right.pattern.static_segments)
        .then_with(|| right.order.cmp(&left.order))
}

fn select_direct_route<'a>(
    routes: &'a [Route],
    path: &str,
    method: &Method,
) -> DirectRouteSelection<'a> {
    let mut allowed_methods = BTreeSet::new();
    let mut candidate: Option<(&Route, RouteParams)> = None;

    for route in routes {
        let Some(params) = route.pattern.matches(path) else {
            continue;
        };
        allowed_methods.insert(route.method.as_str());
        if route.method != *method {
            continue;
        }
        let replace = candidate
            .as_ref()
            .is_none_or(|(current, _)| compare_routes(route, current) == Ordering::Greater);
        if replace {
            candidate = Some((route, params));
        }
    }

    match candidate {
        Some((route, params)) => DirectRouteSelection::Matched(route, params),
        None if !allowed_methods.is_empty() => {
            DirectRouteSelection::MethodNotAllowed(allowed_methods)
        }
        None => DirectRouteSelection::NoMatch,
    }
}
