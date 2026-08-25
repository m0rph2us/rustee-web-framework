use std::{
    cmp::Ordering,
    convert::Infallible,
    str::FromStr,
    sync::{Arc, Mutex},
};

use futures_util::future::BoxFuture;
use http::{StatusCode, Uri, uri::PathAndQuery};
use rustee_core::{BoxCloneServiceExt, Error, IntoResponse, Request, Response, RouteTemplate};
use tower::{Service, util::BoxCloneService};

use super::pattern::NestedPrefix;

#[derive(Clone, Debug)]
struct NestedRoutePrefix(String);

#[derive(Clone)]
pub(super) struct NestedRoute {
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
    pub(super) fn new<S>(prefix: NestedPrefix, service: S, order: usize) -> Self
    where
        S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
        S::Future: Send + 'static,
    {
        Self {
            prefix,
            service: Arc::new(Mutex::new(BoxCloneService::new(service))),
            order,
        }
    }

    pub(super) fn matches(&self, path: &str) -> bool {
        self.prefix.strip(path).is_some()
    }

    pub(super) fn is_more_specific_than(&self, other: &Self) -> bool {
        self.prefix
            .segment_count
            .cmp(&other.prefix.segment_count)
            .then_with(|| other.order.cmp(&self.order))
            == Ordering::Greater
    }

    pub(super) fn call(&self, mut request: Request) -> BoxFuture<'static, Response> {
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
        let service = match self.service.lock() {
            Ok(service) => service.clone(),
            Err(_) => {
                return Box::pin(async {
                    Error::new(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "nested_service_unavailable",
                        "the nested service is unavailable",
                    )
                    .into_response()
                });
            }
        };
        Box::pin(async move {
            match service.call_ready(request).await {
                Ok(response) => response,
                Err(never) => match never {},
            }
        })
    }

    #[cfg(test)]
    pub(super) fn poison_for_test(&self) {
        let _guard = self
            .service
            .lock()
            .expect("nested service lock must be available");
        panic!("test-only nested service poison");
    }
}

pub(super) fn route_template_for_request(
    request: &Request,
    template: &RouteTemplate,
) -> RouteTemplate {
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
