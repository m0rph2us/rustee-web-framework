mod compression;
mod cors;
mod panic;
mod trusted_proxy;

use std::{
    convert::Infallible,
    future::{Ready, ready},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    task::{Context, Poll},
};

use http::{Method, Request as HttpRequest, StatusCode};
use rustee_core::{ConnectionInfo, IntoResponse, Request, Response, empty_body};
use tower::{Layer, Service, ServiceExt};

use crate::{
    CompressionLayer, CorsLayer, PanicCatchLayer, TrustedProxyLayer, TrustedProxyNetwork,
    TrustedProxyPolicy,
};

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

fn readiness_request() -> Request {
    let mut request = HttpRequest::builder()
        .method(Method::GET)
        .uri("/ready")
        .body(empty_body())
        .unwrap();
    request
        .extensions_mut()
        .insert(ConnectionInfo::new(SocketAddr::from(([127, 0, 0, 1], 443))));
    request
}

fn readiness_trusted_proxy_policy() -> TrustedProxyPolicy {
    TrustedProxyPolicy::new([
        TrustedProxyNetwork::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)), 8).unwrap(),
    ])
    .unwrap()
}

#[tokio::test]
async fn cloned_inner_services_are_readied_before_call() {
    let response = PanicCatchLayer::new()
        .layer(CloneRequiresReadiness::default())
        .oneshot(readiness_request())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = CorsLayer::new("https://app.example.test".parse().unwrap())
        .layer(CloneRequiresReadiness::default())
        .oneshot(readiness_request())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = CompressionLayer::new()
        .layer(CloneRequiresReadiness::default())
        .oneshot(readiness_request())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = TrustedProxyLayer::new(readiness_trusted_proxy_policy())
        .layer(CloneRequiresReadiness::default())
        .oneshot(readiness_request())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}
