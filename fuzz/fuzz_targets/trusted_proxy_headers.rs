#![no_main]

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::OnceLock,
};

use http::{HeaderValue, Request as HttpRequest, header::FORWARDED};
use libfuzzer_sys::fuzz_target;
use rustee_core::{ConnectionInfo, empty_body};
use rustee_middleware::{TrustedProxyLayer, TrustedProxyNetwork, TrustedProxyPolicy};
use rustee_router::App;
use tokio::runtime::Runtime;
use tower::{Layer, ServiceExt};

const MAX_HEADER_BYTES: usize = 2 * 1024;

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| Runtime::new().expect("create trusted-proxy fuzz runtime"))
}

fn trusted_policy() -> TrustedProxyPolicy {
    TrustedProxyPolicy::new([
        TrustedProxyNetwork::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)), 8)
            .expect("fixed trusted-proxy network is valid"),
    ])
    .expect("fixed trusted-proxy policy is valid")
}

fn request() -> rustee_core::Request {
    let mut request = HttpRequest::builder()
        .method("GET")
        .uri("/context")
        .body(empty_body())
        .expect("trusted-proxy fuzz request is valid");
    request
        .extensions_mut()
        .insert(ConnectionInfo::new(SocketAddr::from(([10, 2, 3, 4], 443))));
    request
}

fn fuzz_forwarded(value: HeaderValue) {
    let service = TrustedProxyLayer::new(trusted_policy())
        .layer(App::new().get("/context", || async { "ok" }));
    let mut request = request();
    request.headers_mut().insert(FORWARDED, value);

    let _ = runtime().block_on(service.oneshot(request));
}

fn fuzz_x_forwarded(values: &[&[u8]]) {
    let service = TrustedProxyLayer::new(trusted_policy())
        .with_x_forwarded()
        .layer(App::new().get("/context", || async { "ok" }));
    let mut request = request();
    for (name, value) in [
        ("x-forwarded-for", values[0]),
        ("x-forwarded-proto", values[1]),
        ("x-forwarded-host", values[2]),
    ] {
        if let Ok(value) = HeaderValue::from_bytes(value) {
            request.headers_mut().insert(name, value);
        }
    }

    let _ = runtime().block_on(service.oneshot(request));
}

fuzz_target!(|data: &[u8]| {
    let Some((&mode, value)) = data.split_first() else {
        return;
    };
    if value.len() > MAX_HEADER_BYTES {
        return;
    }

    if mode % 2 == 0 {
        let Ok(value) = HeaderValue::from_bytes(value) else {
            return;
        };
        fuzz_forwarded(value);
        return;
    }

    let mut values = value.splitn(3, |byte| *byte == 0);
    let x_forwarded_for = values.next().unwrap_or_default();
    let x_forwarded_proto = values.next().unwrap_or_default();
    let x_forwarded_host = values.next().unwrap_or_default();
    fuzz_x_forwarded(&[x_forwarded_for, x_forwarded_proto, x_forwarded_host]);
});
