use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    str::FromStr,
};

use http::{Request as HttpRequest, StatusCode, header::FORWARDED};
use http_body_util::BodyExt;
use rustee_core::{ConnectionInfo, empty_body};
use rustee_router::App;
use tower::{Layer, ServiceExt};

use crate::{
    ForwardedContext, MAX_FORWARDED_CHAIN_HOPS, MAX_TRUSTED_PROXY_NETWORKS, TrustedProxyError,
    TrustedProxyLayer, TrustedProxyNetwork, TrustedProxyPolicy, X_FORWARDED_FOR, X_FORWARDED_HOST,
    X_FORWARDED_PROTO,
};

fn request(peer: IpAddr, forwarded: Option<&str>) -> rustee_core::Request {
    let mut builder = HttpRequest::builder().method("GET").uri("/context");
    if let Some(forwarded) = forwarded {
        builder = builder.header(FORWARDED, forwarded);
    }
    let mut request = builder.body(empty_body()).unwrap();
    request
        .extensions_mut()
        .insert(ConnectionInfo::new(SocketAddr::new(peer, 443)));
    request
}

fn x_forwarded_request(
    peer: IpAddr,
    client: Option<&str>,
    scheme: Option<&str>,
    host: Option<&str>,
) -> rustee_core::Request {
    let mut builder = HttpRequest::builder().method("GET").uri("/context");
    if let Some(client) = client {
        builder = builder.header(X_FORWARDED_FOR, client);
    }
    if let Some(scheme) = scheme {
        builder = builder.header(X_FORWARDED_PROTO, scheme);
    }
    if let Some(host) = host {
        builder = builder.header(X_FORWARDED_HOST, host);
    }
    let mut request = builder.body(empty_body()).unwrap();
    request
        .extensions_mut()
        .insert(ConnectionInfo::new(SocketAddr::new(peer, 443)));
    request
}

fn trusted_policy() -> TrustedProxyPolicy {
    TrustedProxyPolicy::new([
        TrustedProxyNetwork::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)), 8).unwrap(),
    ])
    .unwrap()
}

fn multi_hop_policy() -> TrustedProxyPolicy {
    TrustedProxyPolicy::new([
        TrustedProxyNetwork::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)), 8).unwrap(),
        TrustedProxyNetwork::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 0)), 24).unwrap(),
    ])
    .unwrap()
    .with_forwarded_chain_hops(1)
    .unwrap()
}

#[test]
fn trusted_proxy_networks_are_bounded_deduplicated_and_redacted_in_debug_output() {
    let network = TrustedProxyNetwork::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)), 8).unwrap();
    let policy = TrustedProxyPolicy::new([network, network]).unwrap();
    let network_debug = format!("{network:?}");
    let debug = format!("{policy:?}");

    assert!(network_debug.contains("address_family: \"ipv4\""));
    assert!(network_debug.contains("prefix_length: 8"));
    assert!(!network_debug.contains("10.0.0.0"));
    assert!(debug.contains("trusted_network_count: 1"));
    assert!(!debug.contains("10.0.0.0"));

    let networks = (0..=MAX_TRUSTED_PROXY_NETWORKS)
        .map(|index| {
            let index = u8::try_from(index).expect("test network count fits an IPv4 octet");
            TrustedProxyNetwork::new(IpAddr::V4(Ipv4Addr::new(198, 51, 100, index)), 32).unwrap()
        })
        .chain(std::iter::once_with(|| {
            panic!("trusted proxy policy must stop after the first excess network")
        }));
    assert_eq!(
        TrustedProxyPolicy::new(networks).unwrap_err(),
        TrustedProxyError::NetworkAllowlistLimit
    );
}

#[tokio::test]
async fn trusted_proxy_normalizes_one_forwarded_hop() {
    let service = TrustedProxyLayer::new(trusted_policy()).layer(App::new().get(
        "/context",
        |context: ForwardedContext| async move {
            format!(
                "{}:{}:{}",
                context.client_ip(),
                context.scheme().unwrap(),
                context.host().unwrap()
            )
        },
    ));
    let response = service
        .oneshot(request(
            IpAddr::from_str("10.2.3.4").unwrap(),
            Some("for=203.0.113.10;proto=https;host=app.example.test"),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn forwarded_context_debug_redacts_client_and_host_values() {
    let service = TrustedProxyLayer::new(trusted_policy()).layer(
        App::new().get("/context", |context: ForwardedContext| async move {
            format!("{context:?}")
        }),
    );
    let response = service
        .oneshot(request(
            IpAddr::from_str("10.2.3.4").unwrap(),
            Some("for=203.0.113.10;proto=https;host=private.example.test"),
        ))
        .await
        .unwrap();
    let debug = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();

    assert!(debug.contains("client_ip: \"[REDACTED]\""));
    assert!(debug.contains("has_scheme: true"));
    assert!(debug.contains("has_host: true"));
    assert!(!debug.contains("203.0.113.10"));
    assert!(!debug.contains("private.example.test"));
    assert!(!debug.contains("https"));
}

#[tokio::test]
async fn trusted_proxy_can_select_a_client_behind_one_trusted_intermediary() {
    let service = TrustedProxyLayer::new(multi_hop_policy()).layer(App::new().get(
        "/context",
        |context: ForwardedContext| async move {
            format!(
                "{}:{}:{}",
                context.client_ip(),
                context.scheme().unwrap(),
                context.host().unwrap()
            )
        },
    ));
    let response = service
        .oneshot(request(
            IpAddr::from_str("10.2.3.4").unwrap(),
            Some("for=203.0.113.10, for=192.0.2.7;proto=https;host=app.example.test"),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "203.0.113.10:https:app.example.test"
    );
}

#[tokio::test]
async fn x_forwarded_family_is_explicit_and_normalizes_a_trusted_chain() {
    let handler = |context: ForwardedContext| async move {
        format!(
            "{}:{}:{}",
            context.client_ip(),
            context.scheme().unwrap(),
            context.host().unwrap()
        )
    };
    let service = TrustedProxyLayer::new(multi_hop_policy())
        .with_x_forwarded()
        .layer(App::new().get("/context", handler));
    let response = service
        .clone()
        .oneshot(x_forwarded_request(
            IpAddr::from_str("10.2.3.4").unwrap(),
            Some("203.0.113.10, 192.0.2.7"),
            Some("https"),
            Some("app.example.test"),
        ))
        .await
        .unwrap();
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "203.0.113.10:https:app.example.test"
    );
    assert_eq!(
        service
            .oneshot(request(
                IpAddr::from_str("10.2.3.4").unwrap(),
                Some("for=203.0.113.10;proto=https;host=app.example.test"),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );

    let default = TrustedProxyLayer::new(trusted_policy()).layer(App::new().get(
        "/context",
        |_: ForwardedContext| async move { "unexpected" },
    ));
    assert_eq!(
        default
            .oneshot(x_forwarded_request(
                IpAddr::from_str("10.2.3.4").unwrap(),
                Some("203.0.113.10"),
                Some("https"),
                Some("app.example.test"),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[tokio::test]
async fn x_forwarded_rejects_duplicate_or_incomplete_trusted_headers() {
    let service = TrustedProxyLayer::new(trusted_policy())
        .with_x_forwarded()
        .layer(App::new().get("/context", || async { "unexpected" }));
    let mut duplicate = x_forwarded_request(
        IpAddr::from_str("10.2.3.4").unwrap(),
        Some("203.0.113.10"),
        None,
        None,
    );
    duplicate
        .headers_mut()
        .append(X_FORWARDED_FOR, "203.0.113.11".parse().unwrap());
    assert_eq!(
        service.clone().oneshot(duplicate).await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        service
            .oneshot(x_forwarded_request(
                IpAddr::from_str("10.2.3.4").unwrap(),
                None,
                Some("https"),
                None,
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn x_forwarded_rejects_malformed_scheme_or_host() {
    let service = TrustedProxyLayer::new(trusted_policy())
        .with_x_forwarded()
        .layer(App::new().get("/context", || async { "unexpected" }));
    assert_eq!(
        service
            .clone()
            .oneshot(x_forwarded_request(
                IpAddr::from_str("10.2.3.4").unwrap(),
                Some("203.0.113.10"),
                Some("HTTPS"),
                None,
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        service
            .oneshot(x_forwarded_request(
                IpAddr::from_str("10.2.3.4").unwrap(),
                Some("203.0.113.10"),
                None,
                Some("user@app.example.test"),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn x_forwarded_rejects_an_oversized_host_before_context_injection() {
    let service = TrustedProxyLayer::new(trusted_policy())
        .with_x_forwarded()
        .layer(App::new().get("/context", || async { "unexpected" }));
    let oversized_host = format!("{}.example.test", "a".repeat(2_048));

    let response = service
        .oneshot(x_forwarded_request(
            IpAddr::from_str("10.2.3.4").unwrap(),
            Some("203.0.113.10"),
            Some("https"),
            Some(&oversized_host),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn single_hop_policy_rejects_chain_or_non_edge_scheme_metadata() {
    let service = TrustedProxyLayer::new(trusted_policy())
        .layer(App::new().get("/context", || async { "unexpected" }));
    let chained = service
        .clone()
        .oneshot(request(
            IpAddr::from_str("10.2.3.4").unwrap(),
            Some("for=203.0.113.10, for=192.0.2.7"),
        ))
        .await
        .unwrap();
    assert_eq!(chained.status(), StatusCode::BAD_REQUEST);

    let conflicting = service
        .oneshot(request(
            IpAddr::from_str("10.2.3.4").unwrap(),
            Some("for=203.0.113.10;proto=https, for=192.0.2.7"),
        ))
        .await
        .unwrap();
    assert_eq!(conflicting.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn forwarded_chain_hops_are_explicit_and_bounded() {
    assert_eq!(
        trusted_policy().with_forwarded_chain_hops(0).unwrap_err(),
        TrustedProxyError::InvalidForwardedChainHops
    );
    assert_eq!(
        trusted_policy()
            .with_forwarded_chain_hops(MAX_FORWARDED_CHAIN_HOPS + 1)
            .unwrap_err(),
        TrustedProxyError::InvalidForwardedChainHops
    );
}

#[tokio::test]
async fn untrusted_peer_cannot_spoof_forwarded_context() {
    let service = TrustedProxyLayer::new(trusted_policy()).layer(App::new().get(
        "/context",
        |_: ForwardedContext| async move { "unexpected" },
    ));
    let response = service
        .oneshot(request(
            IpAddr::from_str("198.51.100.7").unwrap(),
            Some("for=203.0.113.10;proto=https;host=app.example.test"),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), 500);
}

#[tokio::test]
async fn malformed_header_from_trusted_proxy_is_rejected() {
    let service = TrustedProxyLayer::new(trusted_policy())
        .layer(App::new().get("/context", || async { "unexpected" }));
    let response = service
        .oneshot(request(
            IpAddr::from_str("10.2.3.4").unwrap(),
            Some("for=not-an-ip;proto=https"),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
}

#[tokio::test]
async fn duplicate_header_from_trusted_proxy_is_rejected() {
    let service = TrustedProxyLayer::new(trusted_policy())
        .layer(App::new().get("/context", || async { "unexpected" }));
    let mut request = request(
        IpAddr::from_str("10.2.3.4").unwrap(),
        Some("for=203.0.113.10;proto=https"),
    );
    request
        .headers_mut()
        .append(FORWARDED, "for=203.0.113.11;proto=https".parse().unwrap());
    let response = service.oneshot(request).await.unwrap();
    assert_eq!(response.status(), 400);
}
