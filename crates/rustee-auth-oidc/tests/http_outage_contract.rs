//! Real loopback transport-failure contracts for OIDC authentication adapters.

use std::{net::TcpListener, time::Duration};

use http::{Request as HttpRequest, StatusCode};
use http_body_util::BodyExt;
use jsonwebtoken::Algorithm;
use rustee_auth::AuthLayer;
use rustee_auth_oidc::{
    HttpJwksFetcher, HttpOpaqueTokenIntrospector, JwksAuthenticator, OidcClientAuthentication,
    OidcResourceServerConfig, OpaqueIntrospectionConfig, OpaqueTokenAuthenticator,
};
use rustee_core::empty_body;
use rustee_router::App;
use tower::{Layer, ServiceExt};
use url::Url;

const ISSUER: &str = "https://issuer.example.test";
const AUDIENCE: &str = "rustee-api";

#[tokio::test]
async fn jwks_transport_outage_is_a_sanitized_fail_closed_http_response() {
    let endpoint = unavailable_https_url("keys");
    let config =
        OidcResourceServerConfig::new(Algorithm::RS256, ISSUER, AUDIENCE, endpoint.clone())
            .unwrap();
    let verifier = JwksAuthenticator::new(
        config,
        HttpJwksFetcher::new(endpoint, Duration::from_millis(200)).unwrap(),
    );
    let response = AuthLayer::bearer(verifier)
        .layer(App::new().get("/protected", || async { "unexpected" }))
        .oneshot(bearer_request(
            "eyJhbGciOiJSUzI1NiIsImtpZCI6Im1pc3Npbmcta2V5In0.e30.signature",
        ))
        .await
        .unwrap();

    assert_sanitized_unavailable(response, "missing-key").await;
}

#[tokio::test]
async fn opaque_introspection_transport_outage_is_a_sanitized_fail_closed_http_response() {
    let endpoint = unavailable_https_url("oauth2/introspect");
    let config = OpaqueIntrospectionConfig::new(
        ISSUER,
        AUDIENCE,
        endpoint,
        "rustee-resource-server",
        OidcClientAuthentication::None,
    )
    .unwrap()
    .with_cache_ttl(Duration::ZERO);
    let verifier = OpaqueTokenAuthenticator::new(
        config,
        HttpOpaqueTokenIntrospector::new(Duration::from_millis(200)).unwrap(),
    );
    let response = AuthLayer::bearer(verifier)
        .layer(App::new().get("/protected", || async { "unexpected" }))
        .oneshot(bearer_request("opaque-credential-must-not-appear"))
        .await
        .unwrap();

    assert_sanitized_unavailable(response, "opaque-credential-must-not-appear").await;
}

fn unavailable_https_url(path: &str) -> Url {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    Url::parse(&format!("https://{address}/{path}")).unwrap()
}

fn bearer_request(token: &str) -> HttpRequest<rustee_core::Body> {
    HttpRequest::builder()
        .uri("/protected")
        .header("authorization", format!("Bearer {token}"))
        .body(empty_body())
        .unwrap()
}

async fn assert_sanitized_unavailable(response: rustee_core::Response, secret: &str) {
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("authentication_unavailable"));
    assert!(!body.contains(secret));
    assert!(!body.contains("127.0.0.1"));
}
