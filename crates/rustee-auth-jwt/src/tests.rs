use std::time::{SystemTime, UNIX_EPOCH};

use http::{Request as HttpRequest, StatusCode};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use rustee_auth::{AuthError, AuthLayer, AuthUser, BearerAuthenticator};
use rustee_core::empty_body;
use rustee_router::App;
use serde::Serialize;
use tower::{Layer, ServiceExt};

use crate::claims::{ScopeClaim, StringSetClaim, VerifiedClaims};
use crate::{JwtAuthenticator, JwtConfig, JwtConfigurationError};

const SECRET: &[u8] = b"unit-test-secret-with-sufficient-length";
const ISSUER: &str = "https://issuer.example.test";
const AUDIENCE: &str = "rustee-api";

#[derive(Serialize)]
struct Claims<'a> {
    sub: &'a str,
    iss: &'a str,
    aud: &'a str,
    exp: u64,
    nbf: u64,
    tenant: &'a str,
    scope: &'a str,
    roles: &'a [&'a str],
    permissions: &'a [&'a str],
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn token(algorithm: Algorithm, issuer: &str, audience: &str, exp: u64, nbf: u64) -> String {
    encode(
        &Header::new(algorithm),
        &Claims {
            sub: "alice",
            iss: issuer,
            aud: audience,
            exp,
            nbf,
            tenant: "acme",
            scope: "profile:read profile:write",
            roles: &["project-viewer"],
            permissions: &["project:read"],
        },
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap()
}

fn authenticator() -> JwtAuthenticator {
    JwtAuthenticator::from_hmac_secret(
        JwtConfig::new(Algorithm::HS256, ISSUER, AUDIENCE).unwrap(),
        SECRET,
    )
    .unwrap()
}

fn request(token: &str) -> rustee_core::Request {
    HttpRequest::builder()
        .method("GET")
        .uri("/me")
        .header("authorization", format!("Bearer {token}"))
        .body(empty_body())
        .unwrap()
}

#[tokio::test]
async fn authenticates_a_verified_jwt_into_a_principal() {
    let current = now();
    let principal = authenticator()
        .authenticate(&token(
            Algorithm::HS256,
            ISSUER,
            AUDIENCE,
            current + 300,
            current - 1,
        ))
        .await
        .unwrap();

    assert_eq!(principal.subject(), "alice");
    assert_eq!(principal.issuer(), Some(ISSUER));
    assert_eq!(principal.tenant(), Some("acme"));
    assert!(principal.has_scope("profile:read"));
    assert!(principal.has_scope("profile:write"));
    assert!(principal.has_role("project-viewer"));
    assert!(principal.has_permission("project:read"));
}

#[tokio::test]
async fn resource_server_layer_allows_only_a_verified_jwt() {
    let service = AuthLayer::bearer(authenticator()).layer(
        App::new().get("/me", |AuthUser(principal): AuthUser| async move {
            principal.subject().to_owned()
        }),
    );
    let current = now();
    let valid = service
        .clone()
        .oneshot(request(&token(
            Algorithm::HS256,
            ISSUER,
            AUDIENCE,
            current + 300,
            current - 1,
        )))
        .await
        .unwrap();
    let expired = service
        .oneshot(request(&token(
            Algorithm::HS256,
            ISSUER,
            AUDIENCE,
            current - 1,
            current - 300,
        )))
        .await
        .unwrap();

    assert_eq!(valid.status(), StatusCode::OK);
    assert_eq!(expired.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn rejects_a_signed_token_with_an_algorithm_outside_the_allowlist() {
    let current = now();
    let result = authenticator()
        .authenticate(&token(
            Algorithm::HS384,
            ISSUER,
            AUDIENCE,
            current + 300,
            current - 1,
        ))
        .await;

    assert_eq!(result.unwrap_err(), AuthError::RejectedBearerToken);
}

#[tokio::test]
async fn rejects_expired_or_wrong_audience_tokens() {
    let current = now();
    let expired = authenticator()
        .authenticate(&token(
            Algorithm::HS256,
            ISSUER,
            AUDIENCE,
            current - 1,
            current - 300,
        ))
        .await;
    let wrong_audience = authenticator()
        .authenticate(&token(
            Algorithm::HS256,
            ISSUER,
            "another-api",
            current + 300,
            current - 1,
        ))
        .await;

    assert_eq!(expired.unwrap_err(), AuthError::RejectedBearerToken);
    assert_eq!(wrong_audience.unwrap_err(), AuthError::RejectedBearerToken);
}

#[tokio::test]
async fn rejects_wrong_issuer_and_not_yet_valid_tokens() {
    let current = now();
    let wrong_issuer = authenticator()
        .authenticate(&token(
            Algorithm::HS256,
            "https://another-issuer.example.test",
            AUDIENCE,
            current + 300,
            current - 1,
        ))
        .await;
    let not_yet_valid = authenticator()
        .authenticate(&token(
            Algorithm::HS256,
            ISSUER,
            AUDIENCE,
            current + 300,
            current + 60,
        ))
        .await;

    assert_eq!(wrong_issuer.unwrap_err(), AuthError::RejectedBearerToken);
    assert_eq!(not_yet_valid.unwrap_err(), AuthError::RejectedBearerToken);
}

#[test]
fn rejects_a_key_type_that_does_not_match_the_algorithm() {
    let error = JwtAuthenticator::from_hmac_secret(
        JwtConfig::new(Algorithm::RS256, ISSUER, AUDIENCE).unwrap(),
        SECRET,
    )
    .unwrap_err();

    assert_eq!(
        error,
        JwtConfigurationError::AlgorithmKeyMismatch {
            algorithm: Algorithm::RS256,
            key_kind: "an HMAC secret",
        }
    );
}

#[test]
fn jwt_config_debug_redacts_issuer_and_audience() {
    let config = JwtConfig::new(
        Algorithm::HS256,
        "https://private-issuer.example.test",
        "private-audience",
    )
    .unwrap()
    .with_leeway_seconds(42)
    .unwrap();

    let debug = format!("{config:?}");
    assert!(debug.contains("algorithm: HS256"));
    assert!(debug.contains("issuer_configured: true"));
    assert!(debug.contains("audience_configured: true"));
    assert!(debug.contains("leeway_seconds: 42"));
    assert!(!debug.contains("private-issuer"));
    assert!(!debug.contains("private-audience"));
}

#[test]
fn parsed_claim_debug_redacts_identity_and_authorization_values() {
    let claims = VerifiedClaims {
        sub: "private-subject".to_owned(),
        iss: "https://private-issuer.example.test".to_owned(),
        audience_claim: serde_json::json!("private-audience"),
        expiration_claim: serde_json::json!(1_234_567_890),
        not_before_claim: serde_json::json!(1_234_567_000),
        tenant: Some("private-tenant".to_owned()),
        scope: Some(ScopeClaim::SpaceDelimited(
            "private:read private:write".to_owned(),
        )),
        roles: Some(StringSetClaim::Values(vec!["private-role".to_owned()])),
        permissions: Some(StringSetClaim::One("private:permission".to_owned())),
    };

    let debug = format!("{claims:?}");
    for value in [
        "private-subject",
        "private-issuer",
        "private-audience",
        "1234567890",
        "1234567000",
        "private-tenant",
        "private:read",
        "private-role",
        "private:permission",
    ] {
        assert!(
            !debug.contains(value),
            "Debug output must not include {value:?}"
        );
    }
    assert!(debug.contains("has_permissions: true"));
}

#[test]
fn jwt_configuration_bounds_trusted_fields_and_clock_skew() {
    assert_eq!(
        JwtConfig::new(Algorithm::HS256, "i".repeat(2 * 1024 + 1), AUDIENCE),
        Err(JwtConfigurationError::InvalidField { field: "issuer" })
    );
    assert_eq!(
        JwtConfig::new(Algorithm::HS256, ISSUER, "a".repeat(1024 + 1)),
        Err(JwtConfigurationError::InvalidField { field: "audience" })
    );
    assert_eq!(
        JwtConfig::new(Algorithm::HS256, ISSUER, AUDIENCE)
            .unwrap()
            .with_leeway_seconds(301),
        Err(JwtConfigurationError::LeewayTooLarge)
    );
}

#[test]
fn jwt_key_admission_precedes_expensive_key_parsing() {
    let oversized_hmac = JwtAuthenticator::from_hmac_secret(
        JwtConfig::new(Algorithm::HS256, ISSUER, AUDIENCE).unwrap(),
        vec![b'h'; 4 * 1024 + 1],
    );
    assert!(matches!(
        oversized_hmac,
        Err(JwtConfigurationError::VerificationKeyTooLarge)
    ));

    let oversized_rsa = JwtAuthenticator::from_rsa_pem(
        JwtConfig::new(Algorithm::RS256, ISSUER, AUDIENCE).unwrap(),
        &vec![b'p'; 16 * 1024 + 1],
    );
    assert!(matches!(
        oversized_rsa,
        Err(JwtConfigurationError::VerificationKeyTooLarge)
    ));
}

#[test]
fn jwt_hmac_key_admission_enforces_the_selected_algorithm_minimum() {
    for (algorithm, minimum_bytes) in [
        (Algorithm::HS256, 32),
        (Algorithm::HS384, 48),
        (Algorithm::HS512, 64),
    ] {
        let short = JwtAuthenticator::from_hmac_secret(
            JwtConfig::new(algorithm, ISSUER, AUDIENCE).unwrap(),
            vec![b'k'; minimum_bytes - 1],
        );
        assert!(matches!(
            short,
            Err(JwtConfigurationError::VerificationKeyTooShort {
                minimum_bytes: actual_minimum_bytes,
            }) if actual_minimum_bytes == minimum_bytes
        ));

        assert!(
            JwtAuthenticator::from_hmac_secret(
                JwtConfig::new(algorithm, ISSUER, AUDIENCE).unwrap(),
                vec![b'k'; minimum_bytes],
            )
            .is_ok()
        );
    }
}
