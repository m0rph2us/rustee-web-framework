use std::time::Duration;

use jsonwebtoken::Algorithm;
use url::Url;

use crate::{
    HttpJwksFetcher, JwksAuthenticator, OidcResourceServerConfig,
    claims::{Claims, IdTokenClaims},
};

use super::support::{FakeFetcher, FetchError};

#[test]
fn jwks_fetcher_debug_redacts_provider_endpoint() {
    let fetcher = HttpJwksFetcher::new(
        Url::parse("https://private-jwks-fetcher.example.test/keys")
            .expect("test URL must be valid"),
        Duration::from_secs(1),
    )
    .expect("valid JWKS fetcher");

    let debug = format!("{fetcher:?}");
    assert!(debug.contains("url: \"[REDACTED]\""));
    assert!(!debug.contains("private-jwks-fetcher.example.test"));
}

#[test]
fn jwks_authenticator_debug_redacts_issuer() {
    let config = OidcResourceServerConfig::new(
        Algorithm::RS256,
        "https://private-authenticator-issuer.example.test",
        "private-authenticator-audience",
        Url::parse("https://private-authenticator-jwks.example.test/keys")
            .expect("test URL must be valid"),
    )
    .expect("test configuration must be valid");
    let authenticator = JwksAuthenticator::new(config, FakeFetcher::new([Err(FetchError)]));

    let debug = format!("{authenticator:?}");
    assert!(!debug.contains("private-authenticator-issuer.example.test"));
    assert!(!debug.contains("private-authenticator-audience"));
}

#[test]
fn parsed_claim_debug_redacts_identity_and_authorization_values() {
    let access_claims: Claims = serde_json::from_value(serde_json::json!({
        "sub": "private-subject",
        "iss": "https://private-issuer.example.test",
        "aud": "private-audience",
        "exp": 1_234,
        "nbf": 1_000,
        "tenant": "private-tenant",
        "scope": "private:read private:write",
        "roles": ["private-role"],
        "permissions": ["private:permission"],
    }))
    .unwrap();
    let id_token_claims: IdTokenClaims = serde_json::from_value(serde_json::json!({
        "sub": "private-id-subject",
        "iss": "https://private-id-issuer.example.test",
        "aud": "private-id-audience",
        "exp": 1_234,
        "nbf": 1_000,
        "iat": 1_000,
        "nonce": "private-nonce",
        "azp": "private-authorized-party",
        "tenant": "private-id-tenant",
        "scope": ["private:id:read"],
        "roles": "private-id-role",
        "permissions": "private:id:permission",
    }))
    .unwrap();

    let debug = format!("{access_claims:?}{id_token_claims:?}");

    assert!(debug.contains("has_subject: true"));
    assert!(debug.contains("has_nonce: true"));
    for value in [
        "private-subject",
        "private-issuer.example.test",
        "private-audience",
        "private-tenant",
        "private:read",
        "private-role",
        "private:permission",
        "private-id-subject",
        "private-id-issuer.example.test",
        "private-id-audience",
        "private-nonce",
        "private-authorized-party",
        "private-id-tenant",
        "private:id:read",
        "private-id-role",
        "private:id:permission",
    ] {
        assert!(!debug.contains(value));
    }
}
