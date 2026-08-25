use std::time::Duration;

use jsonwebtoken::jwk::{JwkSet, KeyAlgorithm, PublicKeyUse};
use rustee_auth::{AuthError, BearerAuthenticator};

use crate::{IdTokenVerifier, JwksAuthenticator};

use super::support::{
    FakeFetcher, FetchError, ISSUER, config, id_token, id_token_with_issued_at, jwk, jwks, now,
    token,
};

#[tokio::test]
async fn verifies_a_remote_jwks_token_and_reuses_a_fresh_key() {
    let fetcher = FakeFetcher::new([Ok(jwks("primary"))]);
    let authenticator = JwksAuthenticator::new(config(), fetcher.clone());

    let principal = authenticator
        .authenticate(&token(Some("primary")))
        .await
        .expect("matching JWK must validate the signature");
    authenticator
        .authenticate(&token(Some("primary")))
        .await
        .expect("fresh JWK must be cached");

    assert_eq!(principal.subject(), "alice");
    assert_eq!(principal.issuer(), Some(ISSUER));
    assert_eq!(principal.tenant(), Some("acme"));
    assert!(principal.has_scope("profile:read"));
    assert!(principal.has_role("project-viewer"));
    assert!(principal.has_permission("project:read"));
    assert_eq!(fetcher.calls(), 1);
}

#[tokio::test]
async fn unknown_kid_refreshes_once_and_accepts_a_rotated_key() {
    let fetcher = FakeFetcher::new([Ok(jwks("old")), Ok(jwks("rotated"))]);
    let authenticator = JwksAuthenticator::new(config(), fetcher.clone());

    authenticator
        .refresh()
        .await
        .expect("initial JWKS fetch must work");
    let principal = authenticator
        .authenticate(&token(Some("rotated")))
        .await
        .expect("unknown rotated kid must trigger one refresh");

    assert_eq!(principal.subject(), "alice");
    assert_eq!(fetcher.calls(), 2);

    let unknown = authenticator
        .authenticate(&token(Some("unrecognized")))
        .await;
    assert_eq!(unknown.unwrap_err(), AuthError::RejectedBearerToken);
    assert_eq!(fetcher.calls(), 2);
}

#[tokio::test]
async fn rejects_missing_or_untrusted_jwk_keys_without_accepting_the_token() {
    let fetcher = FakeFetcher::new([Ok(jwks("primary"))]);
    let authenticator = JwksAuthenticator::new(config(), fetcher.clone());
    let missing_kid = authenticator.authenticate(&token(None)).await;

    assert_eq!(missing_kid.unwrap_err(), AuthError::RejectedBearerToken);
    assert_eq!(fetcher.calls(), 0);

    let mut encryption_key = jwk("encryption");
    encryption_key.common.public_key_use = Some(PublicKeyUse::Encryption);
    encryption_key.common.key_algorithm = Some(KeyAlgorithm::RS256);
    let untrusted = JwksAuthenticator::new(
        config(),
        FakeFetcher::new([Ok(JwkSet {
            keys: vec![encryption_key],
        })]),
    )
    .authenticate(&token(Some("encryption")))
    .await;

    assert_eq!(untrusted.unwrap_err(), AuthError::RejectedBearerToken);
}

#[tokio::test]
async fn rejects_duplicate_key_ids_and_tampered_signatures() {
    let duplicate = JwksAuthenticator::new(
        config(),
        FakeFetcher::new([Ok(JwkSet {
            keys: vec![jwk("primary"), jwk("primary")],
        })]),
    )
    .authenticate(&token(Some("primary")))
    .await;
    assert_eq!(duplicate.unwrap_err(), AuthError::RejectedBearerToken);

    let authenticator = JwksAuthenticator::new(config(), FakeFetcher::new([Ok(jwks("primary"))]));
    let valid = token(Some("primary"));
    let signature_start = valid.rfind('.').expect("JWT has a signature") + 1;
    let mut tampered = valid.into_bytes();
    tampered[signature_start] = if tampered[signature_start] == b'a' {
        b'b'
    } else {
        b'a'
    };
    let tampered = String::from_utf8(tampered).expect("JWT remains ASCII after tampering");

    let rejected = authenticator.authenticate(&tampered).await;
    assert_eq!(rejected.unwrap_err(), AuthError::RejectedBearerToken);
}

#[tokio::test]
async fn id_token_verification_requires_the_transaction_nonce() {
    let fetcher = FakeFetcher::new([Ok(jwks("primary"))]);
    let authenticator = JwksAuthenticator::new(config(), fetcher.clone());
    let token = id_token("primary", "browser-transaction-nonce", Some(now() - 1));

    let principal = authenticator
        .verify_id_token(&token, "browser-transaction-nonce")
        .await
        .expect("matching nonce and signed ID token must validate");
    let wrong_nonce = authenticator
        .verify_id_token(&token, "a-different-transaction-nonce")
        .await;

    assert_eq!(principal.subject(), "alice");
    assert_eq!(wrong_nonce.unwrap_err(), AuthError::RejectedBearerToken);
    assert_eq!(fetcher.calls(), 1);
}

#[tokio::test]
async fn id_token_without_not_before_claim_remains_valid() {
    let authenticator = JwksAuthenticator::new(config(), FakeFetcher::new([Ok(jwks("primary"))]));

    let principal = authenticator
        .verify_id_token(
            &id_token("primary", "browser-transaction-nonce", None),
            "browser-transaction-nonce",
        )
        .await
        .expect("OIDC ID tokens may omit nbf");

    assert_eq!(principal.subject(), "alice");
}

#[tokio::test]
async fn id_token_issued_beyond_configured_clock_skew_is_rejected() {
    let authenticator = JwksAuthenticator::new(
        config()
            .with_leeway_seconds(30)
            .expect("test leeway must be valid"),
        FakeFetcher::new([Ok(jwks("primary"))]),
    );

    let accepted = authenticator
        .verify_id_token(
            &id_token_with_issued_at(
                "primary",
                "browser-transaction-nonce",
                Some(now() - 1),
                now() + 15,
            ),
            "browser-transaction-nonce",
        )
        .await
        .expect("the configured leeway must apply to an ID token issuance time");
    assert_eq!(accepted.subject(), "alice");

    let result = authenticator
        .verify_id_token(
            &id_token_with_issued_at(
                "primary",
                "browser-transaction-nonce",
                Some(now() - 1),
                now() + 120,
            ),
            "browser-transaction-nonce",
        )
        .await;

    assert_eq!(result.unwrap_err(), AuthError::RejectedBearerToken);
}

#[tokio::test]
async fn cache_expiry_rechecks_the_jwks_in_a_controlled_test_configuration() {
    let fetcher = FakeFetcher::new([Ok(jwks("primary")), Ok(jwks("primary"))]);
    let authenticator = JwksAuthenticator::new(
        config()
            .with_cache_ttl(Duration::ZERO)
            .with_minimum_refresh_interval(Duration::ZERO),
        fetcher.clone(),
    );

    authenticator
        .authenticate(&token(Some("primary")))
        .await
        .expect("first token must validate");
    authenticator
        .authenticate(&token(Some("primary")))
        .await
        .expect("expired cache must refresh before validating");

    assert_eq!(fetcher.calls(), 2);
}

#[tokio::test]
async fn jwks_transport_failure_is_not_reported_as_an_invalid_token() {
    let authenticator = JwksAuthenticator::new(config(), FakeFetcher::new([Err(FetchError)]));

    let error = authenticator
        .authenticate(&token(Some("primary")))
        .await
        .expect_err("failed key fetch must fail closed");

    assert_eq!(error, AuthError::ProviderUnavailable);
}
