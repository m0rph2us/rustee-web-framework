use std::time::Duration;

use rustee_auth::{AuthError, BearerAuthenticator};

use super::{
    super::{OpaqueTokenAuthenticator, OpaqueTokenIntrospection, unix_seconds},
    support::{AUDIENCE, FakeIntrospector, ISSUER, IntrospectionError, active_response, config},
};

#[tokio::test]
async fn validates_active_response_and_caches_only_a_fingerprint() {
    let introspector = FakeIntrospector::new([Ok(active_response())]);
    let authenticator = OpaqueTokenAuthenticator::new(config(), introspector.clone());

    let principal = authenticator
        .authenticate("opaque-token")
        .await
        .expect("active matching response must authenticate");
    authenticator
        .authenticate("opaque-token")
        .await
        .expect("unexpired response must be served from cache");

    assert_eq!(principal.subject(), "alice");
    assert_eq!(principal.issuer(), Some(ISSUER));
    assert_eq!(principal.tenant(), Some("acme"));
    assert!(principal.has_scope("profile:read"));
    assert!(principal.has_role("project-viewer"));
    assert!(principal.has_permission("project:read"));
    assert_eq!(introspector.calls(), 1);
    assert!(!format!("{authenticator:?}").contains("opaque-token"));
}

#[tokio::test]
async fn poisoned_cache_fails_closed_without_introspecting_the_token() {
    let introspector = FakeIntrospector::new([Ok(active_response())]);
    let authenticator = OpaqueTokenAuthenticator::new(config(), introspector.clone());
    authenticator.cache.poison_for_test();

    assert_eq!(
        authenticator
            .authenticate("opaque-token")
            .await
            .unwrap_err(),
        AuthError::ProviderUnavailable
    );
    assert_eq!(introspector.calls(), 0);
}

#[tokio::test]
async fn disabled_cache_never_depends_on_cache_lock_health() {
    for config in [
        config().with_max_cache_entries(0),
        config().with_cache_ttl(Duration::ZERO),
    ] {
        let introspector = FakeIntrospector::new([Ok(active_response())]);
        let authenticator = OpaqueTokenAuthenticator::new(config, introspector.clone());
        authenticator.cache.poison_for_test();

        authenticator
            .authenticate("opaque-token")
            .await
            .expect("disabled cache must not gate remote authentication");
        assert_eq!(introspector.calls(), 1);
    }
}

#[tokio::test]
async fn rejects_inactive_or_mismatched_identity_responses() {
    let inactive = OpaqueTokenAuthenticator::new(
        config(),
        FakeIntrospector::new([Ok(OpaqueTokenIntrospection::inactive())]),
    );
    assert_eq!(
        inactive.authenticate("opaque-token").await.unwrap_err(),
        AuthError::RejectedBearerToken
    );

    let wrong_issuer = OpaqueTokenAuthenticator::new(
        config(),
        FakeIntrospector::new([Ok(OpaqueTokenIntrospection::active(
            "alice",
            "https://other.example.test",
            AUDIENCE,
        )
        .with_expiration(unix_seconds() + 300))]),
    );
    assert_eq!(
        wrong_issuer.authenticate("opaque-token").await.unwrap_err(),
        AuthError::RejectedBearerToken
    );

    let expired = OpaqueTokenAuthenticator::new(
        config(),
        FakeIntrospector::new([Ok(OpaqueTokenIntrospection::active(
            "alice", ISSUER, AUDIENCE,
        )
        .with_expiration(unix_seconds().saturating_sub(1)))]),
    );
    assert_eq!(
        expired.authenticate("opaque-token").await.unwrap_err(),
        AuthError::RejectedBearerToken
    );
}

#[tokio::test]
async fn provider_failure_is_fail_closed_and_cache_requires_an_expiration() {
    let unavailable =
        OpaqueTokenAuthenticator::new(config(), FakeIntrospector::new([Err(IntrospectionError)]));
    assert_eq!(
        unavailable.authenticate("opaque-token").await.unwrap_err(),
        AuthError::ProviderUnavailable
    );

    let no_expiration = OpaqueTokenAuthenticator::new(
        config(),
        FakeIntrospector::new([
            Ok(OpaqueTokenIntrospection::active("alice", ISSUER, AUDIENCE)),
            Ok(OpaqueTokenIntrospection::active("alice", ISSUER, AUDIENCE)),
        ]),
    );
    no_expiration
        .authenticate("opaque-token")
        .await
        .expect("active response without expiration still works");
    no_expiration
        .authenticate("opaque-token")
        .await
        .expect("unbounded response must be checked again");
    assert_eq!(no_expiration.introspector.calls(), 2);
}

#[tokio::test]
async fn cache_capacity_limits_retained_fingerprints() {
    let introspector = FakeIntrospector::new([
        Ok(active_response()),
        Ok(active_response()),
        Ok(active_response()),
    ]);
    let authenticator =
        OpaqueTokenAuthenticator::new(config().with_max_cache_entries(1), introspector.clone());

    authenticator
        .authenticate("opaque-token")
        .await
        .expect("first token must authenticate");
    authenticator
        .authenticate("another-opaque-token")
        .await
        .expect("second token must authenticate without evicting first");
    authenticator
        .authenticate("another-opaque-token")
        .await
        .expect("uncached second token must be remotely checked again");

    assert_eq!(introspector.calls(), 3);
}
