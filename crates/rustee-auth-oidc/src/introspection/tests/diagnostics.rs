use url::Url;

use crate::{
    OidcClientAuthentication, OpaqueIntrospectionConfig, OpaqueTokenIntrospection,
    OpaqueTokenIntrospectionRequest,
};

#[test]
fn request_debug_redacts_the_bearer_credential_and_client_id() {
    let request = OpaqueTokenIntrospectionRequest::new(
        "opaque-token".to_owned(),
        "private-resource-server-client-id".to_owned(),
        OidcClientAuthentication::None,
    );
    let debug = format!("{request:?}");
    assert!(!debug.contains("opaque-token"));
    assert!(!debug.contains("private-resource-server-client-id"));
}

#[test]
fn config_debug_redacts_introspection_identity_metadata() {
    let config = OpaqueIntrospectionConfig::new(
        "https://private-introspection-issuer.example.test",
        "private-introspection-audience",
        Url::parse("https://private-introspection.example.test/oauth2/introspect")
            .expect("test URL must be valid"),
        "private-introspection-client-id",
        OidcClientAuthentication::None,
    )
    .expect("test configuration must be valid");

    let debug = format!("{config:?}");
    assert!(debug.contains("endpoint: \"[REDACTED]\""));
    for value in [
        "private-introspection-issuer.example.test",
        "private-introspection-audience",
        "private-introspection.example.test",
        "private-introspection-client-id",
    ] {
        assert!(
            !debug.contains(value),
            "Debug output must not include {value:?}"
        );
    }
}

#[test]
fn response_debug_redacts_identity_and_authorization_claims() {
    let response = OpaqueTokenIntrospection::active(
        "private-subject",
        "https://private-issuer.example.test",
        "private-audience",
    )
    .with_expiration(1_234)
    .with_not_before(1_000)
    .with_tenant("private-tenant")
    .with_scope("private:read private:write")
    .with_role("private-role")
    .with_permission("private:permission");

    let output = format!("{response:?}");

    assert!(output.contains("active: true"));
    assert!(output.contains("has_permissions: true"));
    for value in [
        "private-subject",
        "private-issuer",
        "private-audience",
        "private-tenant",
        "private:read",
        "private-role",
        "private:permission",
    ] {
        assert!(!output.contains(value));
    }
}
