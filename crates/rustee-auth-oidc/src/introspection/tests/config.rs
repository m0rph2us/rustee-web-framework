use std::time::Duration;

use url::Url;

use crate::{
    HttpOpaqueTokenIntrospector, OidcClientAuthentication, OidcClientSecret,
    OpaqueIntrospectionConfig, OpaqueIntrospectionConfigError,
};

use super::support::{AUDIENCE, ISSUER, config};

#[test]
fn rejects_invalid_config_and_zero_http_timeout() {
    let endpoint =
        Url::parse("http://issuer.example.test/introspect").expect("test URL must parse");
    assert_eq!(
        OpaqueIntrospectionConfig::new(
            ISSUER,
            AUDIENCE,
            endpoint,
            "rustee-resource-server",
            OidcClientAuthentication::None,
        )
        .unwrap_err(),
        OpaqueIntrospectionConfigError::InvalidEndpoint
    );
    assert_eq!(
        HttpOpaqueTokenIntrospector::new(Duration::ZERO).unwrap_err(),
        OpaqueIntrospectionConfigError::ZeroHttpTimeout
    );
}

#[test]
fn configuration_bounds_trusted_values_and_clock_skew() {
    let endpoint =
        Url::parse("https://issuer.example.test/introspect").expect("test URL must parse");
    assert_eq!(
        OpaqueIntrospectionConfig::new(
            "i".repeat(2 * 1024 + 1),
            AUDIENCE,
            endpoint.clone(),
            "rustee-resource-server",
            OidcClientAuthentication::None,
        ),
        Err(OpaqueIntrospectionConfigError::InvalidField)
    );
    assert_eq!(
        config().with_leeway_seconds(301),
        Err(OpaqueIntrospectionConfigError::LeewayTooLarge)
    );

    let secret = OidcClientSecret::new("!".repeat(4 * 1024))
        .expect("bounded client secret must be accepted independently");
    assert_eq!(
        OpaqueIntrospectionConfig::new(
            ISSUER,
            AUDIENCE,
            endpoint,
            "rustee-resource-server",
            OidcClientAuthentication::ClientSecretBasic(secret),
        ),
        Err(OpaqueIntrospectionConfigError::ClientAuthenticationTooLarge)
    );
}
