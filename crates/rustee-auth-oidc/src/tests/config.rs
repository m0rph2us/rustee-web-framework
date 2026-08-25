use jsonwebtoken::Algorithm;
use url::Url;

use crate::{OidcConfigError, OidcResourceServerConfig};

use super::support::{AUDIENCE, ISSUER};

#[test]
fn config_rejects_symmetric_algorithms_and_non_https_endpoints() {
    let hmac = OidcResourceServerConfig::new(
        Algorithm::HS256,
        ISSUER,
        AUDIENCE,
        Url::parse("https://issuer.example.test/jwks").expect("test URL must be valid"),
    );
    let insecure = OidcResourceServerConfig::new(
        Algorithm::RS256,
        ISSUER,
        AUDIENCE,
        Url::parse("http://issuer.example.test/jwks").expect("test URL must be valid"),
    );

    assert_eq!(hmac.unwrap_err(), OidcConfigError::SymmetricAlgorithm);
    assert_eq!(insecure.unwrap_err(), OidcConfigError::InvalidJwksUrl);
}
