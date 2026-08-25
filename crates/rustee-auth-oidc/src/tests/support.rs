use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use futures_util::future::BoxFuture;
use jsonwebtoken::{
    Algorithm, EncodingKey, Header, encode,
    jwk::{Jwk, JwkSet, PublicKeyUse},
};
use serde::Serialize;
use tokio::sync::Mutex;
use url::Url;

use crate::{JwksFetcher, OidcResourceServerConfig};

pub(super) const ISSUER: &str = "https://issuer.example.test";
pub(super) const AUDIENCE: &str = "rustee-api";

const TEST_RSA_PRIVATE_KEY: &str = concat!(
    "-----BEGIN PRIVATE KEY-----\n",
    "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDJETqse41HRBsc\n",
    "7cfcq3ak4oZWFCoZlcic525A3FfO4qW9BMtRO/iXiyCCHn8JhiL9y8j5JdVP2Q9Z\n",
    "IpfElcFd3/guS9w+5RqQGgCR+H56IVUyHZWtTJbKPcwWXQdNUX0rBFcsBzCRESJL\n",
    "eelOEdHIjG7LRkx5l/FUvlqsyHDVJEQsHwegZ8b8C0fz0EgT2MMEdn10t6Ur1rXz\n",
    "jMB/wvCg8vG8lvciXmedyo9xJ8oMOh0wUEgxziVDMMovmC+aJctcHUAYubwoGN8T\n",
    "yzcvnGqL7JSh36Pwy28iPzXZ2RLhAyJFU39vLaHdljwthUaupldlNyCfa6Ofy4qN\n",
    "ctlUPlN1AgMBAAECggEAdESTQjQ70O8QIp1ZSkCYXeZjuhj081CK7jhhp/4ChK7J\n",
    "GlFQZMwiBze7d6K84TwAtfQGZhQ7km25E1kOm+3hIDCoKdVSKch/oL54f/BK6sKl\n",
    "qlIzQEAenho4DuKCm3I4yAw9gEc0DV70DuMTR0LEpYyXcNJY3KNBOTjN5EYQAR9s\n",
    "2MeurpgK2MdJlIuZaIbzSGd+diiz2E6vkmcufJLtmYUT/k/ddWvEtz+1DnO6bRHh\n",
    "xuuDMeJA/lGB/EYloSLtdyCF6sII6C6slJJtgfb0bPy7l8VtL5iDyz46IKyzdyzW\n",
    "tKAn394dm7MYR1RlUBEfqFUyNK7C+pVMVoTwCC2V4QKBgQD64syfiQ2oeUlLYDm4\n",
    "CcKSP3RnES02bcTyEDFSuGyyS1jldI4A8GXHJ/lG5EYgiYa1RUivge4lJrlNfjyf\n",
    "dV230xgKms7+JiXqag1FI+3mqjAgg4mYiNjaao8N8O3/PD59wMPeWYImsWXNyeHS\n",
    "55rUKiHERtCcvdzKl4u35ZtTqQKBgQDNKnX2bVqOJ4WSqCgHRhOm386ugPHfy+8j\n",
    "m6cicmUR46ND6ggBB03bCnEG9OtGisxTo/TuYVRu3WP4KjoJs2LD5fwdwJqpgtHl\n",
    "yVsk45Y1Hfo+7M6lAuR8rzCi6kHHNb0HyBmZjysHWZsn79ZM+sQnLpgaYgQGRbKV\n",
    "DZWlbw7g7QKBgQCl1u+98UGXAP1jFutwbPsx40IVszP4y5ypCe0gqgon3UiY/G+1\n",
    "zTLp79GGe/SjI2VpQ7AlW7TI2A0bXXvDSDi3/5Dfya9ULnFXv9yfvH1QwWToySpW\n",
    "Kvd1gYSoiX84/WCtjZOr0e0HmLIb0vw0hqZA4szJSqoxQgvF22EfIWaIaQKBgQCf\n",
    "34+OmMYw8fEvSCPxDxVvOwW2i7pvV14hFEDYIeZKW2W1HWBhVMzBfFB5SE8yaCQy\n",
    "pRfOzj9aKOCm2FjjiErVNpkQoi6jGtLvScnhZAt/lr2TXTrl8OwVkPrIaN0bG/AS\n",
    "aUYxmBPCpXu3UjhfQiWqFq/mFyzlqlgvuCc9g95HPQKBgAscKP8mLxdKwOgX8yFW\n",
    "GcZ0izY/30012ajdHY+/QK5lsMoxTnn0skdS+spLxaS5ZEO4qvPVb8RAoCkWMMal\n",
    "2pOhmquJQVDPDLuZHdrIiKiDM20dy9sMfHygWcZjQ4WSxf/J7T9canLZIXFhHAZT\n",
    "3wc9h4G8BBCtWN2TN/LsGZdB\n",
    "-----END PRIVATE KEY-----",
);

#[derive(Clone, Debug, thiserror::Error)]
#[error("test JWKS endpoint is unavailable")]
pub(super) struct FetchError;

#[derive(Clone)]
pub(super) struct FakeFetcher {
    replies: Arc<Mutex<VecDeque<Result<JwkSet, FetchError>>>>,
    calls: Arc<AtomicUsize>,
}

impl FakeFetcher {
    pub(super) fn new(replies: impl IntoIterator<Item = Result<JwkSet, FetchError>>) -> Self {
        Self {
            replies: Arc::new(Mutex::new(replies.into_iter().collect())),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(super) fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl JwksFetcher for FakeFetcher {
    type Error = FetchError;

    fn fetch(&self) -> BoxFuture<'static, Result<JwkSet, Self::Error>> {
        let replies = Arc::clone(&self.replies);
        let calls = Arc::clone(&self.calls);
        Box::pin(async move {
            calls.fetch_add(1, Ordering::SeqCst);
            replies.lock().await.pop_front().unwrap_or(Err(FetchError))
        })
    }
}

#[derive(Serialize)]
struct TestClaims<'a> {
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

#[derive(Serialize)]
struct TestIdTokenClaims<'a> {
    sub: &'a str,
    iss: &'a str,
    aud: &'a str,
    exp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    nbf: Option<u64>,
    iat: u64,
    nonce: &'a str,
}

pub(super) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after the Unix epoch")
        .as_secs()
}

pub(super) fn token(kid: Option<&str>) -> String {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = kid.map(ToOwned::to_owned);
    encode(
        &header,
        &TestClaims {
            sub: "alice",
            iss: ISSUER,
            aud: AUDIENCE,
            exp: now() + 300,
            nbf: now() - 1,
            tenant: "acme",
            scope: "profile:read profile:write",
            roles: &["project-viewer"],
            permissions: &["project:read"],
        },
        &EncodingKey::from_rsa_pem(TEST_RSA_PRIVATE_KEY.as_bytes())
            .expect("embedded test key must be valid"),
    )
    .expect("test claims must encode")
}

pub(super) fn id_token(kid: &str, nonce: &str, nbf: Option<u64>) -> String {
    id_token_with_issued_at(kid, nonce, nbf, now())
}

pub(super) fn id_token_with_issued_at(
    kid: &str,
    nonce: &str,
    nbf: Option<u64>,
    issued_at: u64,
) -> String {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(kid.to_owned());
    let current = now();
    encode(
        &header,
        &TestIdTokenClaims {
            sub: "alice",
            iss: ISSUER,
            aud: AUDIENCE,
            exp: current + 300,
            nbf,
            iat: issued_at,
            nonce,
        },
        &EncodingKey::from_rsa_pem(TEST_RSA_PRIVATE_KEY.as_bytes())
            .expect("embedded test key must be valid"),
    )
    .expect("test ID token claims must encode")
}

pub(super) fn jwk(kid: &str) -> Jwk {
    let encoding_key = EncodingKey::from_rsa_pem(TEST_RSA_PRIVATE_KEY.as_bytes())
        .expect("embedded test key must be valid");
    let mut jwk =
        Jwk::from_encoding_key(&encoding_key, Algorithm::RS256).expect("test key must make a JWK");
    jwk.common.key_id = Some(kid.to_owned());
    jwk.common.public_key_use = Some(PublicKeyUse::Signature);
    jwk
}

pub(super) fn jwks(kid: &str) -> JwkSet {
    JwkSet {
        keys: vec![jwk(kid)],
    }
}

pub(super) fn config() -> OidcResourceServerConfig {
    OidcResourceServerConfig::new(
        Algorithm::RS256,
        ISSUER,
        AUDIENCE,
        Url::parse("https://issuer.example.test/.well-known/jwks.json")
            .expect("test URL must be valid"),
    )
    .expect("test configuration must be valid")
}
