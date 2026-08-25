//! Compile-checked JWT-protected Rustee HTTP API example.

use std::{convert::Infallible, env, net::SocketAddr};

use jsonwebtoken::Algorithm;
use rustee::{App, Json, Request, Response, ServerOptions};
use rustee_auth::{AuthLayer, AuthUser};
use rustee_auth_jwt::{JwtAuthenticator, JwtConfig};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower::{Layer, util::BoxCloneService};

const AUDIENCE: &str = "rustee-example-api";
const ISSUER: &str = "https://issuer.example.test";

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Identity {
    subject: String,
}

async fn me(AuthUser(principal): AuthUser) -> Json<Identity> {
    Json(Identity {
        subject: principal.subject().to_owned(),
    })
}

fn app() -> App {
    App::new().get("/me", me)
}

fn service(
    secret: impl AsRef<[u8]>,
) -> Result<BoxCloneService<Request, Response, Infallible>, Box<dyn std::error::Error>> {
    let authenticator = JwtAuthenticator::from_hmac_secret(
        JwtConfig::new(Algorithm::HS256, ISSUER, AUDIENCE)?,
        secret,
    )?;
    Ok(BoxCloneService::new(
        AuthLayer::bearer(authenticator).layer(app()),
    ))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let secret = env::var("JWT_HMAC_SECRET")?;
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 3003))).await?;
    rustee::serve_service_listener_with_options(
        listener,
        service(secret)?,
        ServerOptions::default(),
        std::future::pending(),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use http::{Request as HttpRequest, StatusCode, header};
    use http_body_util::BodyExt;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use rustee::empty_body;
    use serde::Serialize;
    use tower::ServiceExt;

    use super::{AUDIENCE, ISSUER, Identity, service};

    const SECRET: &[u8] = b"local-example-secret-with-sufficient-length";

    #[derive(Serialize)]
    struct Claims<'a> {
        sub: &'a str,
        iss: &'a str,
        aud: &'a str,
        exp: u64,
        nbf: u64,
    }

    fn valid_token() -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the system clock is after the Unix epoch")
            .as_secs();
        encode(
            &Header::default(),
            &Claims {
                sub: "example-user",
                iss: ISSUER,
                aud: AUDIENCE,
                exp: now + 300,
                nbf: now.saturating_sub(1),
            },
            &EncodingKey::from_secret(SECRET),
        )
        .expect("the example claims are valid")
    }

    #[tokio::test]
    async fn verified_bearer_token_exposes_only_the_principal_to_the_handler() {
        let response = service(SECRET)
            .unwrap()
            .oneshot(
                HttpRequest::builder()
                    .uri("/me")
                    .header(header::AUTHORIZATION, format!("Bearer {}", valid_token()))
                    .body(empty_body())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (parts, body) = response.into_parts();

        assert_eq!(parts.status, StatusCode::OK);
        assert_eq!(
            serde_json::from_slice::<Identity>(&body.collect().await.unwrap().to_bytes()).unwrap(),
            Identity {
                subject: "example-user".to_owned(),
            }
        );
    }

    #[tokio::test]
    async fn missing_bearer_token_is_rejected_with_a_standard_challenge() {
        let response = service(SECRET)
            .unwrap()
            .oneshot(
                HttpRequest::builder()
                    .uri("/me")
                    .body(empty_body())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(response.headers()[header::WWW_AUTHENTICATE], "Bearer");
    }
}
