//! Public MCP protected-resource metadata endpoint.

use std::{
    convert::Infallible,
    task::{Context, Poll},
};

use futures_util::future::BoxFuture;
use http::{HeaderValue, Method, StatusCode, header::ALLOW};
use rustee_core::{Error, IntoResponse, Request, Response, json_response_bounded, response};
use serde::Serialize;
use tower::Service;

use crate::config::{
    MAX_AUTHORIZATION_SERVERS, MAX_REQUIRED_SCOPES, MAX_SCOPE_BYTES, MAX_URL_BYTES,
    McpOAuthResourceServerConfig,
};

// URLs and scope tokens have stricter configuration bounds; this leaves room for fixed JSON
// fields and separators while keeping metadata materialization explicitly finite.
const MAX_PROTECTED_RESOURCE_METADATA_BYTES: usize =
    (1 + MAX_AUTHORIZATION_SERVERS) * MAX_URL_BYTES + MAX_REQUIRED_SCOPES * MAX_SCOPE_BYTES + 512;

#[derive(Serialize)]
struct ProtectedResourceMetadata<'a> {
    resource: &'a str,
    authorization_servers: Vec<&'a str>,
    scopes_supported: Vec<&'a str>,
    bearer_methods_supported: [&'static str; 1],
}

/// Public service for one protected-resource metadata document.
///
/// Mount this service at the exact `protected_resource_metadata` path. The metadata document is
/// intentionally unauthenticated: it discloses only public resource and issuer configuration,
/// never accepted tokens, principals, tenant mappings, tool inventory, or authorization policy.
#[derive(Clone, Debug)]
pub struct McpOAuthProtectedResourceMetadata {
    config: McpOAuthResourceServerConfig,
}

impl McpOAuthProtectedResourceMetadata {
    /// Creates the metadata service for a validated resource-server configuration.
    #[must_use]
    pub const fn new(config: McpOAuthResourceServerConfig) -> Self {
        Self { config }
    }

    /// Handles one prefix-stripped request.
    pub fn handle(&self, request: &Request) -> Response {
        if request.uri().path() != "/" {
            return Error::not_found(
                "the requested protected-resource metadata endpoint was not found",
            )
            .into_response();
        }
        if request.method() != Method::GET {
            let mut response = response(StatusCode::METHOD_NOT_ALLOWED, rustee_core::empty_body());
            response
                .headers_mut()
                .insert(ALLOW, HeaderValue::from_static("GET"));
            return response;
        }

        let document = ProtectedResourceMetadata {
            resource: self.config.resource().as_str(),
            authorization_servers: self.config.authorization_servers().collect(),
            scopes_supported: self.config.required_scopes().collect(),
            bearer_methods_supported: ["header"],
        };
        json_response_bounded(
            StatusCode::OK,
            &document,
            MAX_PROTECTED_RESOURCE_METADATA_BYTES,
        )
        .unwrap_or_else(|_| Error::internal().into_response())
    }
}

impl Service<Request> for McpOAuthProtectedResourceMetadata {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let metadata = self.clone();
        Box::pin(async move { Ok(metadata.handle(&request)) })
    }
}

#[cfg(test)]
mod tests {
    use http::{Method, Request as HttpRequest, StatusCode};
    use http_body_util::BodyExt;
    use rustee_core::empty_body;
    use url::Url;

    use crate::config::{MAX_AUTHORIZATION_SERVERS, MAX_URL_BYTES};

    use super::{
        MAX_PROTECTED_RESOURCE_METADATA_BYTES, McpOAuthProtectedResourceMetadata,
        McpOAuthResourceServerConfig,
    };

    #[tokio::test]
    async fn maximum_issuer_configuration_renders_within_the_metadata_budget() {
        let resource = public_url_at_limit("https://api.example.test/");
        let metadata =
            Url::parse("https://api.example.test/.well-known/oauth-protected-resource/mcp")
                .expect("test metadata URL must be valid");
        let authorization_servers = (0..MAX_AUTHORIZATION_SERVERS)
            .map(|index| public_url_at_limit(&format!("https://issuer-{index}.example.test/")))
            .collect::<Vec<_>>();
        let config = McpOAuthResourceServerConfig::new(resource, metadata, authorization_servers)
            .expect("maximum issuer configuration must remain valid");
        let request = HttpRequest::builder()
            .method(Method::GET)
            .uri("/")
            .body(empty_body())
            .expect("test request must be valid");

        let response = McpOAuthProtectedResourceMetadata::new(config).handle(&request);
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("metadata response body must remain readable")
            .to_bytes();

        assert!(body.len() <= MAX_PROTECTED_RESOURCE_METADATA_BYTES);
    }

    fn public_url_at_limit(prefix: &str) -> Url {
        let remaining = MAX_URL_BYTES
            .checked_sub(prefix.len())
            .expect("test URL prefix must fit the configured URL limit");
        Url::parse(&format!("{prefix}{}", "x".repeat(remaining)))
            .expect("test public URL at the configured length limit must parse")
    }
}
