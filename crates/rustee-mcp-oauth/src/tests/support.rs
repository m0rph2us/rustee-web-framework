use http::Request as HttpRequest;
use rustee_ai::{
    ToolApprovalAuditEvent, ToolApprovalAuditSink, ToolExecutionAuditEvent, ToolExecutionAuditSink,
};
use rustee_auth::StaticTokenAuthenticator;
use rustee_core::{empty_body, full_body};
use rustee_mcp::MCP_PROTOCOL_VERSION;
use serde_json::json;
use url::Url;

use crate::McpOAuthResourceServerConfig;

pub(super) fn config() -> McpOAuthResourceServerConfig {
    McpOAuthResourceServerConfig::new(
        Url::parse("https://api.example.test/mcp").expect("test resource URL must parse"),
        Url::parse("https://api.example.test/.well-known/oauth-protected-resource/mcp")
            .expect("test metadata URL must parse"),
        [Url::parse("https://issuer.example.test").expect("test issuer URL must parse")],
    )
    .expect("test configuration must be valid")
    .with_required_scopes(["mcp:tools", "mcp:resources"])
    .expect("test scopes must be valid")
}

pub(super) fn request(
    method: http::Method,
    uri: &str,
    token: Option<&str>,
) -> rustee_core::Request {
    let mut builder = HttpRequest::builder().method(method).uri(uri);
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder
        .body(empty_body())
        .expect("test request must be valid")
}

pub(super) fn authenticator() -> StaticTokenAuthenticator {
    let mut authenticator = StaticTokenAuthenticator::new();
    authenticator
        .insert(
            "full-access",
            rustee_auth::Principal::new("alice")
                .expect("test principal must be valid")
                .with_issuer("https://issuer.example.test")
                .expect("test issuer must be valid")
                .with_scope("mcp:tools")
                .expect("test scope must be valid")
                .with_scope("mcp:resources")
                .expect("test scope must be valid"),
        )
        .expect("test token must be valid");
    authenticator
        .insert(
            "limited-access",
            rustee_auth::Principal::new("bob")
                .expect("test principal must be valid")
                .with_issuer("https://issuer.example.test")
                .expect("test issuer must be valid")
                .with_scope("mcp:tools")
                .expect("test scope must be valid"),
        )
        .expect("test token must be valid");
    authenticator
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct NoopAudit;

impl ToolApprovalAuditSink for NoopAudit {
    type Error = std::convert::Infallible;

    fn record_approved(
        &self,
        _: ToolApprovalAuditEvent,
    ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(async { Ok(()) })
    }
}

impl ToolExecutionAuditSink for NoopAudit {
    fn record_outcome(
        &self,
        _: ToolExecutionAuditEvent,
    ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(async { Ok(()) })
    }
}

pub(super) fn mcp_initialize_request(token: Option<&str>) -> rustee_core::Request {
    let mut builder = HttpRequest::builder()
        .method(http::Method::POST)
        .uri("/mcp")
        .header(http::header::CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder
        .body(full_body(
            serde_json::to_vec(&json!({
                "jsonrpc":"2.0",
                "id":1,
                "method":"initialize",
                "params":{"protocolVersion":MCP_PROTOCOL_VERSION}
            }))
            .expect("test JSON-RPC request must encode"),
        ))
        .expect("test request must be valid")
}
