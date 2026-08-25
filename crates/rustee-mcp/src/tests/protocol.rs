use std::sync::atomic::Ordering;

use http::{HeaderValue, Request as HttpRequest};
use rustee_core::full_body;
use serde_json::{Value, json};
use tower::Service;

use crate::{MCP_PROTOCOL_VERSION, McpServerConfig};

use super::support::{protocol_request, request, response_json, server, server_configured};

#[tokio::test]
async fn origin_header_is_fail_closed_and_requires_an_explicit_allowed_origin() {
    let (default_server, _, _) = server(["orders.lookup"]);
    let mut rejected = request(&json!({
        "jsonrpc":"2.0",
        "id":1,
        "method":"initialize",
        "params":{"protocolVersion":MCP_PROTOCOL_VERSION}
    }));
    rejected.headers_mut().insert(
        http::header::ORIGIN,
        HeaderValue::from_static("https://untrusted.example"),
    );
    let rejected = default_server.handle(rejected).await;
    assert_eq!(rejected.status(), http::StatusCode::FORBIDDEN);

    let (mut allowed_server, _, _) = server(["orders.lookup"]);
    allowed_server.config = McpServerConfig::new("rustee-mcp-test", "0.1.0")
        .unwrap()
        .with_allowed_origins(["https://console.example"])
        .unwrap();
    let mut allowed = protocol_request(&json!({
        "jsonrpc":"2.0","id":2,"method":"tools/list","params":{}
    }));
    allowed.headers_mut().insert(
        http::header::ORIGIN,
        HeaderValue::from_static("https://console.example"),
    );
    let allowed = allowed_server.handle(allowed).await;
    assert_eq!(allowed.status(), http::StatusCode::OK);

    let mut malformed = request(&json!({
        "jsonrpc":"2.0",
        "id":3,
        "method":"initialize",
        "params":{"protocolVersion":MCP_PROTOCOL_VERSION}
    }));
    malformed.headers_mut().insert(
        http::header::ORIGIN,
        HeaderValue::from_static("not-an-origin"),
    );
    let malformed = allowed_server.handle(malformed).await;
    assert_eq!(malformed.status(), http::StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn duplicate_mcp_control_headers_fail_closed_before_dispatch() {
    let (mut server, calls, _) = server(["orders.lookup"]);
    server.config = McpServerConfig::new("rustee-mcp-test", "0.1.0")
        .unwrap()
        .with_allowed_origins(["https://console.example"])
        .unwrap();

    let request_body = json!({
        "jsonrpc":"2.0","id":4,"method":"tools/list","params":{}
    });
    let mut duplicate_origin = protocol_request(&request_body);
    duplicate_origin.headers_mut().insert(
        http::header::ORIGIN,
        HeaderValue::from_static("https://console.example"),
    );
    duplicate_origin.headers_mut().append(
        http::header::ORIGIN,
        HeaderValue::from_static("https://untrusted.example"),
    );
    assert_eq!(
        server.handle(duplicate_origin).await.status(),
        http::StatusCode::FORBIDDEN
    );

    let mut duplicate_content_type = protocol_request(&request_body);
    duplicate_content_type.headers_mut().append(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain"),
    );
    assert_eq!(
        server.handle(duplicate_content_type).await.status(),
        http::StatusCode::UNSUPPORTED_MEDIA_TYPE
    );

    let mut duplicate_protocol = protocol_request(&request_body);
    duplicate_protocol.headers_mut().append(
        "mcp-protocol-version",
        HeaderValue::from_static("2024-11-05"),
    );
    let response = response_json(server.handle(duplicate_protocol).await).await;
    assert_eq!(response["error"]["code"], -32600);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn initializes_lists_only_allowed_tools_and_accepts_notification() {
    let (server, _, _) = server(["orders.lookup"]);
    let initialize = server
        .handle(request(&json!({
            "jsonrpc":"2.0",
            "id":1,
            "method":"initialize",
            "params":{"protocolVersion":MCP_PROTOCOL_VERSION}
        })))
        .await;
    let initialize = response_json(initialize).await;
    assert_eq!(
        initialize["result"]["protocolVersion"],
        MCP_PROTOCOL_VERSION
    );
    assert_eq!(
        initialize["result"]["capabilities"]["tools"]["listChanged"],
        false
    );

    let notification = server
        .handle(request(&json!({
            "jsonrpc":"2.0",
            "method":"notifications/initialized"
        })))
        .await;
    assert_eq!(notification.status(), http::StatusCode::ACCEPTED);

    let listed = server
        .handle(protocol_request(&json!({
            "jsonrpc":"2.0","id":2,"method":"tools/list","params":{}
        })))
        .await;
    let listed = response_json(listed).await;
    assert_eq!(listed["result"]["tools"].as_array().unwrap().len(), 1);
    assert_eq!(listed["result"]["tools"][0]["name"], "orders.lookup");
}

#[tokio::test]
async fn rejects_missing_protocol_header_and_oversized_body_without_executing() {
    let (server, calls, _) = server_configured(["orders.lookup"], 64);
    let missing_header = server
        .handle(request(&json!({
            "jsonrpc":"2.0","id":5,"method":"tools/list","params":{}
        })))
        .await;
    let missing_header = response_json(missing_header).await;
    assert_eq!(missing_header["error"]["code"], -32600);

    let oversized = HttpRequest::builder()
        .method(http::Method::POST)
        .uri("/")
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(full_body("x".repeat(65)))
        .unwrap();
    let oversized = server.handle(oversized).await;
    assert_eq!(oversized.status(), http::StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn bounds_remote_request_ids_and_oversized_success_responses() {
    let (mut server, calls, _) = server(["orders.lookup"]);
    server.config = McpServerConfig::new("rustee-mcp-test", "0.1.0")
        .unwrap()
        .with_max_response_bytes(80)
        .unwrap();

    let oversized_id = "untrusted-request-id".repeat(8);
    let invalid = response_json(
        server
            .handle(protocol_request(&json!({
                "jsonrpc":"2.0","id":oversized_id,"method":"tools/list","params":{}
            })))
            .await,
    )
    .await;
    assert_eq!(invalid["error"]["code"], -32600);
    assert_eq!(invalid["id"], Value::Null);
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let response = response_json(
        server
            .handle(protocol_request(&json!({
                "jsonrpc":"2.0","id":30,"method":"tools/list","params":{}
            })))
            .await,
    )
    .await;
    assert_eq!(response["error"]["code"], -32000);
    assert_eq!(
        response["error"]["message"],
        "response body exceeds configured limit"
    );
}

#[tokio::test]
async fn rejects_tool_discovery_larger_than_its_explicit_item_limit() {
    let (mut server, _, _) = server(["orders.lookup", "orders.history"]);
    server.config = McpServerConfig::new("rustee-mcp-test", "0.1.0")
        .unwrap()
        .with_max_tool_items(1)
        .unwrap();

    let response = response_json(
        server
            .handle(protocol_request(&json!({
                "jsonrpc":"2.0","id":31,"method":"tools/list","params":{}
            })))
            .await,
    )
    .await;
    assert_eq!(response["error"]["code"], -32000);
    assert_eq!(
        response["error"]["message"],
        "tool list exceeds configured limit"
    );
}

#[tokio::test]
async fn service_contract_mounts_below_a_prefix_and_rejects_child_paths() {
    let (server, _, _) = server(["orders.lookup"]);
    let app = rustee_router::App::new().nest("/mcp", server.clone());
    let mut mounted = protocol_request(&json!({
        "jsonrpc":"2.0","id":6,"method":"tools/list","params":{}
    }));
    *mounted.uri_mut() = "/mcp".parse().unwrap();
    let response = app.call(mounted).await;
    assert_eq!(response.status(), http::StatusCode::OK);

    let mut server = server;
    let response = server
        .call(protocol_request(&json!({
            "jsonrpc":"2.0","id":6,"method":"tools/list","params":{}
        })))
        .await
        .unwrap();
    assert_eq!(response.status(), http::StatusCode::OK);

    let child = HttpRequest::builder()
        .method(http::Method::POST)
        .uri("/child")
        .header(http::header::CONTENT_TYPE, "application/json")
        .header("mcp-protocol-version", MCP_PROTOCOL_VERSION)
        .body(full_body("{}"))
        .unwrap();
    let response = server.call(child).await.unwrap();
    assert_eq!(response.status(), http::StatusCode::NOT_FOUND);
}
