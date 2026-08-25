//! Streamable HTTP and standalone MCP server-sent-event regression coverage.

use super::*;

#[tokio::test]
async fn discovery_accepts_a_bounded_sse_result_after_a_notification() {
    let notification = json!({
        "jsonrpc":"2.0",
        "method":"notifications/message",
        "params":{"level":"info"}
    });
    let (endpoint, server) = server(vec![
        json_reply(1, &initialize_result(), None),
        status_reply(202),
        sse_reply(
            2,
            &json!({"tools":[{"name":"orders.sse","inputSchema":{"type":"object"}}]}),
            &[notification],
        ),
    ])
    .await;
    let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
    client.initialize().await.unwrap();

    let tools = client.list_tools().await.unwrap();
    assert_eq!(tools[0].name(), "orders.sse");

    let requests = server.await.unwrap();
    assert!(requests[2].contains("accept: application/json, text/event-stream"));
}

#[tokio::test]
async fn sse_resumption_uses_get_last_event_id_without_replaying_the_post() {
    let (endpoint, server) = server(vec![
        json_reply(1, &initialize_result(), Some("session-resume")),
        status_reply(202),
        sse_body_reply("id: stream-cursor-1\nretry: 1\ndata:\n\n"),
        sse_reply(
            2,
            &json!({"tools":[{"name":"orders.resumed","inputSchema":{"type":"object"}}]}),
            &[],
        ),
    ])
    .await;
    let config = McpHttpConfig::new(endpoint)
        .unwrap()
        .with_automatic_sse_resumption(1, Duration::from_millis(2), Duration::from_millis(5))
        .unwrap();
    let client = McpHttpClient::new(config).unwrap();
    client.initialize().await.unwrap();

    let tools = client.list_tools().await.unwrap();
    assert_eq!(tools[0].name(), "orders.resumed");

    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 4);
    assert!(requests[2].starts_with("post /mcp http/1.1\r\n"));
    assert!(requests[2].contains("\"method\":\"tools/list\""));
    assert!(requests[3].starts_with("get /mcp http/1.1\r\n"));
    assert!(requests[3].contains("accept: text/event-stream"));
    assert!(requests[3].contains("last-event-id: stream-cursor-1"));
    assert!(requests[3].contains("mcp-session-id: session-resume"));
    assert!(requests[3].contains("mcp-protocol-version: 2025-11-25"));
}

#[tokio::test]
async fn sse_resumption_stops_after_its_bounded_get_attempt_without_replaying_the_post() {
    let (endpoint, server) = server(vec![
        json_reply(1, &initialize_result(), None),
        status_reply(202),
        sse_body_reply("id: stream-cursor-1\ndata:\n\n"),
        sse_body_reply("id: stream-cursor-2\ndata:\n\n"),
    ])
    .await;
    let config = McpHttpConfig::new(endpoint)
        .unwrap()
        .with_automatic_sse_resumption(1, Duration::from_millis(1), Duration::from_millis(1))
        .unwrap();
    let client = McpHttpClient::new(config).unwrap();
    client.initialize().await.unwrap();

    assert_eq!(
        client.list_tools().await.unwrap_err(),
        McpError::SseStreamTerminated
    );

    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 4);
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("\"method\":\"tools/list\""))
            .count(),
        1
    );
    assert!(requests[3].starts_with("get /mcp http/1.1\r\n"));
    assert!(requests[3].contains("last-event-id: stream-cursor-1"));
}

#[tokio::test]
async fn sse_resumption_session_expiry_clears_state_without_replaying_the_post() {
    let (endpoint, server) = server(vec![
        json_reply(1, &initialize_result(), Some("session-expired")),
        status_reply(202),
        sse_body_reply("id: stream-cursor-1\ndata:\n\n"),
        not_found_reply(),
    ])
    .await;
    let config = McpHttpConfig::new(endpoint)
        .unwrap()
        .with_automatic_sse_resumption(1, Duration::from_millis(1), Duration::from_millis(1))
        .unwrap();
    let client = McpHttpClient::new(config).unwrap();
    client.initialize().await.unwrap();

    assert_eq!(
        client.list_tools().await.unwrap_err(),
        McpError::SessionExpired
    );
    assert_eq!(
        client.list_tools().await.unwrap_err(),
        McpError::NotInitialized
    );

    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 4);
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("\"method\":\"tools/list\""))
            .count(),
        1
    );
    assert!(requests[3].starts_with("get /mcp http/1.1\r\n"));
    assert!(requests[3].contains("mcp-session-id: session-expired"));
}

#[tokio::test]
async fn standalone_sse_get_delivers_only_explicit_untrusted_notifications() {
    let notification = json!({
        "jsonrpc":"2.0",
        "method":"notifications/resources/list_changed",
        "params":{"private":"customer-7"}
    });
    let (endpoint, server) = server(vec![
        json_reply(1, &initialize_result(), Some("session-events")),
        status_reply(202),
        sse_body_reply(&format!("id: event-1\ndata: {notification}\n\n")),
    ])
    .await;
    let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
    client.initialize().await.unwrap();

    let mut stream = client.open_server_event_stream().await.unwrap();
    let received = stream.next_notification().await.unwrap();
    assert_eq!(received.method(), "notifications/resources/list_changed");
    assert_eq!(received.params(), notification.get("params"));
    assert!(!format!("{received:?}").contains("customer-7"));
    stream.close();

    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 3);
    assert!(requests[2].starts_with("get /mcp http/1.1\r\n"));
    assert!(requests[2].contains("accept: text/event-stream"));
    assert!(requests[2].contains("mcp-session-id: session-events"));
    assert!(requests[2].contains("mcp-protocol-version: 2025-11-25"));
}

#[tokio::test]
async fn standalone_sse_get_resumes_with_last_event_id_without_a_post() {
    let first = json!({
        "jsonrpc":"2.0",
        "method":"notifications/progress",
        "params":{"progress":1}
    });
    let second = json!({
        "jsonrpc":"2.0",
        "method":"notifications/progress",
        "params":{"progress":2}
    });
    let (endpoint, server) = server(vec![
        json_reply(1, &initialize_result(), Some("session-events")),
        status_reply(202),
        sse_body_reply(&format!("id: event-1\nretry: 1\ndata: {first}\n\n")),
        sse_body_reply(&format!("id: event-2\ndata: {second}\n\n")),
    ])
    .await;
    let config = McpHttpConfig::new(endpoint)
        .unwrap()
        .with_automatic_sse_resumption(1, Duration::from_millis(2), Duration::from_millis(5))
        .unwrap();
    let client = McpHttpClient::new(config).unwrap();
    client.initialize().await.unwrap();

    let mut stream = client.open_server_event_stream().await.unwrap();
    assert_eq!(
        stream.next_notification().await.unwrap().params(),
        first.get("params")
    );
    assert_eq!(
        stream.next_notification().await.unwrap().params(),
        second.get("params")
    );

    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 4);
    assert!(requests[2].starts_with("get /mcp http/1.1\r\n"));
    assert!(!requests[2].contains("last-event-id:"));
    assert!(requests[3].starts_with("get /mcp http/1.1\r\n"));
    assert!(requests[3].contains("last-event-id: event-1"));
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.starts_with("post /mcp http/1.1\r\n"))
            .count(),
        2
    );
}

#[tokio::test]
async fn standalone_sse_get_rejects_a_declared_body_above_its_configured_limit() {
    let (endpoint, server) = server(vec![
        json_reply(1, &initialize_result(), Some("session-events")),
        status_reply(202),
        sse_header_reply(257),
    ])
    .await;
    let config = McpHttpConfig::new(endpoint)
        .unwrap()
        .with_max_response_bytes(256)
        .unwrap();
    let client = McpHttpClient::new(config).unwrap();
    client.initialize().await.unwrap();

    assert_eq!(
        client.open_server_event_stream().await.unwrap_err(),
        McpError::ResponseTooLarge
    );
    server.await.unwrap();
}

#[tokio::test]
async fn standalone_sse_get_applies_the_response_limit_across_resumptions() {
    let first = json!({
        "jsonrpc":"2.0",
        "method":"notifications/progress",
        "params":{"progress":1,"detail":"x".repeat(256)}
    });
    let second = json!({
        "jsonrpc":"2.0",
        "method":"notifications/progress",
        "params":{"progress":2,"detail":"x".repeat(256)}
    });
    let first_body = format!("id: event-1\nretry: 1\ndata: {first}\n\n");
    let second_body = format!("id: event-2\ndata: {second}\n\n");
    let max_response_bytes = first_body.len().max(second_body.len());
    let (endpoint, server) = server(vec![
        json_reply(1, &initialize_result(), Some("session-events")),
        status_reply(202),
        sse_body_reply(&first_body),
        sse_body_reply(&second_body),
    ])
    .await;
    let config = McpHttpConfig::new(endpoint)
        .unwrap()
        .with_max_response_bytes(max_response_bytes)
        .unwrap()
        .with_automatic_sse_resumption(1, Duration::from_millis(1), Duration::from_millis(1))
        .unwrap();
    let client = McpHttpClient::new(config).unwrap();
    client.initialize().await.unwrap();

    let mut stream = client.open_server_event_stream().await.unwrap();
    assert_eq!(
        stream.next_notification().await.unwrap().params(),
        first.get("params")
    );
    assert_eq!(
        stream.next_notification().await.unwrap_err(),
        McpError::ResponseTooLarge
    );
    server.await.unwrap();
}

#[tokio::test]
async fn standalone_sse_get_rejects_server_requests_without_exposing_details() {
    let server_request = json!({
        "jsonrpc":"2.0",
        "id":"private-server-request",
        "method":"sampling/createMessage",
        "params":{"secret":"do not expose"}
    });
    let (endpoint, server) = server(vec![
        json_reply(1, &initialize_result(), None),
        status_reply(202),
        sse_body_reply(&format!("data: {server_request}\n\n")),
    ])
    .await;
    let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
    client.initialize().await.unwrap();

    let mut stream = client.open_server_event_stream().await.unwrap();
    let error = stream.next_notification().await.unwrap_err();
    assert_eq!(error, McpError::MalformedResponse);
    assert!(!error.to_string().contains("private-server-request"));
    server.await.unwrap();
}

#[tokio::test]
async fn standalone_sse_get_session_expiry_clears_local_state() {
    let (endpoint, server) = server(vec![
        json_reply(1, &initialize_result(), Some("session-events")),
        status_reply(202),
        not_found_reply(),
    ])
    .await;
    let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
    client.initialize().await.unwrap();

    assert_eq!(
        client.open_server_event_stream().await.unwrap_err(),
        McpError::SessionExpired
    );
    assert_eq!(
        client.open_server_event_stream().await.unwrap_err(),
        McpError::NotInitialized
    );

    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 3);
    assert!(requests[2].starts_with("get /mcp http/1.1\r\n"));
    assert!(requests[2].contains("mcp-session-id: session-events"));
}

#[tokio::test]
async fn standalone_sse_get_treats_a_server_405_as_a_closed_stream() {
    let (endpoint, server) = server(vec![
        json_reply(1, &initialize_result(), None),
        status_reply(202),
        "HTTP/1.1 405 Method Not Allowed\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
            .to_owned(),
    ])
    .await;
    let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
    client.initialize().await.unwrap();

    assert_eq!(
        client.open_server_event_stream().await.unwrap_err(),
        McpError::SseStreamTerminated
    );
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 3);
    assert!(requests[2].starts_with("get /mcp http/1.1\r\n"));
}

#[tokio::test]
async fn sse_response_bound_and_missing_terminal_result_are_sanitized() {
    let oversized = format!("data: {}", "x".repeat(512));
    let (endpoint, oversized_server) = server(vec![
        json_reply(1, &initialize_result(), None),
        status_reply(202),
        sse_body_reply(&oversized),
    ])
    .await;
    let config = McpHttpConfig::new(endpoint)
        .unwrap()
        .with_max_response_bytes(256)
        .unwrap();
    let client = McpHttpClient::new(config).unwrap();
    client.initialize().await.unwrap();
    assert_eq!(
        client.list_tools().await.unwrap_err(),
        McpError::ResponseTooLarge
    );
    oversized_server.await.unwrap();

    let (endpoint, server) = server(vec![
        json_reply(1, &initialize_result(), None),
        status_reply(202),
        sse_body_reply("event: message\n\n: keepalive\n\n"),
    ])
    .await;
    let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
    client.initialize().await.unwrap();
    assert_eq!(
        client.list_tools().await.unwrap_err(),
        McpError::SseStreamTerminated
    );
    server.await.unwrap();
}

#[tokio::test]
async fn sse_server_request_is_rejected_without_exposing_its_detail() {
    let server_request = json!({
        "jsonrpc":"2.0",
        "id":"private-server-request",
        "method":"sampling/createMessage",
        "params":{"secret":"do not expose"}
    });
    let (endpoint, server) = server(vec![
        json_reply(1, &initialize_result(), None),
        status_reply(202),
        sse_body_reply(&format!("data: {server_request}\n\n")),
    ])
    .await;
    let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
    client.initialize().await.unwrap();

    let error = client.list_tools().await.unwrap_err();
    assert_eq!(error, McpError::MalformedResponse);
    assert!(!error.to_string().contains("private-server-request"));
    server.await.unwrap();
}

#[test]
fn sse_frame_parser_handles_crlf_multiline_data_and_ignores_non_data_events() {
    let mut frame_buffer = b"event: message\r\ndata: {\"jsonrpc\":\"2.0\",\r\ndata: \"method\":\"notifications/progress\"}\r\n\r\n".to_vec();
    let frame = take_sse_frame(&mut frame_buffer).unwrap();
    assert_eq!(
        sse_payload(&frame).unwrap(),
        Some("{\"jsonrpc\":\"2.0\",\n\"method\":\"notifications/progress\"}".to_owned())
    );
    assert!(
        sse_payload(b"event: message\nretry: 10\n\n")
            .unwrap()
            .is_none()
    );
    let mut cr_only_buffer = b"data: {\"jsonrpc\":\"2.0\"}\r\r".to_vec();
    let frame = take_sse_frame(&mut cr_only_buffer).unwrap();
    assert_eq!(
        sse_payload(&frame).unwrap(),
        Some("{\"jsonrpc\":\"2.0\"}".to_owned())
    );
}
