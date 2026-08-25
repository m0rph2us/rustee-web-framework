//! Shared bounded loopback MCP fixture and approval helpers.

use std::{convert::Infallible, fmt::Write as _, sync::Arc};

use rustee_ai::{
    AiExecutionContext, ToolApprovalDecision, ToolApprovalPolicy, ToolCall, ToolExecutionContext,
    ToolRisk,
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Mutex,
};

use crate::MCP_PROTOCOL_VERSION;

#[derive(Clone, Copy)]
pub(super) struct Approve;

impl ToolApprovalPolicy for Approve {
    type Error = Infallible;

    fn approve(
        &self,
        _: AiExecutionContext,
        _: ToolCall,
        _: ToolRisk,
    ) -> futures_util::future::BoxFuture<'static, Result<ToolApprovalDecision, Self::Error>> {
        Box::pin(futures_util::future::ready(Ok(
            ToolApprovalDecision::Approved,
        )))
    }
}

pub(super) fn tool_context() -> ToolExecutionContext {
    ToolExecutionContext::new(
        AiExecutionContext::new("tenant-a", "user-7").unwrap(),
        "external:order:7",
    )
    .unwrap()
}

pub(super) async fn server(
    replies: Vec<String>,
) -> (url::Url, tokio::task::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint =
        url::Url::parse(&format!("http://{}/mcp", listener.local_addr().unwrap())).unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&requests);
    let server = tokio::spawn(async move {
        for reply in replies {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_request(&mut stream).await;
            captured.lock().await.push(request);
            stream.write_all(reply.as_bytes()).await.unwrap();
        }
        captured.lock().await.clone()
    });
    (endpoint, server)
}

pub(super) async fn read_request(stream: &mut tokio::net::TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 1024];
    loop {
        let read = stream.read(&mut buffer).await.unwrap();
        assert_ne!(read, 0);
        bytes.extend_from_slice(&buffer[..read]);
        let Some(headers_end) = bytes.windows(4).position(|value| value == b"\r\n\r\n") else {
            continue;
        };
        let headers = std::str::from_utf8(&bytes[..headers_end]).unwrap();
        let content_length = headers
            .lines()
            .find_map(|line| line.strip_prefix("content-length: "))
            .map_or(0, |value| value.parse::<usize>().unwrap());
        if bytes.len() >= headers_end + 4 + content_length {
            return String::from_utf8(bytes).unwrap().to_ascii_lowercase();
        }
    }
}

pub(super) fn json_reply(id: u64, result: &Value, session_id: Option<&str>) -> String {
    let body = json!({"jsonrpc":"2.0","id":id,"result":result}).to_string();
    let session = session_id
        .map(|value| format!("mcp-session-id: {value}\r\n"))
        .unwrap_or_default();
    format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n{session}content-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}

pub(super) fn sse_reply(id: u64, result: &Value, notifications: &[Value]) -> String {
    let mut body = String::new();
    for (index, notification) in notifications.iter().enumerate() {
        write!(
            body,
            "id: notification-{index}\r\nevent: message\r\ndata: {notification}\r\n\r\n"
        )
        .expect("writing to String cannot fail");
    }
    write!(
        body,
        "id: result-{id}\r\nevent: message\r\ndata: {}\r\n\r\n",
        json!({"jsonrpc":"2.0","id":id,"result":result})
    )
    .expect("writing to String cannot fail");
    sse_body_reply(&body)
}

pub(super) fn sse_body_reply(body: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream; charset=utf-8\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}

pub(super) fn sse_header_reply(content_length: usize) -> String {
    format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream; charset=utf-8\r\ncontent-length: {content_length}\r\nconnection: close\r\n\r\n"
    )
}

pub(super) fn status_reply(status: u16) -> String {
    format!("HTTP/1.1 {status} Accepted\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
}

pub(super) fn not_found_reply() -> String {
    "HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_owned()
}

pub(super) fn initialize_result() -> Value {
    json!({
        "protocolVersion":MCP_PROTOCOL_VERSION,
        "capabilities":{},
        "serverInfo":{"name":"fixture","version":"0.1.0"}
    })
}

pub(super) fn context_replies() -> Vec<String> {
    vec![
        json_reply(1, &context_initialize_result(), Some("context-session")),
        status_reply(202),
        json_reply(
            2,
            &json!({
                "resources":[{
                    "uri":"resource://tenant-a/customer/7",
                    "name":"customer-record",
                    "mimeType":"text/plain",
                    "size":31
                }],
                "nextCursor":"resource-page-2"
            }),
            None,
        ),
        json_reply(3, &json!({"resources":[]}), None),
        json_reply(
            4,
            &json!({
                "resourceTemplates":[{
                    "uriTemplate":"resource://tenant-a/customer/{id}",
                    "name":"customer-by-id",
                    "mimeType":"text/plain"
                }]
            }),
            None,
        ),
        json_reply(
            5,
            &json!({
                "contents":[{
                    "uri":"resource://tenant-a/customer/7",
                    "mimeType":"text/plain",
                    "text":"private customer context"
                }]
            }),
            None,
        ),
        json_reply(
            6,
            &json!({
                "prompts":[{
                    "name":"customer-summary",
                    "arguments":[{"name":"customer_id","required":true}]
                }]
            }),
            None,
        ),
        json_reply(
            7,
            &json!({
                "description":"Summarize one selected customer record.",
                "messages":[
                    {"role":"user","content":{"type":"text","text":"Summarize the selected customer."}},
                    {"role":"assistant","content":{"type":"resource_link","uri":"resource://tenant-a/customer/7","name":"customer-record","mimeType":"text/plain"}}
                ]
            }),
            None,
        ),
    ]
}

fn context_initialize_result() -> Value {
    json!({
        "protocolVersion":MCP_PROTOCOL_VERSION,
        "capabilities":{"resources":{},"prompts":{}},
        "serverInfo":{"name":"fixture","version":"0.1.0"}
    })
}

pub(super) fn assert_context_request_sequence(requests: &[String]) {
    assert_eq!(requests.len(), 8);
    assert!(requests[2].contains("\"method\":\"resources/list\""));
    assert!(requests[3].contains("\"cursor\":\"resource-page-2\""));
    assert!(requests[4].contains("\"method\":\"resources/templates/list\""));
    assert!(requests[5].contains("\"method\":\"resources/read\""));
    assert!(requests[6].contains("\"method\":\"prompts/list\""));
    assert!(requests[7].contains("\"method\":\"prompts/get\""));
    assert!(requests[7].contains("\"customer_id\":\"7\""));
    assert!(
        requests[1..]
            .iter()
            .all(|request| request.contains("mcp-session-id: context-session"))
    );
    assert!(
        requests
            .iter()
            .all(|request| !request.contains("tools/call"))
    );
}
