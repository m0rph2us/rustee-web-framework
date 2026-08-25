//! Shared local HTTP fixture and request factory for `OpenAI` adapter regressions.

use rustee_ai::{ChatMessage, ChatRequest, MessageRole, ToolDefinition};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};
use url::Url;

pub(super) fn request() -> ChatRequest {
    ChatRequest::new(
        "gpt-test",
        [ChatMessage::new(MessageRole::User, "what is the status?").unwrap()],
    )
    .unwrap()
    .with_tools([ToolDefinition::new("lookup_order", json!({"type":"object"})).unwrap()])
}

pub(super) async fn response_server(
    content_type: &'static str,
    body: String,
) -> (Url, oneshot::Receiver<String>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = Url::parse(&format!("http://{}/v1/", listener.local_addr().unwrap())).unwrap();
    let (request_sender, request) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_http_request(&mut socket).await;
        request_sender.send(request).unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    (url, request, server)
}

pub(super) async fn declared_length_response_server(
    content_type: &'static str,
    declared_length: usize,
) -> (Url, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = Url::parse(&format!("http://{}/v1/", listener.local_addr().unwrap())).unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_http_request(&mut socket).await;
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ncontent-length: {declared_length}\r\nconnection: keep-alive\r\n\r\n"
        );
        socket.write_all(response.as_bytes()).await.unwrap();
        socket.shutdown().await.unwrap();
    });
    (url, server)
}

pub(super) async fn read_http_request(socket: &mut tokio::net::TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let read = socket.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0);
        bytes.extend_from_slice(&chunk[..read]);
        let Some(headers_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let headers = std::str::from_utf8(&bytes[..headers_end]).unwrap();
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.split_once(':')
                    .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .map(|(_, value)| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        if bytes.len() >= headers_end + 4 + content_length {
            return String::from_utf8(bytes).unwrap();
        }
    }
}
