//! MCP HTTP request-admission and timeout regression coverage.

use super::*;

#[tokio::test]
async fn initialization_rejects_an_oversized_http_request_before_transport() {
    let config = McpHttpConfig::new(url::Url::parse("http://127.0.0.1:1/mcp").unwrap())
        .unwrap()
        .with_max_request_bytes(1)
        .unwrap();
    let client = McpHttpClient::new(config).unwrap();

    assert_eq!(
        client.initialize().await.unwrap_err(),
        McpError::RequestTooLarge
    );
}

#[tokio::test]
async fn injected_client_still_enforces_the_configured_request_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (request_received, request_was_received) = oneshot::channel();
    let (release_server, release) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _request = read_request(&mut stream).await;
        let _ = request_received.send(());
        let _ = release.await;
    });
    let config = McpHttpConfig::new(url::Url::parse(&format!("http://{address}/mcp")).unwrap())
        .unwrap()
        .with_request_timeout(Duration::from_millis(10))
        .unwrap();
    let transport = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let client = McpHttpClient::with_client(transport, config);

    let result = client.initialize().await;
    timeout(Duration::from_secs(1), request_was_received)
        .await
        .unwrap()
        .unwrap();
    let _ = release_server.send(());
    server.await.unwrap();
    assert_eq!(result.unwrap_err(), McpError::Transport);
}
