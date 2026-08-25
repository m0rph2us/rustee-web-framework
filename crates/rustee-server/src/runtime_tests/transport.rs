//! Basic listener and HTTP/1 transport regression coverage.

use super::*;

#[tokio::test]
async fn serves_a_real_http1_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = App::new().get("/health", || async { "ok" });
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        serve_listener(listener, app, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let response = raw_request(
        address,
        b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response.ends_with("\r\n\r\nok"));

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}
