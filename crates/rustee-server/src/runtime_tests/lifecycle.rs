//! Shutdown signaling and active-request draining regression coverage.

use super::*;

#[tokio::test]
async fn connection_shutdown_observes_a_signal_sent_before_subscription() {
    let (shutdown_sender, mut shutdown) = tokio::sync::watch::channel(false);
    shutdown_sender.send(true).unwrap();

    assert!(super::super::wait_for_connection_shutdown(&mut shutdown).await);
}

#[tokio::test]
async fn shutdown_drains_an_active_request_before_returning() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let started_handler = Arc::clone(&started);
    let release_handler = Arc::clone(&release);
    let app = App::new().get("/slow", move || {
        let started = Arc::clone(&started_handler);
        let release = Arc::clone(&release_handler);
        async move {
            started.notify_one();
            release.notified().await;
            "drained"
        }
    });
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        serve_listener(listener, app, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let client = tokio::spawn(raw_request(
        address,
        b"GET /slow HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    ));
    started.notified().await;
    let _ = shutdown_tx.send(());
    tokio::task::yield_now().await;
    assert!(!server.is_finished());

    release.notify_one();
    let response = client.await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    server.await.unwrap();
}
