//! Tower service readiness and layered transport integration regression coverage.

use super::*;

#[derive(Default)]
struct CloneRequiresReadiness {
    ready: bool,
}

impl Clone for CloneRequiresReadiness {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl Service<Request> for CloneRequiresReadiness {
    type Response = Response;
    type Error = Infallible;
    type Future = Ready<Result<Response, Infallible>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.ready = true;
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: Request) -> Self::Future {
        assert!(self.ready, "clone-local poll_ready must precede call");
        self.ready = false;
        ready(Ok("ready".into_response()))
    }
}

async fn panic_handler() {
    panic!("private handler panic detail");
}

#[tokio::test]
async fn server_readies_a_cloned_service_before_call() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        serve_service_listener_with_options(
            listener,
            CloneRequiresReadiness::default(),
            ServerOptions::default(),
            async move {
                let _ = shutdown_rx.await;
            },
        )
        .await
        .unwrap();
    });

    let response = raw_request(
        address,
        b"GET /ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response.ends_with("\r\n\r\nready"));

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn real_tcp_panic_boundary_returns_a_redacted_internal_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = PanicCatchLayer::new().layer(App::new().get("/panic", panic_handler));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        serve_service_listener_with_options(listener, app, ServerOptions::default(), async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let response = raw_request(
        address,
        b"GET /panic HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 500 Internal Server Error\r\n"));
    assert!(response.contains("\"code\":\"internal_error\""));
    assert!(!response.contains("private handler panic detail"));

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn real_tcp_requests_receive_a_generated_request_id() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = RequestIdLayer::new().layer(App::new().get("/health", || async { "ok" }));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        serve_service_listener_with_options(listener, app, ServerOptions::default(), async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let response = raw_request(
        address,
        b"GET /health HTTP/1.1\r\nHost: localhost\r\nX-Request-Id: client-controlled\r\nConnection: close\r\n\r\n",
    )
    .await;
    let request_id = response
        .lines()
        .find_map(|line| line.strip_prefix("x-request-id: "))
        .expect("response must include a generated request ID");
    assert_ne!(request_id, "client-controlled");
    assert_eq!(request_id.len(), 32);
    assert!(request_id.bytes().all(|byte| byte.is_ascii_hexdigit()));

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn real_tcp_requests_receive_transport_connection_info() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = App::new().get("/peer", |connection: ConnectionInfo| async move {
        connection.peer_addr().ip().to_string()
    });
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
        b"GET /peer HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(response.ends_with("\r\n\r\n127.0.0.1"));

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn tower_layered_service_serves_cors_preflight_over_tcp() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = CorsLayer::new("https://app.example.test".parse().unwrap())
        .layer(App::new().get("/resource", || async { "resource" }));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        serve_service_listener_with_options(
            listener,
            service,
            ServerOptions::default(),
            async move {
                let _ = shutdown_rx.await;
            },
        )
        .await
        .unwrap();
    });

    let response = raw_request(
        address,
        b"OPTIONS /resource HTTP/1.1\r\nHost: localhost\r\nOrigin: https://app.example.test\r\nAccess-Control-Request-Method: GET\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 204 No Content\r\n"));
    assert!(response.contains(&format!(
        "{}: https://app.example.test",
        ACCESS_CONTROL_ALLOW_ORIGIN.as_str()
    )));

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}
