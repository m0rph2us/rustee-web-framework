//! HTTP/1 connection and request-limit regression coverage.

use super::*;

#[tokio::test]
async fn header_read_timeout_closes_an_incomplete_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        serve_listener_with_options(
            listener,
            App::new().get("/health", || async { "ok" }),
            ServerOptions {
                header_read_timeout: Duration::from_millis(20),
                ..ServerOptions::default()
            },
            async move {
                let _ = shutdown_rx.await;
            },
        )
        .await
        .unwrap();
    });

    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n")
        .await
        .unwrap();
    let mut byte = [0_u8; 1];
    let result = timeout(Duration::from_secs(1), stream.read(&mut byte))
        .await
        .expect("incomplete request should be closed after the header deadline");
    assert!(
        matches!(result, Ok(0) | Err(_)),
        "incomplete request must not remain open: {result:?}"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn http1_buffer_limit_closes_an_oversized_incomplete_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = Arc::clone(&calls);
    let app = App::new().get("/health", move || {
        let calls = Arc::clone(&handler_calls);
        async move {
            calls.fetch_add(1, Ordering::Relaxed);
            "ok"
        }
    });
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        serve_listener_with_options(
            listener,
            app,
            ServerOptions {
                max_http1_buffer_bytes: MIN_HTTP1_BUFFER_BYTES,
                ..ServerOptions::default()
            },
            async move {
                let _ = shutdown_rx.await;
            },
        )
        .await
        .unwrap();
    });

    let mut stream = TcpStream::connect(address).await.unwrap();
    let request = format!(
        "GET /health HTTP/1.1\r\nHost: localhost\r\nX-Padding: {}\r\n",
        "a".repeat(MIN_HTTP1_BUFFER_BYTES)
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    let result = timeout(Duration::from_secs(1), stream.read_to_end(&mut response))
        .await
        .expect("oversized incomplete request should be closed promptly");
    if result.is_ok() {
        assert!(!response.starts_with(b"HTTP/1.1 200 OK\r\n"));
    }
    assert_eq!(calls.load(Ordering::Relaxed), 0);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn connection_limit_closes_an_excess_connection_before_request_execution() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let started_handler = Arc::clone(&started);
    let release_handler = Arc::clone(&release);
    let app = App::new().get("/hold", move || {
        let started = Arc::clone(&started_handler);
        let release = Arc::clone(&release_handler);
        async move {
            started.notify_one();
            release.notified().await;
            "released"
        }
    });
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        serve_listener_with_options(
            listener,
            app,
            ServerOptions {
                max_connections: 1,
                ..ServerOptions::default()
            },
            async move {
                let _ = shutdown_rx.await;
            },
        )
        .await
        .unwrap();
    });

    let mut held = TcpStream::connect(address).await.unwrap();
    held.write_all(b"GET /hold HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("first connection should begin handling its request");

    let mut excess = TcpStream::connect(address).await.unwrap();
    let mut byte = [0_u8; 1];
    let result = match excess
        .write_all(b"GET /hold HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
    {
        Ok(()) => timeout(Duration::from_secs(1), excess.read(&mut byte))
            .await
            .expect("excess connection should be closed promptly"),
        Err(error) => Err(error),
    };
    assert!(
        matches!(result, Ok(0) | Err(_)),
        "excess connection must not receive an application response: {result:?}"
    );

    release.notify_one();
    let mut response = Vec::new();
    timeout(Duration::from_secs(1), held.read_to_end(&mut response))
        .await
        .expect("held connection should finish after release")
        .unwrap();
    assert!(
        String::from_utf8(response)
            .unwrap()
            .starts_with("HTTP/1.1 200 OK\r\n")
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn concurrency_limit_rejects_before_second_handler_execution() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let handler_calls = Arc::clone(&calls);
    let started_handler = Arc::clone(&started);
    let release_handler = Arc::clone(&release);
    let app = App::new().get("/hold", move || {
        let calls = Arc::clone(&handler_calls);
        let started = Arc::clone(&started_handler);
        let release = Arc::clone(&release_handler);
        async move {
            calls.fetch_add(1, Ordering::Relaxed);
            started.notify_one();
            release.notified().await;
            "released"
        }
    });
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        serve_listener_with_options(
            listener,
            app,
            ServerOptions {
                max_in_flight_requests: 1,
                ..ServerOptions::default()
            },
            async move {
                let _ = shutdown_rx.await;
            },
        )
        .await
        .unwrap();
    });

    let mut held = TcpStream::connect(address).await.unwrap();
    held.write_all(b"GET /hold HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("first request should begin handling");

    let rejected = raw_request(
        address,
        b"GET /hold HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(rejected.starts_with("HTTP/1.1 503 Service Unavailable\r\n"));
    assert_eq!(calls.load(Ordering::Relaxed), 1);

    release.notify_one();
    let mut response = Vec::new();
    timeout(Duration::from_secs(1), held.read_to_end(&mut response))
        .await
        .expect("held request should finish after release")
        .unwrap();
    assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn body_limit_returns_payload_too_large() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = App::new().post("/echo", |body: Bytes| async move { body });
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        serve_listener_with_options(
            listener,
            app,
            ServerOptions {
                max_body_bytes: 3,
                ..ServerOptions::default()
            },
            async move {
                let _ = shutdown_rx.await;
            },
        )
        .await
        .unwrap();
    });

    let response = raw_request(
        address,
        b"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\nConnection: close\r\n\r\nbody",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 413 Payload Too Large\r\n"));

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn declared_body_limit_rejects_without_waiting_for_the_payload() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = Arc::clone(&calls);
    let app = App::new().post("/ignore", move || {
        let calls = Arc::clone(&handler_calls);
        async move {
            calls.fetch_add(1, Ordering::Relaxed);
            "handler must not run"
        }
    });
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        serve_listener_with_options(
            listener,
            app,
            ServerOptions {
                max_body_bytes: 3,
                ..ServerOptions::default()
            },
            async move {
                let _ = shutdown_rx.await;
            },
        )
        .await
        .unwrap();
    });

    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(
            b"POST /ignore HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    timeout(Duration::from_secs(1), stream.read_to_end(&mut response))
        .await
        .expect("declared oversized payload should be rejected before it is uploaded")
        .unwrap();
    assert!(response.starts_with(b"HTTP/1.1 413 Payload Too Large\r\n"));
    assert_eq!(calls.load(Ordering::Relaxed), 0);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn declared_body_limit_rejects_before_handler_execution() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = Arc::clone(&calls);
    let app = App::new().post("/ignore", move || {
        let calls = Arc::clone(&handler_calls);
        async move {
            calls.fetch_add(1, Ordering::Relaxed);
            "handler must not run"
        }
    });
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        serve_listener_with_options(
            listener,
            app,
            ServerOptions {
                max_body_bytes: 3,
                ..ServerOptions::default()
            },
            async move {
                let _ = shutdown_rx.await;
            },
        )
        .await
        .unwrap();
    });

    let response = raw_request(
        address,
        b"POST /ignore HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\nConnection: close\r\n\r\nbody",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 413 Payload Too Large\r\n"));
    assert_eq!(calls.load(Ordering::Relaxed), 0);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn chunked_body_limit_rejects_after_stream_admission_before_handler_execution() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = Arc::clone(&calls);
    let app = App::new().post("/echo", move |body: Bytes| {
        let calls = Arc::clone(&handler_calls);
        async move {
            calls.fetch_add(1, Ordering::Relaxed);
            body
        }
    });
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        serve_listener_with_options(
            listener,
            app,
            ServerOptions {
                max_body_bytes: 3,
                ..ServerOptions::default()
            },
            async move {
                let _ = shutdown_rx.await;
            },
        )
        .await
        .unwrap();
    });

    let response = raw_request(
        address,
        b"POST /echo HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4\r\nbody\r\n0\r\n\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 413 Payload Too Large\r\n"));
    assert_eq!(calls.load(Ordering::Relaxed), 0);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn request_timeout_returns_timeout_response() {
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
            "finished too late"
        }
    });
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        serve_listener_with_options(
            listener,
            app,
            ServerOptions {
                request_timeout: Duration::from_millis(5),
                ..ServerOptions::default()
            },
            async move {
                let _ = shutdown_rx.await;
            },
        )
        .await
        .unwrap();
    });

    let response = raw_request(
        address,
        b"GET /slow HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("slow handler should begin before its request deadline");
    assert!(response.starts_with("HTTP/1.1 408 Request Timeout\r\n"));

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}
