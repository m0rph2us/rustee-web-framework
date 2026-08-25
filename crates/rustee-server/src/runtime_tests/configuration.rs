//! Server option validation regression coverage.

use super::*;

#[tokio::test]
async fn invalid_server_limits_and_timeouts_are_rejected_before_accepting_connections() {
    let invalid_options = [
        ServerOptions {
            max_body_bytes: 0,
            ..ServerOptions::default()
        },
        ServerOptions {
            max_http1_buffer_bytes: 0,
            ..ServerOptions::default()
        },
        ServerOptions {
            max_connections: 0,
            ..ServerOptions::default()
        },
        ServerOptions {
            max_in_flight_requests: 0,
            ..ServerOptions::default()
        },
        ServerOptions {
            header_read_timeout: Duration::ZERO,
            ..ServerOptions::default()
        },
        ServerOptions {
            request_timeout: Duration::ZERO,
            ..ServerOptions::default()
        },
        ServerOptions {
            graceful_shutdown_timeout: Duration::ZERO,
            ..ServerOptions::default()
        },
    ];

    for options in invalid_options {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let error =
            serve_listener_with_options(listener, App::new(), options, std::future::pending())
                .await
                .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            "Rustee server limits and timeouts must be greater than zero"
        );
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let error = serve_listener_with_options(
        listener,
        App::new(),
        ServerOptions {
            max_http1_buffer_bytes: MIN_HTTP1_BUFFER_BYTES - 1,
            ..ServerOptions::default()
        },
        std::future::pending(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "Rustee HTTP/1 buffer limit must be at least 8192 bytes"
    );

    for options in [
        ServerOptions {
            max_connections: tokio::sync::Semaphore::MAX_PERMITS + 1,
            ..ServerOptions::default()
        },
        ServerOptions {
            max_in_flight_requests: tokio::sync::Semaphore::MAX_PERMITS + 1,
            ..ServerOptions::default()
        },
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let error =
            serve_listener_with_options(listener, App::new(), options, std::future::pending())
                .await
                .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            "Rustee server concurrency limits exceed the supported maximum"
        );
    }
}
