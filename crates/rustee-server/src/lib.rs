//! Tokio and Hyper transport for Rustee applications.

use std::{convert::Infallible, future::Future, io, net::SocketAddr, sync::Arc, time::Duration};

use http_body_util::{BodyExt, Limited};
use hyper::{
    Request as HyperRequest, StatusCode, body::Incoming, server::conn::http1, service::service_fn,
};
use hyper_util::rt::TokioIo;
use rustee_core::{ConnectionInfo, Error, IntoResponse, Request};
use rustee_router::App;
use tokio::{
    net::TcpListener,
    sync::{Semaphore, watch},
    task::JoinSet,
    time::timeout,
};
use tower::{Service, util::BoxCloneService};

/// Transport limits that are deliberately explicit rather than unbounded defaults.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServerOptions {
    /// Maximum number of bytes accepted from one request body.
    pub max_body_bytes: usize,
    /// Maximum duration allowed for application handler execution.
    pub request_timeout: Duration,
    /// Maximum number of handlers that may execute at once for this listener.
    pub max_in_flight_requests: usize,
    /// Maximum time to let active connections drain after shutdown starts.
    pub graceful_shutdown_timeout: Duration,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            max_body_bytes: 2 * 1024 * 1024,
            request_timeout: Duration::from_secs(30),
            max_in_flight_requests: 1_024,
            graceful_shutdown_timeout: Duration::from_secs(10),
        }
    }
}

/// Binds an HTTP/1.1 listener and serves an application until its task is cancelled.
///
/// Rustee's initial transport contract is cleartext HTTP/1.1. Put TLS and HTTP/2 at a
/// trusted reverse proxy until their support is introduced through a dedicated ADR.
///
/// # Errors
///
/// Returns an I/O error when the listener cannot bind or accept a connection.
pub async fn serve(address: SocketAddr, app: App) -> io::Result<()> {
    serve_with_options(address, app, ServerOptions::default()).await
}

/// Binds a listener with explicit operational limits and serves an application.
///
/// # Errors
///
/// Returns an I/O error when the listener cannot bind, cannot accept a connection, or an invalid
/// server limit is supplied.
pub async fn serve_with_options(
    address: SocketAddr,
    app: App,
    options: ServerOptions,
) -> io::Result<()> {
    let listener = TcpListener::bind(address).await?;
    serve_listener_with_options(listener, app, options, std::future::pending()).await
}

/// Serves an already-bound listener until `shutdown` resolves.
///
/// # Errors
///
/// Returns an I/O error when accepting a connection fails or the configured server limits are
/// invalid.
pub async fn serve_listener<Shutdown>(
    listener: TcpListener,
    app: App,
    shutdown: Shutdown,
) -> io::Result<()>
where
    Shutdown: Future<Output = ()> + Send,
{
    serve_listener_with_options(listener, app, ServerOptions::default(), shutdown).await
}

/// Serves an already-bound listener with explicit operational limits until `shutdown` resolves.
///
/// # Errors
///
/// Returns an I/O error when accepting a connection fails or the configured server limits are
/// invalid.
pub async fn serve_listener_with_options<Shutdown>(
    listener: TcpListener,
    app: App,
    options: ServerOptions,
    shutdown: Shutdown,
) -> io::Result<()>
where
    Shutdown: Future<Output = ()> + Send,
{
    serve_service_listener_with_options(listener, app, options, shutdown).await
}

/// Serves a Tower-compatible Rustee service until `shutdown` resolves.
///
/// # Errors
///
/// Returns an I/O error when accepting a connection fails or the configured server limits are
/// invalid.
pub async fn serve_service_listener_with_options<ServiceType, Shutdown>(
    listener: TcpListener,
    service: ServiceType,
    options: ServerOptions,
    shutdown: Shutdown,
) -> io::Result<()>
where
    ServiceType: Service<Request, Response = rustee_core::Response, Error = Infallible>
        + Clone
        + Send
        + 'static,
    ServiceType::Future: Send + 'static,
    Shutdown: Future<Output = ()> + Send,
{
    if options.max_body_bytes == 0 || options.max_in_flight_requests == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Rustee server limits must be greater than zero",
        ));
    }

    tokio::pin!(shutdown);
    let concurrency = Arc::new(Semaphore::new(options.max_in_flight_requests));
    let service = BoxCloneService::new(service);
    let (connection_shutdown_tx, connection_shutdown_rx) = watch::channel(false);
    let mut connections = JoinSet::new();

    loop {
        tokio::select! {
            () = &mut shutdown => {
                let _ = connection_shutdown_tx.send(true);
                let drain = async {
                    while connections.join_next().await.is_some() {}
                };
                if timeout(options.graceful_shutdown_timeout, drain).await.is_err() {
                    connections.abort_all();
                    while connections.join_next().await.is_some() {}
                }
                return Ok(());
            },
            joined = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = joined {
                    tracing::debug!(%error, "HTTP connection task ended unexpectedly");
                }
            }
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let service = service.clone();
                let concurrency = concurrency.clone();
                let mut connection_shutdown = connection_shutdown_rx.clone();
                connections.spawn(async move {
                    let service = service_fn(move |request: HyperRequest<Incoming>| {
                        let mut service = service.clone();
                        let concurrency = concurrency.clone();
                        async move {
                            let request = into_rustee_request(request, options.max_body_bytes, peer);
                            let response = match concurrency.try_acquire_owned() {
                                Ok(permit) => {
                                    let response = match timeout(options.request_timeout, service.call(request)).await {
                                        Ok(Ok(response)) => response,
                                        Ok(Err(never)) => match never {},
                                        Err(_) => Error::new(
                                            StatusCode::REQUEST_TIMEOUT,
                                            "request_timeout",
                                            "the request exceeded the configured timeout",
                                        ).into_response(),
                                    };
                                    drop(permit);
                                    response
                                }
                                Err(_) => Error::new(
                                    StatusCode::SERVICE_UNAVAILABLE,
                                    "concurrency_limit_exceeded",
                                    "the server is handling too many requests",
                                ).into_response(),
                            };
                            Ok::<_, Infallible>(response)
                        }
                    });

                    let connection = http1::Builder::new()
                        .keep_alive(true)
                        .serve_connection(TokioIo::new(stream), service);
                    tokio::pin!(connection);

                    tokio::select! {
                        result = &mut connection => {
                            if let Err(error) = result {
                                tracing::debug!(%peer, %error, "HTTP/1 connection ended with an error");
                            }
                        }
                        changed = connection_shutdown.changed() => {
                            if changed.is_ok() {
                                connection.as_mut().graceful_shutdown();
                                if let Err(error) = connection.await {
                                    tracing::debug!(%peer, %error, "HTTP/1 connection did not drain cleanly");
                                }
                            }
                        }
                    }
                });
            }
        }
    }
}

fn into_rustee_request(
    request: HyperRequest<Incoming>,
    max_body_bytes: usize,
    peer_addr: SocketAddr,
) -> Request {
    let mut request = request.map(|body| Limited::new(body, max_body_bytes).boxed_unsync());
    request
        .extensions_mut()
        .insert(ConnectionInfo::new(peer_addr));
    request
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use bytes::Bytes;
    use http::header::ACCESS_CONTROL_ALLOW_ORIGIN;
    use rustee_core::ConnectionInfo;
    use rustee_middleware::{CorsLayer, PanicCatchLayer};
    use rustee_observability::RequestIdLayer;
    use rustee_router::App;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::{Notify, oneshot},
        time::timeout,
    };

    use super::{
        ServerOptions, serve_listener, serve_listener_with_options,
        serve_service_listener_with_options,
    };
    use tower::Layer;

    async fn raw_request(address: std::net::SocketAddr, request: &[u8]) -> String {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(request).await.unwrap();
        let mut response = Vec::new();
        timeout(Duration::from_secs(2), stream.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        String::from_utf8(response).unwrap()
    }

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

    #[tokio::test]
    async fn real_tcp_panic_boundary_returns_a_redacted_internal_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = PanicCatchLayer::new().layer(App::new().get("/panic", || async {
            panic!("private handler panic detail");
            #[allow(unreachable_code)]
            "unreachable"
        }));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            serve_service_listener_with_options(
                listener,
                app,
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
            serve_service_listener_with_options(
                listener,
                app,
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
    async fn request_timeout_returns_timeout_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = App::new().get("/slow", || async {
            tokio::time::sleep(Duration::from_millis(40)).await;
            "finished too late"
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
        assert!(response.starts_with("HTTP/1.1 408 Request Timeout\r\n"));

        let _ = shutdown_tx.send(());
        server.await.unwrap();
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
}
