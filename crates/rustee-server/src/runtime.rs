//! HTTP/1 listener lifecycle and request dispatch.

use std::{convert::Infallible, future::Future, io, net::SocketAddr, sync::Arc, time::Duration};

use http_body_util::{BodyExt, Limited};
use hyper::header::CONTENT_LENGTH;
use hyper::{
    Request as HyperRequest, StatusCode, body::Incoming, server::conn::http1, service::service_fn,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use rustee_core::{BoxCloneServiceExt, ConnectionInfo, Error, IntoResponse, Request, Response};
use rustee_router::App;
use tokio::{
    net::TcpListener,
    sync::{Semaphore, watch},
    task::JoinSet,
    time::timeout,
};
use tower::{Service, util::BoxCloneService};

use crate::ServerOptions;

const MAX_HTTP1_REQUEST_HEADERS: usize = 100;

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
    options.validate()?;

    tokio::pin!(shutdown);
    let connections_limit = Arc::new(Semaphore::new(options.max_connections));
    let concurrency = Arc::new(Semaphore::new(options.max_in_flight_requests));
    let service = BoxCloneService::new(service);
    let (connection_shutdown_tx, connection_shutdown_rx) = watch::channel(false);
    let mut connections = JoinSet::new();

    loop {
        tokio::select! {
            () = &mut shutdown => {
                let _ = connection_shutdown_tx.send(true);
                drain_connections(&mut connections, options.graceful_shutdown_timeout).await;
                return Ok(());
            },
            joined = connections.join_next(), if !connections.is_empty() => {
                if matches!(joined, Some(Err(_))) {
                    tracing::debug!("HTTP connection task ended unexpectedly");
                }
            }
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let Ok(connection_permit) = connections_limit.clone().try_acquire_owned() else {
                    tracing::debug!("HTTP connection limit exceeded");
                    continue;
                };
                let service = service.clone();
                let concurrency = concurrency.clone();
                let mut connection_shutdown = connection_shutdown_rx.clone();
                connections.spawn(async move {
                    let _connection_permit = connection_permit;
                    let request_service = service_fn(move |request: HyperRequest<Incoming>| {
                        let service = service.clone();
                        let concurrency = concurrency.clone();
                        async move {
                            Ok::<_, Infallible>(
                                dispatch_request(request, service, concurrency, options, peer).await,
                            )
                        }
                    });

                    let mut builder = http1::Builder::new();
                    builder
                        .keep_alive(true)
                        .timer(TokioTimer::new())
                        .max_headers(MAX_HTTP1_REQUEST_HEADERS)
                        .max_buf_size(options.max_http1_buffer_bytes)
                        .header_read_timeout(options.header_read_timeout);
                    let connection = builder.serve_connection(TokioIo::new(stream), request_service);
                    tokio::pin!(connection);

                    tokio::select! {
                        result = &mut connection => {
                            if result.is_err() {
                                tracing::debug!("HTTP/1 connection ended with an error");
                            }
                        }
                        shutdown_requested = wait_for_connection_shutdown(&mut connection_shutdown) => {
                            if shutdown_requested {
                                connection.as_mut().graceful_shutdown();
                                if connection.await.is_err() {
                                    tracing::debug!("HTTP/1 connection did not drain cleanly");
                                }
                            }
                        }
                    }
                });
            }
        }
    }
}

async fn dispatch_request(
    request: HyperRequest<Incoming>,
    service: BoxCloneService<Request, Response, Infallible>,
    concurrency: Arc<Semaphore>,
    options: ServerOptions,
    peer: SocketAddr,
) -> Response {
    if declared_body_exceeds_limit(&request, options.max_body_bytes) {
        return Error::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "request body exceeds the configured limit",
        )
        .into_response();
    }

    let request = into_rustee_request(request, options.max_body_bytes, peer);
    let Ok(permit) = concurrency.try_acquire_owned() else {
        return Error::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "concurrency_limit_exceeded",
            "the server is handling too many requests",
        )
        .into_response();
    };

    let response = match timeout(options.request_timeout, service.call_ready(request)).await {
        Ok(Ok(response)) => response,
        Ok(Err(never)) => match never {},
        Err(_) => Error::new(
            StatusCode::REQUEST_TIMEOUT,
            "request_timeout",
            "the request exceeded the configured timeout",
        )
        .into_response(),
    };
    drop(permit);
    response
}

async fn drain_connections(connections: &mut JoinSet<()>, graceful_shutdown_timeout: Duration) {
    let drain = async { while connections.join_next().await.is_some() {} };
    if timeout(graceful_shutdown_timeout, drain).await.is_err() {
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    }
}

async fn wait_for_connection_shutdown(shutdown: &mut watch::Receiver<bool>) -> bool {
    if *shutdown.borrow_and_update() {
        return true;
    }
    shutdown.changed().await.is_ok() && *shutdown.borrow_and_update()
}

fn declared_body_exceeds_limit(request: &HyperRequest<Incoming>, max_body_bytes: usize) -> bool {
    request
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length > max_body_bytes as u64)
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
#[path = "runtime_tests.rs"]
mod tests;
