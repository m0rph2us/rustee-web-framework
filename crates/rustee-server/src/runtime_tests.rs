use std::{
    convert::Infallible,
    future::{Ready, ready},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use http::header::ACCESS_CONTROL_ALLOW_ORIGIN;
use rustee_core::{ConnectionInfo, IntoResponse, Request, Response};
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
    ServerOptions, serve_listener, serve_listener_with_options, serve_service_listener_with_options,
};
use crate::options::MIN_HTTP1_BUFFER_BYTES;
use tower::{Layer, Service};

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

#[path = "runtime_tests/configuration.rs"]
mod configuration;
#[path = "runtime_tests/lifecycle.rs"]
mod lifecycle;
#[path = "runtime_tests/limits.rs"]
mod limits;
#[path = "runtime_tests/service.rs"]
mod service;
#[path = "runtime_tests/transport.rs"]
mod transport;
