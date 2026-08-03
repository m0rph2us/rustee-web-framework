//! Linux-only Nginx reverse-proxy interoperability contract.

use std::{
    env, fs, io,
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener as StdTcpListener},
    path::PathBuf,
    process::{self, Command},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use http::{HeaderValue, header::CACHE_CONTROL};
use rustee_core::IntoResponse;
use rustee_middleware::{
    CompressionLayer, ForwardedContext, TrustedProxyLayer, TrustedProxyNetwork, TrustedProxyPolicy,
};
use rustee_router::App;
use rustee_server::{ServerOptions, serve_service_listener_with_options};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, oneshot},
    time::{sleep, timeout},
};
use tower::Layer;

const NGINX_IMAGE: &str = "nginx:1.30.4-alpine";
const READY_ATTEMPTS: usize = 40;
const READY_DELAY: Duration = Duration::from_millis(50);

#[tokio::test]
#[ignore = "requires Linux Docker host networking; CI sets RUSTEE_NGINX_PROXY_CONTRACT=1"]
async fn nginx_replaces_client_forwarded_headers_before_rustee_trusts_them() {
    assert_eq!(
        env::var("RUSTEE_NGINX_PROXY_CONTRACT").as_deref(),
        Ok("1"),
        "set RUSTEE_NGINX_PROXY_CONTRACT=1 on a Linux Docker host to run this contract"
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_address = listener.local_addr().unwrap();
    let proxy_address = SocketAddr::from(([127, 0, 0, 1], available_loopback_port()));
    let trusted_proxy =
        TrustedProxyPolicy::new([
            TrustedProxyNetwork::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 32).unwrap(),
        ])
        .unwrap();
    let service = TrustedProxyLayer::new(trusted_proxy).layer(App::new().get(
        "/context",
        |context: ForwardedContext| async move {
            format!(
                "{}:{}:{}",
                context.client_ip(),
                context.scheme().unwrap_or_default(),
                context.host().unwrap_or_default()
            )
        },
    ));
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

    let proxy = NginxProxy::start(proxy_address, backend_address);
    let response = wait_for_proxy(proxy_address).await.unwrap_or_else(|error| {
        panic!(
            "Nginx proxy did not become ready: {error}\n{}",
            proxy.logs()
        )
    });
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(
        response.ends_with("\r\n\r\n127.0.0.1:https:public.example.test"),
        "Nginx must replace the client-supplied Forwarded value: {response}"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
#[ignore = "requires Linux Docker host networking; CI sets RUSTEE_NGINX_PROXY_CONTRACT=1"]
async fn nginx_cache_keeps_gzip_and_identity_representations_distinct() {
    assert_eq!(
        env::var("RUSTEE_NGINX_PROXY_CONTRACT").as_deref(),
        Ok("1"),
        "set RUSTEE_NGINX_PROXY_CONTRACT=1 on a Linux Docker host to run this contract"
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_address = listener.local_addr().unwrap();
    let proxy_address = SocketAddr::from(([127, 0, 0, 1], available_loopback_port()));
    let origin_requests = Arc::new(AtomicUsize::new(0));
    let handler_requests = Arc::clone(&origin_requests);
    let service = CompressionLayer::new()
        .with_brotli(false)
        .layer(App::new().get("/document", move || {
            let handler_requests = Arc::clone(&handler_requests);
            async move {
                let request_number = handler_requests.fetch_add(1, Ordering::SeqCst) + 1;
                let mut response =
                    format!("Rustee cache document {request_number}").into_response();
                response.headers_mut().insert(
                    CACHE_CONTROL,
                    HeaderValue::from_static("public, max-age=60"),
                );
                response
            }
        }));
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

    let proxy = NginxProxy::start_cached(proxy_address, backend_address);
    let gzip_miss = wait_for_cached_proxy(proxy_address)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "Nginx cache proxy did not become ready: {error}\n{}",
                proxy.logs()
            )
        });
    assert_cache_response(&gzip_miss, "MISS", Some("gzip"));

    let gzip_hit = cached_request(proxy_address, "gzip").await.unwrap();
    assert_cache_response(&gzip_hit, "HIT", Some("gzip"));

    let identity_miss = cached_request(proxy_address, "identity").await.unwrap();
    assert_cache_response(&identity_miss, "MISS", None);

    let identity_hit = cached_request(proxy_address, "identity").await.unwrap();
    assert_cache_response(&identity_hit, "HIT", None);

    let gzip_hit_again = cached_request(proxy_address, "gzip").await.unwrap();
    assert_cache_response(&gzip_hit_again, "HIT", Some("gzip"));
    assert_eq!(origin_requests.load(Ordering::SeqCst), 2);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
#[ignore = "requires Linux Docker host networking; CI sets RUSTEE_NGINX_PROXY_CONTRACT=1"]
async fn nginx_forwards_an_active_request_while_rustee_drains() {
    assert_eq!(
        env::var("RUSTEE_NGINX_PROXY_CONTRACT").as_deref(),
        Ok("1"),
        "set RUSTEE_NGINX_PROXY_CONTRACT=1 on a Linux Docker host to run this contract"
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_address = listener.local_addr().unwrap();
    let proxy_address = SocketAddr::from(([127, 0, 0, 1], available_loopback_port()));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let started_handler = Arc::clone(&started);
    let release_handler = Arc::clone(&release);
    let service = App::new().get("/slow", move || {
        let started = Arc::clone(&started_handler);
        let release = Arc::clone(&release_handler);
        async move {
            started.notify_one();
            release.notified().await;
            "drained through nginx"
        }
    });
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        serve_service_listener_with_options(
            listener,
            service,
            ServerOptions {
                graceful_shutdown_timeout: Duration::from_secs(1),
                ..ServerOptions::default()
            },
            async move {
                let _ = shutdown_rx.await;
            },
        )
        .await
        .unwrap();
    });

    let proxy = NginxProxy::start(proxy_address, backend_address);
    let client = tokio::spawn(slow_request_when_proxy_is_ready(proxy_address));
    if timeout(Duration::from_secs(2), started.notified())
        .await
        .is_err()
    {
        panic!(
            "Nginx did not forward the active request before the deadline\n{}",
            proxy.logs()
        );
    }

    let _ = shutdown_tx.send(());
    sleep(Duration::from_millis(20)).await;
    assert!(
        !server.is_finished(),
        "Rustee must wait for the proxied active request during graceful shutdown"
    );

    release.notify_one();
    let response = timeout(Duration::from_secs(1), client)
        .await
        .expect("Nginx did not finish forwarding the drained response")
        .unwrap()
        .unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response.ends_with("\r\n\r\ndrained through nginx"));
    timeout(Duration::from_secs(1), server)
        .await
        .expect("Rustee did not exit after the proxied request drained")
        .unwrap();
}

fn available_loopback_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn wait_for_proxy(address: SocketAddr) -> io::Result<String> {
    let request = b"GET /context HTTP/1.1\r\nHost: public.example.test\r\nForwarded: for=203.0.113.99;proto=http;host=spoofed.example.test\r\nConnection: close\r\n\r\n";
    let mut last_error = None;

    for _ in 0..READY_ATTEMPTS {
        match raw_request(address, request).await {
            Ok(response) if response.starts_with("HTTP/1.1 200 OK\r\n") => return Ok(response),
            Ok(response) => last_error = Some(format!("unexpected response: {response}")),
            Err(error) => last_error = Some(error.to_string()),
        }
        sleep(READY_DELAY).await;
    }

    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        last_error.unwrap_or_else(|| "proxy did not respond".to_owned()),
    ))
}

async fn raw_request(address: SocketAddr, request: &[u8]) -> io::Result<String> {
    let response = raw_response(address, request).await?;
    String::from_utf8(response).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

async fn raw_response(address: SocketAddr, request: &[u8]) -> io::Result<Vec<u8>> {
    let mut stream = timeout(Duration::from_millis(250), TcpStream::connect(address))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "proxy connection timed out"))??;
    stream.write_all(request).await?;
    let mut response = Vec::new();
    timeout(
        Duration::from_millis(500),
        stream.read_to_end(&mut response),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "proxy response timed out"))??;
    Ok(response)
}

async fn slow_request_when_proxy_is_ready(address: SocketAddr) -> io::Result<String> {
    let request = b"GET /slow HTTP/1.1\r\nHost: drain.example.test\r\nConnection: close\r\n\r\n";
    let mut last_error = None;

    for _ in 0..READY_ATTEMPTS {
        match raw_request(address, request).await {
            Ok(response) => return Ok(response),
            Err(error) => last_error = Some(error.to_string()),
        }
        sleep(READY_DELAY).await;
    }

    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        last_error.unwrap_or_else(|| "proxy did not accept the active request".to_owned()),
    ))
}

async fn wait_for_cached_proxy(address: SocketAddr) -> io::Result<CachedResponse> {
    let mut last_error = None;

    for _ in 0..READY_ATTEMPTS {
        match cached_request(address, "gzip").await {
            Ok(response) if response.status == 200 => return Ok(response),
            Ok(response) => last_error = Some(format!("unexpected status: {}", response.status)),
            Err(error) => last_error = Some(error.to_string()),
        }
        sleep(READY_DELAY).await;
    }

    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        last_error.unwrap_or_else(|| "cache proxy did not respond".to_owned()),
    ))
}

async fn cached_request(address: SocketAddr, accept_encoding: &str) -> io::Result<CachedResponse> {
    let request = format!(
        "GET /document HTTP/1.1\r\nHost: cache.example.test\r\nAccept-Encoding: {accept_encoding}\r\nConnection: close\r\n\r\n"
    );
    let response = raw_response(address, request.as_bytes()).await?;
    CachedResponse::parse(&response)
}

fn assert_cache_response(response: &CachedResponse, cache_status: &str, coding: Option<&str>) {
    assert_eq!(response.status, 200);
    assert_eq!(response.header("x-rustee-cache"), Some(cache_status));
    assert_eq!(response.header("content-encoding"), coding);
    assert_eq!(response.header("vary"), Some("Accept-Encoding"));
}

struct CachedResponse {
    status: u16,
    headers: Vec<(String, String)>,
}

impl CachedResponse {
    fn parse(response: &[u8]) -> io::Result<Self> {
        let Some(header_end) = response.windows(4).position(|window| window == b"\r\n\r\n") else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "response did not contain headers",
            ));
        };
        let head = std::str::from_utf8(&response[..header_end])
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let mut lines = head.split("\r\n");
        let status = lines
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|status| status.parse().ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid status line"))?;
        let headers = lines
            .filter_map(|line| {
                let (name, value) = line.split_once(':')?;
                Some((name.trim().to_ascii_lowercase(), value.trim().to_owned()))
            })
            .collect();
        Ok(Self { status, headers })
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(header_name, _)| header_name == name)
            .map(|(_, value)| value.as_str())
    }
}

struct NginxProxy {
    name: String,
    config_path: PathBuf,
}

impl NginxProxy {
    fn start(proxy_address: SocketAddr, backend_address: SocketAddr) -> Self {
        Self::start_with_config(proxy_address, nginx_config(proxy_address, backend_address))
    }

    fn start_cached(proxy_address: SocketAddr, backend_address: SocketAddr) -> Self {
        Self::start_with_config(
            proxy_address,
            nginx_cache_config(proxy_address, backend_address),
        )
    }

    fn start_with_config(proxy_address: SocketAddr, config: String) -> Self {
        let name = format!("rustee-nginx-{}-{}", process::id(), proxy_address.port());
        let config_path = env::temp_dir().join(format!("{name}.conf"));
        fs::write(&config_path, config).unwrap();
        let volume = format!("{}:/etc/nginx/nginx.conf:ro", config_path.display());
        let output = Command::new("docker")
            .arg("run")
            .arg("--detach")
            .arg("--rm")
            .arg("--name")
            .arg(&name)
            .arg("--network")
            .arg("host")
            .arg("--volume")
            .arg(volume)
            .arg(NGINX_IMAGE)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "failed to start Nginx proxy: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Self { name, config_path }
    }

    fn logs(&self) -> String {
        Command::new("docker")
            .arg("logs")
            .arg(&self.name)
            .output()
            .map_or_else(
                |error| error.to_string(),
                |output| String::from_utf8_lossy(&output.stderr).into_owned(),
            )
    }
}

impl Drop for NginxProxy {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .arg("rm")
            .arg("--force")
            .arg(&self.name)
            .output();
        let _ = fs::remove_file(&self.config_path);
    }
}

fn nginx_config(proxy_address: SocketAddr, backend_address: SocketAddr) -> String {
    format!(
        "events {{}}\nhttp {{\n  server {{\n    listen {proxy_address};\n    location / {{\n      proxy_set_header Host public.example.test;\n      proxy_set_header Forwarded \"for=$remote_addr;proto=https;host=public.example.test\";\n      proxy_pass http://{backend_address};\n    }}\n  }}\n}}\n"
    )
}

fn nginx_cache_config(proxy_address: SocketAddr, backend_address: SocketAddr) -> String {
    format!(
        "events {{}}\nhttp {{\n  proxy_cache_path /var/cache/nginx/rustee levels=1:2 keys_zone=rustee_cache:1m inactive=1m;\n  server {{\n    listen {proxy_address};\n    location / {{\n      proxy_cache rustee_cache;\n      proxy_cache_valid 200 1m;\n      add_header X-Rustee-Cache $upstream_cache_status always;\n      proxy_pass http://{backend_address};\n    }}\n  }}\n}}\n"
    )
}
