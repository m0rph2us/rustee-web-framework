use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::SystemTime,
};

use futures_util::future::BoxFuture;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Mutex as AsyncMutex,
};
use url::Url;

use super::authorization::pkce_challenge;
use super::{
    HttpMcpOAuthDiscovery, HttpMcpOAuthTokenExchanger, InMemoryMcpOAuthTokenStore,
    InMemoryMcpOAuthTransactionStore, MAX_DISCOVERY_RESPONSE_BYTES, MAX_TOKEN_RESPONSE_BYTES,
    McpOAuthAccessToken, McpOAuthAuthorizationCallback, McpOAuthAuthorizationFlow,
    McpOAuthAuthorizationServerMetadata, McpOAuthClientConfig, McpOAuthError,
    McpOAuthPendingAuthorization, McpOAuthRefreshRequest, McpOAuthRevocationRequest,
    McpOAuthRevocationTokenType, McpOAuthTokenExchangeRequest, McpOAuthTokenExchanger,
    McpOAuthTokenRevoker, McpOAuthTokenSet, McpOAuthTokenStore, McpOAuthTokenStoreKey,
    McpOAuthTransactionStore, McpOAuthValueGenerator, UuidMcpOAuthValueGenerator,
    is_json_content_type,
};

const RESOURCE: &str = "https://mcp.example.test/mcp";
const CLIENT_ID: &str = "rustee-mcp-client";
const REDIRECT_URI: &str = "https://app.example.test/mcp/callback";
const ISSUER: &str = "https://auth.example.test";
const AUTHORIZATION_ENDPOINT: &str = "https://auth.example.test/authorize";
const TOKEN_ENDPOINT: &str = "https://auth.example.test/token";

#[derive(Clone, Debug, thiserror::Error)]
#[error("test token service failure")]
struct TestError;

#[derive(Clone)]
struct SequenceGenerator(Arc<StdMutex<VecDeque<String>>>);

impl SequenceGenerator {
    fn new(values: impl IntoIterator<Item = String>) -> Self {
        Self(Arc::new(StdMutex::new(values.into_iter().collect())))
    }
}

impl McpOAuthValueGenerator for SequenceGenerator {
    fn generate(&self) -> String {
        self.0
            .lock()
            .expect("test OAuth value generator lock must not be poisoned")
            .pop_front()
            .expect("test OAuth values must be available")
    }
}

#[derive(Clone)]
struct FixedTransactionStore {
    transaction: Arc<StdMutex<Option<McpOAuthPendingAuthorization>>>,
}

impl FixedTransactionStore {
    fn new(transaction: McpOAuthPendingAuthorization) -> Self {
        Self {
            transaction: Arc::new(StdMutex::new(Some(transaction))),
        }
    }
}

impl McpOAuthTransactionStore for FixedTransactionStore {
    type Error = TestError;

    fn save(
        &self,
        transaction: McpOAuthPendingAuthorization,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let stored = Arc::clone(&self.transaction);
        Box::pin(async move {
            *stored.lock().map_err(|_| TestError)? = Some(transaction);
            Ok(())
        })
    }

    fn take(
        &self,
        _: String,
    ) -> BoxFuture<'static, Result<Option<McpOAuthPendingAuthorization>, Self::Error>> {
        let stored = Arc::clone(&self.transaction);
        Box::pin(async move {
            let transaction = stored.lock().map_err(|_| TestError)?.take();
            Ok(transaction)
        })
    }
}

#[derive(Clone, Default)]
struct RecordingExchanger {
    exchange_requests: Arc<AsyncMutex<Vec<McpOAuthTokenExchangeRequest>>>,
    refresh_requests: Arc<AsyncMutex<Vec<McpOAuthRefreshRequest>>>,
    exchange_calls: Arc<AtomicUsize>,
    refresh_calls: Arc<AtomicUsize>,
}

#[derive(Clone, Default)]
struct RecordingRevoker {
    requests: Arc<AsyncMutex<Vec<McpOAuthRevocationRequest>>>,
    calls: Arc<AtomicUsize>,
}

impl McpOAuthTokenRevoker for RecordingRevoker {
    type Error = TestError;

    fn revoke(
        &self,
        endpoint: Url,
        request: McpOAuthRevocationRequest,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let requests = Arc::clone(&self.requests);
        let calls = Arc::clone(&self.calls);
        Box::pin(async move {
            assert_eq!(endpoint.as_str(), "https://auth.example.test/revoke");
            calls.fetch_add(1, Ordering::SeqCst);
            requests.lock().await.push(request);
            Ok(())
        })
    }
}

impl McpOAuthTokenExchanger for RecordingExchanger {
    type Error = TestError;

    fn exchange(
        &self,
        endpoint: Url,
        request: McpOAuthTokenExchangeRequest,
    ) -> BoxFuture<'static, Result<McpOAuthTokenSet, Self::Error>> {
        let requests = Arc::clone(&self.exchange_requests);
        let calls = Arc::clone(&self.exchange_calls);
        Box::pin(async move {
            assert_eq!(endpoint.as_str(), TOKEN_ENDPOINT);
            let resource = request.resource().clone();
            calls.fetch_add(1, Ordering::SeqCst);
            requests.lock().await.push(request);
            token_set(
                resource,
                "initial-access-token",
                Some("initial-refresh-token".to_owned()),
            )
        })
    }

    fn refresh(
        &self,
        endpoint: Url,
        request: McpOAuthRefreshRequest,
    ) -> BoxFuture<'static, Result<McpOAuthTokenSet, Self::Error>> {
        let requests = Arc::clone(&self.refresh_requests);
        let calls = Arc::clone(&self.refresh_calls);
        Box::pin(async move {
            assert_eq!(endpoint.as_str(), TOKEN_ENDPOINT);
            let resource = request.resource().clone();
            calls.fetch_add(1, Ordering::SeqCst);
            requests.lock().await.push(request);
            token_set(
                resource,
                "refreshed-access-token",
                Some("rotated-refresh-token".to_owned()),
            )
        })
    }
}

fn token_set(
    resource: Url,
    access_token: &str,
    refresh_token: Option<String>,
) -> Result<McpOAuthTokenSet, TestError> {
    let access_token =
        McpOAuthAccessToken::new(access_token, None).expect("test access token must be valid");
    McpOAuthTokenSet::new(resource, access_token, refresh_token).map_err(|_| TestError)
}

fn config() -> McpOAuthClientConfig {
    McpOAuthClientConfig::new(
        Url::parse(RESOURCE).expect("test resource must parse"),
        CLIENT_ID,
        Url::parse(REDIRECT_URI).expect("test redirect URI must parse"),
    )
    .expect("test client configuration must be valid")
    .with_scope("orders:read")
    .expect("test scope must be valid")
}

fn provider() -> McpOAuthAuthorizationServerMetadata {
    McpOAuthAuthorizationServerMetadata::new(
        Url::parse(ISSUER).expect("test issuer must parse"),
        Url::parse(AUTHORIZATION_ENDPOINT).expect("test authorization endpoint must parse"),
        Url::parse(TOKEN_ENDPOINT).expect("test token endpoint must parse"),
    )
    .expect("test provider metadata must be valid")
}

fn provider_with_revocation() -> McpOAuthAuthorizationServerMetadata {
    provider()
        .with_revocation_endpoint(
            Url::parse("https://auth.example.test/revoke")
                .expect("test revocation endpoint must parse"),
        )
        .expect("test revocation endpoint must be valid")
}

fn flow(
    exchanger: RecordingExchanger,
) -> McpOAuthAuthorizationFlow<
    InMemoryMcpOAuthTransactionStore,
    RecordingExchanger,
    SequenceGenerator,
> {
    flow_with_transactions(InMemoryMcpOAuthTransactionStore::default(), exchanger)
}

fn flow_with_transactions<S>(
    transactions: S,
    exchanger: RecordingExchanger,
) -> McpOAuthAuthorizationFlow<S, RecordingExchanger, SequenceGenerator>
where
    S: McpOAuthTransactionStore,
{
    McpOAuthAuthorizationFlow::new(
        config(),
        provider(),
        transactions,
        exchanger,
        SequenceGenerator::new(["s".repeat(43), "v".repeat(43)]),
    )
}

async fn json_endpoint_once(body: &str) -> (Url, tokio::task::JoinHandle<String>) {
    json_endpoint_with_response(body.to_owned(), true).await
}

async fn chunked_json_endpoint_once(body: String) -> (Url, tokio::task::JoinHandle<String>) {
    json_endpoint_with_response(body, false).await
}

async fn json_endpoint_with_response(
    body: String,
    include_content_length: bool,
) -> (Url, tokio::task::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test token endpoint must bind loopback");
    let address = listener
        .local_addr()
        .expect("test token endpoint must expose its address");
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener
            .accept()
            .await
            .expect("test token endpoint must receive one connection");
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            let read = socket
                .read(&mut buffer)
                .await
                .expect("test token endpoint must read request");
            assert!(read > 0, "test client must send a complete request");
            bytes.extend_from_slice(&buffer[..read]);
            let Some(header_end) = bytes.windows(4).position(|bytes| bytes == b"\r\n\r\n") else {
                continue;
            };
            let headers = std::str::from_utf8(&bytes[..header_end])
                .expect("test request headers must be UTF-8");
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.split_once(':')
                        .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                        .map(|(_, value)| {
                            value
                                .trim()
                                .parse::<usize>()
                                .expect("content length must parse")
                        })
                })
                .unwrap_or(0);
            if bytes.len() >= header_end + 4 + content_length {
                break;
            }
        }
        let request = String::from_utf8(bytes).expect("test request must be UTF-8");
        let response = if include_content_length {
            format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
        } else {
            format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n{:X}\r\n{body}\r\n0\r\n\r\n",
                body.len()
            )
        };
        socket
            .write_all(response.as_bytes())
            .await
            .expect("test token endpoint must respond");
        request
    });
    (
        Url::parse(&format!("http://{address}/token"))
            .expect("loopback token endpoint URL must parse"),
        task,
    )
}

mod config;
mod discovery;
mod http;
mod lifecycle;
mod model;
