//! Read-only `RabbitMQ` management API audit for `Rustee` quorum job topology.
//!
//! This crate is deliberately separate from the AMQP worker. It uses management credentials only
//! to fetch one queue snapshot, then validates the effective queue type, durability, delayed
//! retry, delivery limit, and DLX route expected by a [`RabbitMqWorkerConfig`]. It never creates
//! or mutates `RabbitMQ` topology.

use std::{collections::BTreeMap, fmt, time::Duration};

use reqwest::{Client, StatusCode};
use rustee_jobs::RetryPolicy;
use rustee_jobs_rabbitmq::RabbitMqWorkerConfig;
use serde::Deserialize;
use serde_json::Value;
use url::{Host, Url};

/// Redacted settings for a read-only `RabbitMQ` Management HTTP API client.
#[derive(Clone, Eq, PartialEq)]
pub struct RabbitMqManagementConfig {
    base_url: Url,
    username: String,
    password: String,
    vhost: String,
    request_timeout: Duration,
}

impl RabbitMqManagementConfig {
    /// Creates settings for one management API virtual host.
    ///
    /// HTTP is permitted only for loopback test endpoints. Every non-loopback management endpoint
    /// must use HTTPS and a monitor-only account.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqManagementConfigError`] for an unsafe URL or invalid credentials/vhost.
    pub fn new(
        mut base_url: Url,
        username: impl Into<String>,
        password: impl Into<String>,
        vhost: impl Into<String>,
    ) -> Result<Self, RabbitMqManagementConfigError> {
        let username = username.into();
        let password = password.into();
        let vhost = vhost.into();
        if !valid_base_url(&base_url) {
            return Err(RabbitMqManagementConfigError::InvalidBaseUrl);
        }
        if username.trim().is_empty() || password.is_empty() || vhost.contains('\0') {
            return Err(RabbitMqManagementConfigError::InvalidCredentialOrVhost);
        }
        if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }
        Ok(Self {
            base_url,
            username,
            password,
            vhost,
            request_timeout: Duration::from_secs(5),
        })
    }

    /// Sets a non-zero bounded management API request timeout.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqManagementConfigError::ZeroTimeout`] for a zero duration.
    pub fn with_request_timeout(
        mut self,
        request_timeout: Duration,
    ) -> Result<Self, RabbitMqManagementConfigError> {
        if request_timeout.is_zero() {
            return Err(RabbitMqManagementConfigError::ZeroTimeout);
        }
        self.request_timeout = request_timeout;
        Ok(self)
    }
}

impl fmt::Debug for RabbitMqManagementConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RabbitMqManagementConfig")
            .field("base_url", &self.base_url)
            .field("vhost", &self.vhost)
            .field("request_timeout", &self.request_timeout)
            .finish_non_exhaustive()
    }
}

/// Invalid management audit client configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RabbitMqManagementConfigError {
    /// The base URL was unsafe or was not a clean HTTP(S) URL without embedded credentials.
    #[error(
        "RabbitMQ management base URL must use HTTPS unless it is loopback, without credentials, query, or fragment"
    )]
    InvalidBaseUrl,
    /// The read-only account or virtual host was malformed.
    #[error("RabbitMQ management username, password, and virtual host must be non-empty and valid")]
    InvalidCredentialOrVhost,
    /// Requests must use a finite timeout.
    #[error("RabbitMQ management request timeout must be non-zero")]
    ZeroTimeout,
}

/// Read-only topology auditor for one worker source queue.
#[derive(Clone, Debug)]
pub struct RabbitMqTopologyAuditor {
    client: Client,
    config: RabbitMqManagementConfig,
    worker: RabbitMqWorkerConfig,
    retry_policy: RetryPolicy,
}

impl RabbitMqTopologyAuditor {
    /// Builds a read-only auditor for one worker and retry policy.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqManagementError::Client`] when the HTTP client cannot be initialized.
    pub fn new(
        config: RabbitMqManagementConfig,
        worker: RabbitMqWorkerConfig,
        retry_policy: RetryPolicy,
    ) -> Result<Self, RabbitMqManagementError> {
        let client = Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|_| RabbitMqManagementError::Client)?;
        Ok(Self {
            client,
            config,
            worker,
            retry_policy,
        })
    }

    /// Fetches and validates the configured source queue without changing broker state.
    ///
    /// # Errors
    ///
    /// Returns a sanitized management transport, response, or topology error.
    pub async fn audit(&self) -> Result<RabbitMqTopologyReport, RabbitMqManagementError> {
        let endpoint = queue_endpoint(
            &self.config.base_url,
            &self.config.vhost,
            self.worker.queue(),
        )
        .map_err(|_| RabbitMqManagementError::InvalidEndpoint)?;
        let response = self
            .client
            .get(endpoint)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .send()
            .await
            .map_err(|_| RabbitMqManagementError::Request)?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(RabbitMqManagementError::QueueNotFound);
        }
        if !response.status().is_success() {
            return Err(RabbitMqManagementError::Request);
        }
        let snapshot = response
            .json::<QueueSnapshot>()
            .await
            .map_err(|_| RabbitMqManagementError::MalformedResponse)?;
        self.audit_snapshot(&snapshot)
    }

    /// Validates a management queue snapshot, useful for deployment tests and recorded audits.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqManagementError::TopologyMismatch`] when the snapshot does not prove
    /// the quorum queue and effective delayed-retry/DLQ policy expected by the worker.
    pub fn audit_snapshot(
        &self,
        snapshot: &QueueSnapshot,
    ) -> Result<RabbitMqTopologyReport, RabbitMqManagementError> {
        if snapshot.queue_type != "quorum" || !snapshot.durable || snapshot.auto_delete {
            return Err(RabbitMqManagementError::TopologyMismatch);
        }
        let values = snapshot.effective_values();
        let native = self.worker.native_retry();
        let min = duration_millis(native.minimum_delay())
            .ok_or(RabbitMqManagementError::TopologyMismatch)?;
        let max = duration_millis(native.maximum_delay())
            .ok_or(RabbitMqManagementError::TopologyMismatch)?;
        let delivery_limit = values
            .integer("delivery-limit")
            .ok_or(RabbitMqManagementError::TopologyMismatch)?;
        if values.text("delayed-retry-type") != Some("failed")
            || values.integer("delayed-retry-min") != Some(min)
            || values.integer("delayed-retry-max") != Some(max)
            || delivery_limit < i64::from(self.retry_policy.max_deliveries)
            || values.text("dead-letter-exchange") != Some(self.worker.dead_letter_exchange())
            || values.text("dead-letter-routing-key") != Some(self.worker.dead_letter_routing_key())
        {
            return Err(RabbitMqManagementError::TopologyMismatch);
        }
        Ok(RabbitMqTopologyReport {
            delivery_limit: u16::try_from(delivery_limit).unwrap_or(u16::MAX),
        })
    }
}

/// Safe result of one successful topology audit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RabbitMqTopologyReport {
    delivery_limit: u16,
}
impl RabbitMqTopologyReport {
    #[must_use]
    pub const fn delivery_limit(self) -> u16 {
        self.delivery_limit
    }
}

/// Sanitized failures from the read-only management audit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RabbitMqManagementError {
    #[error("RabbitMQ management client initialization failed")]
    Client,
    #[error("RabbitMQ management endpoint was invalid")]
    InvalidEndpoint,
    #[error("RabbitMQ management request failed")]
    Request,
    #[error("RabbitMQ management queue was not found")]
    QueueNotFound,
    #[error("RabbitMQ management response was malformed")]
    MalformedResponse,
    #[error("RabbitMQ queue topology does not match the Rustee worker contract")]
    TopologyMismatch,
}

/// Minimal management queue response used for topology auditing.
#[derive(Clone, Debug, Deserialize)]
pub struct QueueSnapshot {
    #[serde(rename = "type")]
    queue_type: String,
    durable: bool,
    auto_delete: bool,
    #[serde(default)]
    arguments: BTreeMap<String, Value>,
    #[serde(default)]
    effective_policy_definition: BTreeMap<String, Value>,
}
impl QueueSnapshot {
    fn effective_values(&self) -> EffectiveValues<'_> {
        EffectiveValues {
            policy: &self.effective_policy_definition,
            arguments: &self.arguments,
        }
    }
}
struct EffectiveValues<'a> {
    policy: &'a BTreeMap<String, Value>,
    arguments: &'a BTreeMap<String, Value>,
}
impl EffectiveValues<'_> {
    fn value(&self, key: &str) -> Option<&Value> {
        self.arguments
            .get(&format!("x-{key}"))
            .or_else(|| self.policy.get(key))
    }
    fn text(&self, key: &str) -> Option<&str> {
        self.value(key)?.as_str()
    }
    fn integer(&self, key: &str) -> Option<i64> {
        self.value(key)?.as_i64()
    }
}
fn queue_endpoint(base: &Url, vhost: &str, queue: &str) -> Result<Url, url::ParseError> {
    base.join(&format!(
        "api/queues/{}/{}",
        urlencoding(vhost),
        urlencoding(queue)
    ))
}
fn urlencoding(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}
fn duration_millis(value: Duration) -> Option<i64> {
    i64::try_from(value.as_millis()).ok()
}
fn valid_base_url(value: &Url) -> bool {
    matches!(value.scheme(), "http" | "https")
        && value.host().is_some()
        && value.username().is_empty()
        && value.password().is_none()
        && value.query().is_none()
        && value.fragment().is_none()
        && (value.scheme() == "https" || is_loopback_host(value.host().as_ref()))
}

fn is_loopback_host(host: Option<&Host<&str>>) -> bool {
    match host {
        Some(Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(host)) => host.is_loopback(),
        Some(Host::Ipv6(host)) => host.is_loopback(),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::*;
    fn auditor() -> RabbitMqTopologyAuditor {
        let config = RabbitMqManagementConfig::new(
            Url::parse("http://localhost:15672/").unwrap(),
            "monitor",
            "secret",
            "/",
        )
        .unwrap();
        let worker = RabbitMqWorkerConfig::new(
            "jobs",
            "worker",
            rustee_jobs_rabbitmq::RabbitMqNativeRetryConfig::new(
                Duration::from_millis(10),
                Duration::from_millis(30),
            )
            .unwrap(),
            "jobs.dlx",
            "dead-letter",
        )
        .unwrap();
        RabbitMqTopologyAuditor::new(
            config,
            worker,
            RetryPolicy {
                max_deliveries: 3,
                initial_backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(30),
            },
        )
        .unwrap()
    }
    #[test]
    fn audit_accepts_effective_quorum_policy() {
        let snapshot: QueueSnapshot = serde_json::from_value(serde_json::json!({"type":"quorum","durable":true,"auto_delete":false,"effective_policy_definition":{"delayed-retry-type":"failed","delayed-retry-min":10,"delayed-retry-max":30,"delivery-limit":3,"dead-letter-exchange":"jobs.dlx","dead-letter-routing-key":"dead-letter"}})).unwrap();
        assert_eq!(
            auditor()
                .audit_snapshot(&snapshot)
                .unwrap()
                .delivery_limit(),
            3
        );
    }
    #[test]
    fn audit_rejects_missing_delivery_limit_route_or_credentials_in_debug() {
        let snapshot: QueueSnapshot = serde_json::from_value(serde_json::json!({"type":"quorum","durable":true,"auto_delete":false,"effective_policy_definition":{"delayed-retry-type":"failed","delayed-retry-min":10,"delayed-retry-max":30,"delivery-limit":3,"dead-letter-exchange":"jobs.dlx"}})).unwrap();
        assert_eq!(
            auditor().audit_snapshot(&snapshot),
            Err(RabbitMqManagementError::TopologyMismatch)
        );
        let config = RabbitMqManagementConfig::new(
            Url::parse("https://rabbit.example.test/").unwrap(),
            "monitor",
            "secret",
            "/",
        )
        .unwrap();
        assert!(!format!("{config:?}").contains("secret"));
        assert!(
            RabbitMqManagementConfig::new(
                Url::parse("http://rabbitmq.internal:15672/").unwrap(),
                "monitor",
                "secret",
                "/",
            )
            .is_err()
        );
        assert!(
            RabbitMqManagementConfig::new(
                Url::parse("http://192.0.2.10:15672/").unwrap(),
                "monitor",
                "secret",
                "/",
            )
            .is_err()
        );
        assert!(
            RabbitMqManagementConfig::new(
                Url::parse("http://[::1]:15672/").unwrap(),
                "monitor",
                "secret",
                "/",
            )
            .is_ok()
        );
    }

    #[tokio::test]
    async fn audit_uses_one_encoded_read_only_management_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, request_rx) = mpsc::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 4096];
            let read = socket.read(&mut request).await.unwrap();
            request_tx
                .send(String::from_utf8(request[..read].to_vec()).unwrap())
                .unwrap();
            let body = "{\"type\":\"quorum\",\"durable\":true,\"auto_delete\":false,\"effective_policy_definition\":{\"delayed-retry-type\":\"failed\",\"delayed-retry-min\":10,\"delayed-retry-max\":30,\"delivery-limit\":3,\"dead-letter-exchange\":\"jobs.dlx\",\"dead-letter-routing-key\":\"dead-letter\"}}";
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let config = RabbitMqManagementConfig::new(
            Url::parse(&format!("http://{address}/")).unwrap(),
            "monitor",
            "secret",
            "/",
        )
        .unwrap();
        let worker = RabbitMqWorkerConfig::new(
            "jobs/one",
            "worker",
            rustee_jobs_rabbitmq::RabbitMqNativeRetryConfig::new(
                Duration::from_millis(10),
                Duration::from_millis(30),
            )
            .unwrap(),
            "jobs.dlx",
            "dead-letter",
        )
        .unwrap();
        let report = RabbitMqTopologyAuditor::new(
            config,
            worker,
            RetryPolicy {
                max_deliveries: 3,
                initial_backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(30),
            },
        )
        .unwrap()
        .audit()
        .await
        .unwrap();
        assert_eq!(report.delivery_limit(), 3);
        let request = request_rx.recv().unwrap();
        assert!(request.starts_with("GET /api/queues/%2F/jobs%2Fone HTTP/1.1"));
        assert!(request.contains("authorization: Basic bW9uaXRvcjpzZWNyZXQ="));
        server.await.unwrap();
    }
}
