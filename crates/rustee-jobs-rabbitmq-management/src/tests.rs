use std::{sync::mpsc, time::Duration};

use rustee_jobs::RetryPolicy;
use rustee_jobs_rabbitmq::{RabbitMqNativeRetryConfig, RabbitMqWorkerConfig};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use url::Url;

use crate::{
    QueueSnapshot, RabbitMqManagementConfig, RabbitMqManagementConfigError,
    RabbitMqManagementError, RabbitMqTopologyAuditor,
};

fn auditor() -> RabbitMqTopologyAuditor {
    let config = RabbitMqManagementConfig::new(
        Url::parse("http://localhost:15672/").unwrap(),
        "monitor",
        "secret",
        "/",
    )
    .unwrap();
    let worker = worker("jobs");
    RabbitMqTopologyAuditor::new(config, worker, retry_policy()).unwrap()
}

fn worker(queue: &str) -> RabbitMqWorkerConfig {
    RabbitMqWorkerConfig::new(
        queue,
        "worker",
        RabbitMqNativeRetryConfig::new(Duration::from_millis(10), Duration::from_millis(30))
            .unwrap(),
        "jobs.dlx",
        "dead-letter",
    )
    .unwrap()
}

const fn retry_policy() -> RetryPolicy {
    RetryPolicy {
        max_deliveries: 3,
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(30),
    }
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
        "tenant-acme",
    )
    .unwrap();
    let debug = format!("{config:?}");
    assert!(!debug.contains("secret"));
    assert!(!debug.contains("rabbit.example.test"));
    assert!(!debug.contains("tenant-acme"));
    assert!(debug.contains("base_url_length"));
    assert!(debug.contains("vhost_length"));
    assert_eq!(
        config.clone().with_max_response_bytes(0).unwrap_err(),
        RabbitMqManagementConfigError::ZeroResponseLimit
    );
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
    for vhost in [".", ".."] {
        assert!(
            RabbitMqManagementConfig::new(
                Url::parse("https://rabbit.example.test/").unwrap(),
                "monitor",
                "secret",
                vhost,
            )
            .is_err()
        );
    }
}

#[test]
fn management_snapshot_debug_output_redacts_remote_policy_metadata() {
    let snapshot: QueueSnapshot = serde_json::from_value(serde_json::json!({
        "type": "private-queue-type",
        "durable": true,
        "auto_delete": false,
        "arguments": {
            "x-private-argument-key": "private-argument-value",
        },
        "effective_policy_definition": {
            "private-policy-key": "private-policy-value",
        },
    }))
    .unwrap();

    let output = format!("{snapshot:?}");

    for sensitive in [
        "private-queue-type",
        "x-private-argument-key",
        "private-argument-value",
        "private-policy-key",
        "private-policy-value",
    ] {
        assert!(!output.contains(sensitive));
    }
    assert!(output.contains("argument_count: 1"));
    assert!(output.contains("effective_policy_value_count: 1"));
    assert!(output.contains("[REDACTED]"));
}

#[test]
fn auditor_debug_does_not_delegate_to_http_client_or_topology_diagnostics() {
    let debug = format!("{:?}", auditor());

    assert!(!debug.contains("localhost"));
    assert!(!debug.contains("jobs.dlx"));
    assert!(!debug.contains("dead-letter"));
    assert!(debug.contains("[REDACTED]"));
    assert!(debug.contains("retry_policy"));
}

#[test]
fn audit_rejects_delivery_limits_outside_the_rustee_contract() {
    for delivery_limit in [-1_i64, i64::from(u16::MAX) + 1] {
        let snapshot: QueueSnapshot = serde_json::from_value(serde_json::json!({
            "type": "quorum",
            "durable": true,
            "auto_delete": false,
            "effective_policy_definition": {
                "delayed-retry-type": "failed",
                "delayed-retry-min": 10,
                "delayed-retry-max": 30,
                "delivery-limit": delivery_limit,
                "dead-letter-exchange": "jobs.dlx",
                "dead-letter-routing-key": "dead-letter",
            },
        }))
        .unwrap();

        assert_eq!(
            auditor().audit_snapshot(&snapshot),
            Err(RabbitMqManagementError::TopologyMismatch)
        );
    }
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
    let report = RabbitMqTopologyAuditor::new(config, worker("jobs/one"), retry_policy())
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

#[tokio::test]
async fn audit_rejects_dot_queue_segments_before_network_dispatch() {
    let config = RabbitMqManagementConfig::new(
        Url::parse("http://127.0.0.1:1/").unwrap(),
        "monitor",
        "secret",
        "/",
    )
    .unwrap();

    for queue in [".", ".."] {
        let auditor =
            RabbitMqTopologyAuditor::new(config.clone(), worker(queue), retry_policy()).unwrap();
        assert_eq!(
            auditor.audit().await,
            Err(RabbitMqManagementError::InvalidEndpoint)
        );
    }
}

#[tokio::test]
async fn audit_rejects_a_response_above_its_configured_limit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        let _ = socket.read(&mut request).await.unwrap();
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 17\r\nconnection: keep-alive\r\n\r\n",
        )
        .await
        .unwrap();
        socket.shutdown().await.unwrap();
    });
    let config = RabbitMqManagementConfig::new(
        Url::parse(&format!("http://{address}/")).unwrap(),
        "monitor",
        "secret",
        "/",
    )
    .unwrap()
    .with_max_response_bytes(16)
    .unwrap();
    let auditor = RabbitMqTopologyAuditor::new(config, worker("jobs"), retry_policy()).unwrap();

    assert_eq!(
        auditor.audit().await,
        Err(RabbitMqManagementError::ResponseTooLarge)
    );
    server.await.unwrap();
}
