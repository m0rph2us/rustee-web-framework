use std::{sync::mpsc, time::Duration};

use rustee_events::Event;
use rustee_events_schema::{EventSchema, EventSchemaRegistry, SchemaCompatibility, SchemaSubject};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};
use url::Url;

use crate::{
    ConfluentSchemaRegistry, ConfluentSchemaRegistryAuth, ConfluentSchemaRegistryConfig,
    ConfluentSchemaRegistryConfigError, ConfluentSchemaRegistryError,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
struct AccountOpenedV1 {
    account_id: String,
}

impl Event for AccountOpenedV1 {
    const TYPE: &'static str = "account.opened";
    const VERSION: u16 = 1;
}

fn schema() -> EventSchema {
    EventSchema::json::<AccountOpenedV1>(
        SchemaSubject::new("account.opened-value").unwrap(),
        SchemaCompatibility::Backward,
        r#"{"type":"object","properties":{"account_id":{"type":"string"}},"required":["account_id"]}"#,
    )
    .unwrap()
}

#[test]
fn configuration_requires_safe_endpoint_and_redacts_connection_values() {
    let config = ConfluentSchemaRegistryConfig::new(
        Url::parse("https://registry.example.test/api/").unwrap(),
        ConfluentSchemaRegistryAuth::Basic {
            api_key: "key".to_owned(),
            api_secret: "secret".to_owned(),
        },
    )
    .unwrap();
    let debug = format!("{config:?}");
    assert!(!debug.contains("registry.example.test"));
    assert!(!debug.contains("key"));
    assert!(!debug.contains("secret"));
    assert!(debug.contains("base_url: \"[REDACTED]\""));
    assert_eq!(
        config.clone().with_max_response_bytes(0).unwrap_err(),
        ConfluentSchemaRegistryConfigError::ZeroResponseLimit
    );
    assert!(
        ConfluentSchemaRegistryConfig::new(
            Url::parse("http://registry.example.test/").unwrap(),
            ConfluentSchemaRegistryAuth::None,
        )
        .is_err()
    );
    assert!(
        ConfluentSchemaRegistryConfig::new(
            Url::parse("https://user:secret@registry.example.test/").unwrap(),
            ConfluentSchemaRegistryAuth::None,
        )
        .is_err()
    );
    assert!(
        ConfluentSchemaRegistryConfig::new(
            Url::parse("http://127.0.0.1:8081/").unwrap(),
            ConfluentSchemaRegistryAuth::Bearer(" ".to_owned()),
        )
        .is_err()
    );
}

#[tokio::test]
async fn adapter_registers_then_rechecks_the_exact_json_artifact() {
    let schema = schema();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (requests_tx, requests_rx) = mpsc::channel();
    let source = schema.definition().to_owned();
    let server = tokio::spawn(async move {
        for (index, response) in [
            response(200, r#"{"compatibilityLevel":"BACKWARD"}"#),
            response(404, r#"{"error_code":40403,"message":"Schema not found"}"#),
            response(200, r#"{"id":17}"#),
            response(200, &format!(r#"{{"subject":"account.opened-value","version":1,"id":17,"schemaType":"JSON","schema":{source:?}}}"#)),
        ]
        .into_iter()
        .enumerate()
        {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut socket).await;
            requests_tx.send(request).unwrap();
            socket.write_all(response.as_bytes()).await.unwrap();
            if index == 3 {
                break;
            }
        }
    });
    let registry = ConfluentSchemaRegistry::new(
        ConfluentSchemaRegistryConfig::new(
            Url::parse(&format!("http://{address}/")).unwrap(),
            ConfluentSchemaRegistryAuth::Basic {
                api_key: "key".to_owned(),
                api_secret: "secret".to_owned(),
            },
        )
        .unwrap(),
    )
    .unwrap();

    let registration = registry.register_or_verify(&schema).await.unwrap();
    assert_eq!(registration.subject(), schema.subject());
    assert_eq!(registration.version(), schema.version());
    assert_eq!(registration.fingerprint(), schema.fingerprint());

    let requests = (0..4)
        .map(|_| requests_rx.recv().unwrap())
        .collect::<Vec<_>>();
    assert!(
        requests[0].starts_with("GET /config/account.opened-value?defaultToGlobal=true HTTP/1.1")
    );
    assert!(requests[1].starts_with("POST /subjects/account.opened-value HTTP/1.1"));
    assert!(requests[2].starts_with("POST /subjects/account.opened-value/versions HTTP/1.1"));
    let body = requests[2].split_once("\r\n\r\n").unwrap().1;
    let payload: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(payload["schemaType"], "JSON");
    assert_eq!(payload["schema"], schema.definition());
    assert!(
        requests
            .iter()
            .all(|request| request.contains("authorization: Basic a2V5OnNlY3JldA="))
    );
    server.await.unwrap();
}

#[tokio::test]
async fn adapter_rejects_policy_or_exact_artifact_drift_without_registering() {
    let schema = schema();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_http_request(&mut socket).await;
        socket
            .write_all(response(200, r#"{"compatibilityLevel":"FULL"}"#).as_bytes())
            .await
            .unwrap();
    });
    let registry = test_registry(&address.to_string());
    assert_eq!(
        registry.register_or_verify(&schema).await,
        Err(ConfluentSchemaRegistryError::CompatibilityPolicyMismatch)
    );
    server.await.unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_http_request(&mut socket).await;
        socket
            .write_all(response(200, r#"{"compatibilityLevel":"BACKWARD"}"#).as_bytes())
            .await
            .unwrap();
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_http_request(&mut socket).await;
        socket
            .write_all(response(200, r#"{"subject":"account.opened-value","version":1,"id":17,"schemaType":"JSON","schema":"{}"}"#).as_bytes())
            .await
            .unwrap();
    });
    let registry = test_registry(&address.to_string());
    assert_eq!(
        registry.register_or_verify(&schema).await,
        Err(ConfluentSchemaRegistryError::ArtifactMismatch)
    );
    server.await.unwrap();
}

#[tokio::test]
async fn adapter_rejects_a_response_above_its_configured_limit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_http_request(&mut socket).await;
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 17\r\nconnection: keep-alive\r\n\r\n",
        )
        .await
        .unwrap();
        socket.shutdown().await.unwrap();
    });
    let registry = ConfluentSchemaRegistry::new(
        ConfluentSchemaRegistryConfig::new(
            Url::parse(&format!("http://{address}/")).unwrap(),
            ConfluentSchemaRegistryAuth::None,
        )
        .unwrap()
        .with_max_response_bytes(16)
        .unwrap(),
    )
    .unwrap();

    assert_eq!(
        registry.register_or_verify(&schema()).await,
        Err(ConfluentSchemaRegistryError::ResponseTooLarge)
    );
    server.await.unwrap();
}

#[tokio::test]
async fn injected_client_still_enforces_the_configured_request_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (request_received, request_was_received) = oneshot::channel();
    let (release_server, release) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _request = read_http_request(&mut socket).await;
        let _ = request_received.send(());
        let _ = release.await;
    });
    let config = ConfluentSchemaRegistryConfig::new(
        Url::parse(&format!("http://{address}/")).unwrap(),
        ConfluentSchemaRegistryAuth::None,
    )
    .unwrap()
    .with_request_timeout(Duration::from_millis(10))
    .unwrap();
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let registry = ConfluentSchemaRegistry::with_client(client, config);

    let result = registry.register_or_verify(&schema()).await;
    tokio::time::timeout(Duration::from_secs(1), request_was_received)
        .await
        .unwrap()
        .unwrap();
    let _ = release_server.send(());
    server.await.unwrap();
    assert_eq!(result, Err(ConfluentSchemaRegistryError::Request));
}

fn test_registry(address: &str) -> ConfluentSchemaRegistry {
    ConfluentSchemaRegistry::new(
        ConfluentSchemaRegistryConfig::new(
            Url::parse(&format!("http://{address}/")).unwrap(),
            ConfluentSchemaRegistryAuth::None,
        )
        .unwrap(),
    )
    .unwrap()
}

fn response(status: u16, body: &str) -> String {
    format!(
        "HTTP/1.1 {status} test\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}

async fn read_http_request(socket: &mut tokio::net::TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let read = socket.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0);
        bytes.extend_from_slice(&chunk[..read]);
        let Some(headers_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let headers = std::str::from_utf8(&bytes[..headers_end]).unwrap();
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.split_once(':')
                    .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .map(|(_, value)| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        if bytes.len() >= headers_end + 4 + content_length {
            return String::from_utf8(bytes).unwrap();
        }
    }
}
