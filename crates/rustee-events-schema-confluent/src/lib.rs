//! Explicit Confluent Schema Registry verification for Rustee event schema artifacts.
//!
//! The adapter is a deployment-time [`EventSchemaRegistry`] implementation. It validates a
//! registry's effective compatibility setting, looks up an exact `JSON` schema artifact, and
//! registers it only when absent. It does not alter Kafka records, headers, topics, consumer
//! offsets, or retry a request. Applications choose when to invoke an [`EventSchemaCatalog`].

use std::{fmt, time::Duration};

use futures_util::future::BoxFuture;
use reqwest::{Client, RequestBuilder, StatusCode};
use rustee_events_schema::{
    EventSchema, EventSchemaCatalog, EventSchemaRegistry, RegisteredEventSchema,
    SchemaCompatibility,
};
use serde::Deserialize;
use serde_json::json;
use url::{Host, Url};

const SCHEMA_REGISTRY_ACCEPT: &str = "application/vnd.schemaregistry.v1+json";

/// Authentication applied to a Confluent Schema Registry request.
#[derive(Clone, Eq, PartialEq)]
pub enum ConfluentSchemaRegistryAuth {
    /// A Confluent API key and secret sent using HTTP Basic authentication.
    Basic {
        /// Registry API key.
        api_key: String,
        /// Registry API secret.
        api_secret: String,
    },
    /// An application-managed OAuth or service bearer token.
    Bearer(String),
    /// No HTTP authorization header; intended for a loopback test registry or an externally
    /// authenticated TLS client injected with [`ConfluentSchemaRegistry::with_client`].
    None,
}

impl ConfluentSchemaRegistryAuth {
    fn validate(&self) -> bool {
        match self {
            Self::Basic {
                api_key,
                api_secret,
            } => !api_key.trim().is_empty() && !api_secret.is_empty(),
            Self::Bearer(token) => !token.trim().is_empty(),
            Self::None => true,
        }
    }
}

impl fmt::Debug for ConfluentSchemaRegistryAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Basic { .. } => "basic",
            Self::Bearer(_) => "bearer",
            Self::None => "none",
        };
        formatter
            .debug_struct("ConfluentSchemaRegistryAuth")
            .field("kind", &kind)
            .finish()
    }
}

/// Redacted configuration for a Confluent Schema Registry deployment endpoint.
#[derive(Clone, Eq, PartialEq)]
pub struct ConfluentSchemaRegistryConfig {
    base_url: Url,
    auth: ConfluentSchemaRegistryAuth,
    request_timeout: Duration,
}

impl ConfluentSchemaRegistryConfig {
    /// Creates configuration for one registry endpoint.
    ///
    /// Non-loopback registry endpoints must use HTTPS. The URL must not contain credentials,
    /// query parameters, or fragments; use [`ConfluentSchemaRegistryAuth`] or an injected TLS
    /// client for credentials instead.
    ///
    /// # Errors
    ///
    /// Returns [`ConfluentSchemaRegistryConfigError`] for an unsafe endpoint or empty credential.
    pub fn new(
        mut base_url: Url,
        auth: ConfluentSchemaRegistryAuth,
    ) -> Result<Self, ConfluentSchemaRegistryConfigError> {
        if !valid_base_url(&base_url) {
            return Err(ConfluentSchemaRegistryConfigError::InvalidBaseUrl);
        }
        if !auth.validate() {
            return Err(ConfluentSchemaRegistryConfigError::InvalidAuthentication);
        }
        if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }
        Ok(Self {
            base_url,
            auth,
            request_timeout: Duration::from_secs(5),
        })
    }

    /// Sets the bounded timeout for one registry HTTP request.
    ///
    /// # Errors
    ///
    /// Returns [`ConfluentSchemaRegistryConfigError::ZeroTimeout`] when `request_timeout` is zero.
    pub fn with_request_timeout(
        mut self,
        request_timeout: Duration,
    ) -> Result<Self, ConfluentSchemaRegistryConfigError> {
        if request_timeout.is_zero() {
            return Err(ConfluentSchemaRegistryConfigError::ZeroTimeout);
        }
        self.request_timeout = request_timeout;
        Ok(self)
    }
}

impl fmt::Debug for ConfluentSchemaRegistryConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfluentSchemaRegistryConfig")
            .field("base_url", &self.base_url)
            .field("auth", &self.auth)
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

/// Invalid Confluent Schema Registry adapter configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConfluentSchemaRegistryConfigError {
    /// The endpoint was not a clean HTTPS URL or a loopback HTTP test URL.
    #[error(
        "Confluent Schema Registry URL must use HTTPS unless it is loopback, without credentials, query, or fragment"
    )]
    InvalidBaseUrl,
    /// Configured Basic or bearer authentication was blank.
    #[error("Confluent Schema Registry authentication must not be blank")]
    InvalidAuthentication,
    /// The adapter must bound every registry request.
    #[error("Confluent Schema Registry request timeout must be non-zero")]
    ZeroTimeout,
}

/// Explicit deployment-time Confluent Schema Registry adapter.
#[derive(Clone, Debug)]
pub struct ConfluentSchemaRegistry {
    client: Client,
    config: ConfluentSchemaRegistryConfig,
}

impl ConfluentSchemaRegistry {
    /// Builds an adapter with a TLS-enabled client and the configured request timeout.
    ///
    /// # Errors
    ///
    /// Returns [`ConfluentSchemaRegistryError::Client`] when the HTTP client cannot be built.
    pub fn new(
        config: ConfluentSchemaRegistryConfig,
    ) -> Result<Self, ConfluentSchemaRegistryError> {
        let client = Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|_| ConfluentSchemaRegistryError::Client)?;
        Ok(Self { client, config })
    }

    /// Injects a client for an application-owned proxy, mTLS configuration, or contract test.
    #[must_use]
    pub fn with_client(client: Client, config: ConfluentSchemaRegistryConfig) -> Self {
        Self { client, config }
    }

    /// Verifies or registers every schema in deterministic catalog order.
    ///
    /// This is an explicit deployment call. It does not start a background task or attach to a
    /// Kafka producer or consumer.
    ///
    /// # Errors
    ///
    /// Returns a sanitized adapter error for transport, policy, compatibility, or remote artifact
    /// mismatch failures.
    pub async fn verify_catalog(
        &self,
        catalog: &EventSchemaCatalog,
    ) -> Result<(), rustee_events_schema::SchemaVerificationError<ConfluentSchemaRegistryError>>
    {
        catalog.verify(self).await
    }

    async fn register_or_verify_schema(
        &self,
        schema: &EventSchema,
    ) -> Result<RegisteredEventSchema, ConfluentSchemaRegistryError> {
        self.verify_policy(schema).await?;
        if let Some(remote) = self.lookup(schema).await? {
            return Self::verify_remote(schema, &remote);
        }

        self.register(schema).await?;
        let remote = self
            .lookup(schema)
            .await?
            .ok_or(ConfluentSchemaRegistryError::RegistrationNotVisible)?;
        Self::verify_remote(schema, &remote)
    }

    async fn verify_policy(
        &self,
        schema: &EventSchema,
    ) -> Result<(), ConfluentSchemaRegistryError> {
        let mut endpoint = self.endpoint(&format!(
            "config/{}",
            path_segment(schema.subject().as_str())
        ))?;
        endpoint
            .query_pairs_mut()
            .append_pair("defaultToGlobal", "true");
        let response = self
            .authorize(
                self.client
                    .get(endpoint)
                    .header("accept", SCHEMA_REGISTRY_ACCEPT),
            )
            .send()
            .await
            .map_err(|_| ConfluentSchemaRegistryError::Request)?;
        if !response.status().is_success() {
            return Err(ConfluentSchemaRegistryError::Request);
        }
        let policy = response
            .json::<CompatibilityResponse>()
            .await
            .map_err(|_| ConfluentSchemaRegistryError::MalformedResponse)?;
        if policy.compatibility_level != confluent_compatibility(schema.compatibility()) {
            return Err(ConfluentSchemaRegistryError::CompatibilityPolicyMismatch);
        }
        Ok(())
    }

    async fn lookup(
        &self,
        schema: &EventSchema,
    ) -> Result<Option<RemoteSchema>, ConfluentSchemaRegistryError> {
        let endpoint = self.endpoint(&format!(
            "subjects/{}",
            path_segment(schema.subject().as_str())
        ))?;
        let response = self
            .authorize(
                self.client
                    .post(endpoint)
                    .header("accept", SCHEMA_REGISTRY_ACCEPT)
                    .json(&schema_request(schema)),
            )
            .send()
            .await
            .map_err(|_| ConfluentSchemaRegistryError::Request)?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(ConfluentSchemaRegistryError::Request);
        }
        response
            .json::<RemoteSchema>()
            .await
            .map(Some)
            .map_err(|_| ConfluentSchemaRegistryError::MalformedResponse)
    }

    async fn register(&self, schema: &EventSchema) -> Result<(), ConfluentSchemaRegistryError> {
        let endpoint = self.endpoint(&format!(
            "subjects/{}/versions",
            path_segment(schema.subject().as_str())
        ))?;
        let response = self
            .authorize(
                self.client
                    .post(endpoint)
                    .header("accept", SCHEMA_REGISTRY_ACCEPT)
                    .json(&schema_request(schema)),
            )
            .send()
            .await
            .map_err(|_| ConfluentSchemaRegistryError::Request)?;
        if response.status() == StatusCode::CONFLICT {
            return Err(ConfluentSchemaRegistryError::IncompatibleSchema);
        }
        if !response.status().is_success() {
            return Err(ConfluentSchemaRegistryError::RegistrationRejected);
        }
        response
            .json::<RegistrationResponse>()
            .await
            .map(|_| ())
            .map_err(|_| ConfluentSchemaRegistryError::MalformedResponse)
    }

    fn verify_remote(
        schema: &EventSchema,
        remote: &RemoteSchema,
    ) -> Result<RegisteredEventSchema, ConfluentSchemaRegistryError> {
        if remote.subject != schema.subject().as_str()
            || remote.version != schema.version()
            || remote.schema_type.as_deref() != Some("JSON")
            || remote.schema != schema.definition()
        {
            return Err(ConfluentSchemaRegistryError::ArtifactMismatch);
        }
        Ok(RegisteredEventSchema::new(
            schema.subject().clone(),
            schema.version(),
            schema.fingerprint(),
        ))
    }

    fn endpoint(&self, path: &str) -> Result<Url, ConfluentSchemaRegistryError> {
        self.config
            .base_url
            .join(path)
            .map_err(|_| ConfluentSchemaRegistryError::InvalidEndpoint)
    }

    fn authorize(&self, request: RequestBuilder) -> RequestBuilder {
        match &self.config.auth {
            ConfluentSchemaRegistryAuth::Basic {
                api_key,
                api_secret,
            } => request.basic_auth(api_key, Some(api_secret)),
            ConfluentSchemaRegistryAuth::Bearer(token) => request.bearer_auth(token),
            ConfluentSchemaRegistryAuth::None => request,
        }
    }
}

impl EventSchemaRegistry for ConfluentSchemaRegistry {
    type Error = ConfluentSchemaRegistryError;

    fn register_or_verify<'a>(
        &'a self,
        schema: &'a EventSchema,
    ) -> BoxFuture<'a, Result<RegisteredEventSchema, Self::Error>> {
        Box::pin(async move { self.register_or_verify_schema(schema).await })
    }
}

/// Sanitized Confluent Schema Registry adapter failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConfluentSchemaRegistryError {
    /// The HTTP client could not be constructed.
    #[error("Confluent Schema Registry client initialization failed")]
    Client,
    /// A validated base URL could not construct the requested API path.
    #[error("Confluent Schema Registry endpoint was invalid")]
    InvalidEndpoint,
    /// A request failed or returned an unexpected non-success status.
    #[error("Confluent Schema Registry request failed")]
    Request,
    /// A successful response did not match the expected response shape.
    #[error("Confluent Schema Registry response was malformed")]
    MalformedResponse,
    /// The remote effective compatibility policy did not equal the local schema declaration.
    #[error("Confluent Schema Registry compatibility policy did not match the local declaration")]
    CompatibilityPolicyMismatch,
    /// The registry rejected a schema as incompatible with its subject history.
    #[error("Confluent Schema Registry rejected the schema as incompatible")]
    IncompatibleSchema,
    /// The registry did not accept the requested registration.
    #[error("Confluent Schema Registry rejected the schema registration")]
    RegistrationRejected,
    /// A successful registration could not be looked up again as an exact artifact.
    #[error("Confluent Schema Registry registration was not visible for exact verification")]
    RegistrationNotVisible,
    /// The remote subject, version, format, or source did not equal the local schema artifact.
    #[error("Confluent Schema Registry returned a different schema artifact")]
    ArtifactMismatch,
}

#[derive(Deserialize)]
struct CompatibilityResponse {
    #[serde(rename = "compatibilityLevel")]
    compatibility_level: String,
}

#[derive(Deserialize)]
struct RemoteSchema {
    subject: String,
    version: u16,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
    schema: String,
}

#[derive(Deserialize)]
struct RegistrationResponse {
    #[serde(rename = "id")]
    _id: i64,
}

fn schema_request(schema: &EventSchema) -> serde_json::Value {
    json!({
        "schemaType": "JSON",
        "schema": schema.definition(),
    })
}

fn confluent_compatibility(compatibility: SchemaCompatibility) -> &'static str {
    match compatibility {
        SchemaCompatibility::Backward => "BACKWARD",
        SchemaCompatibility::Forward => "FORWARD",
        SchemaCompatibility::Full => "FULL",
        SchemaCompatibility::None => "NONE",
    }
}

fn path_segment(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
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

    use rustee_events::Event;
    use rustee_events_schema::{EventSchema, SchemaSubject};
    use serde::{Deserialize, Serialize};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::*;

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
    fn configuration_requires_safe_endpoint_and_redacts_credentials() {
        let config = ConfluentSchemaRegistryConfig::new(
            Url::parse("https://registry.example.test/api/").unwrap(),
            ConfluentSchemaRegistryAuth::Basic {
                api_key: "key".to_owned(),
                api_secret: "secret".to_owned(),
            },
        )
        .unwrap();
        let debug = format!("{config:?}");
        assert!(!debug.contains("key"));
        assert!(!debug.contains("secret"));
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
            requests[0]
                .starts_with("GET /config/account.opened-value?defaultToGlobal=true HTTP/1.1")
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
            let Some(headers_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n")
            else {
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
}
