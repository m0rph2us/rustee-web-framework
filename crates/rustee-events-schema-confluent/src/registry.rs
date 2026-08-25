use futures_util::future::BoxFuture;
use reqwest::Client;
use rustee_events_schema::{
    EventSchema, EventSchemaCatalog, EventSchemaRegistry, RegisteredEventSchema,
};

use crate::{
    ConfluentSchemaRegistryConfig, ConfluentSchemaRegistryError,
    transport::SchemaRegistryTransport,
    wire::{RemoteSchema, confluent_compatibility},
};

/// Explicit deployment-time Confluent Schema Registry adapter.
#[derive(Clone, Debug)]
pub struct ConfluentSchemaRegistry {
    transport: SchemaRegistryTransport,
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
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| ConfluentSchemaRegistryError::Client)?;
        Ok(Self::with_client(client, config))
    }

    /// Injects a client for an application-owned proxy, mTLS configuration, or contract test.
    ///
    /// Each registry request still enforces the timeout in `config`. The injected client owns
    /// redirect policy; disable automatic redirects to preserve the configured endpoint boundary.
    #[must_use]
    pub fn with_client(client: Client, config: ConfluentSchemaRegistryConfig) -> Self {
        Self {
            transport: SchemaRegistryTransport::new(client, config),
        }
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
        if let Some(remote) = self.transport.lookup(schema).await? {
            return Self::verify_remote(schema, &remote);
        }

        self.transport.register(schema).await?;
        let remote = self
            .transport
            .lookup(schema)
            .await?
            .ok_or(ConfluentSchemaRegistryError::RegistrationNotVisible)?;
        Self::verify_remote(schema, &remote)
    }

    async fn verify_policy(
        &self,
        schema: &EventSchema,
    ) -> Result<(), ConfluentSchemaRegistryError> {
        let policy = self
            .transport
            .compatibility(schema.subject().as_str())
            .await?;
        if policy.compatibility_level != confluent_compatibility(schema.compatibility()) {
            return Err(ConfluentSchemaRegistryError::CompatibilityPolicyMismatch);
        }
        Ok(())
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
