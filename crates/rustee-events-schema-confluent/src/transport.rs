use futures_util::StreamExt;
use reqwest::{Client, RequestBuilder, StatusCode};
use rustee_events_schema::EventSchema;
use serde::de::DeserializeOwned;
use url::Url;

use crate::{
    ConfluentSchemaRegistryAuth, ConfluentSchemaRegistryConfig, ConfluentSchemaRegistryError,
    wire::{CompatibilityResponse, RegistrationResponse, RemoteSchema, schema_request},
};

const SCHEMA_REGISTRY_ACCEPT: &str = "application/vnd.schemaregistry.v1+json";

#[derive(Clone, Debug)]
pub(crate) struct SchemaRegistryTransport {
    client: Client,
    config: ConfluentSchemaRegistryConfig,
}

impl SchemaRegistryTransport {
    pub(crate) const fn new(client: Client, config: ConfluentSchemaRegistryConfig) -> Self {
        Self { client, config }
    }

    pub(crate) async fn compatibility(
        &self,
        subject: &str,
    ) -> Result<CompatibilityResponse, ConfluentSchemaRegistryError> {
        let mut endpoint = self.endpoint(&format!("config/{}", path_segment(subject)))?;
        endpoint
            .query_pairs_mut()
            .append_pair("defaultToGlobal", "true");
        let response = self
            .authorize(
                self.client
                    .get(endpoint)
                    .timeout(self.config.request_timeout)
                    .header("accept", SCHEMA_REGISTRY_ACCEPT),
            )
            .send()
            .await
            .map_err(|_| ConfluentSchemaRegistryError::Request)?;
        if !response.status().is_success() {
            return Err(ConfluentSchemaRegistryError::Request);
        }
        decode_json_response(response, self.config.max_response_bytes).await
    }

    pub(crate) async fn lookup(
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
                    .timeout(self.config.request_timeout)
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
        decode_json_response(response, self.config.max_response_bytes)
            .await
            .map(Some)
    }

    pub(crate) async fn register(
        &self,
        schema: &EventSchema,
    ) -> Result<(), ConfluentSchemaRegistryError> {
        let endpoint = self.endpoint(&format!(
            "subjects/{}/versions",
            path_segment(schema.subject().as_str())
        ))?;
        let response = self
            .authorize(
                self.client
                    .post(endpoint)
                    .timeout(self.config.request_timeout)
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
        let _: RegistrationResponse =
            decode_json_response(response, self.config.max_response_bytes).await?;
        Ok(())
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

async fn decode_json_response<T>(
    response: reqwest::Response,
    max_response_bytes: usize,
) -> Result<T, ConfluentSchemaRegistryError>
where
    T: DeserializeOwned,
{
    if !has_schema_registry_json_content_type(response.headers()) {
        return Err(ConfluentSchemaRegistryError::MalformedResponse);
    }
    if response
        .content_length()
        .is_some_and(|length| length > max_response_bytes as u64)
    {
        return Err(ConfluentSchemaRegistryError::ResponseTooLarge);
    }

    let mut body = Vec::new();
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|_| ConfluentSchemaRegistryError::Request)?;
        if chunk.len() > max_response_bytes.saturating_sub(body.len()) {
            return Err(ConfluentSchemaRegistryError::ResponseTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|_| ConfluentSchemaRegistryError::MalformedResponse)
}

fn has_schema_registry_json_content_type(headers: &reqwest::header::HeaderMap) -> bool {
    let Some(media_type) = single_content_type(headers) else {
        return false;
    };
    [
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json",
        "application/json",
    ]
    .into_iter()
    .any(|expected| media_type.eq_ignore_ascii_case(expected))
}

fn single_content_type(headers: &reqwest::header::HeaderMap) -> Option<&str> {
    let mut values = headers.get_all(reqwest::header::CONTENT_TYPE).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    let value = value.to_str().ok()?;
    value.split(';').next().map(str::trim)
}

fn path_segment(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

#[cfg(test)]
mod tests {
    use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};

    use super::has_schema_registry_json_content_type;

    #[test]
    fn registry_json_response_requires_one_supported_media_type() {
        let mut headers = HeaderMap::new();
        for media_type in [
            "application/vnd.schemaregistry.v1+json; charset=utf-8",
            "application/vnd.schemaregistry+json",
            "application/json",
        ] {
            headers.insert(CONTENT_TYPE, HeaderValue::from_static(media_type));
            assert!(has_schema_registry_json_content_type(&headers));
        }

        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/vnd.schemaregistry.v2+json"),
        );
        assert!(!has_schema_registry_json_content_type(&headers));

        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/vnd.schemaregistry.v1+json"),
        );
        headers.append(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        assert!(!has_schema_registry_json_content_type(&headers));

        headers.clear();
        assert!(!has_schema_registry_json_content_type(&headers));
    }
}
