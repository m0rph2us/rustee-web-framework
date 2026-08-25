//! Explicit bounded stdio MCP resource and resource-template operations.

use std::collections::BTreeSet;

use serde_json::{Value, json};

use super::super::{McpStdioClient, connection::StdioConnection, should_discard_connection};
use crate::{
    McpError, McpResource, McpResourceContents, McpResourceTemplate,
    context::{
        parse_resource, parse_resource_contents, parse_resource_template,
        valid_context_request_string,
    },
    protocol::{next_cursor, paginated_params},
};

impl McpStdioClient {
    /// Discovers application-selected MCP resources without fetching or forwarding their content.
    ///
    /// # Errors
    ///
    /// Returns a not-initialized, unsupported-capability, or sanitized transport/protocol/bounds
    /// failure.
    pub async fn list_resources(&self) -> Result<Vec<McpResource>, McpError> {
        let mut connection = self.state.connection.lock().await;
        let result = match connection.as_mut() {
            Some(connection) => self.list_resources_on(connection).await,
            None => Err(McpError::NotInitialized),
        };
        let stale = should_discard_connection(&result)
            .then(|| connection.take())
            .flatten();
        drop(connection);
        self.finish_request(result, stale).await
    }

    async fn list_resources_on(
        &self,
        connection: &mut StdioConnection,
    ) -> Result<Vec<McpResource>, McpError> {
        if !connection.capabilities.resources {
            return Err(McpError::UnsupportedCapability);
        }
        let mut cursor = None;
        let mut cursors = BTreeSet::new();
        let mut uris = BTreeSet::new();
        let mut resources = Vec::new();
        for _ in 0..self.config.max_list_pages {
            let result = self
                .request_on(
                    connection,
                    self.next_request_id(),
                    "resources/list",
                    paginated_params(cursor.as_deref()),
                )
                .await?;
            let discovered = result
                .get("resources")
                .and_then(Value::as_array)
                .ok_or(McpError::MalformedResponse)?;
            for value in discovered {
                let resource = parse_resource(value)?;
                if !uris.insert(resource.uri().as_str().to_owned()) {
                    return Err(McpError::MalformedResponse);
                }
                resources.push(resource);
                if resources.len() > self.config.max_context_items {
                    return Err(McpError::ContextLimit);
                }
            }
            let Some(next) = next_cursor(&result)? else {
                return Ok(resources);
            };
            if !cursors.insert(next.clone()) {
                return Err(McpError::MalformedResponse);
            }
            cursor = Some(next);
        }
        Err(McpError::ContextLimit)
    }

    /// Discovers parameterized MCP resource templates without expanding or fetching them.
    ///
    /// # Errors
    ///
    /// Returns a not-initialized, unsupported-capability, or sanitized transport/protocol/bounds
    /// failure.
    pub async fn list_resource_templates(&self) -> Result<Vec<McpResourceTemplate>, McpError> {
        let mut connection = self.state.connection.lock().await;
        let result = match connection.as_mut() {
            Some(connection) => self.list_resource_templates_on(connection).await,
            None => Err(McpError::NotInitialized),
        };
        let stale = should_discard_connection(&result)
            .then(|| connection.take())
            .flatten();
        drop(connection);
        self.finish_request(result, stale).await
    }

    async fn list_resource_templates_on(
        &self,
        connection: &mut StdioConnection,
    ) -> Result<Vec<McpResourceTemplate>, McpError> {
        if !connection.capabilities.resources {
            return Err(McpError::UnsupportedCapability);
        }
        let mut cursor = None;
        let mut cursors = BTreeSet::new();
        let mut names = BTreeSet::new();
        let mut templates = Vec::new();
        for _ in 0..self.config.max_list_pages {
            let result = self
                .request_on(
                    connection,
                    self.next_request_id(),
                    "resources/templates/list",
                    paginated_params(cursor.as_deref()),
                )
                .await?;
            let discovered = result
                .get("resourceTemplates")
                .and_then(Value::as_array)
                .ok_or(McpError::MalformedResponse)?;
            for value in discovered {
                let template = parse_resource_template(value)?;
                if !names.insert(template.name().to_owned()) {
                    return Err(McpError::MalformedResponse);
                }
                templates.push(template);
                if templates.len() > self.config.max_context_items {
                    return Err(McpError::ContextLimit);
                }
            }
            let Some(next) = next_cursor(&result)? else {
                return Ok(templates);
            };
            if !cursors.insert(next.clone()) {
                return Err(McpError::MalformedResponse);
            }
            cursor = Some(next);
        }
        Err(McpError::ContextLimit)
    }

    /// Reads one explicitly selected MCP resource without adding it to an AI request.
    ///
    /// # Errors
    ///
    /// Returns invalid-local-input or sanitized remote capability, transport, protocol, or bounds
    /// failures.
    pub async fn read_resource(
        &self,
        uri: &url::Url,
    ) -> Result<Vec<McpResourceContents>, McpError> {
        if !valid_context_request_string(uri.as_str(), self.config.max_context_bytes) {
            return Err(McpError::InvalidContextRequest);
        }
        let mut connection = self.state.connection.lock().await;
        let result = match connection.as_mut() {
            Some(connection) => self.read_resource_on(connection, uri).await,
            None => Err(McpError::NotInitialized),
        };
        let stale = should_discard_connection(&result)
            .then(|| connection.take())
            .flatten();
        drop(connection);
        self.finish_request(result, stale).await
    }

    async fn read_resource_on(
        &self,
        connection: &mut StdioConnection,
        uri: &url::Url,
    ) -> Result<Vec<McpResourceContents>, McpError> {
        if !connection.capabilities.resources {
            return Err(McpError::UnsupportedCapability);
        }
        let result = self
            .request_on(
                connection,
                self.next_request_id(),
                "resources/read",
                json!({"uri":uri.as_str()}),
            )
            .await?;
        let contents = result
            .get("contents")
            .and_then(Value::as_array)
            .filter(|contents| {
                !contents.is_empty() && contents.len() <= self.config.max_context_items
            })
            .ok_or(McpError::MalformedResponse)?;
        let mut total_bytes = 0_usize;
        let mut parsed = Vec::with_capacity(contents.len());
        for value in contents {
            let content = parse_resource_contents(
                value,
                self.config.max_context_bytes.saturating_sub(total_bytes),
            )?;
            if content.uri() != uri {
                return Err(McpError::MalformedResponse);
            }
            total_bytes = total_bytes.saturating_add(content.data().byte_len());
            if total_bytes > self.config.max_context_bytes {
                return Err(McpError::ContextLimit);
            }
            parsed.push(content);
        }
        Ok(parsed)
    }
}
