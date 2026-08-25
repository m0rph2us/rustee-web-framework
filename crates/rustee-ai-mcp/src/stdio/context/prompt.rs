//! Explicit bounded stdio MCP prompt discovery and retrieval.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use super::super::{McpStdioClient, connection::StdioConnection, should_discard_connection};
use crate::{
    McpError, McpPrompt, McpPromptResult,
    context::{
        parse_prompt, parse_prompt_result, valid_context_name, valid_context_request_string,
    },
    protocol::{next_cursor, paginated_params},
};

impl McpStdioClient {
    /// Discovers user-selectable MCP prompt declarations without requesting their messages.
    ///
    /// # Errors
    ///
    /// Returns a not-initialized, unsupported-capability, or sanitized transport/protocol/bounds
    /// failure.
    pub async fn list_prompts(&self) -> Result<Vec<McpPrompt>, McpError> {
        let mut connection = self.state.connection.lock().await;
        let result = match connection.as_mut() {
            Some(connection) => self.list_prompts_on(connection).await,
            None => Err(McpError::NotInitialized),
        };
        let stale = should_discard_connection(&result)
            .then(|| connection.take())
            .flatten();
        drop(connection);
        self.finish_request(result, stale).await
    }

    async fn list_prompts_on(
        &self,
        connection: &mut StdioConnection,
    ) -> Result<Vec<McpPrompt>, McpError> {
        if !connection.capabilities.prompts {
            return Err(McpError::UnsupportedCapability);
        }
        let mut cursor = None;
        let mut cursors = BTreeSet::new();
        let mut names = BTreeSet::new();
        let mut prompts = Vec::new();
        for _ in 0..self.config.max_list_pages {
            let result = self
                .request_on(
                    connection,
                    self.next_request_id(),
                    "prompts/list",
                    paginated_params(cursor.as_deref()),
                )
                .await?;
            let discovered = result
                .get("prompts")
                .and_then(Value::as_array)
                .ok_or(McpError::MalformedResponse)?;
            for value in discovered {
                let prompt = parse_prompt(value, self.config.max_context_items)?;
                if !names.insert(prompt.name().to_owned()) {
                    return Err(McpError::MalformedResponse);
                }
                prompts.push(prompt);
                if prompts.len() > self.config.max_context_items {
                    return Err(McpError::ContextLimit);
                }
            }
            let Some(next) = next_cursor(&result)? else {
                return Ok(prompts);
            };
            if !cursors.insert(next.clone()) {
                return Err(McpError::MalformedResponse);
            }
            cursor = Some(next);
        }
        Err(McpError::ContextLimit)
    }

    /// Gets one explicitly selected MCP prompt without adding it to an AI request.
    ///
    /// # Errors
    ///
    /// Returns invalid-local-input or sanitized remote capability, transport, protocol, or bounds
    /// failures.
    pub async fn get_prompt(
        &self,
        name: &str,
        arguments: &BTreeMap<String, String>,
    ) -> Result<McpPromptResult, McpError> {
        if !valid_context_name(name) || arguments.len() > self.config.max_context_items {
            return Err(McpError::InvalidContextRequest);
        }
        let mut total_argument_bytes = 0_usize;
        for (key, value) in arguments {
            if !valid_context_name(key)
                || !valid_context_request_string(value, self.config.max_context_bytes)
            {
                return Err(McpError::InvalidContextRequest);
            }
            total_argument_bytes = total_argument_bytes.saturating_add(value.len());
            if total_argument_bytes > self.config.max_context_bytes {
                return Err(McpError::InvalidContextRequest);
            }
        }
        let mut connection = self.state.connection.lock().await;
        let result = match connection.as_mut() {
            Some(connection) => self.get_prompt_on(connection, name, arguments).await,
            None => Err(McpError::NotInitialized),
        };
        let stale = should_discard_connection(&result)
            .then(|| connection.take())
            .flatten();
        drop(connection);
        self.finish_request(result, stale).await
    }

    async fn get_prompt_on(
        &self,
        connection: &mut StdioConnection,
        name: &str,
        arguments: &BTreeMap<String, String>,
    ) -> Result<McpPromptResult, McpError> {
        if !connection.capabilities.prompts {
            return Err(McpError::UnsupportedCapability);
        }
        let mut params = serde_json::Map::new();
        params.insert("name".to_owned(), Value::String(name.to_owned()));
        if !arguments.is_empty() {
            params.insert(
                "arguments".to_owned(),
                serde_json::to_value(arguments).map_err(|_| McpError::MalformedResponse)?,
            );
        }
        let result = self
            .request_on(
                connection,
                self.next_request_id(),
                "prompts/get",
                Value::Object(params),
            )
            .await?;
        parse_prompt_result(
            &result,
            self.config.max_context_items,
            self.config.max_context_bytes,
        )
    }
}
