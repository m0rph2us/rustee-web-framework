//! Explicit bounded MCP resources and prompts over an initialized HTTP session.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};
use url::Url;

use super::McpHttpClient;
use crate::{
    McpError, McpPrompt, McpPromptResult, McpResource, McpResourceContents, McpResourceTemplate,
    context::{
        parse_prompt, parse_prompt_result, parse_resource, parse_resource_contents,
        parse_resource_template, valid_context_name, valid_context_request_string,
    },
    protocol::{next_cursor, paginated_params},
};

impl McpHttpClient {
    /// Discovers application-selected MCP resources without fetching or forwarding their content.
    ///
    /// Resource metadata remains untrusted. A caller must still apply resource-specific access,
    /// consent, classification, and context-budget policy before calling [`Self::read_resource`]
    /// or using its result in an AI request.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::NotInitialized`], [`McpError::UnsupportedCapability`], or a sanitized
    /// transport/protocol/bounds failure.
    pub async fn list_resources(&self) -> Result<Vec<McpResource>, McpError> {
        let session = self.session().await.ok_or(McpError::NotInitialized)?;
        if !session.capabilities.resources {
            return Err(McpError::UnsupportedCapability);
        }
        let mut cursor = None;
        let mut cursors = BTreeSet::new();
        let mut uris = BTreeSet::new();
        let mut resources = Vec::new();
        for _ in 0..self.config.max_list_pages {
            let result = self
                .request(
                    Some(&session),
                    self.next_request_id(),
                    "resources/list",
                    paginated_params(cursor.as_deref()),
                )
                .await?
                .result;
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
    /// Returns [`McpError::NotInitialized`], [`McpError::UnsupportedCapability`], or a sanitized
    /// transport/protocol/bounds failure.
    pub async fn list_resource_templates(&self) -> Result<Vec<McpResourceTemplate>, McpError> {
        let session = self.session().await.ok_or(McpError::NotInitialized)?;
        if !session.capabilities.resources {
            return Err(McpError::UnsupportedCapability);
        }
        let mut cursor = None;
        let mut cursors = BTreeSet::new();
        let mut names = BTreeSet::new();
        let mut templates = Vec::new();
        for _ in 0..self.config.max_list_pages {
            let result = self
                .request(
                    Some(&session),
                    self.next_request_id(),
                    "resources/templates/list",
                    paginated_params(cursor.as_deref()),
                )
                .await?
                .result;
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
    /// Returns [`McpError::InvalidContextRequest`] for an invalid local URI,
    /// [`McpError::UnsupportedCapability`] when the server did not negotiate resources, or a
    /// sanitized remote failure.
    pub async fn read_resource(&self, uri: &Url) -> Result<Vec<McpResourceContents>, McpError> {
        if !valid_context_request_string(uri.as_str(), self.config.max_context_bytes) {
            return Err(McpError::InvalidContextRequest);
        }
        let session = self.session().await.ok_or(McpError::NotInitialized)?;
        if !session.capabilities.resources {
            return Err(McpError::UnsupportedCapability);
        }
        let result = self
            .request(
                Some(&session),
                self.next_request_id(),
                "resources/read",
                json!({"uri":uri.as_str()}),
            )
            .await?
            .result;
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

    /// Discovers user-selectable MCP prompt declarations without requesting their messages.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::NotInitialized`], [`McpError::UnsupportedCapability`], or a sanitized
    /// transport/protocol/bounds failure.
    pub async fn list_prompts(&self) -> Result<Vec<McpPrompt>, McpError> {
        let session = self.session().await.ok_or(McpError::NotInitialized)?;
        if !session.capabilities.prompts {
            return Err(McpError::UnsupportedCapability);
        }
        let mut cursor = None;
        let mut cursors = BTreeSet::new();
        let mut names = BTreeSet::new();
        let mut prompts = Vec::new();
        for _ in 0..self.config.max_list_pages {
            let result = self
                .request(
                    Some(&session),
                    self.next_request_id(),
                    "prompts/list",
                    paginated_params(cursor.as_deref()),
                )
                .await?
                .result;
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

    /// Gets one explicitly selected user-controlled MCP prompt without adding it to an AI request.
    ///
    /// The application owns consent, argument selection, content inspection, and any later model
    /// context rendering.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::InvalidContextRequest`] for unsafe local input,
    /// [`McpError::UnsupportedCapability`] when prompts were not negotiated, or a sanitized remote
    /// failure.
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
        let session = self.session().await.ok_or(McpError::NotInitialized)?;
        if !session.capabilities.prompts {
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
            .request(
                Some(&session),
                self.next_request_id(),
                "prompts/get",
                Value::Object(params),
            )
            .await?
            .result;
        parse_prompt_result(
            &result,
            self.config.max_context_items,
            self.config.max_context_bytes,
        )
    }
}
