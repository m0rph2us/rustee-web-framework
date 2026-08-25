//! Untrusted MCP prompt declarations and bounded multimodal content decoding.

use std::fmt;

use serde_json::Value;

use crate::McpError;

use super::resource::parse_resource_link;
use super::{
    McpResourceContents, McpResourceLink, decode_binary, optional_metadata, parse_name,
    parse_resource_contents, required_string, valid_mime_type,
};

/// An untrusted prompt declaration supplied by an MCP server.
#[derive(Clone, Eq, PartialEq)]
pub struct McpPrompt {
    name: String,
    title: Option<String>,
    description: Option<String>,
    arguments: Vec<McpPromptArgument>,
}

impl McpPrompt {
    /// Returns the stable remote prompt name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns optional remote display metadata.
    #[must_use]
    pub fn title(&self) -> Option<&str> {
        self.title.as_deref()
    }

    /// Returns optional remote description metadata.
    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Returns declared prompt arguments without resolving or rendering them.
    #[must_use]
    pub fn arguments(&self) -> &[McpPromptArgument] {
        &self.arguments
    }
}

impl fmt::Debug for McpPrompt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpPrompt")
            .field("name_length", &self.name.len())
            .field("title_length", &self.title.as_ref().map(String::len))
            .field(
                "description_length",
                &self.description.as_ref().map(String::len),
            )
            .field("argument_count", &self.arguments.len())
            .finish()
    }
}

/// An untrusted prompt argument declaration.
#[derive(Clone, Eq, PartialEq)]
pub struct McpPromptArgument {
    name: String,
    description: Option<String>,
    required: bool,
}

impl McpPromptArgument {
    /// Returns the argument name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns optional remote argument documentation.
    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Returns whether the server marked this argument as required.
    #[must_use]
    pub const fn required(&self) -> bool {
        self.required
    }
}

impl fmt::Debug for McpPromptArgument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpPromptArgument")
            .field("name_length", &self.name.len())
            .field(
                "description_length",
                &self.description.as_ref().map(String::len),
            )
            .field("required", &self.required)
            .finish()
    }
}

/// Untrusted prompt messages returned by an explicit MCP `prompts/get` request.
#[derive(Clone, Eq, PartialEq)]
pub struct McpPromptResult {
    description: Option<String>,
    messages: Vec<McpPromptMessage>,
}

impl McpPromptResult {
    /// Returns optional remote prompt documentation.
    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Returns the untrusted remote messages in server order.
    #[must_use]
    pub fn messages(&self) -> &[McpPromptMessage] {
        &self.messages
    }
}

impl fmt::Debug for McpPromptResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpPromptResult")
            .field(
                "description_length",
                &self.description.as_ref().map(String::len),
            )
            .field("message_count", &self.messages.len())
            .finish()
    }
}

/// One untrusted remote prompt message.
#[derive(Clone, Eq, PartialEq)]
pub struct McpPromptMessage {
    role: McpPromptRole,
    content: McpPromptContent,
}

impl McpPromptMessage {
    /// Returns the remote-declared role.
    #[must_use]
    pub const fn role(&self) -> McpPromptRole {
        self.role
    }

    /// Returns the untrusted remote content.
    #[must_use]
    pub const fn content(&self) -> &McpPromptContent {
        &self.content
    }
}

impl fmt::Debug for McpPromptMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpPromptMessage")
            .field("role", &self.role)
            .field("content", &self.content)
            .finish()
    }
}

/// The two MCP prompt roles.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpPromptRole {
    /// A user message.
    User,
    /// An assistant message.
    Assistant,
}

/// One untrusted multimodal MCP prompt content block.
#[derive(Clone, Eq, PartialEq)]
pub enum McpPromptContent {
    /// Text content.
    Text(String),
    /// Base64-decoded image data.
    Image {
        /// Raw decoded image bytes.
        data: Vec<u8>,
        /// Remote-declared image media type.
        mime_type: String,
    },
    /// Base64-decoded audio data.
    Audio {
        /// Raw decoded audio bytes.
        data: Vec<u8>,
        /// Remote-declared audio media type.
        mime_type: String,
    },
    /// An embedded resource body.
    Resource(McpResourceContents),
    /// A resource reference that this client does not fetch automatically.
    ResourceLink(McpResourceLink),
}

impl McpPromptContent {
    fn byte_len(&self) -> usize {
        match self {
            Self::Text(text) => text.len(),
            Self::Image { data, .. } | Self::Audio { data, .. } => data.len(),
            Self::Resource(resource) => resource.data().byte_len(),
            Self::ResourceLink(_) => 0,
        }
    }
}

impl fmt::Debug for McpPromptContent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text(text) => formatter
                .debug_struct("Text")
                .field("byte_length", &text.len())
                .finish(),
            Self::Image { data, mime_type } => formatter
                .debug_struct("Image")
                .field("byte_length", &data.len())
                .field("mime_type", mime_type)
                .finish(),
            Self::Audio { data, mime_type } => formatter
                .debug_struct("Audio")
                .field("byte_length", &data.len())
                .field("mime_type", mime_type)
                .finish(),
            Self::Resource(resource) => formatter.debug_tuple("Resource").field(resource).finish(),
            Self::ResourceLink(link) => formatter.debug_tuple("ResourceLink").field(link).finish(),
        }
    }
}

pub(crate) fn parse_prompt(value: &Value, max_items: usize) -> Result<McpPrompt, McpError> {
    let arguments = match value.get("arguments") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(arguments)) if arguments.len() <= max_items => arguments
            .iter()
            .map(parse_prompt_argument)
            .collect::<Result<Vec<_>, _>>()?,
        _ => return Err(McpError::MalformedResponse),
    };
    let mut names = std::collections::BTreeSet::new();
    if arguments
        .iter()
        .any(|argument| !names.insert(argument.name.clone()))
    {
        return Err(McpError::MalformedResponse);
    }
    Ok(McpPrompt {
        name: parse_name(required_string(value, "name")?)?,
        title: optional_metadata(value, "title")?,
        description: optional_metadata(value, "description")?,
        arguments,
    })
}

pub(crate) fn parse_prompt_result(
    value: &Value,
    max_items: usize,
    max_content_bytes: usize,
) -> Result<McpPromptResult, McpError> {
    let messages = value
        .get("messages")
        .and_then(Value::as_array)
        .filter(|messages| messages.len() <= max_items)
        .ok_or(McpError::MalformedResponse)?;
    let mut total_bytes = 0_usize;
    let mut parsed = Vec::with_capacity(messages.len());
    for message in messages {
        let role = match required_string(message, "role")? {
            "user" => McpPromptRole::User,
            "assistant" => McpPromptRole::Assistant,
            _ => return Err(McpError::MalformedResponse),
        };
        let content = parse_prompt_content(
            message.get("content").ok_or(McpError::MalformedResponse)?,
            max_content_bytes.saturating_sub(total_bytes),
        )?;
        total_bytes = total_bytes.saturating_add(content.byte_len());
        if total_bytes > max_content_bytes {
            return Err(McpError::ContextLimit);
        }
        parsed.push(McpPromptMessage { role, content });
    }
    Ok(McpPromptResult {
        description: optional_metadata(value, "description")?,
        messages: parsed,
    })
}

fn parse_prompt_argument(value: &Value) -> Result<McpPromptArgument, McpError> {
    Ok(McpPromptArgument {
        name: parse_name(required_string(value, "name")?)?,
        description: optional_metadata(value, "description")?,
        required: match value.get("required") {
            None => false,
            Some(Value::Bool(required)) => *required,
            Some(_) => return Err(McpError::MalformedResponse),
        },
    })
}

fn parse_prompt_content(
    value: &Value,
    max_content_bytes: usize,
) -> Result<McpPromptContent, McpError> {
    match required_string(value, "type")? {
        "text" => {
            let text = required_string(value, "text")?;
            if text.len() > max_content_bytes {
                return Err(McpError::ContextLimit);
            }
            Ok(McpPromptContent::Text(text.to_owned()))
        }
        "image" => Ok(McpPromptContent::Image {
            data: decode_binary(required_string(value, "data")?, max_content_bytes)?,
            mime_type: required_mime_type(value)?.to_owned(),
        }),
        "audio" => Ok(McpPromptContent::Audio {
            data: decode_binary(required_string(value, "data")?, max_content_bytes)?,
            mime_type: required_mime_type(value)?.to_owned(),
        }),
        "resource" => Ok(McpPromptContent::Resource(parse_resource_contents(
            value.get("resource").ok_or(McpError::MalformedResponse)?,
            max_content_bytes,
        )?)),
        "resource_link" => Ok(McpPromptContent::ResourceLink(parse_resource_link(value)?)),
        _ => Err(McpError::MalformedResponse),
    }
}

fn required_mime_type(value: &Value) -> Result<&str, McpError> {
    let mime_type = required_string(value, "mimeType")?;
    valid_mime_type(mime_type)
        .then_some(mime_type)
        .ok_or(McpError::MalformedResponse)
}
