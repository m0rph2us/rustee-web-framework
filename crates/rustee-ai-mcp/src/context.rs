use std::fmt;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::Value;
use url::Url;

use crate::McpError;

pub(crate) const MAX_CONTEXT_NAME_BYTES: usize = 128;
const MAX_CONTEXT_URI_BYTES: usize = 4096;
const MAX_METADATA_BYTES: usize = 8192;
const MAX_MIME_TYPE_BYTES: usize = 256;

#[derive(Clone, Copy, Default, Eq, PartialEq)]
pub(crate) struct McpServerCapabilities {
    pub(crate) resources: bool,
    pub(crate) prompts: bool,
}

pub(crate) fn parse_server_capabilities(value: &Value) -> Result<McpServerCapabilities, McpError> {
    let capabilities = value.as_object().ok_or(McpError::MalformedResponse)?;
    let resources = capabilities.get("resources").map_or(Ok(false), |value| {
        value
            .is_object()
            .then_some(true)
            .ok_or(McpError::MalformedResponse)
    })?;
    let prompts = capabilities.get("prompts").map_or(Ok(false), |value| {
        value
            .is_object()
            .then_some(true)
            .ok_or(McpError::MalformedResponse)
    })?;
    Ok(McpServerCapabilities { resources, prompts })
}

/// An untrusted resource made visible by an MCP server.
#[derive(Clone, Eq, PartialEq)]
pub struct McpResource {
    uri: Url,
    name: String,
    title: Option<String>,
    description: Option<String>,
    mime_type: Option<String>,
    size: Option<u64>,
}

impl McpResource {
    /// Returns the remote resource URI. It remains untrusted application input.
    #[must_use]
    pub const fn uri(&self) -> &Url {
        &self.uri
    }

    /// Returns the stable remote resource name.
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

    /// Returns the server-declared MIME type without interpreting it.
    #[must_use]
    pub fn mime_type(&self) -> Option<&str> {
        self.mime_type.as_deref()
    }

    /// Returns the server-declared byte size when available.
    #[must_use]
    pub const fn size(&self) -> Option<u64> {
        self.size
    }
}

impl fmt::Debug for McpResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpResource")
            .field("uri", &self.uri)
            .field("name", &self.name)
            .field("title_length", &self.title.as_ref().map(String::len))
            .field(
                "description_length",
                &self.description.as_ref().map(String::len),
            )
            .field("mime_type", &self.mime_type)
            .field("size", &self.size)
            .finish()
    }
}

/// An untrusted parameterized MCP resource declaration.
#[derive(Clone, Eq, PartialEq)]
pub struct McpResourceTemplate {
    uri_template: String,
    name: String,
    title: Option<String>,
    description: Option<String>,
    mime_type: Option<String>,
}

impl McpResourceTemplate {
    /// Returns the opaque server-defined URI template.
    #[must_use]
    pub fn uri_template(&self) -> &str {
        &self.uri_template
    }

    /// Returns the stable remote template name.
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

    /// Returns the server-declared MIME type without interpreting it.
    #[must_use]
    pub fn mime_type(&self) -> Option<&str> {
        self.mime_type.as_deref()
    }
}

impl fmt::Debug for McpResourceTemplate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpResourceTemplate")
            .field("uri_template_length", &self.uri_template.len())
            .field("name", &self.name)
            .field("title_length", &self.title.as_ref().map(String::len))
            .field(
                "description_length",
                &self.description.as_ref().map(String::len),
            )
            .field("mime_type", &self.mime_type)
            .finish()
    }
}

/// Untrusted resource contents returned by an explicit MCP `resources/read` request.
#[derive(Clone, Eq, PartialEq)]
pub struct McpResourceContents {
    uri: Url,
    mime_type: Option<String>,
    data: McpResourceData,
}

impl McpResourceContents {
    /// Returns the URI the server associated with these contents.
    #[must_use]
    pub const fn uri(&self) -> &Url {
        &self.uri
    }

    /// Returns the declared MIME type without interpreting it.
    #[must_use]
    pub fn mime_type(&self) -> Option<&str> {
        self.mime_type.as_deref()
    }

    /// Returns remote text or decoded binary data. Do not add it to a model request without policy.
    #[must_use]
    pub const fn data(&self) -> &McpResourceData {
        &self.data
    }
}

impl fmt::Debug for McpResourceContents {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpResourceContents")
            .field("uri", &self.uri)
            .field("mime_type", &self.mime_type)
            .field("data", &self.data)
            .finish()
    }
}

/// Decoded remote resource body. The bytes remain untrusted.
#[derive(Clone, Eq, PartialEq)]
pub enum McpResourceData {
    /// UTF-8 text exactly as returned by the remote server.
    Text(String),
    /// Base64-decoded binary content.
    Blob(Vec<u8>),
}

impl McpResourceData {
    pub(crate) fn byte_len(&self) -> usize {
        match self {
            Self::Text(text) => text.len(),
            Self::Blob(blob) => blob.len(),
        }
    }
}

impl fmt::Debug for McpResourceData {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text(text) => formatter
                .debug_struct("Text")
                .field("byte_length", &text.len())
                .finish(),
            Self::Blob(blob) => formatter
                .debug_struct("Blob")
                .field("byte_length", &blob.len())
                .finish(),
        }
    }
}

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
            .field("name", &self.name)
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
            .field("name", &self.name)
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
#[derive(Clone, Debug, Eq, PartialEq)]
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
    Image { data: Vec<u8>, mime_type: String },
    /// Base64-decoded audio data.
    Audio { data: Vec<u8>, mime_type: String },
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
            Self::Resource(resource) => resource.data.byte_len(),
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

/// An untrusted remote resource reference embedded in a prompt message.
#[derive(Clone, Eq, PartialEq)]
pub struct McpResourceLink {
    uri: Url,
    name: String,
    title: Option<String>,
    description: Option<String>,
    mime_type: Option<String>,
    size: Option<u64>,
}

impl McpResourceLink {
    /// Returns the opaque remote URI. Rustee does not fetch it automatically.
    #[must_use]
    pub const fn uri(&self) -> &Url {
        &self.uri
    }

    /// Returns the remote link name.
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

    /// Returns the server-declared MIME type without interpreting it.
    #[must_use]
    pub fn mime_type(&self) -> Option<&str> {
        self.mime_type.as_deref()
    }

    /// Returns the declared byte size when available.
    #[must_use]
    pub const fn size(&self) -> Option<u64> {
        self.size
    }
}

impl fmt::Debug for McpResourceLink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpResourceLink")
            .field("uri", &self.uri)
            .field("name", &self.name)
            .field("title_length", &self.title.as_ref().map(String::len))
            .field(
                "description_length",
                &self.description.as_ref().map(String::len),
            )
            .field("mime_type", &self.mime_type)
            .field("size", &self.size)
            .finish()
    }
}

pub(crate) fn parse_resource(value: &Value) -> Result<McpResource, McpError> {
    Ok(McpResource {
        uri: parse_uri(required_string(value, "uri")?)?,
        name: parse_name(required_string(value, "name")?)?,
        title: optional_metadata(value, "title")?,
        description: optional_metadata(value, "description")?,
        mime_type: optional_mime_type(value)?,
        size: optional_size(value)?,
    })
}

pub(crate) fn parse_resource_template(value: &Value) -> Result<McpResourceTemplate, McpError> {
    let uri_template = required_string(value, "uriTemplate")?;
    if !valid_uri_template(uri_template) {
        return Err(McpError::MalformedResponse);
    }
    Ok(McpResourceTemplate {
        uri_template: uri_template.to_owned(),
        name: parse_name(required_string(value, "name")?)?,
        title: optional_metadata(value, "title")?,
        description: optional_metadata(value, "description")?,
        mime_type: optional_mime_type(value)?,
    })
}

pub(crate) fn parse_resource_contents(
    value: &Value,
    max_content_bytes: usize,
) -> Result<McpResourceContents, McpError> {
    let uri = parse_uri(required_string(value, "uri")?)?;
    let mime_type = optional_mime_type(value)?;
    let text = value.get("text");
    let blob = value.get("blob");
    let data = match (text, blob) {
        (Some(Value::String(text)), None) if text.len() <= max_content_bytes => {
            McpResourceData::Text(text.clone())
        }
        (Some(Value::String(_)), None) => return Err(McpError::ContextLimit),
        (None, Some(Value::String(blob))) => {
            McpResourceData::Blob(decode_binary(blob, max_content_bytes)?)
        }
        _ => return Err(McpError::MalformedResponse),
    };
    Ok(McpResourceContents {
        uri,
        mime_type,
        data,
    })
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

pub(crate) fn valid_context_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_CONTEXT_NAME_BYTES
        && value.chars().all(|character| !character.is_control())
}

pub(crate) fn valid_context_request_string(value: &str, max_content_bytes: usize) -> bool {
    value.len() <= max_content_bytes && !value.contains('\0')
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

fn parse_resource_link(value: &Value) -> Result<McpResourceLink, McpError> {
    Ok(McpResourceLink {
        uri: parse_uri(required_string(value, "uri")?)?,
        name: parse_name(required_string(value, "name")?)?,
        title: optional_metadata(value, "title")?,
        description: optional_metadata(value, "description")?,
        mime_type: optional_mime_type(value)?,
        size: optional_size(value)?,
    })
}

fn parse_uri(value: &str) -> Result<Url, McpError> {
    if value.len() > MAX_CONTEXT_URI_BYTES || value.contains('\0') {
        return Err(McpError::MalformedResponse);
    }
    Url::parse(value).map_err(|_| McpError::MalformedResponse)
}

fn parse_name(value: &str) -> Result<String, McpError> {
    valid_context_name(value)
        .then(|| value.to_owned())
        .ok_or(McpError::MalformedResponse)
}

fn required_string<'a>(value: &'a Value, key: &str) -> Result<&'a str, McpError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or(McpError::MalformedResponse)
}

fn optional_metadata(value: &Value, key: &str) -> Result<Option<String>, McpError> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if valid_metadata(value) => Ok(Some(value.clone())),
        Some(_) => Err(McpError::MalformedResponse),
    }
}

fn optional_mime_type(value: &Value) -> Result<Option<String>, McpError> {
    match value.get("mimeType") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if valid_mime_type(value) => Ok(Some(value.clone())),
        Some(_) => Err(McpError::MalformedResponse),
    }
}

fn required_mime_type(value: &Value) -> Result<&str, McpError> {
    let mime_type = required_string(value, "mimeType")?;
    valid_mime_type(mime_type)
        .then_some(mime_type)
        .ok_or(McpError::MalformedResponse)
}

fn optional_size(value: &Value) -> Result<Option<u64>, McpError> {
    match value.get("size") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(size)) => size.as_u64().ok_or(McpError::MalformedResponse).map(Some),
        Some(_) => Err(McpError::MalformedResponse),
    }
}

fn decode_binary(value: &str, max_content_bytes: usize) -> Result<Vec<u8>, McpError> {
    let max_encoded_bytes = (max_content_bytes.saturating_add(2) / 3).saturating_mul(4);
    if value.len() > max_encoded_bytes {
        return Err(McpError::ContextLimit);
    }
    let data = STANDARD
        .decode(value)
        .map_err(|_| McpError::MalformedResponse)?;
    (data.len() <= max_content_bytes)
        .then_some(data)
        .ok_or(McpError::ContextLimit)
}

fn valid_uri_template(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_CONTEXT_URI_BYTES
        && !value.contains('\0')
        && !value.chars().any(char::is_control)
}

fn valid_metadata(value: &str) -> bool {
    value.len() <= MAX_METADATA_BYTES && !value.contains('\0')
}

fn valid_mime_type(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_MIME_TYPE_BYTES
        && value.bytes().all(|byte| matches!(byte, 0x21..=0x7e))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        McpPromptContent, McpPromptRole, McpResourceData, parse_prompt_result,
        parse_resource_contents,
    };
    use crate::McpError;

    #[test]
    fn prompt_content_is_decoded_bounded_and_redacted() {
        let prompt = parse_prompt_result(
            &json!({"messages":[
                {"role":"user","content":{"type":"text","text":"hello"}},
                {"role":"assistant","content":{"type":"image","mimeType":"image/png","data":"AQI="}}
            ]}),
            4,
            16,
        )
        .unwrap();
        assert_eq!(prompt.messages()[0].role(), McpPromptRole::User);
        assert!(
            matches!(prompt.messages()[1].content(), McpPromptContent::Image { data, .. } if data == &[1, 2])
        );
        assert!(!format!("{prompt:?}").contains("hello"));
    }

    #[test]
    fn resource_content_rejects_ambiguous_or_oversized_data() {
        let resource =
            parse_resource_contents(&json!({"uri":"resource://tenant-a/doc","text":"text"}), 8)
                .unwrap();
        assert!(matches!(resource.data(), McpResourceData::Text(text) if text == "text"));
        assert_eq!(
            parse_resource_contents(
                &json!({"uri":"resource://tenant-a/doc","text":"text","blob":"dGV4dA=="}),
                8,
            )
            .unwrap_err(),
            McpError::MalformedResponse
        );
        assert_eq!(
            parse_resource_contents(&json!({"uri":"resource://tenant-a/doc","blob":"AQID"}), 2)
                .unwrap_err(),
            McpError::ContextLimit
        );
        assert_eq!(
            parse_resource_contents(&json!({"uri":"resource://tenant-a/doc","text":"text"}), 2)
                .unwrap_err(),
            McpError::ContextLimit
        );
    }
}
