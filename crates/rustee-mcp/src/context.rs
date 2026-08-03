use std::{collections::BTreeSet, fmt};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use url::Url;

const MAX_NAME_BYTES: usize = 128;
const MAX_URI_TEMPLATE_BYTES: usize = 4096;
const MAX_METADATA_BYTES: usize = 8192;
const MAX_MIME_TYPE_BYTES: usize = 256;

/// Invalid application-supplied MCP resource or prompt data.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum McpContextValueError {
    /// A name was blank, too long, or contained a control character.
    #[error("MCP context name must be non-blank, bounded, and free of control characters")]
    InvalidName,
    /// A URI template was blank, too long, or contained a control character.
    #[error("MCP resource URI template must be non-blank, bounded, and free of control characters")]
    InvalidUriTemplate,
    /// Text metadata was too large or contained a NUL byte.
    #[error("MCP context metadata must be bounded and free of NUL bytes")]
    InvalidMetadata,
    /// A MIME type was blank, too long, or included non-visible ASCII bytes.
    #[error("MCP context MIME type must be bounded visible ASCII")]
    InvalidMimeType,
}

/// Application-owned resource metadata exposed through MCP `resources/list`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpServerResource {
    uri: Url,
    name: String,
    title: Option<String>,
    description: Option<String>,
    mime_type: Option<String>,
    size: Option<u64>,
}

impl McpServerResource {
    /// Creates minimal resource metadata.
    ///
    /// # Errors
    ///
    /// Returns [`McpContextValueError::InvalidName`] for an unsafe display name.
    pub fn new(uri: Url, name: impl Into<String>) -> Result<Self, McpContextValueError> {
        Ok(Self {
            uri,
            name: valid_name(name.into())?,
            title: None,
            description: None,
            mime_type: None,
            size: None,
        })
    }

    /// Adds optional resource title metadata.
    ///
    /// # Errors
    ///
    /// Returns [`McpContextValueError::InvalidMetadata`] for unsafe metadata.
    pub fn with_title(mut self, title: impl Into<String>) -> Result<Self, McpContextValueError> {
        self.title = Some(valid_metadata(title.into())?);
        Ok(self)
    }

    /// Adds optional resource description metadata.
    ///
    /// # Errors
    ///
    /// Returns [`McpContextValueError::InvalidMetadata`] for unsafe metadata.
    pub fn with_description(
        mut self,
        description: impl Into<String>,
    ) -> Result<Self, McpContextValueError> {
        self.description = Some(valid_metadata(description.into())?);
        Ok(self)
    }

    /// Adds an optional declared MIME type.
    ///
    /// # Errors
    ///
    /// Returns [`McpContextValueError::InvalidMimeType`] for an unsafe MIME type.
    pub fn with_mime_type(
        mut self,
        mime_type: impl Into<String>,
    ) -> Result<Self, McpContextValueError> {
        self.mime_type = Some(valid_mime_type(mime_type.into())?);
        Ok(self)
    }

    /// Adds an optional application-known byte size.
    #[must_use]
    pub const fn with_size(mut self, size: u64) -> Self {
        self.size = Some(size);
        self
    }

    #[must_use]
    pub const fn uri(&self) -> &Url {
        &self.uri
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn wire(&self) -> Value {
        let mut value = serde_json::Map::new();
        value.insert("uri".to_owned(), Value::String(self.uri.to_string()));
        value.insert("name".to_owned(), Value::String(self.name.clone()));
        optional_string(&mut value, "title", self.title.as_ref());
        optional_string(&mut value, "description", self.description.as_ref());
        optional_string(&mut value, "mimeType", self.mime_type.as_ref());
        if let Some(size) = self.size {
            value.insert("size".to_owned(), Value::from(size));
        }
        Value::Object(value)
    }
}

/// Application-owned parameterized resource metadata exposed through MCP templates/list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpServerResourceTemplate {
    uri_template: String,
    name: String,
    title: Option<String>,
    description: Option<String>,
    mime_type: Option<String>,
}

impl McpServerResourceTemplate {
    /// Creates minimal parameterized resource metadata.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsafe template or name.
    pub fn new(
        uri_template: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<Self, McpContextValueError> {
        let uri_template = uri_template.into();
        if uri_template.is_empty()
            || uri_template.len() > MAX_URI_TEMPLATE_BYTES
            || uri_template.chars().any(char::is_control)
        {
            return Err(McpContextValueError::InvalidUriTemplate);
        }
        Ok(Self {
            uri_template,
            name: valid_name(name.into())?,
            title: None,
            description: None,
            mime_type: None,
        })
    }

    /// Adds optional template title metadata.
    ///
    /// # Errors
    ///
    /// Returns [`McpContextValueError::InvalidMetadata`] for unsafe metadata.
    pub fn with_title(mut self, title: impl Into<String>) -> Result<Self, McpContextValueError> {
        self.title = Some(valid_metadata(title.into())?);
        Ok(self)
    }

    /// Adds optional template description metadata.
    ///
    /// # Errors
    ///
    /// Returns [`McpContextValueError::InvalidMetadata`] for unsafe metadata.
    pub fn with_description(
        mut self,
        description: impl Into<String>,
    ) -> Result<Self, McpContextValueError> {
        self.description = Some(valid_metadata(description.into())?);
        Ok(self)
    }

    /// Adds an optional declared MIME type.
    ///
    /// # Errors
    ///
    /// Returns [`McpContextValueError::InvalidMimeType`] for an unsafe MIME type.
    pub fn with_mime_type(
        mut self,
        mime_type: impl Into<String>,
    ) -> Result<Self, McpContextValueError> {
        self.mime_type = Some(valid_mime_type(mime_type.into())?);
        Ok(self)
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn wire(&self) -> Value {
        let mut value = serde_json::Map::new();
        value.insert(
            "uriTemplate".to_owned(),
            Value::String(self.uri_template.clone()),
        );
        value.insert("name".to_owned(), Value::String(self.name.clone()));
        optional_string(&mut value, "title", self.title.as_ref());
        optional_string(&mut value, "description", self.description.as_ref());
        optional_string(&mut value, "mimeType", self.mime_type.as_ref());
        Value::Object(value)
    }
}

/// Application-owned resource contents returned through MCP `resources/read`.
#[derive(Clone, Eq, PartialEq)]
pub struct McpServerResourceContents {
    uri: Url,
    mime_type: Option<String>,
    data: McpServerResourceData,
}

impl McpServerResourceContents {
    /// Creates UTF-8 text resource contents.
    #[must_use]
    pub fn text(uri: Url, text: impl Into<String>) -> Self {
        Self {
            uri,
            mime_type: None,
            data: McpServerResourceData::Text(text.into()),
        }
    }

    /// Creates binary resource contents.
    #[must_use]
    pub fn blob(uri: Url, blob: impl Into<Vec<u8>>) -> Self {
        Self {
            uri,
            mime_type: None,
            data: McpServerResourceData::Blob(blob.into()),
        }
    }

    /// Adds an optional declared MIME type.
    ///
    /// # Errors
    ///
    /// Returns [`McpContextValueError::InvalidMimeType`] for an unsafe MIME type.
    pub fn with_mime_type(
        mut self,
        mime_type: impl Into<String>,
    ) -> Result<Self, McpContextValueError> {
        self.mime_type = Some(valid_mime_type(mime_type.into())?);
        Ok(self)
    }

    #[must_use]
    pub const fn uri(&self) -> &Url {
        &self.uri
    }

    pub(crate) fn wire(&self) -> Value {
        let mut value = serde_json::Map::new();
        value.insert("uri".to_owned(), Value::String(self.uri.to_string()));
        optional_string(&mut value, "mimeType", self.mime_type.as_ref());
        match &self.data {
            McpServerResourceData::Text(text) => {
                value.insert("text".to_owned(), Value::String(text.clone()));
            }
            McpServerResourceData::Blob(blob) => {
                value.insert("blob".to_owned(), Value::String(STANDARD.encode(blob)));
            }
        }
        Value::Object(value)
    }
}

impl fmt::Debug for McpServerResourceContents {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerResourceContents")
            .field("uri", &self.uri)
            .field("mime_type", &self.mime_type)
            .field("data", &self.data)
            .finish()
    }
}

/// Application-owned UTF-8 or binary resource body.
#[derive(Clone, Eq, PartialEq)]
pub enum McpServerResourceData {
    /// UTF-8 text.
    Text(String),
    /// Arbitrary binary data encoded as base64 on the wire.
    Blob(Vec<u8>),
}

impl fmt::Debug for McpServerResourceData {
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

/// Application-owned prompt declaration exposed through MCP `prompts/list`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpServerPrompt {
    name: String,
    title: Option<String>,
    description: Option<String>,
    arguments: Vec<McpServerPromptArgument>,
}

impl McpServerPrompt {
    /// Creates a prompt declaration.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsafe name or duplicate argument names.
    pub fn new(
        name: impl Into<String>,
        arguments: Vec<McpServerPromptArgument>,
    ) -> Result<Self, McpContextValueError> {
        let mut names = BTreeSet::new();
        if arguments
            .iter()
            .any(|argument| !names.insert(argument.name.clone()))
        {
            return Err(McpContextValueError::InvalidName);
        }
        Ok(Self {
            name: valid_name(name.into())?,
            title: None,
            description: None,
            arguments,
        })
    }

    /// Adds optional prompt title metadata.
    ///
    /// # Errors
    ///
    /// Returns [`McpContextValueError::InvalidMetadata`] for unsafe metadata.
    pub fn with_title(mut self, title: impl Into<String>) -> Result<Self, McpContextValueError> {
        self.title = Some(valid_metadata(title.into())?);
        Ok(self)
    }

    /// Adds optional prompt description metadata.
    ///
    /// # Errors
    ///
    /// Returns [`McpContextValueError::InvalidMetadata`] for unsafe metadata.
    pub fn with_description(
        mut self,
        description: impl Into<String>,
    ) -> Result<Self, McpContextValueError> {
        self.description = Some(valid_metadata(description.into())?);
        Ok(self)
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn wire(&self) -> Value {
        let mut value = serde_json::Map::new();
        value.insert("name".to_owned(), Value::String(self.name.clone()));
        optional_string(&mut value, "title", self.title.as_ref());
        optional_string(&mut value, "description", self.description.as_ref());
        value.insert(
            "arguments".to_owned(),
            Value::Array(
                self.arguments
                    .iter()
                    .map(McpServerPromptArgument::wire)
                    .collect(),
            ),
        );
        Value::Object(value)
    }
}

/// Application-owned prompt argument declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpServerPromptArgument {
    name: String,
    description: Option<String>,
    required: bool,
}

impl McpServerPromptArgument {
    /// Creates a prompt argument declaration.
    ///
    /// # Errors
    ///
    /// Returns [`McpContextValueError::InvalidName`] for an unsafe name.
    pub fn new(name: impl Into<String>, required: bool) -> Result<Self, McpContextValueError> {
        Ok(Self {
            name: valid_name(name.into())?,
            description: None,
            required,
        })
    }

    /// Adds optional prompt argument documentation.
    ///
    /// # Errors
    ///
    /// Returns [`McpContextValueError::InvalidMetadata`] for unsafe metadata.
    pub fn with_description(
        mut self,
        description: impl Into<String>,
    ) -> Result<Self, McpContextValueError> {
        self.description = Some(valid_metadata(description.into())?);
        Ok(self)
    }

    fn wire(&self) -> Value {
        let mut value = serde_json::Map::new();
        value.insert("name".to_owned(), Value::String(self.name.clone()));
        optional_string(&mut value, "description", self.description.as_ref());
        value.insert("required".to_owned(), Value::Bool(self.required));
        Value::Object(value)
    }
}

/// Application-owned result returned from an explicit MCP `prompts/get` request.
#[derive(Clone, Eq, PartialEq)]
pub struct McpServerPromptResult {
    description: Option<String>,
    messages: Vec<McpServerPromptMessage>,
}

impl McpServerPromptResult {
    /// Creates prompt messages in their application-selected order.
    #[must_use]
    pub fn new(messages: Vec<McpServerPromptMessage>) -> Self {
        Self {
            description: None,
            messages,
        }
    }

    /// Adds optional prompt result documentation.
    ///
    /// # Errors
    ///
    /// Returns [`McpContextValueError::InvalidMetadata`] for unsafe metadata.
    pub fn with_description(
        mut self,
        description: impl Into<String>,
    ) -> Result<Self, McpContextValueError> {
        self.description = Some(valid_metadata(description.into())?);
        Ok(self)
    }

    pub(crate) fn wire(&self) -> Value {
        let mut value = serde_json::Map::new();
        optional_string(&mut value, "description", self.description.as_ref());
        value.insert(
            "messages".to_owned(),
            Value::Array(
                self.messages
                    .iter()
                    .map(McpServerPromptMessage::wire)
                    .collect(),
            ),
        );
        Value::Object(value)
    }

    pub(crate) fn message_count(&self) -> usize {
        self.messages.len()
    }
}

impl fmt::Debug for McpServerPromptResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerPromptResult")
            .field(
                "description_length",
                &self.description.as_ref().map(String::len),
            )
            .field("message_count", &self.messages.len())
            .finish()
    }
}

/// Application-owned message supplied through an MCP prompt result.
#[derive(Clone, Eq, PartialEq)]
pub struct McpServerPromptMessage {
    role: McpServerPromptRole,
    content: McpServerPromptContent,
}

impl McpServerPromptMessage {
    /// Creates a user prompt message.
    #[must_use]
    pub const fn user(content: McpServerPromptContent) -> Self {
        Self {
            role: McpServerPromptRole::User,
            content,
        }
    }

    /// Creates an assistant prompt message.
    #[must_use]
    pub const fn assistant(content: McpServerPromptContent) -> Self {
        Self {
            role: McpServerPromptRole::Assistant,
            content,
        }
    }

    fn wire(&self) -> Value {
        json!({"role":self.role.wire(),"content":self.content.wire()})
    }
}

impl fmt::Debug for McpServerPromptMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerPromptMessage")
            .field("role", &self.role)
            .field("content", &self.content)
            .finish()
    }
}

/// MCP prompt message role.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpServerPromptRole {
    /// User role.
    User,
    /// Assistant role.
    Assistant,
}

impl McpServerPromptRole {
    const fn wire(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
        }
    }
}

/// Application-owned content block in an MCP prompt result.
#[derive(Clone, Eq, PartialEq)]
pub enum McpServerPromptContent {
    /// UTF-8 text content.
    Text(String),
    /// Binary image content with an explicit MIME type.
    Image { data: Vec<u8>, mime_type: String },
    /// Binary audio content with an explicit MIME type.
    Audio { data: Vec<u8>, mime_type: String },
    /// Embedded resource content.
    Resource(McpServerResourceContents),
    /// Resource metadata that clients must not fetch automatically.
    ResourceLink(McpServerResource),
}

impl McpServerPromptContent {
    /// Creates image content.
    ///
    /// # Errors
    ///
    /// Returns [`McpContextValueError::InvalidMimeType`] for an unsafe MIME type.
    pub fn image(
        data: impl Into<Vec<u8>>,
        mime_type: impl Into<String>,
    ) -> Result<Self, McpContextValueError> {
        Ok(Self::Image {
            data: data.into(),
            mime_type: valid_mime_type(mime_type.into())?,
        })
    }

    /// Creates audio content.
    ///
    /// # Errors
    ///
    /// Returns [`McpContextValueError::InvalidMimeType`] for an unsafe MIME type.
    pub fn audio(
        data: impl Into<Vec<u8>>,
        mime_type: impl Into<String>,
    ) -> Result<Self, McpContextValueError> {
        Ok(Self::Audio {
            data: data.into(),
            mime_type: valid_mime_type(mime_type.into())?,
        })
    }

    fn wire(&self) -> Value {
        match self {
            Self::Text(text) => json!({"type":"text","text":text}),
            Self::Image { data, mime_type } => {
                json!({"type":"image","data":STANDARD.encode(data),"mimeType":mime_type})
            }
            Self::Audio { data, mime_type } => {
                json!({"type":"audio","data":STANDARD.encode(data),"mimeType":mime_type})
            }
            Self::Resource(resource) => json!({"type":"resource","resource":resource.wire()}),
            Self::ResourceLink(resource) => {
                let mut value = resource.wire();
                value["type"] = Value::String("resource_link".to_owned());
                value
            }
        }
    }
}

impl fmt::Debug for McpServerPromptContent {
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
            Self::ResourceLink(resource) => formatter
                .debug_tuple("ResourceLink")
                .field(resource)
                .finish(),
        }
    }
}

fn valid_name(value: String) -> Result<String, McpContextValueError> {
    if value.trim().is_empty()
        || value.len() > MAX_NAME_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(McpContextValueError::InvalidName);
    }
    Ok(value)
}

fn valid_metadata(value: String) -> Result<String, McpContextValueError> {
    if value.len() > MAX_METADATA_BYTES || value.contains('\0') {
        return Err(McpContextValueError::InvalidMetadata);
    }
    Ok(value)
}

fn valid_mime_type(value: String) -> Result<String, McpContextValueError> {
    if value.is_empty()
        || value.len() > MAX_MIME_TYPE_BYTES
        || !value.bytes().all(|byte| matches!(byte, 0x21..=0x7e))
    {
        return Err(McpContextValueError::InvalidMimeType);
    }
    Ok(value)
}

fn optional_string(object: &mut serde_json::Map<String, Value>, key: &str, value: Option<&String>) {
    if let Some(value) = value {
        object.insert(key.to_owned(), Value::String(value.clone()));
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use url::Url;

    use super::{
        McpContextValueError, McpServerPromptContent, McpServerPromptMessage,
        McpServerPromptResult, McpServerResource, McpServerResourceContents,
    };

    #[test]
    fn rejects_unsafe_public_identifiers_and_mime_types() {
        let uri = Url::parse("resource://tenant-a/customer/7").unwrap();
        assert_eq!(
            McpServerResource::new(uri.clone(), " \t ").unwrap_err(),
            McpContextValueError::InvalidName
        );
        assert_eq!(
            McpServerResource::new(uri, "customer")
                .unwrap()
                .with_mime_type("application/json\n")
                .unwrap_err(),
            McpContextValueError::InvalidMimeType
        );
    }

    #[test]
    fn encodes_binary_and_embedded_context_without_debug_payloads() {
        let uri = Url::parse("resource://tenant-a/customer/7").unwrap();
        let content = McpServerResourceContents::blob(uri.clone(), b"top-secret".to_vec())
            .with_mime_type("application/octet-stream")
            .unwrap();
        assert!(!format!("{content:?}").contains("top-secret"));
        assert_eq!(
            content.wire(),
            json!({
                "uri":"resource://tenant-a/customer/7",
                "mimeType":"application/octet-stream",
                "blob":"dG9wLXNlY3JldA=="
            })
        );

        let result = McpServerPromptResult::new(vec![McpServerPromptMessage::user(
            McpServerPromptContent::Resource(content),
        )]);
        assert_eq!(
            result.wire(),
            json!({
                "messages":[{
                    "role":"user",
                    "content":{"type":"resource","resource":{
                        "uri":"resource://tenant-a/customer/7",
                        "mimeType":"application/octet-stream",
                        "blob":"dG9wLXNlY3JldA=="
                    }}
                }]
            })
        );
    }

    #[test]
    fn resource_link_keeps_metadata_as_a_link() {
        let resource = McpServerResource::new(
            Url::parse("resource://tenant-a/customer/7").unwrap(),
            "customer-profile",
        )
        .unwrap();
        let result = McpServerPromptResult::new(vec![McpServerPromptMessage::assistant(
            McpServerPromptContent::ResourceLink(resource),
        )]);
        assert_eq!(
            result.wire(),
            json!({
                "messages":[{
                    "role":"assistant",
                    "content":{
                        "uri":"resource://tenant-a/customer/7",
                        "name":"customer-profile",
                        "type":"resource_link"
                    }
                }]
            })
        );
    }
}
