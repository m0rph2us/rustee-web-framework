//! Application-owned MCP prompt models, wire encoding, and diagnostics.

use std::{collections::BTreeSet, fmt};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

use super::{
    ContextWireBudget, McpContextValueError, McpServerResource, McpServerResourceContents,
    optional_string, valid_metadata, valid_mime_type, valid_name,
};

/// Application-owned prompt declaration exposed through MCP `prompts/list`.
#[derive(Clone, Eq, PartialEq)]
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

    /// Returns the application-owned prompt name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn wire(&self, budget: &mut ContextWireBudget) -> Option<Value> {
        budget.reserve_text(&self.name)?;
        budget.reserve_optional_text(self.title.as_deref())?;
        budget.reserve_optional_text(self.description.as_deref())?;
        let mut value = serde_json::Map::new();
        value.insert("name".to_owned(), Value::String(self.name.clone()));
        optional_string(&mut value, "title", self.title.as_ref());
        optional_string(&mut value, "description", self.description.as_ref());
        value.insert(
            "arguments".to_owned(),
            Value::Array(
                self.arguments
                    .iter()
                    .map(|argument| argument.wire(budget))
                    .collect::<Option<Vec<_>>>()?,
            ),
        );
        Some(Value::Object(value))
    }
}

impl fmt::Debug for McpServerPrompt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerPrompt")
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

/// Application-owned prompt argument declaration.
#[derive(Clone, Eq, PartialEq)]
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

    fn wire(&self, budget: &mut ContextWireBudget) -> Option<Value> {
        budget.reserve_text(&self.name)?;
        budget.reserve_optional_text(self.description.as_deref())?;
        let mut value = serde_json::Map::new();
        value.insert("name".to_owned(), Value::String(self.name.clone()));
        optional_string(&mut value, "description", self.description.as_ref());
        value.insert("required".to_owned(), Value::Bool(self.required));
        Some(Value::Object(value))
    }
}

impl fmt::Debug for McpServerPromptArgument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerPromptArgument")
            .field("name_length", &self.name.len())
            .field(
                "description_length",
                &self.description.as_ref().map(String::len),
            )
            .field("required", &self.required)
            .finish()
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

    pub(crate) fn wire(&self, budget: &mut ContextWireBudget) -> Option<Value> {
        budget.reserve_optional_text(self.description.as_deref())?;
        let mut value = serde_json::Map::new();
        optional_string(&mut value, "description", self.description.as_ref());
        value.insert(
            "messages".to_owned(),
            Value::Array(
                self.messages
                    .iter()
                    .map(|message| message.wire(budget))
                    .collect::<Option<Vec<_>>>()?,
            ),
        );
        Some(Value::Object(value))
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

    fn wire(&self, budget: &mut ContextWireBudget) -> Option<Value> {
        Some(json!({"role":self.role.wire(),"content":self.content.wire(budget)?}))
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
    Image {
        /// Raw image bytes.
        data: Vec<u8>,
        /// Validated image media type.
        mime_type: String,
    },
    /// Binary audio content with an explicit MIME type.
    Audio {
        /// Raw audio bytes.
        data: Vec<u8>,
        /// Validated audio media type.
        mime_type: String,
    },
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

    fn wire(&self, budget: &mut ContextWireBudget) -> Option<Value> {
        match self {
            Self::Text(text) => {
                budget.reserve_text(text)?;
                Some(json!({"type":"text","text":text}))
            }
            Self::Image { data, mime_type } => {
                budget.reserve_base64(data)?;
                budget.reserve_text(mime_type)?;
                Some(json!({"type":"image","data":STANDARD.encode(data),"mimeType":mime_type}))
            }
            Self::Audio { data, mime_type } => {
                budget.reserve_base64(data)?;
                budget.reserve_text(mime_type)?;
                Some(json!({"type":"audio","data":STANDARD.encode(data),"mimeType":mime_type}))
            }
            Self::Resource(resource) => {
                Some(json!({"type":"resource","resource":resource.wire(budget)?}))
            }
            Self::ResourceLink(resource) => {
                let mut value = resource.wire(budget)?;
                value["type"] = Value::String("resource_link".to_owned());
                Some(value)
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
