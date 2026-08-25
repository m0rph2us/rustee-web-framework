//! Application-owned MCP resource metadata, contents, bounded wire encoding, and diagnostics.

use std::fmt;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::Value;
use url::Url;

use super::{
    ContextWireBudget, McpContextValueError, is_valid_resource_uri, optional_string,
    valid_metadata, valid_mime_type, valid_name,
};

/// Application-owned resource metadata exposed through MCP `resources/list`.
#[derive(Clone, Eq, PartialEq)]
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

    /// Returns the application-owned resource URI.
    #[must_use]
    pub const fn uri(&self) -> &Url {
        &self.uri
    }

    /// Returns the application-owned resource name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn wire(&self, budget: &mut ContextWireBudget) -> Option<Value> {
        budget.reserve_text(self.uri.as_str())?;
        budget.reserve_text(&self.name)?;
        budget.reserve_optional_text(self.title.as_deref())?;
        budget.reserve_optional_text(self.description.as_deref())?;
        budget.reserve_optional_text(self.mime_type.as_deref())?;
        let mut value = serde_json::Map::new();
        value.insert(
            "uri".to_owned(),
            Value::String(self.uri.as_str().to_owned()),
        );
        value.insert("name".to_owned(), Value::String(self.name.clone()));
        optional_string(&mut value, "title", self.title.as_ref());
        optional_string(&mut value, "description", self.description.as_ref());
        optional_string(&mut value, "mimeType", self.mime_type.as_ref());
        if let Some(size) = self.size {
            value.insert("size".to_owned(), Value::from(size));
        }
        Some(Value::Object(value))
    }
}

impl fmt::Debug for McpServerResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerResource")
            .field("uri_length", &self.uri.as_str().len())
            .field("has_query", &self.uri.query().is_some())
            .field("has_fragment", &self.uri.fragment().is_some())
            .field("name_length", &self.name.len())
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

/// Application-owned parameterized resource metadata exposed through MCP templates/list.
#[derive(Clone, Eq, PartialEq)]
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
        if !is_valid_resource_uri(&uri_template) {
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

    /// Returns the application-owned resource template name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn wire(&self, budget: &mut ContextWireBudget) -> Option<Value> {
        budget.reserve_text(&self.uri_template)?;
        budget.reserve_text(&self.name)?;
        budget.reserve_optional_text(self.title.as_deref())?;
        budget.reserve_optional_text(self.description.as_deref())?;
        budget.reserve_optional_text(self.mime_type.as_deref())?;
        let mut value = serde_json::Map::new();
        value.insert(
            "uriTemplate".to_owned(),
            Value::String(self.uri_template.clone()),
        );
        value.insert("name".to_owned(), Value::String(self.name.clone()));
        optional_string(&mut value, "title", self.title.as_ref());
        optional_string(&mut value, "description", self.description.as_ref());
        optional_string(&mut value, "mimeType", self.mime_type.as_ref());
        Some(Value::Object(value))
    }
}

impl fmt::Debug for McpServerResourceTemplate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerResourceTemplate")
            .field("uri_template_length", &self.uri_template.len())
            .field("name_length", &self.name.len())
            .field("title_length", &self.title.as_ref().map(String::len))
            .field(
                "description_length",
                &self.description.as_ref().map(String::len),
            )
            .field("mime_type", &self.mime_type)
            .finish()
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

    /// Returns the resource URI associated with these contents.
    #[must_use]
    pub const fn uri(&self) -> &Url {
        &self.uri
    }

    pub(crate) fn wire(&self, budget: &mut ContextWireBudget) -> Option<Value> {
        budget.reserve_text(self.uri.as_str())?;
        budget.reserve_optional_text(self.mime_type.as_deref())?;
        match &self.data {
            McpServerResourceData::Text(text) => budget.reserve_text(text)?,
            McpServerResourceData::Blob(blob) => budget.reserve_base64(blob)?,
        }
        let mut value = serde_json::Map::new();
        value.insert(
            "uri".to_owned(),
            Value::String(self.uri.as_str().to_owned()),
        );
        optional_string(&mut value, "mimeType", self.mime_type.as_ref());
        match &self.data {
            McpServerResourceData::Text(text) => {
                value.insert("text".to_owned(), Value::String(text.clone()));
            }
            McpServerResourceData::Blob(blob) => {
                value.insert("blob".to_owned(), Value::String(STANDARD.encode(blob)));
            }
        }
        Some(Value::Object(value))
    }
}

impl fmt::Debug for McpServerResourceContents {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerResourceContents")
            .field("uri_length", &self.uri.as_str().len())
            .field("has_query", &self.uri.query().is_some())
            .field("has_fragment", &self.uri.fragment().is_some())
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
