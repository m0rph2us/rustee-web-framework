//! Untrusted MCP resource models, bounded decoding, and content-free diagnostics.

use std::fmt;

use serde_json::Value;
use url::Url;

use super::{
    decode_binary, optional_metadata, optional_mime_type, optional_size, parse_name, parse_uri,
    required_string, valid_uri_template,
};
use crate::McpError;

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
            .field("uri_length", &self.uri.as_str().len())
            .field("has_query", &self.uri.query().is_some())
            .field("has_fragment", &self.uri.fragment().is_some())
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

pub(super) fn parse_resource_link(value: &Value) -> Result<McpResourceLink, McpError> {
    Ok(McpResourceLink {
        uri: parse_uri(required_string(value, "uri")?)?,
        name: parse_name(required_string(value, "name")?)?,
        title: optional_metadata(value, "title")?,
        description: optional_metadata(value, "description")?,
        mime_type: optional_mime_type(value)?,
        size: optional_size(value)?,
    })
}
