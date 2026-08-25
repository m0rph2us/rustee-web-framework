//! Provider-visible tool declarations, calls, and validation metadata.

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Maximum UTF-8 byte length accepted for a provider-visible tool name.
pub const MAX_TOOL_NAME_BYTES: usize = 255;
/// Maximum UTF-8 byte length accepted for a provider tool-call identifier.
pub const MAX_TOOL_CALL_ID_BYTES: usize = 255;

/// Invalid tool definition or call metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ToolError {
    /// Name was blank or had unsupported characters.
    #[error("AI tool name must use ASCII letters, digits, underscore, hyphen, or dot")]
    InvalidName,
    /// Name exceeded the durable tool-metadata limit.
    #[error("AI tool name exceeded the supported length")]
    ToolNameTooLong,
    /// Provider call ID was blank.
    #[error("AI tool call ID must not be blank")]
    BlankCallId,
    /// Provider call ID was longer than durable tool metadata supports.
    #[error("AI tool call ID exceeded the supported length")]
    ToolCallIdTooLong,
    /// Provider call ID contained a NUL byte.
    #[error("AI tool call ID must not contain a NUL byte")]
    ToolCallIdContainsNul,
}

/// Provider-visible tool declaration. The schema is an application validation input, not permission.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct ToolDefinition {
    name: String,
    input_schema: Value,
}

#[derive(Deserialize)]
struct SerializedToolDefinition {
    name: String,
    input_schema: Value,
}

impl<'de> Deserialize<'de> for ToolDefinition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serialized = SerializedToolDefinition::deserialize(deserializer)?;
        Self::new(serialized.name, serialized.input_schema).map_err(serde::de::Error::custom)
    }
}

impl ToolDefinition {
    /// Creates a tool declaration with a stable ASCII name.
    ///
    /// Names may contain ASCII letters, digits, underscore, hyphen, and dot so application tools
    /// can preserve explicitly registered remote-tool namespaces.
    ///
    /// # Errors
    ///
    /// Returns [`ToolError::InvalidName`] or [`ToolError::ToolNameTooLong`] when `name` is not
    /// a portable bounded identifier.
    pub fn new(name: impl Into<String>, input_schema: Value) -> Result<Self, ToolError> {
        let name = name.into();
        validate_tool_name(&name)?;
        Ok(Self { name, input_schema })
    }

    /// Returns the stable tool name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns JSON schema for application-side argument validation.
    #[must_use]
    pub const fn input_schema(&self) -> &Value {
        &self.input_schema
    }
}

impl fmt::Debug for ToolDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolDefinition")
            .field("name", &self.name)
            .field("input_schema_kind", &json_kind(&self.input_schema))
            .field(
                "input_schema_member_count",
                &json_member_count(&self.input_schema),
            )
            .finish()
    }
}

/// A model-requested tool call that still requires application approval.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct ToolCall {
    id: String,
    name: String,
    arguments: Value,
}

#[derive(Deserialize)]
struct SerializedToolCall {
    id: String,
    name: String,
    arguments: Value,
}

impl<'de> Deserialize<'de> for ToolCall {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serialized = SerializedToolCall::deserialize(deserializer)?;
        Self::new(serialized.id, serialized.name, serialized.arguments)
            .map_err(serde::de::Error::custom)
    }
}

impl fmt::Debug for ToolCall {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolCall")
            .field("id", &"[REDACTED]")
            .field("name", &self.name)
            .field("arguments_kind", &json_kind(&self.arguments))
            .field(
                "arguments_member_count",
                &json_member_count(&self.arguments),
            )
            .finish()
    }
}

impl ToolCall {
    /// Creates a tool call with validated call and tool names.
    ///
    /// # Errors
    ///
    /// Returns [`ToolError`] when the ID or name is invalid or exceeds the shared durable
    /// metadata limits.
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        arguments: Value,
    ) -> Result<Self, ToolError> {
        let id = id.into();
        if id.trim().is_empty() {
            return Err(ToolError::BlankCallId);
        }
        if id.len() > MAX_TOOL_CALL_ID_BYTES {
            return Err(ToolError::ToolCallIdTooLong);
        }
        if id.contains('\0') {
            return Err(ToolError::ToolCallIdContainsNul);
        }
        let name = name.into();
        validate_tool_name(&name)?;
        Ok(Self {
            id,
            name,
            arguments,
        })
    }

    /// Returns provider call ID.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns requested tool name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns untrusted tool arguments.
    #[must_use]
    pub const fn arguments(&self) -> &Value {
        &self.arguments
    }
}

/// Side-effect classification declared for one application tool.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolRisk {
    /// The tool is intended to read already-authorized data only.
    ReadOnly,
    /// The tool requires an explicit user confirmation before execution.
    RequiresConfirmation,
    /// The tool can make a privileged or consequential change.
    Privileged,
}

fn json_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn json_member_count(value: &Value) -> Option<usize> {
    match value {
        Value::Array(values) => Some(values.len()),
        Value::Object(values) => Some(values.len()),
        _ => None,
    }
}

fn validate_tool_name(name: &str) -> Result<(), ToolError> {
    if name.len() > MAX_TOOL_NAME_BYTES {
        return Err(ToolError::ToolNameTooLong);
    }
    if name.is_empty()
        || !name
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, '_' | '-' | '.'))
    {
        return Err(ToolError::InvalidName);
    }
    Ok(())
}
