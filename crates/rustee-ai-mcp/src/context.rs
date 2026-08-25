use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::Value;
use url::Url;

use crate::McpError;

pub(crate) const MAX_CONTEXT_NAME_BYTES: usize = 128;
const MAX_CONTEXT_URI_BYTES: usize = 4096;
const MAX_METADATA_BYTES: usize = 8192;
const MAX_MIME_TYPE_BYTES: usize = 256;

mod prompt;
mod resource;

pub use prompt::{
    McpPrompt, McpPromptArgument, McpPromptContent, McpPromptMessage, McpPromptResult,
    McpPromptRole,
};
pub use resource::{
    McpResource, McpResourceContents, McpResourceData, McpResourceLink, McpResourceTemplate,
};

pub(crate) use prompt::{parse_prompt, parse_prompt_result};
pub(crate) use resource::{parse_resource, parse_resource_contents, parse_resource_template};

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

pub(crate) fn valid_context_name(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_CONTEXT_NAME_BYTES
        && value.chars().all(|character| !character.is_control())
}

pub(crate) fn valid_context_request_string(value: &str, max_content_bytes: usize) -> bool {
    value.len() <= max_content_bytes && !value.contains('\0')
}

pub(super) fn parse_uri(value: &str) -> Result<Url, McpError> {
    if value.len() > MAX_CONTEXT_URI_BYTES || value.contains('\0') {
        return Err(McpError::MalformedResponse);
    }
    Url::parse(value).map_err(|_| McpError::MalformedResponse)
}

pub(super) fn parse_name(value: &str) -> Result<String, McpError> {
    valid_context_name(value)
        .then(|| value.to_owned())
        .ok_or(McpError::MalformedResponse)
}

pub(super) fn required_string<'a>(value: &'a Value, key: &str) -> Result<&'a str, McpError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or(McpError::MalformedResponse)
}

pub(super) fn optional_metadata(value: &Value, key: &str) -> Result<Option<String>, McpError> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if valid_metadata(value) => Ok(Some(value.clone())),
        Some(_) => Err(McpError::MalformedResponse),
    }
}

pub(super) fn optional_mime_type(value: &Value) -> Result<Option<String>, McpError> {
    match value.get("mimeType") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if valid_mime_type(value) => Ok(Some(value.clone())),
        Some(_) => Err(McpError::MalformedResponse),
    }
}

pub(super) fn optional_size(value: &Value) -> Result<Option<u64>, McpError> {
    match value.get("size") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(size)) => size.as_u64().ok_or(McpError::MalformedResponse).map(Some),
        Some(_) => Err(McpError::MalformedResponse),
    }
}

pub(super) fn decode_binary(value: &str, max_content_bytes: usize) -> Result<Vec<u8>, McpError> {
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

pub(super) fn valid_uri_template(value: &str) -> bool {
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
        McpPromptContent, McpPromptRole, McpResourceData, parse_prompt, parse_prompt_result,
        parse_resource, parse_resource_contents, parse_resource_template, valid_context_name,
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
        let message_debug = format!("{:?}", prompt.messages()[0]);
        assert!(message_debug.contains("McpPromptMessage"));
        assert!(message_debug.contains("role: User"));
        assert!(message_debug.contains("byte_length: 5"));
        assert!(!message_debug.contains("hello"));
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

    #[test]
    fn context_names_require_visible_content() {
        assert!(valid_context_name("customer-summary"));
        assert!(!valid_context_name(""));
        assert!(!valid_context_name("   "));
        assert!(!valid_context_name("\u{2003}"));
    }

    #[test]
    fn resource_debug_does_not_expose_remote_uri_capability_data() {
        const SENSITIVE_URI: &str =
            "https://alice:top-secret@example.test/customer?access_token=capability#fragment";
        const SENSITIVE_NAME: &str = "customer-secret";

        let resource = parse_resource(&json!({"uri":SENSITIVE_URI,"name":SENSITIVE_NAME})).unwrap();
        let contents =
            parse_resource_contents(&json!({"uri":SENSITIVE_URI,"text":"context"}), 16).unwrap();
        let template = parse_resource_template(&json!({
            "uriTemplate":"resource://tenant-a/{customer-secret}",
            "name":SENSITIVE_NAME
        }))
        .unwrap();
        let prompt_definition = parse_prompt(
            &json!({
                "name":SENSITIVE_NAME,
                "arguments":[{"name":SENSITIVE_NAME,"required":true}]
            }),
            1,
        )
        .unwrap();
        let prompt = parse_prompt_result(
            &json!({"messages":[{"role":"user","content":{"type":"resource_link","uri":SENSITIVE_URI,"name":SENSITIVE_NAME}}]}),
            1,
            16,
        )
        .unwrap();
        let debug = format!("{resource:?}{contents:?}{template:?}{prompt_definition:?}{prompt:?}");

        assert!(!debug.contains(SENSITIVE_URI));
        assert!(!debug.contains("top-secret"));
        assert!(!debug.contains("access_token"));
        assert!(!debug.contains(SENSITIVE_NAME));
        assert!(debug.contains("uri_length"));
        assert!(debug.contains("has_query: true"));
        assert!(debug.contains("has_fragment: true"));
        assert!(debug.contains("name_length"));
    }
}
