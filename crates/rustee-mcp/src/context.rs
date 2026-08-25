use serde_json::Value;

const MAX_CONTEXT_NAME_BYTES: usize = 128;
pub(super) const MAX_RESOURCE_URI_BYTES: usize = 4096;
const MAX_METADATA_BYTES: usize = 8192;
const MAX_MIME_TYPE_BYTES: usize = 256;

mod budget;
mod prompt;
mod provider;
mod resource;

pub(crate) use budget::ContextWireBudget;
pub use prompt::{
    McpServerPrompt, McpServerPromptArgument, McpServerPromptContent, McpServerPromptMessage,
    McpServerPromptResult, McpServerPromptRole,
};
pub use provider::{
    DenyAllMcpContextProvider, DenyAllMcpContextProviderError, McpContextCapabilities,
    McpContextProvider,
};
pub use resource::{
    McpServerResource, McpServerResourceContents, McpServerResourceData, McpServerResourceTemplate,
};

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

pub(super) fn valid_name(value: String) -> Result<String, McpContextValueError> {
    if !is_valid_context_name(&value) {
        return Err(McpContextValueError::InvalidName);
    }
    Ok(value)
}

pub(super) fn is_valid_context_name(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_CONTEXT_NAME_BYTES
        && !value.chars().any(char::is_control)
}

pub(super) fn is_valid_resource_uri(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_RESOURCE_URI_BYTES
        && !value.chars().any(char::is_control)
}

pub(super) fn valid_metadata(value: String) -> Result<String, McpContextValueError> {
    if value.len() > MAX_METADATA_BYTES || value.contains('\0') {
        return Err(McpContextValueError::InvalidMetadata);
    }
    Ok(value)
}

pub(super) fn valid_mime_type(value: String) -> Result<String, McpContextValueError> {
    if value.is_empty()
        || value.len() > MAX_MIME_TYPE_BYTES
        || !value.bytes().all(|byte| matches!(byte, 0x21..=0x7e))
    {
        return Err(McpContextValueError::InvalidMimeType);
    }
    Ok(value)
}

pub(super) fn optional_string(
    object: &mut serde_json::Map<String, Value>,
    key: &str,
    value: Option<&String>,
) {
    if let Some(value) = value {
        object.insert(key.to_owned(), Value::String(value.clone()));
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use url::Url;

    use super::{
        ContextWireBudget, McpContextValueError, McpServerPrompt, McpServerPromptArgument,
        McpServerPromptContent, McpServerPromptMessage, McpServerPromptResult, McpServerResource,
        McpServerResourceContents, McpServerResourceTemplate,
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
        assert_eq!(
            McpServerResourceTemplate::new("x".repeat(4097), "customer").unwrap_err(),
            McpContextValueError::InvalidUriTemplate
        );
    }

    #[test]
    fn wire_budget_rejects_binary_content_before_base64_materialization() {
        let content = McpServerResourceContents::blob(
            Url::parse("resource://tenant-a/customer/7").unwrap(),
            vec![7_u8; 32],
        );
        let mut budget = ContextWireBudget::new(32);

        assert!(content.wire(&mut budget).is_none());
    }

    #[test]
    fn encodes_binary_and_embedded_context_without_debug_payloads() {
        let uri = Url::parse("resource://tenant-a/customer/7").unwrap();
        let content = McpServerResourceContents::blob(uri.clone(), b"top-secret".to_vec())
            .with_mime_type("application/octet-stream")
            .unwrap();
        assert!(!format!("{content:?}").contains("top-secret"));
        let mut budget = ContextWireBudget::new(1024);
        assert_eq!(
            content.wire(&mut budget).unwrap(),
            json!({
                "uri":"resource://tenant-a/customer/7",
                "mimeType":"application/octet-stream",
                "blob":"dG9wLXNlY3JldA=="
            })
        );

        let result = McpServerPromptResult::new(vec![McpServerPromptMessage::user(
            McpServerPromptContent::Resource(content),
        )]);
        let mut budget = ContextWireBudget::new(1024);
        assert_eq!(
            result.wire(&mut budget).unwrap(),
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
        let mut budget = ContextWireBudget::new(1024);
        assert_eq!(
            result.wire(&mut budget).unwrap(),
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

    #[test]
    fn context_debug_is_content_free_for_application_owned_values() {
        const SENSITIVE_URI: &str =
            "https://alice:top-secret@example.test/customer?access_token=capability#fragment";

        let resource =
            McpServerResource::new(Url::parse(SENSITIVE_URI).unwrap(), "customer-secret")
                .unwrap()
                .with_title("private customer")
                .unwrap()
                .with_description("private customer metadata")
                .unwrap();
        let contents =
            McpServerResourceContents::text(Url::parse(SENSITIVE_URI).unwrap(), "secret");
        let template = McpServerResourceTemplate::new(
            "resource://customer/{customer-secret}?access_token=capability",
            "customer-secret",
        )
        .unwrap()
        .with_description("private template metadata")
        .unwrap();
        let argument = McpServerPromptArgument::new("customer-secret", true)
            .unwrap()
            .with_description("private argument metadata")
            .unwrap();
        let prompt = McpServerPrompt::new("customer-secret", vec![argument.clone()])
            .unwrap()
            .with_description("private prompt metadata")
            .unwrap();
        let text_message = McpServerPromptMessage::user(McpServerPromptContent::Text(
            "private prompt body".to_owned(),
        ));
        let linked_resource_message = McpServerPromptMessage::assistant(
            McpServerPromptContent::ResourceLink(resource.clone()),
        );
        let prompt_result =
            McpServerPromptResult::new(vec![text_message.clone(), linked_resource_message.clone()]);
        let debug = format!(
            "{resource:?}{contents:?}{template:?}{argument:?}{prompt:?}{text_message:?}{linked_resource_message:?}{prompt_result:?}"
        );

        assert!(!debug.contains(SENSITIVE_URI));
        assert!(!debug.contains("top-secret"));
        assert!(!debug.contains("access_token"));
        assert!(!debug.contains("customer-secret"));
        assert!(!debug.contains("private"));
        assert!(debug.contains("uri_length"));
        assert!(debug.contains("uri_template_length"));
        assert!(debug.contains("name_length"));
        assert!(debug.contains("McpServerPromptMessage"));
        assert!(debug.contains("byte_length"));
    }
}
