//! Provider-neutral chat protocol and provider-visible tool declarations.

mod error;
mod message;
mod request;
mod response;
mod tool;

pub use error::{RequestError, ResponseError, StructuredOutputError};
pub use message::{ChatMessage, MessageRole};
pub use request::{ChatRequest, MAX_MODEL_ALIAS_BYTES, ModelAliasError, validate_model_alias};
pub use response::{ChatResponse, Usage};

pub use tool::{
    MAX_TOOL_CALL_ID_BYTES, MAX_TOOL_NAME_BYTES, ToolCall, ToolDefinition, ToolError, ToolRisk,
};

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use serde::Deserialize;
    use serde_json::json;

    use crate::ToolResult;

    use super::{
        ChatMessage, ChatRequest, ChatResponse, MAX_MODEL_ALIAS_BYTES, MessageRole, RequestError,
        ResponseError, ToolCall, ToolDefinition, Usage,
    };

    #[test]
    fn protocol_debug_output_redacts_tool_data_and_provider_metadata() {
        let definition = ToolDefinition::new(
            "orders.lookup",
            json!({"description":"private schema detail","properties":{"id":{"type":"string"}}}),
        )
        .unwrap();
        let call = ToolCall::new(
            "provider-call-secret",
            "orders.lookup",
            json!({"customer_note":"private tool argument"}),
        )
        .unwrap();
        let request = ChatRequest::new(
            "support.default",
            [ChatMessage::new(MessageRole::User, "private customer prompt").unwrap()],
        )
        .unwrap()
        .with_tools([definition.clone()]);
        let response = ChatResponse::new(
            "provider-response-secret",
            "support.default",
            "private provider completion",
            [call.clone()],
            Usage::default(),
        )
        .unwrap();

        let output = format!("{definition:?} {call:?} {request:?} {response:?}");
        let request_debug = format!("{request:?}");

        for value in [
            "private schema detail",
            "provider-call-secret",
            "private tool argument",
            "private customer prompt",
            "provider-response-secret",
            "private provider completion",
        ] {
            assert!(!output.contains(value));
        }
        assert!(output.contains("orders.lookup"));
        assert!(output.contains("arguments_kind: \"object\""));
        assert!(output.contains("tool_call_count: 1"));
        assert!(!request_debug.contains("orders.lookup"));
        assert!(request_debug.contains("tool_count: 1"));
    }

    #[test]
    fn deserialization_revalidates_protocol_and_tool_invariants() {
        assert!(
            serde_json::from_value::<ChatMessage>(json!({
                "role":"user",
                "content":" ",
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<ChatRequest>(json!({
                "model":" ",
                "messages":[],
                "tools":[],
                "tool_results":[],
            }))
            .is_err()
        );
        for model in [
            "m".repeat(MAX_MODEL_ALIAS_BYTES + 1),
            "model\0alias".to_owned(),
        ] {
            assert!(
                serde_json::from_value::<ChatRequest>(json!({
                    "model": model,
                    "messages":[{"role":"user","content":"request"}],
                    "tools":[],
                    "tool_results":[],
                }))
                .is_err()
            );
        }
        assert!(
            serde_json::from_value::<ToolDefinition>(json!({
                "name":"invalid tool",
                "input_schema":{"type":"object"},
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<ToolCall>(json!({
                "id":" ",
                "name":"orders.lookup",
                "arguments":{},
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<ToolResult>(json!({
                "call_id":"call-1",
                "name":"invalid tool",
                "content":{"status":"ignored"},
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<ChatResponse>(json!({
                "id":"",
                "model":"provider-model",
                "content":"response",
                "tool_calls":[],
                "usage":{"input_tokens":1,"output_tokens":1},
            }))
            .is_err()
        );
        for model in [
            "m".repeat(MAX_MODEL_ALIAS_BYTES + 1),
            "model\0alias".to_owned(),
        ] {
            assert!(
                serde_json::from_value::<ChatResponse>(json!({
                    "id":"response-1",
                    "model": model,
                    "content":"response",
                    "tool_calls":[],
                    "usage":{"input_tokens":1,"output_tokens":1},
                }))
                .is_err()
            );
        }

        let response = ChatResponse::new(
            "response-1",
            "provider-model",
            "response",
            [],
            Usage {
                input_tokens: 1,
                output_tokens: 1,
            },
        )
        .unwrap();
        let restored =
            serde_json::from_value::<ChatResponse>(serde_json::to_value(&response).unwrap())
                .unwrap();
        assert_eq!(restored, response);
    }

    #[test]
    fn model_alias_uses_the_shared_durable_limit_for_requests_and_responses() {
        let message = ChatMessage::new(MessageRole::User, "request").unwrap();
        assert!(ChatRequest::new("m".repeat(MAX_MODEL_ALIAS_BYTES), [message.clone()]).is_ok());
        assert_eq!(
            ChatRequest::new("m".repeat(MAX_MODEL_ALIAS_BYTES + 1), [message.clone()]).unwrap_err(),
            RequestError::ModelAliasTooLong
        );
        assert_eq!(
            ChatRequest::new("model\0alias", [message]).unwrap_err(),
            RequestError::ModelAliasContainsNul
        );
        assert!(
            ChatResponse::new(
                "response-1",
                "m".repeat(MAX_MODEL_ALIAS_BYTES),
                "response",
                [],
                Usage::default(),
            )
            .is_ok()
        );
        assert_eq!(
            ChatResponse::new(
                "response-1",
                "m".repeat(MAX_MODEL_ALIAS_BYTES + 1),
                "response",
                [],
                Usage::default(),
            )
            .unwrap_err(),
            ResponseError::ModelAliasTooLong
        );
        assert_eq!(
            ChatResponse::new(
                "response-1",
                "model\0alias",
                "response",
                [],
                Usage::default(),
            )
            .unwrap_err(),
            ResponseError::ModelAliasContainsNul
        );
    }

    #[derive(Deserialize)]
    struct NumberOutput {
        #[serde(rename = "answer")]
        _answer: u64,
    }

    #[test]
    fn structured_output_error_redacts_model_content_and_preserves_its_source() {
        let response = ChatResponse::new(
            "provider-response",
            "support.default",
            r#"{"answer":"private model output"}"#,
            [],
            Usage::default(),
        )
        .unwrap();

        let Err(error) = response.parse_json::<NumberOutput>() else {
            panic!("model output with a string answer must not parse as a number");
        };

        assert_eq!(error.to_string(), "AI structured output was invalid JSON");
        assert_eq!(format!("{error:?}"), "StructuredOutputError::Deserialize");
        assert!(!error.to_string().contains("private model output"));
        assert!(!format!("{error:?}").contains("private model output"));
        assert!(StdError::source(&error).is_some());
    }
}
