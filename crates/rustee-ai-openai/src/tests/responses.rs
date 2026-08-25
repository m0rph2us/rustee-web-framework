//! `OpenAI` Responses request and non-streaming response regression coverage.

use super::*;

#[test]
fn request_mapping_uses_responses_input_and_function_tool_shape() {
    assert_eq!(
        request_body(&request(), OPENAI_BATCH_FILE_MAX_BYTES).unwrap(),
        json!({
            "model":"gpt-test",
            "input":[{
                "type":"message",
                "role":"user",
                "content":[{"type":"input_text", "text":"what is the status?"}],
            }],
            "tools":[{
                "type":"function",
                "name":"lookup_order",
                "parameters":{"type":"object"},
            }],
        })
    );
}

#[test]
fn response_mapping_collects_text_tool_calls_and_usage() {
    let response = decode_response(&json!({
        "id":"resp_1",
        "model":"gpt-test-2026",
        "status":"completed",
        "output":[
            {"type":"message","content":[{"type":"output_text","text":"Checking. "}]},
            {"type":"function_call","call_id":"call_1","name":"lookup_order","arguments":r#"{"id":7}"#},
            {"type":"message","content":[{"type":"output_text","text":"Done."}]}
        ],
        "usage":{"input_tokens":10,"output_tokens":4}
    }))
    .unwrap();

    assert_eq!(response.content(), "Checking. Done.");
    assert_eq!(response.tool_calls()[0].id(), "call_1");
    assert_eq!(response.usage().total_tokens(), 14);
}

#[test]
fn request_mapping_sends_approved_tool_results_as_function_outputs() {
    let tool_result = serde_json::from_value(json!({
        "call_id":"call_1",
        "name":"lookup_order",
        "content":{"status":"found"}
    }))
    .unwrap();
    let body = request_body(
        &request().with_tool_results([tool_result]),
        OPENAI_BATCH_FILE_MAX_BYTES,
    )
    .unwrap();

    assert_eq!(
        body["input"][1],
        json!({
            "type":"function_call_output",
            "call_id":"call_1",
            "output":"{\"status\":\"found\"}"
        })
    );
}

#[test]
fn request_mapping_rejects_an_oversized_tool_result_before_string_conversion() {
    let tool_result = serde_json::from_value(json!({
        "call_id":"call_1",
        "name":"lookup_order",
        "content":{"status":"too large"}
    }))
    .unwrap();

    assert!(matches!(
        request_body(&request().with_tool_results([tool_result]), 8),
        Err(OpenAiError::RequestTooLarge)
    ));
}

#[test]
fn incomplete_responses_are_not_returned_as_successes() {
    assert!(matches!(
        decode_response(&json!({"status":"incomplete"})),
        Err(OpenAiError::IncompleteResponse)
    ));
}

#[tokio::test]
async fn provider_sends_a_responses_request_and_decodes_the_response() {
    let (url, captured_request, server) = response_server(
        "application/json",
        json!({
            "id":"resp_network",
            "model":"gpt-network",
            "output":[{"type":"message","content":[{"type":"output_text","text":"network ok"}]}],
            "usage":{"input_tokens":3,"output_tokens":2}
        })
        .to_string(),
    )
    .await;
    let provider = OpenAiResponsesProvider::new(
        OpenAiConfig::new("sk-contract")
            .unwrap()
            .with_base_url(url)
            .unwrap(),
    )
    .unwrap();

    let response = provider.complete(request()).await.unwrap();
    let sent = captured_request.await.unwrap();
    server.await.unwrap();

    assert!(sent.starts_with("POST /v1/responses HTTP/1.1\r\n"));
    assert!(sent.contains("authorization: Bearer sk-contract\r\n"));
    assert!(sent.contains("\"model\":\"gpt-test\""));
    assert_eq!(response.content(), "network ok");
}

#[tokio::test]
async fn responses_provider_rejects_a_declared_body_above_its_configured_limit() {
    let (url, server) = declared_length_response_server("application/json", 17).await;
    let provider = OpenAiResponsesProvider::new(
        OpenAiConfig::new("sk-contract")
            .unwrap()
            .with_base_url(url)
            .unwrap()
            .with_max_response_bytes(16)
            .unwrap(),
    )
    .unwrap();

    assert!(matches!(
        provider.complete(request()).await,
        Err(OpenAiError::ResponseTooLarge)
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn responses_provider_rejects_an_oversized_request_before_network_dispatch() {
    let provider = OpenAiResponsesProvider::new(
        OpenAiConfig::new("sk-contract")
            .unwrap()
            .with_max_request_bytes(1)
            .unwrap(),
    )
    .unwrap();

    assert!(matches!(
        provider.complete(request()).await,
        Err(OpenAiError::RequestTooLarge)
    ));
}

#[tokio::test]
async fn responses_provider_rejects_an_unexpected_json_response_content_type() {
    let (url, _captured_request, server) = response_server(
        "text/plain",
        json!({
            "id":"resp_1",
            "model":"gpt-test",
            "status":"completed",
            "output":[]
        })
        .to_string(),
    )
    .await;
    let provider = OpenAiResponsesProvider::new(
        OpenAiConfig::new("sk-contract")
            .unwrap()
            .with_base_url(url)
            .unwrap(),
    )
    .unwrap();

    assert!(matches!(
        provider.complete(request()).await,
        Err(OpenAiError::UnexpectedContentType)
    ));
    server.await.unwrap();
}
