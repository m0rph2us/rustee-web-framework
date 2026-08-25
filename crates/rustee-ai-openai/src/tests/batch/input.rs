//! Batch input construction and typed request mapping regression coverage.

use super::super::*;

#[test]
fn batch_input_constructor_and_file_decoder_are_bounded_and_redacted() {
    let input = OpenAiBatchInputJsonl::new(b"private prompt\n".to_vec()).unwrap();
    assert_eq!(input.len(), 15);
    assert!(!input.is_empty());
    assert!(!format!("{input:?}").contains("private prompt"));
    assert_eq!(
        OpenAiBatchInputJsonl::new(Vec::new()).unwrap_err(),
        OpenAiBatchInputError::Empty
    );
    assert!(
        decode_batch_input_file(&json!({
            "id":"file_input_1",
            "purpose":"assistants"
        }))
        .is_err()
    );
}

#[test]
fn batch_input_builder_serializes_only_a_validated_generic_envelope() {
    let row = OpenAiBatchInputRow::new(
        "row_1",
        OpenAiBatchEndpoint::Responses,
        json!({"model":"gpt-test","input":"private prompt"}),
    )
    .unwrap();
    assert!(!format!("{row:?}").contains("private prompt"));
    let mut builder = OpenAiBatchJsonlBuilder::new(OpenAiBatchEndpoint::Responses);
    builder.push(row).unwrap();
    assert_eq!(
        builder
            .push(
                OpenAiBatchInputRow::new(
                    "row_1",
                    OpenAiBatchEndpoint::Responses,
                    json!({"model":"gpt-test"}),
                )
                .unwrap(),
            )
            .unwrap_err(),
        OpenAiBatchInputError::DuplicateCustomId
    );
    assert!(
        builder
            .push(
                OpenAiBatchInputRow::new(
                    "row_2",
                    OpenAiBatchEndpoint::Embeddings,
                    json!({"model":"embedding-test"}),
                )
                .unwrap(),
            )
            .is_err()
    );
    let input = builder.build().unwrap();
    assert!(!format!("{input:?}").contains("private prompt"));
    let envelope: Value = serde_json::from_slice(input.bytes()).unwrap();
    assert_eq!(envelope["custom_id"], "row_1");
    assert_eq!(envelope["method"], "POST");
    assert_eq!(envelope["url"], "/v1/responses");
    assert_eq!(envelope["body"]["input"], "private prompt");
    assert!(matches!(
        OpenAiBatchInputRow::new("bad id", OpenAiBatchEndpoint::Responses, json!({})),
        Err(OpenAiBatchInputError::UnsafeCustomId)
    ));
    assert!(matches!(
        OpenAiBatchInputRow::new("row_3", OpenAiBatchEndpoint::Responses, json!(null)),
        Err(OpenAiBatchInputError::BodyMustBeObject)
    ));
}

#[test]
fn responses_batch_row_reuses_the_typed_chat_request_mapping() {
    let row = OpenAiBatchInputRow::from_chat_request("row_typed_1", &request()).unwrap();
    assert!(!format!("{row:?}").contains("what is the status"));
    let mut builder = OpenAiBatchJsonlBuilder::new(OpenAiBatchEndpoint::Responses);
    builder.push(row).unwrap();
    let input = builder.build().unwrap();
    let envelope: Value = serde_json::from_slice(input.bytes()).unwrap();

    assert_eq!(envelope["url"], "/v1/responses");
    assert_eq!(envelope["body"]["model"], "gpt-test");
    assert_eq!(envelope["body"]["input"][0]["type"], "message");
    assert_eq!(envelope["body"]["tools"][0]["name"], "lookup_order");

    let unsupported = ChatRequest::new(
        "gpt-test",
        [ChatMessage::new(MessageRole::Tool, "private tool message").unwrap()],
    )
    .unwrap();
    assert!(matches!(
        OpenAiBatchInputRow::from_chat_request("row_typed_2", &unsupported),
        Err(OpenAiBatchResponsesRowError::Request {
            source: OpenAiError::UnsupportedToolMessage,
        })
    ));
}

#[test]
fn typed_batch_row_errors_have_category_only_debug_output() {
    let responses = OpenAiBatchResponsesRowError::Request {
        source: OpenAiError::UnsupportedToolMessage,
    };
    assert_eq!(
        format!("{responses:?}"),
        "OpenAiBatchResponsesRowError { kind: \"request\" }"
    );

    let embeddings = OpenAiBatchEmbeddingsRowError::Input {
        source: OpenAiBatchInputError::UnsafeCustomId,
    };
    assert_eq!(
        format!("{embeddings:?}"),
        "OpenAiBatchEmbeddingsRowError { kind: \"input\" }"
    );
}

#[test]
fn embeddings_batch_row_preserves_typed_input_order() {
    let row = OpenAiBatchInputRow::from_embedding_inputs(
        "row_embedding_1",
        "text-embedding-3-small",
        &[
            EmbeddingInput::new("chunk-1", "first private text").unwrap(),
            EmbeddingInput::new("chunk-2", "second private text").unwrap(),
        ],
    )
    .unwrap();
    assert!(!format!("{row:?}").contains("first private text"));
    let mut builder = OpenAiBatchJsonlBuilder::new(OpenAiBatchEndpoint::Embeddings);
    builder.push(row).unwrap();
    let input = builder.build().unwrap();
    let envelope: Value = serde_json::from_slice(input.bytes()).unwrap();

    assert_eq!(envelope["url"], "/v1/embeddings");
    assert_eq!(envelope["body"]["model"], "text-embedding-3-small");
    assert_eq!(
        envelope["body"]["input"],
        json!(["first private text", "second private text"])
    );
    assert!(matches!(
        OpenAiBatchInputRow::from_embedding_inputs(
            "row_embedding_2",
            "bad model",
            &[EmbeddingInput::new("chunk-1", "text").unwrap()],
        ),
        Err(OpenAiBatchEmbeddingsRowError::UnsafeModel)
    ));
    assert!(matches!(
        OpenAiBatchInputRow::from_embedding_inputs(
            "row_embedding_3",
            "text-embedding-3-small",
            &[],
        ),
        Err(OpenAiBatchEmbeddingsRowError::EmptyInputs)
    ));
}
