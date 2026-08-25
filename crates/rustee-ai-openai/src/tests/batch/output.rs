//! Batch output parsing and typed response conversion regression coverage.

use super::super::*;

#[test]
fn batch_output_rows_keep_model_content_explicit_and_fail_closed() {
    let content = OpenAiBatchFileContent::from_bytes(
        concat!(
            "{\"custom_id\":\"row_1\",\"response\":{\"status_code\":200,\"request_id\":\"req_1\",\"body\":{\"output\":\"private completion\"}},\"error\":null}\n",
            "{\"custom_id\":\"row_2\",\"response\":null,\"error\":{\"code\":\"batch_cancelled\",\"message\":\"private provider message\"}}\n"
        )
        .as_bytes()
        .to_vec(),
    );
    let mut rows = content.output_rows_with_limit(2).unwrap();
    let success = rows.next().unwrap().unwrap();
    assert_eq!(success.custom_id(), "row_1");
    let response = match success.into_outcome() {
        OpenAiBatchRowOutcome::Response(response) => response,
        OpenAiBatchRowOutcome::Error(_) => panic!("expected successful provider row"),
    };
    assert_eq!(response.status_code(), 200);
    assert_eq!(response.request_id(), "req_1");
    let body = response.into_body();
    assert!(!format!("{body:?}").contains("private completion"));
    assert_eq!(body.into_json()["output"], "private completion");

    let failure = rows.next().unwrap().unwrap();
    let error = match failure.into_outcome() {
        OpenAiBatchRowOutcome::Response(_) => panic!("expected failed provider row"),
        OpenAiBatchRowOutcome::Error(error) => error,
    };
    assert_eq!(error.code(), "batch_cancelled");
    assert!(!format!("{error:?}").contains("private provider message"));
    assert!(rows.next().is_none());

    let malformed =
        OpenAiBatchFileContent::from_bytes(b"{not json}\n{\"custom_id\":\"row_3\"}\n".to_vec());
    let mut rows = malformed.output_rows();
    assert!(rows.next().unwrap().is_err());
    assert!(rows.next().is_none());
}

#[test]
fn responses_batch_body_decoding_is_explicit_and_reuses_response_validation() {
    let body = OpenAiBatchResponseBody::from_json(json!({
        "id":"resp_batch_1",
        "model":"gpt-batch",
        "output":[{"type":"message","content":[{"type":"output_text","text":"private completion"}]}],
        "usage":{"input_tokens":2,"output_tokens":1}
    }));
    assert!(!format!("{body:?}").contains("private completion"));
    let response = body.into_chat_response().unwrap();
    assert_eq!(response.content(), "private completion");
    assert_eq!(response.usage().total_tokens(), 3);

    let malformed = OpenAiBatchResponseBody::from_json(json!({"output": []}));
    assert!(matches!(
        malformed.into_chat_response(),
        Err(OpenAiError::MalformedResponse)
    ));
}

#[test]
fn embeddings_batch_body_decoding_is_explicit_and_preserves_input_order() {
    let body = OpenAiBatchResponseBody::from_json(json!({
        "data":[
            {"index":1,"embedding":[2.0,-1.0]},
            {"index":0,"embedding":[0.25,0.5]}
        ]
    }));
    assert!(!format!("{body:?}").contains("0.25"));
    let embeddings = body.into_embeddings(2).unwrap();
    assert_eq!(embeddings[0].values(), &[0.25, 0.5]);
    assert_eq!(embeddings[1].values(), &[2.0, -1.0]);

    let malformed =
        OpenAiBatchResponseBody::from_json(json!({"data":[{"index":0,"embedding":[0.25]}]}));
    assert!(matches!(
        malformed.into_embeddings(2),
        Err(OpenAiError::MalformedEmbeddingResponse)
    ));
}

#[test]
fn batch_output_rows_enforce_an_application_row_bound() {
    let content = OpenAiBatchFileContent::from_bytes(
        concat!(
            "{\"custom_id\":\"row_1\",\"response\":null,\"error\":{\"code\":\"batch_expired\"}}\n",
            "{\"custom_id\":\"row_2\",\"response\":null,\"error\":{\"code\":\"batch_expired\"}}\n"
        )
        .as_bytes()
        .to_vec(),
    );
    assert!(content.output_rows_with_limit(0).is_err());
    let mut rows = content.output_rows_with_limit(1).unwrap();
    assert!(rows.next().unwrap().is_ok());
    assert!(rows.next().unwrap().is_err());
    assert!(rows.next().is_none());
}
