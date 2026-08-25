//! `OpenAI` Embeddings request and response regression coverage.

use super::*;

#[test]
fn embedding_request_mapping_keeps_batch_input_order() {
    let inputs = vec![
        EmbeddingInput::new("chunk-1", "first text").unwrap(),
        EmbeddingInput::new("chunk-2", "second text").unwrap(),
    ];

    assert_eq!(
        embedding_request_body("text-embedding-test", &inputs),
        json!({
            "model":"text-embedding-test",
            "input":["first text", "second text"],
        })
    );
    assert!(!format!("{:?}", inputs[0]).contains("first text"));
}

#[test]
fn embedding_batch_validation_rejects_empty_and_configured_overages() {
    let one_input_limit = EmbeddingBatchLimits::new(1, 16).expect("test limits are valid");
    assert!(matches!(
        validate_embedding_batch(&[], one_input_limit),
        Err(OpenAiError::EmptyEmbeddingBatch)
    ));
    assert!(matches!(
        validate_embedding_batch(
            &[
                EmbeddingInput::new("chunk-1", "text").unwrap(),
                EmbeddingInput::new("chunk-2", "text").unwrap(),
            ],
            one_input_limit,
        ),
        Err(OpenAiError::EmbeddingBatchInputLimit)
    ));
    assert!(matches!(
        validate_embedding_batch(
            &[EmbeddingInput::new("chunk-1", "content exceeds limit").unwrap()],
            one_input_limit,
        ),
        Err(OpenAiError::EmbeddingBatchContentLimit)
    ));
}

#[tokio::test]
async fn embeddings_provider_rejects_an_empty_batch_before_network_dispatch() {
    let provider = OpenAiEmbeddingsProvider::new(OpenAiConfig::new("sk-contract").unwrap())
        .expect("embedding client is valid");

    assert!(matches!(
        provider
            .embed("text-embedding-test".to_owned(), Vec::new())
            .await,
        Err(OpenAiError::EmptyEmbeddingBatch)
    ));
}

#[tokio::test]
async fn embeddings_provider_enforces_configured_batch_limits_before_network_dispatch() {
    let provider = OpenAiEmbeddingsProvider::new(
        OpenAiConfig::new("sk-contract")
            .unwrap()
            .with_embedding_batch_limits(
                EmbeddingBatchLimits::new(1, 4).expect("test limits are valid"),
            ),
    )
    .expect("embedding client is valid");

    assert!(matches!(
        provider
            .embed(
                "text-embedding-test".to_owned(),
                vec![EmbeddingInput::new("chunk-1", "five!").unwrap()],
            )
            .await,
        Err(OpenAiError::EmbeddingBatchContentLimit)
    ));
}

#[tokio::test]
async fn embeddings_provider_reorders_indexed_response_and_redacts_input_debug() {
    let (url, captured_request, server) = response_server(
        "application/json",
        json!({
            "data":[
                {"index":1,"embedding":[2.0,-1.0]},
                {"index":0,"embedding":[0.25,0.5]}
            ]
        })
        .to_string(),
    )
    .await;
    let provider = OpenAiEmbeddingsProvider::new(
        OpenAiConfig::new("sk-contract")
            .unwrap()
            .with_base_url(url)
            .unwrap(),
    )
    .unwrap();

    let embeddings = provider
        .embed(
            "text-embedding-test".to_owned(),
            vec![
                EmbeddingInput::new("chunk-1", "first text").unwrap(),
                EmbeddingInput::new("chunk-2", "second text").unwrap(),
            ],
        )
        .await
        .unwrap();
    let sent = captured_request.await.unwrap();
    server.await.unwrap();

    assert!(sent.starts_with("POST /v1/embeddings HTTP/1.1\r\n"));
    assert!(sent.contains("authorization: Bearer sk-contract\r\n"));
    assert!(sent.contains("\"model\":\"text-embedding-test\""));
    assert!(sent.contains("\"input\":[\"first text\",\"second text\"]"));
    assert_eq!(embeddings[0].values(), &[0.25, 0.5]);
    assert_eq!(embeddings[1].values(), &[2.0, -1.0]);
}

#[tokio::test]
async fn embeddings_provider_rejects_a_body_above_its_configured_limit() {
    let (url, _captured_request, server) = response_server(
        "application/json",
        json!({"data":[{"index":0,"embedding":[0.25,0.5]}]}).to_string(),
    )
    .await;
    let provider = OpenAiEmbeddingsProvider::new(
        OpenAiConfig::new("sk-contract")
            .unwrap()
            .with_base_url(url)
            .unwrap()
            .with_max_response_bytes(16)
            .unwrap(),
    )
    .unwrap();

    assert!(matches!(
        provider
            .embed(
                "text-embedding-test".to_owned(),
                vec![EmbeddingInput::new("chunk-1", "first text").unwrap()],
            )
            .await,
        Err(OpenAiError::ResponseTooLarge)
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn embeddings_provider_rejects_an_oversized_request_before_network_dispatch() {
    let provider = OpenAiEmbeddingsProvider::new(
        OpenAiConfig::new("sk-contract")
            .unwrap()
            .with_max_request_bytes(1)
            .unwrap(),
    )
    .unwrap();

    assert!(matches!(
        provider
            .embed(
                "text-embedding-test".to_owned(),
                vec![EmbeddingInput::new("chunk-1", "first text").unwrap()],
            )
            .await,
        Err(OpenAiError::RequestTooLarge)
    ));
}

#[test]
fn embedding_response_rejects_duplicate_or_missing_indexes() {
    assert!(matches!(
        decode_embeddings(
            &json!({
                "data":[
                    {"index":0,"embedding":[0.25]},
                    {"index":0,"embedding":[0.5]}
                ]
            }),
            2,
        ),
        Err(OpenAiError::MalformedEmbeddingResponse)
    ));
}
