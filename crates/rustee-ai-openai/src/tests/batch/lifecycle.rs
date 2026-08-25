//! Batch lifecycle dispatch and opt-in live qualification coverage.

use super::super::*;

#[tokio::test]
async fn batch_path_operations_reject_dot_segments_before_network_dispatch() {
    let provider = OpenAiBatchProvider::new(OpenAiConfig::new("sk-contract").unwrap()).unwrap();

    for dot_segment in [".", ".."] {
        let receipt = AiBatchReceipt::new(dot_segment).unwrap();
        assert!(matches!(
            provider.retrieve(&receipt).await,
            Err(OpenAiError::MalformedBatch)
        ));
        assert!(matches!(
            provider.cancel(&receipt).await,
            Err(OpenAiError::MalformedBatch)
        ));
        assert!(matches!(
            provider.download_batch_file(dot_segment).await,
            Err(OpenAiError::MalformedBatchFile)
        ));
        assert!(matches!(
            provider.delete_batch_file(dot_segment).await,
            Err(OpenAiError::MalformedBatchFile)
        ));
    }
}

#[tokio::test]
async fn batch_submit_rejects_an_oversized_request_before_network_dispatch() {
    let provider = OpenAiBatchProvider::new(
        OpenAiConfig::new("sk-contract")
            .unwrap()
            .with_max_request_bytes(1)
            .unwrap(),
    )
    .unwrap();

    assert!(matches!(
        provider
            .submit(
                AiBatchReference::new("tenant-a.v1", "catalog-1", "run-1").unwrap(),
                OpenAiBatchRequest::new("file_input_1", OpenAiBatchEndpoint::Responses).unwrap(),
            )
            .await,
        Err(OpenAiError::RequestTooLarge)
    ));
}

#[tokio::test]
async fn batch_provider_submits_uploaded_file_with_the_stable_run_key() {
    let (url, captured_request, server) = response_server(
        "application/json",
        json!({
            "id":"batch_1",
            "status":"validating",
            "request_counts":{"total":0,"completed":0,"failed":0},
            "output_file_id":null,
            "error_file_id":null
        })
        .to_string(),
    )
    .await;
    let config = OpenAiConfig::new("sk-secret")
        .unwrap()
        .with_base_url(url)
        .unwrap();
    let provider = OpenAiBatchProvider::new(config).unwrap();

    let receipt = provider
        .submit(
            AiBatchReference::new("tenant-a.v1", "catalog-1", "run-1").unwrap(),
            OpenAiBatchRequest::new("file_input_1", OpenAiBatchEndpoint::Responses)
                .unwrap()
                .with_output_expiration(
                    OpenAiBatchOutputExpiration::new(Duration::from_secs(60 * 60)).unwrap(),
                ),
        )
        .await
        .unwrap();
    let sent = captured_request.await.unwrap();
    server.await.unwrap();

    assert_eq!(receipt.provider_batch_id(), "batch_1");
    assert!(sent.starts_with("POST /v1/batches HTTP/1.1\r\n"));
    assert!(sent.contains("\"input_file_id\":\"file_input_1\""));
    assert!(sent.contains("\"endpoint\":\"/v1/responses\""));
    assert!(sent.contains("\"rustee_run_key\":\"run-1\""));
    assert!(sent.contains("\"output_expires_after\":{\"anchor\":\"created_at\",\"seconds\":3600}"));
    assert!(!sent.contains("private prompt"));
}

#[tokio::test]
async fn batch_provider_retrieves_and_cancels_without_downloading_result_contents() {
    let (retrieve_url, captured_retrieve, retrieve_server) = response_server(
        "application/json",
        json!({
            "id":"batch_2",
            "status":"in_progress",
            "request_counts":{"total":5,"completed":2,"failed":0},
            "output_file_id":null,
            "error_file_id":null
        })
        .to_string(),
    )
    .await;
    let config = OpenAiConfig::new("sk-secret")
        .unwrap()
        .with_base_url(retrieve_url)
        .unwrap();
    let provider = OpenAiBatchProvider::new(config).unwrap();
    let receipt = AiBatchReceipt::new("batch_2").unwrap();
    let snapshot = provider.retrieve(&receipt).await.unwrap();
    let sent = captured_retrieve.await.unwrap();
    retrieve_server.await.unwrap();

    assert_eq!(snapshot.status(), OpenAiBatchStatus::InProgress);
    assert!(sent.starts_with("GET /v1/batches/batch_2 HTTP/1.1\r\n"));

    let (cancel_url, captured_cancel, cancel_server) = response_server(
        "application/json",
        json!({
            "id":"batch_2",
            "status":"cancelling",
            "request_counts":{"total":5,"completed":2,"failed":0},
            "output_file_id":null,
            "error_file_id":null
        })
        .to_string(),
    )
    .await;
    let cancel_provider = OpenAiBatchProvider::new(
        OpenAiConfig::new("sk-secret")
            .unwrap()
            .with_base_url(cancel_url)
            .unwrap(),
    )
    .unwrap();
    let cancelled = cancel_provider.cancel(&receipt).await.unwrap();
    let sent = captured_cancel.await.unwrap();
    cancel_server.await.unwrap();

    assert_eq!(cancelled.status(), OpenAiBatchStatus::Cancelling);
    assert!(sent.starts_with("POST /v1/batches/batch_2/cancel HTTP/1.1\r\n"));
}

#[tokio::test]
#[ignore = "requires RUSTEE_OPENAI_BATCH_LIVE=1, OPENAI_API_KEY, RUSTEE_OPENAI_BATCH_MODEL, and RUSTEE_OPENAI_BATCH_RUN_KEY; creates a billable provider Batch then requests cancellation"]
async fn live_batch_lifecycle_is_explicitly_opt_in() {
    assert_eq!(
        std::env::var("RUSTEE_OPENAI_BATCH_LIVE").as_deref(),
        Ok("1"),
        "set RUSTEE_OPENAI_BATCH_LIVE=1 only after approving provider spend and artifact retention"
    );
    let api_key = std::env::var("OPENAI_API_KEY").expect("OPENAI_API_KEY is required");
    let model =
        std::env::var("RUSTEE_OPENAI_BATCH_MODEL").expect("RUSTEE_OPENAI_BATCH_MODEL is required");
    let run_key = std::env::var("RUSTEE_OPENAI_BATCH_RUN_KEY")
        .expect("RUSTEE_OPENAI_BATCH_RUN_KEY is required");
    let provider = OpenAiBatchProvider::new(OpenAiConfig::new(api_key).unwrap()).unwrap();
    let mut builder = OpenAiBatchJsonlBuilder::new(OpenAiBatchEndpoint::Responses);
    builder
        .push(
            OpenAiBatchInputRow::new(
                "rustee-live-batch-qualification-1",
                OpenAiBatchEndpoint::Responses,
                json!({
                    "model": model,
                    "input": "Reply with exactly: ok",
                    "max_output_tokens": 16,
                }),
            )
            .unwrap(),
        )
        .unwrap();
    let input_file = provider
        .upload_batch_input(builder.build().unwrap())
        .await
        .unwrap();
    let reference =
        AiBatchReference::new("rustee-live", "openai-batch-qualification", run_key).unwrap();
    let receipt = provider
        .submit(
            reference,
            OpenAiBatchRequest::from_uploaded_input(&input_file, OpenAiBatchEndpoint::Responses),
        )
        .await
        .unwrap();
    let retrieved = provider.retrieve(&receipt).await.unwrap();
    assert_eq!(retrieved.receipt(), &receipt);
    let cancelled = provider.cancel(&receipt).await.unwrap();
    assert_eq!(cancelled.receipt(), &receipt);
}
