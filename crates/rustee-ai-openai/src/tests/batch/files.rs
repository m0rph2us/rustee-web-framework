//! Batch provider-file transfer and acknowledgement regression coverage.

use super::super::*;

#[tokio::test]
async fn batch_file_upload_and_download_stay_explicit_and_content_redacted() {
    let input = OpenAiBatchInputJsonl::new(
        b"{\"custom_id\":\"row-1\",\"body\":\"private prompt\"}\n".to_vec(),
    )
    .unwrap();
    assert!(!format!("{input:?}").contains("private prompt"));
    let (upload_url, captured_upload, upload_server) = response_server(
        "application/json",
        json!({"id":"file_input_2","purpose":"batch"}).to_string(),
    )
    .await;
    let uploader = OpenAiBatchProvider::new(
        OpenAiConfig::new("sk-secret")
            .unwrap()
            .with_base_url(upload_url)
            .unwrap(),
    )
    .unwrap();
    let input_file = uploader.upload_batch_input(input).await.unwrap();
    let sent = captured_upload.await.unwrap();
    upload_server.await.unwrap();

    assert_eq!(input_file.id(), "file_input_2");
    assert!(sent.starts_with("POST /v1/files HTTP/1.1\r\n"));
    assert!(sent.contains("name=\"purpose\""));
    assert!(sent.contains("\r\n\r\nbatch\r\n"));
    assert!(sent.contains("filename=\"rustee-batch-input.jsonl\""));
    assert!(sent.contains("private prompt"));
    let request =
        OpenAiBatchRequest::from_uploaded_input(&input_file, OpenAiBatchEndpoint::Responses);
    assert_eq!(request.input_file_id(), "file_input_2");

    let output = "{\"custom_id\":\"row-1\",\"response\":{\"status_code\":200}}\n";
    let (download_url, captured_download, download_server) =
        response_server("application/jsonl", output.to_owned()).await;
    let downloader = OpenAiBatchProvider::new(
        OpenAiConfig::new("sk-secret")
            .unwrap()
            .with_base_url(download_url)
            .unwrap(),
    )
    .unwrap();
    let content = downloader
        .download_batch_file("file_output_2")
        .await
        .unwrap();
    let sent = captured_download.await.unwrap();
    download_server.await.unwrap();

    assert!(sent.starts_with("GET /v1/files/file_output_2/content HTTP/1.1\r\n"));
    assert_eq!(content.len(), output.len());
    assert!(!format!("{content:?}").contains("status_code"));
    assert_eq!(content.into_bytes(), output.as_bytes());
}

#[tokio::test]
async fn batch_file_upload_rejects_a_response_above_its_configured_limit() {
    let (url, captured_request, server) = response_server(
        "application/json",
        json!({"id":"file_input_oversized","purpose":"batch"}).to_string(),
    )
    .await;
    let provider = OpenAiBatchProvider::new(
        OpenAiConfig::new("sk-secret")
            .unwrap()
            .with_base_url(url)
            .unwrap()
            .with_max_response_bytes(16)
            .unwrap(),
    )
    .unwrap();

    assert!(matches!(
        provider
            .upload_batch_input(OpenAiBatchInputJsonl::new(b"{}\n".to_vec()).unwrap())
            .await,
        Err(OpenAiError::ResponseTooLarge)
    ));
    let sent = captured_request.await.unwrap();
    server.await.unwrap();
    assert!(sent.starts_with("POST /v1/files HTTP/1.1\r\n"));
}

#[tokio::test]
async fn batch_file_download_rejects_a_content_length_above_the_configured_bound() {
    let (url, captured_request, server) =
        response_server("application/jsonl", "large".into()).await;
    let provider = OpenAiBatchProvider::new(
        OpenAiConfig::new("sk-secret")
            .unwrap()
            .with_base_url(url)
            .unwrap()
            .with_max_batch_file_bytes(4)
            .unwrap(),
    )
    .unwrap();

    assert!(matches!(
        provider.download_batch_file("file_output_3").await,
        Err(OpenAiError::BatchFileTooLarge)
    ));
    let sent = captured_request.await.unwrap();
    server.await.unwrap();
    assert!(sent.starts_with("GET /v1/files/file_output_3/content HTTP/1.1\r\n"));
}

#[tokio::test]
async fn batch_file_deletion_requires_an_exact_provider_acknowledgement() {
    let (url, captured_request, server) = response_server(
        "application/json",
        json!({"id":"file_output_4","object":"file","deleted":true}).to_string(),
    )
    .await;
    let provider = OpenAiBatchProvider::new(
        OpenAiConfig::new("sk-secret")
            .unwrap()
            .with_base_url(url)
            .unwrap(),
    )
    .unwrap();

    let deletion = provider.delete_batch_file("file_output_4").await.unwrap();
    let sent = captured_request.await.unwrap();
    server.await.unwrap();
    assert_eq!(deletion.id(), "file_output_4");
    assert!(!format!("{deletion:?}").contains("file_output_4"));
    assert!(sent.starts_with("DELETE /v1/files/file_output_4 HTTP/1.1\r\n"));
}
