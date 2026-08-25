//! `OpenAI` configuration and transport-boundary regression coverage.

use super::*;
use crate::MAX_OPENAI_API_KEY_BYTES;

#[tokio::test]
async fn injected_client_still_enforces_the_configured_request_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (request_received, request_was_received) = oneshot::channel();
    let (release_server, release) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _request = read_http_request(&mut socket).await;
        let _ = request_received.send(());
        let _ = release.await;
    });
    let config = OpenAiConfig::new("test-key")
        .unwrap()
        .with_base_url(Url::parse(&format!("http://{address}/v1/")).unwrap())
        .unwrap()
        .with_request_timeout(Duration::from_millis(10))
        .unwrap();
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let provider = OpenAiResponsesProvider::with_client(client, config);

    let result = provider.complete(request()).await;
    tokio::time::timeout(Duration::from_secs(1), request_was_received)
        .await
        .unwrap()
        .unwrap();
    let _ = release_server.send(());
    server.await.unwrap();
    assert!(matches!(result, Err(OpenAiError::Transport)));
}

#[test]
fn configuration_redacts_credentials_and_validates_bounds() {
    let config = OpenAiConfig::new("sk-secret")
        .unwrap()
        .with_base_url(
            Url::parse("https://private-openai-gateway.example.test/v1/")
                .expect("test URL must be valid"),
        )
        .unwrap();
    let debug = format!("{config:?}");
    assert!(!debug.contains("sk-secret"));
    assert!(!debug.contains("private-openai-gateway.example.test"));
    assert_eq!(
        OpenAiConfig::new(" ").unwrap_err(),
        OpenAiConfigError::BlankApiKey
    );
    for api_key in ["api-key\r\n", "api-key\u{0000}"] {
        assert_eq!(
            OpenAiConfig::new(api_key).unwrap_err(),
            OpenAiConfigError::InvalidApiKey
        );
    }
    assert_eq!(
        OpenAiConfig::new("a".repeat(MAX_OPENAI_API_KEY_BYTES + 1)).unwrap_err(),
        OpenAiConfigError::InvalidApiKey
    );
    assert!(OpenAiConfig::new("gateway:opaque-key").is_ok());
    assert_eq!(
        config
            .clone()
            .with_base_url(Url::parse("ftp://example.test/v1/").unwrap())
            .unwrap_err(),
        OpenAiConfigError::InvalidBaseUrl
    );
    assert_eq!(
        config
            .clone()
            .with_base_url(Url::parse("https://user:secret@gateway.example.test/v1/").unwrap())
            .unwrap_err(),
        OpenAiConfigError::InvalidBaseUrl
    );
    assert_eq!(
        config.clone().with_max_batch_file_bytes(0).unwrap_err(),
        OpenAiConfigError::InvalidBatchFileLimit
    );
    assert_eq!(
        config.clone().with_max_response_bytes(0).unwrap_err(),
        OpenAiConfigError::ZeroResponseLimit
    );
    assert_eq!(
        config.clone().with_max_request_bytes(0).unwrap_err(),
        OpenAiConfigError::ZeroRequestLimit
    );
    assert_eq!(
        config
            .with_max_batch_file_bytes(OPENAI_BATCH_FILE_MAX_BYTES + 1)
            .unwrap_err(),
        OpenAiConfigError::InvalidBatchFileLimit
    );

    let limits = EmbeddingBatchLimits::new(2, 128).expect("test limits are valid");
    let embeddings = OpenAiEmbeddingsProvider::new(
        OpenAiConfig::new("sk-contract")
            .unwrap()
            .with_embedding_batch_limits(limits),
    )
    .expect("embedding client is valid");
    assert_eq!(embeddings.batch_limits(), limits);
}
