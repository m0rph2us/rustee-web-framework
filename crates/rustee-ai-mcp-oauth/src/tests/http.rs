//! HTTP OAuth adapter regression coverage using the shared loopback fixture.

use super::*;

#[tokio::test]
async fn revoker_posts_the_selected_secret_without_a_resource_parameter() {
    let (endpoint, request_task) = json_endpoint_once("{}").await;
    let revoker = HttpMcpOAuthTokenExchanger::new(&config()).expect("HTTP revoker must initialize");
    revoker
        .revoke(
            endpoint,
            McpOAuthRevocationRequest::for_test(
                CLIENT_ID.to_owned(),
                "revocation-refresh-token".to_owned(),
                McpOAuthRevocationTokenType::RefreshToken,
                Url::parse(RESOURCE).unwrap(),
            ),
        )
        .await
        .expect("bounded loopback revocation must succeed");
    let request = request_task
        .await
        .expect("test revocation endpoint task must complete");
    let (_, body) = request
        .split_once("\r\n\r\n")
        .expect("HTTP revocation request must contain a body");
    let form = url::form_urlencoded::parse(body.as_bytes())
        .into_owned()
        .collect::<BTreeMap<_, _>>();
    assert_eq!(form.get("client_id"), Some(&CLIENT_ID.to_owned()));
    assert_eq!(
        form.get("token"),
        Some(&"revocation-refresh-token".to_owned())
    );
    assert_eq!(
        form.get("token_type_hint"),
        Some(&"refresh_token".to_owned())
    );
    assert!(!form.contains_key("resource"));
}

#[tokio::test]
async fn exchanger_posts_pkce_and_resource_to_the_selected_token_endpoint() {
    let (endpoint, request_task) = json_endpoint_once(
        r#"{"access_token":"http-access-token","token_type":"Bearer","expires_in":60,"refresh_token":"http-refresh-token"}"#,
    )
    .await;
    let exchanger =
        HttpMcpOAuthTokenExchanger::new(&config()).expect("HTTP token exchanger must initialize");
    let token_set = exchanger
        .exchange(
            endpoint,
            McpOAuthTokenExchangeRequest {
                client_id: CLIENT_ID.to_owned(),
                code: "issued-code".to_owned(),
                redirect_uri: Url::parse(REDIRECT_URI).unwrap(),
                code_verifier: "v".repeat(43),
                resource: Url::parse(RESOURCE).unwrap(),
            },
        )
        .await
        .expect("bounded loopback token request must succeed");
    let request = request_task
        .await
        .expect("test token endpoint task must complete");
    let (_, body) = request
        .split_once("\r\n\r\n")
        .expect("HTTP request must contain a body");
    let form = url::form_urlencoded::parse(body.as_bytes())
        .into_owned()
        .collect::<BTreeMap<_, _>>();
    assert!(
        request
            .to_ascii_lowercase()
            .contains("accept: application/json")
    );
    assert_eq!(
        form.get("grant_type"),
        Some(&"authorization_code".to_owned())
    );
    assert_eq!(form.get("client_id"), Some(&CLIENT_ID.to_owned()));
    assert_eq!(form.get("code"), Some(&"issued-code".to_owned()));
    assert_eq!(form.get("redirect_uri"), Some(&REDIRECT_URI.to_owned()));
    assert_eq!(form.get("code_verifier"), Some(&"v".repeat(43)));
    assert_eq!(form.get("resource"), Some(&RESOURCE.to_owned()));
    let secrets = token_set.into_secrets();
    assert_eq!(secrets.access_token_for_encryption(), "http-access-token");
    assert_eq!(
        secrets.refresh_token_for_encryption(),
        Some("http-refresh-token")
    );
}

#[tokio::test]
async fn exchanger_rejects_an_oversized_chunked_token_response() {
    let body = format!(
        r#"{{"access_token":"{}","token_type":"Bearer"}}"#,
        "a".repeat(MAX_TOKEN_RESPONSE_BYTES)
    );
    let (endpoint, request_task) = chunked_json_endpoint_once(body).await;
    let exchanger =
        HttpMcpOAuthTokenExchanger::new(&config()).expect("HTTP token exchanger must initialize");

    assert_eq!(
        exchanger
            .exchange(
                endpoint,
                McpOAuthTokenExchangeRequest {
                    client_id: CLIENT_ID.to_owned(),
                    code: "issued-code".to_owned(),
                    redirect_uri: Url::parse(REDIRECT_URI).unwrap(),
                    code_verifier: "v".repeat(43),
                    resource: Url::parse(RESOURCE).unwrap(),
                },
            )
            .await,
        Err(McpOAuthError::TokenExchangeUnavailable)
    );
    let request = request_task
        .await
        .expect("test token endpoint task must complete");
    assert!(request.starts_with("POST /token HTTP/1.1\r\n"));
}

#[tokio::test]
async fn discovery_rejects_oversized_chunked_metadata() {
    let (endpoint, request_task) =
        chunked_json_endpoint_once("x".repeat(MAX_DISCOVERY_RESPONSE_BYTES + 1)).await;
    let resource = endpoint.join("mcp").unwrap();
    let discovery = HttpMcpOAuthDiscovery::new(
        &McpOAuthClientConfig::new(resource, CLIENT_ID, Url::parse(REDIRECT_URI).unwrap()).unwrap(),
    )
    .unwrap();

    assert_eq!(
        discovery
            .discover_resource_metadata_from_headers(&reqwest::header::HeaderMap::new())
            .await,
        Err(McpOAuthError::InvalidMetadata)
    );
    let request = request_task
        .await
        .expect("test discovery endpoint task must complete");
    assert!(request.starts_with("GET /.well-known/oauth-protected-resource/mcp HTTP/1.1\r\n"));
}

#[test]
fn json_content_type_requires_an_exact_media_type() {
    use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};

    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    assert!(is_json_content_type(&headers));

    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/jsonp"));
    assert!(!is_json_content_type(&headers));

    headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/json"));
    assert!(!is_json_content_type(&headers));

    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.append(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    assert!(!is_json_content_type(&headers));

    headers.clear();
    assert!(!is_json_content_type(&headers));
}
