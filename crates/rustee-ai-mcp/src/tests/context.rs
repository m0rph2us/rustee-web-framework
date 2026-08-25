//! Read-only MCP context discovery regression coverage.

use super::*;

#[tokio::test]
async fn context_discovery_and_reads_stay_explicit_and_bounded() {
    let (endpoint, server) = server(context_replies()).await;
    let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
    client.initialize().await.unwrap();

    let resources = client.list_resources().await.unwrap();
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].name(), "customer-record");
    let templates = client.list_resource_templates().await.unwrap();
    assert_eq!(
        templates[0].uri_template(),
        "resource://tenant-a/customer/{id}"
    );

    let contents = client.read_resource(resources[0].uri()).await.unwrap();
    assert!(matches!(
        contents[0].data(),
        McpResourceData::Text(text) if text == "private customer context"
    ));
    assert!(!format!("{:?}", contents[0]).contains("private customer context"));

    let prompts = client.list_prompts().await.unwrap();
    assert!(prompts[0].arguments()[0].required());
    let arguments = BTreeMap::from([("customer_id".to_owned(), "7".to_owned())]);
    let prompt = client
        .get_prompt("customer-summary", &arguments)
        .await
        .unwrap();
    assert_eq!(prompt.messages().len(), 2);
    assert!(matches!(
        prompt.messages()[0].content(),
        McpPromptContent::Text(text) if text == "Summarize the selected customer."
    ));
    assert!(!format!("{prompt:?}").contains("Summarize the selected customer."));

    assert_context_request_sequence(&server.await.unwrap());
}

#[tokio::test]
async fn context_capability_gate_does_not_send_an_unsupported_request() {
    let (endpoint, server) = server(vec![
        json_reply(1, &initialize_result(), None),
        status_reply(202),
    ])
    .await;
    let client = McpHttpClient::new(McpHttpConfig::new(endpoint).unwrap()).unwrap();
    client.initialize().await.unwrap();

    assert_eq!(
        client.list_resources().await.unwrap_err(),
        McpError::UnsupportedCapability
    );
    assert_eq!(
        client.list_prompts().await.unwrap_err(),
        McpError::UnsupportedCapability
    );

    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 2);
}
