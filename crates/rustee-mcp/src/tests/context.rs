use std::{collections::BTreeMap, convert::Infallible};

use serde_json::json;
use url::Url;

use crate::{
    MCP_PROTOCOL_VERSION, McpContextCapabilities, McpContextProvider, McpServerConfig,
    McpServerPrompt, McpServerPromptArgument, McpServerPromptContent, McpServerPromptMessage,
    McpServerPromptResult, McpServerResource, McpServerResourceContents, McpServerResourceTemplate,
};

use super::support::{protocol_request, request, response_json, server};

#[tokio::test]
async fn serves_explicit_context_with_the_same_authenticated_request_boundary() {
    let (server, _, _) = server(["orders.lookup"]);
    let server = server.with_context_provider(AuthorizedContext);

    let initialize = response_json(
        server
            .handle(request(&json!({
                "jsonrpc":"2.0",
                "id":20,
                "method":"initialize",
                "params":{"protocolVersion":MCP_PROTOCOL_VERSION}
            })))
            .await,
    )
    .await;
    assert_eq!(
        initialize["result"]["capabilities"]["resources"]["subscribe"],
        false
    );
    assert_eq!(
        initialize["result"]["capabilities"]["prompts"]["listChanged"],
        false
    );

    let resources = response_json(
        server
            .handle(protocol_request(&json!({
                "jsonrpc":"2.0","id":21,"method":"resources/list","params":{}
            })))
            .await,
    )
    .await;
    assert_eq!(
        resources["result"]["resources"][0]["uri"],
        "resource://tenant-a/customer/7"
    );
    assert_eq!(
        resources["result"]["resources"][0]["mimeType"],
        "application/json"
    );

    let templates = response_json(
        server
            .handle(protocol_request(&json!({
                "jsonrpc":"2.0","id":22,"method":"resources/templates/list","params":{}
            })))
            .await,
    )
    .await;
    assert_eq!(
        templates["result"]["resourceTemplates"][0]["uriTemplate"],
        "resource://tenant-a/customer/{customer_id}"
    );

    let resource = response_json(
        server
            .handle(protocol_request(&json!({
                "jsonrpc":"2.0","id":23,"method":"resources/read",
                "params":{"uri":"resource://tenant-a/customer/7"}
            })))
            .await,
    )
    .await;
    assert_eq!(
        resource["result"]["contents"][0]["text"],
        "{\"customer_id\":\"7\"}"
    );

    let prompts = response_json(
        server
            .handle(protocol_request(&json!({
                "jsonrpc":"2.0","id":24,"method":"prompts/list","params":{}
            })))
            .await,
    )
    .await;
    assert_eq!(prompts["result"]["prompts"][0]["name"], "customer-summary");
    assert_eq!(
        prompts["result"]["prompts"][0]["arguments"][0]["required"],
        true
    );

    let prompt = response_json(
        server
            .handle(protocol_request(&json!({
                "jsonrpc":"2.0","id":25,"method":"prompts/get",
                "params":{"name":"customer-summary","arguments":{"customer_id":"7"}}
            })))
            .await,
    )
    .await;
    assert_eq!(prompt["result"]["messages"][0]["role"], "user");
    assert_eq!(
        prompt["result"]["messages"][0]["content"]["text"],
        "Summarize customer 7."
    );
}

#[tokio::test]
async fn fail_closed_context_is_not_advertised_or_invoked() {
    let (server, _, _) = server(["orders.lookup"]);
    let initialize = response_json(
        server
            .handle(request(&json!({
                "jsonrpc":"2.0","id":26,"method":"initialize",
                "params":{"protocolVersion":MCP_PROTOCOL_VERSION}
            })))
            .await,
    )
    .await;
    assert!(
        initialize["result"]["capabilities"]
            .get("resources")
            .is_none()
    );
    assert!(
        initialize["result"]["capabilities"]
            .get("prompts")
            .is_none()
    );

    let response = response_json(
        server
            .handle(protocol_request(&json!({
                "jsonrpc":"2.0","id":27,"method":"resources/list","params":{}
            })))
            .await,
    )
    .await;
    assert_eq!(response["error"]["code"], -32601);
}

#[tokio::test]
async fn rejects_unbounded_or_inconsistent_context_provider_results() {
    let (mut server, _, _) = server(["orders.lookup"]);
    server.config = McpServerConfig::new("rustee-mcp-test", "0.1.0")
        .unwrap()
        .with_max_context_items(1)
        .unwrap();
    let server = server.with_context_provider(InvalidContext);

    let list = response_json(
        server
            .handle(protocol_request(&json!({
                "jsonrpc":"2.0","id":28,"method":"resources/list","params":{}
            })))
            .await,
    )
    .await;
    assert_eq!(list["error"]["code"], -32000);

    let read = response_json(
        server
            .handle(protocol_request(&json!({
                "jsonrpc":"2.0","id":29,"method":"resources/read",
                "params":{"uri":"resource://tenant-a/customer/7"}
            })))
            .await,
    )
    .await;
    assert_eq!(read["error"]["code"], -32000);
}

#[derive(Clone)]
struct AuthorizedContext;

impl McpContextProvider for AuthorizedContext {
    type Error = Infallible;

    fn capabilities(&self) -> McpContextCapabilities {
        McpContextCapabilities::default()
            .with_resources()
            .with_prompts()
    }

    fn list_resources(
        &self,
        _: &rustee_core::Request,
    ) -> Result<Vec<McpServerResource>, Self::Error> {
        Ok(vec![
            McpServerResource::new(
                Url::parse("resource://tenant-a/customer/7").unwrap(),
                "customer-profile",
            )
            .unwrap()
            .with_mime_type("application/json")
            .unwrap(),
        ])
    }

    fn list_resource_templates(
        &self,
        _: &rustee_core::Request,
    ) -> Result<Vec<McpServerResourceTemplate>, Self::Error> {
        Ok(vec![
            McpServerResourceTemplate::new(
                "resource://tenant-a/customer/{customer_id}",
                "customer-profile",
            )
            .unwrap(),
        ])
    }

    fn read_resource(
        &self,
        _: &rustee_core::Request,
        uri: &Url,
    ) -> Result<Vec<McpServerResourceContents>, Self::Error> {
        assert_eq!(uri.as_str(), "resource://tenant-a/customer/7");
        Ok(vec![
            McpServerResourceContents::text(uri.clone(), "{\"customer_id\":\"7\"}")
                .with_mime_type("application/json")
                .unwrap(),
        ])
    }

    fn list_prompts(&self, _: &rustee_core::Request) -> Result<Vec<McpServerPrompt>, Self::Error> {
        Ok(vec![
            McpServerPrompt::new(
                "customer-summary",
                vec![McpServerPromptArgument::new("customer_id", true).unwrap()],
            )
            .unwrap(),
        ])
    }

    fn get_prompt(
        &self,
        _: &rustee_core::Request,
        name: &str,
        arguments: &BTreeMap<String, String>,
    ) -> Result<McpServerPromptResult, Self::Error> {
        assert_eq!(name, "customer-summary");
        assert_eq!(arguments.get("customer_id"), Some(&"7".to_owned()));
        Ok(McpServerPromptResult::new(vec![
            McpServerPromptMessage::user(McpServerPromptContent::Text(
                "Summarize customer 7.".to_owned(),
            )),
        ]))
    }
}

#[derive(Clone)]
struct InvalidContext;

impl McpContextProvider for InvalidContext {
    type Error = Infallible;

    fn capabilities(&self) -> McpContextCapabilities {
        McpContextCapabilities::default().with_resources()
    }

    fn list_resources(
        &self,
        _: &rustee_core::Request,
    ) -> Result<Vec<McpServerResource>, Self::Error> {
        Ok(["one", "two"]
            .into_iter()
            .map(|name| {
                McpServerResource::new(
                    Url::parse(&format!("resource://tenant-a/{name}")).unwrap(),
                    name,
                )
                .unwrap()
            })
            .collect())
    }

    fn list_resource_templates(
        &self,
        _: &rustee_core::Request,
    ) -> Result<Vec<McpServerResourceTemplate>, Self::Error> {
        Ok(Vec::new())
    }

    fn read_resource(
        &self,
        _: &rustee_core::Request,
        _: &Url,
    ) -> Result<Vec<McpServerResourceContents>, Self::Error> {
        Ok(vec![McpServerResourceContents::text(
            Url::parse("resource://tenant-a/another-customer").unwrap(),
            "unexpected",
        )])
    }

    fn list_prompts(&self, _: &rustee_core::Request) -> Result<Vec<McpServerPrompt>, Self::Error> {
        Ok(Vec::new())
    }

    fn get_prompt(
        &self,
        _: &rustee_core::Request,
        _: &str,
        _: &BTreeMap<String, String>,
    ) -> Result<McpServerPromptResult, Self::Error> {
        Ok(McpServerPromptResult::new(Vec::new()))
    }
}
