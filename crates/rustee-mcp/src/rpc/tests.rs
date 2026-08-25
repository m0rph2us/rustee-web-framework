use http::{Request as HttpRequest, header::CONTENT_TYPE};
use rustee_core::empty_body;
use serde_json::json;

use super::{
    RpcRequest, is_json_request, parse_prompt_get, parse_resource_uri,
    response::MAX_PROTOCOL_ERROR_RESPONSE_BYTES, rpc_error_response, rpc_result_response,
    tool_success,
};

#[test]
fn rpc_request_requires_a_bounded_versioned_shape() {
    let request = RpcRequest::parse(&json!({
        "jsonrpc":"2.0",
        "id":"request-1",
        "method":"tools/list",
        "params":{}
    }))
    .unwrap();
    assert_eq!(request.id, Some(json!("request-1")));
    assert_eq!(request.method, "tools/list");
    assert_eq!(request.params, json!({}));

    for value in [
        json!({"id":1,"method":"tools/list"}),
        json!({"jsonrpc":"2.0","id":1,"method":""}),
        json!({"jsonrpc":"2.0","id":true,"method":"tools/list"}),
        json!({"jsonrpc":"2.0","id":"x".repeat(129),"method":"tools/list"}),
    ] {
        assert!(RpcRequest::parse(&value).is_err());
    }
}

#[test]
fn json_request_admission_matches_standard_application_media_types() {
    for (content_type, accepted) in [
        ("application/json; charset=utf-8", true),
        ("APPLICATION/PROBLEM+JSON; charset=utf-8", true),
        ("application/problem+Json", true),
        ("text/problem+json", false),
        ("application/jsonp", false),
        ("application/+json", false),
    ] {
        let request = HttpRequest::builder()
            .header(CONTENT_TYPE, content_type)
            .body(empty_body())
            .unwrap();
        assert_eq!(is_json_request(&request), accepted, "{content_type}");
    }
}

#[test]
fn bounds_tool_result_text_before_wire_assembly() {
    assert!(tool_success(&json!({"result":"x".repeat(32)}), 16).is_none());
}

#[test]
fn context_request_identifiers_share_context_admission() {
    assert!(parse_resource_uri(&json!({"uri":"resource://tenant-a/customer/7"})).is_some());
    assert!(parse_resource_uri(&json!({"uri":"x".repeat(4097)})).is_none());
    assert!(parse_resource_uri(&json!({"uri":"resource://tenant-a/\u{0000}"})).is_none());

    assert!(parse_prompt_get(&json!({"name":"customer-summary"})).is_some());
    assert!(parse_prompt_get(&json!({"name":"x".repeat(129)})).is_none());
    assert!(parse_prompt_get(&json!({"name":" \t "})).is_none());
    assert!(
        parse_prompt_get(&json!({"name":"customer-summary","arguments":{"\u{0000}":"7"}}))
            .is_none()
    );
    assert!(
        parse_prompt_get(&json!({"name":"customer-summary","arguments":{" \t ":"7"}})).is_none()
    );
}

#[tokio::test]
async fn oversized_success_uses_a_protocol_error_fallback() {
    use http_body_util::BodyExt;

    let response =
        rpc_result_response(&json!(30), &json!({"tools":[{"name":"orders.lookup"}]}), 24);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(body["id"], serde_json::Value::Null);
    assert_eq!(body["error"]["code"], -32000);
}

#[tokio::test]
async fn fixed_protocol_error_bounds_an_escaping_request_id() {
    use http_body_util::BodyExt;

    let id = serde_json::Value::String("\"".repeat(128));
    let response = rpc_error_response(&id, -32601, "method not found");
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    assert!(bytes.len() <= MAX_PROTOCOL_ERROR_RESPONSE_BYTES);
    assert_eq!(body["id"], id);
    assert_eq!(body["error"]["code"], -32601);
}
