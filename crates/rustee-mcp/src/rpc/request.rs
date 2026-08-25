//! Bounded MCP JSON-RPC request admission and parameter parsing.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use http::header::CONTENT_TYPE;
use http_body_util::BodyExt;
use rustee_core::{Request, is_standard_json_media_type};
use serde_json::{Value, json};
use url::Url;

use crate::{
    MCP_PROTOCOL_VERSION,
    context::{is_valid_context_name, is_valid_resource_uri},
    header::{HeaderAdmission, admit_single_header},
};

const MAX_REQUEST_ID_BYTES: usize = 128;
const MAX_CONTEXT_ARGUMENTS: usize = 64;
const MAX_CONTEXT_ARGUMENT_VALUE_BYTES: usize = 8192;

pub(crate) struct RpcRequest {
    pub(crate) id: Option<Value>,
    pub(crate) method: String,
    pub(crate) params: Value,
}

impl RpcRequest {
    pub(crate) fn parse(value: &Value) -> Result<Self, ()> {
        if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            return Err(());
        }
        let method = value
            .get("method")
            .and_then(Value::as_str)
            .filter(|method| !method.is_empty())
            .ok_or(())?
            .to_owned();
        let id = match value.get("id") {
            None => None,
            Some(Value::String(id)) if id.len() <= MAX_REQUEST_ID_BYTES => {
                Some(Value::String(id.clone()))
            }
            Some(Value::Number(id)) if id.to_string().len() <= MAX_REQUEST_ID_BYTES => {
                Some(Value::Number(id.clone()))
            }
            Some(_) => return Err(()),
        };
        let params = value.get("params").cloned().unwrap_or_else(|| json!({}));
        Ok(Self { id, method, params })
    }
}

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_parse_request(input: &[u8]) {
    let Ok(value) = serde_json::from_slice::<Value>(input) else {
        return;
    };
    let Ok(request) = RpcRequest::parse(&value) else {
        return;
    };
    match request.method.as_str() {
        "tools/list" | "resources/list" | "resources/templates/list" | "prompts/list" => {
            let _ = valid_list_params(&request.params);
        }
        "tools/call" => {
            let _ = parse_tool_call(&request.params);
        }
        "resources/read" => {
            let _ = parse_resource_uri(&request.params);
        }
        "prompts/get" => {
            let _ = parse_prompt_get(&request.params);
        }
        _ => {}
    }
}

pub(crate) enum RequestBodyError {
    TooLarge,
    Read,
}

pub(crate) async fn collect_limited(
    request: &mut Request,
    limit: usize,
) -> Result<Bytes, RequestBodyError> {
    let mut body = Vec::new();
    while let Some(frame) = request.body_mut().frame().await {
        let frame = frame.map_err(|_| RequestBodyError::Read)?;
        if let Ok(data) = frame.into_data() {
            if body.len().saturating_add(data.len()) > limit {
                return Err(RequestBodyError::TooLarge);
            }
            body.extend_from_slice(&data);
        }
    }
    Ok(Bytes::from(body))
}

pub(crate) fn is_json_request(request: &Request) -> bool {
    matches!(
        admit_single_header(request.headers(), CONTENT_TYPE),
        HeaderAdmission::Valid(value) if is_standard_json_media_type(value)
    )
}

pub(crate) fn valid_protocol_header(request: &Request) -> bool {
    matches!(
        admit_single_header(request.headers(), "mcp-protocol-version"),
        HeaderAdmission::Valid(value) if value == MCP_PROTOCOL_VERSION
    )
}

pub(crate) fn parse_tool_call(params: &Value) -> Option<(String, Value)> {
    let name = params.get("name")?.as_str()?.to_owned();
    let arguments = match params.get("arguments") {
        None => Value::Object(serde_json::Map::default()),
        Some(arguments) if arguments.is_object() => arguments.clone(),
        Some(_) => return None,
    };
    Some((name, arguments))
}

pub(crate) fn valid_list_params(params: &Value) -> bool {
    params.is_object() && params.get("cursor").is_none()
}

pub(crate) fn parse_resource_uri(params: &Value) -> Option<Url> {
    let object = params.as_object()?;
    if object.len() != 1 {
        return None;
    }
    let uri = object.get("uri")?.as_str()?;
    if !is_valid_resource_uri(uri) {
        return None;
    }
    Url::parse(uri).ok()
}

pub(crate) fn parse_prompt_get(params: &Value) -> Option<(&str, BTreeMap<String, String>)> {
    let object = params.as_object()?;
    let name = object.get("name")?.as_str()?;
    if !is_valid_context_name(name) {
        return None;
    }
    let arguments = match object.get("arguments") {
        None => BTreeMap::new(),
        Some(Value::Object(arguments)) if arguments.len() <= MAX_CONTEXT_ARGUMENTS => arguments
            .iter()
            .map(|(key, value)| {
                let value = value.as_str()?;
                (is_valid_context_name(key)
                    && value.len() <= MAX_CONTEXT_ARGUMENT_VALUE_BYTES
                    && !value.contains('\0'))
                .then(|| (key.clone(), value.to_owned()))
            })
            .collect::<Option<BTreeMap<_, _>>>()?,
        Some(_) => return None,
    };
    (object.len() == 1 || (object.len() == 2 && object.contains_key("arguments")))
        .then_some((name, arguments))
}

pub(crate) fn unique_values<Key>(mut values: impl Iterator<Item = Key>) -> bool
where
    Key: Ord,
{
    let mut seen = BTreeSet::new();
    values.all(|value| seen.insert(value))
}
