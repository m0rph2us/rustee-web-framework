//! Internal facade for bounded MCP JSON-RPC request handling and response encoding.
//!
//! Input admission and output encoding have separate byte budgets and failure behavior. Keeping
//! them in child modules makes that boundary explicit while preserving one internal RPC import
//! surface for the server implementation.

mod request;
mod response;

pub(super) use request::{
    RequestBodyError, RpcRequest, collect_limited, is_json_request, parse_prompt_get,
    parse_resource_uri, parse_tool_call, unique_values, valid_list_params, valid_protocol_header,
};
pub(super) use response::{
    response_limit_error, rpc_error_response, rpc_result_response, tool_failure, tool_success,
};

/// Runs the bounded MCP server request parser against one fuzz input.
#[cfg(feature = "fuzzing")]
pub(super) fn fuzz_parse_request(input: &[u8]) {
    request::fuzz_parse_request(input);
}

#[cfg(test)]
mod tests;
