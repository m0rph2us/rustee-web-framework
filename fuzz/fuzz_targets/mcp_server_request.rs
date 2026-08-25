#![no_main]

use libfuzzer_sys::fuzz_target;

const MAX_MCP_SERVER_REQUEST_BYTES: usize = 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_MCP_SERVER_REQUEST_BYTES {
        return;
    }
    rustee_mcp::fuzz_parse_request(data);
});
