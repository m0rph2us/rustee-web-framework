#![no_main]

use libfuzzer_sys::fuzz_target;

const MAX_STDIO_MESSAGE_BYTES: usize = 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_STDIO_MESSAGE_BYTES {
        return;
    }
    rustee_ai_mcp::fuzz_parse_stdio_message(data);
});
