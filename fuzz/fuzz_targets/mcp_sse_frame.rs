#![no_main]

use libfuzzer_sys::fuzz_target;

const MAX_SSE_FRAME_BYTES: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_SSE_FRAME_BYTES {
        return;
    }
    rustee_ai_mcp::fuzz_parse_sse_frame(data);
});
