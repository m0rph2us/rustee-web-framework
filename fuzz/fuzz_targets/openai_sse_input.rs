#![no_main]

use libfuzzer_sys::fuzz_target;

const MAX_OPENAI_SSE_INPUT_BYTES: usize = 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_OPENAI_SSE_INPUT_BYTES {
        return;
    }
    rustee_ai_openai::fuzz_parse_sse_input(data);
});
