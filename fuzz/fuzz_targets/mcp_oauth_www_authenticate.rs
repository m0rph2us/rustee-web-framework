#![no_main]

use libfuzzer_sys::fuzz_target;

const MAX_WWW_AUTHENTICATE_BYTES: usize = 8 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_WWW_AUTHENTICATE_BYTES {
        return;
    }
    rustee_ai_mcp_oauth::fuzz_www_authenticate_challenges(data);
});
