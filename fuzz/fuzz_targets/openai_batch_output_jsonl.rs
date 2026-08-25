#![no_main]

use libfuzzer_sys::fuzz_target;

const MAX_OPENAI_BATCH_OUTPUT_BYTES: usize = 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_OPENAI_BATCH_OUTPUT_BYTES {
        return;
    }
    rustee_ai_openai::fuzz_parse_batch_output_jsonl(data);
});
