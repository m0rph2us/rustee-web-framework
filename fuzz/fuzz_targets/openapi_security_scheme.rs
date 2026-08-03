#![no_main]

use libfuzzer_sys::fuzz_target;
use rustee_openapi::{OpenApiApiKeyLocation, OpenApiSecurityScheme};

fuzz_target!(|data: &[u8]| {
    let Ok(name) = std::str::from_utf8(data) else {
        return;
    };

    let _ = OpenApiSecurityScheme::api_key(name, OpenApiApiKeyLocation::Header);
});
