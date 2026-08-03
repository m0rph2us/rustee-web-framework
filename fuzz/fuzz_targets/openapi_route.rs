#![no_main]

use libfuzzer_sys::fuzz_target;
use rustee_openapi::OpenApiRoute;

fuzz_target!(|data: &[u8]| {
    let Ok(route) = std::str::from_utf8(data) else {
        return;
    };

    let _ = OpenApiRoute::from_rustee(route);
});
