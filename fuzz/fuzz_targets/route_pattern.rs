#![no_main]

use http::Method;
use libfuzzer_sys::fuzz_target;
use rustee_router::App;

fuzz_target!(|data: &[u8]| {
    let Ok(path) = std::str::from_utf8(data) else {
        return;
    };

    let _ = App::new().try_route(Method::GET, path, || async { "matched" });
    let _ = App::new().try_nest(path, App::new());
});
