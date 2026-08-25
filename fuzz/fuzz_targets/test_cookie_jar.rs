#![no_main]

use std::sync::OnceLock;

use http::{HeaderMap, StatusCode, header::SET_COOKIE};
use libfuzzer_sys::fuzz_target;
use rustee_core::{empty_body, response};
use rustee_router::App;
use rustee_test::TestApp;
use tokio::runtime::Runtime;

const MAX_SET_COOKIE_BYTES: usize = 16 * 1024;

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| Runtime::new().expect("create cookie fuzz runtime"))
}

fn app() -> App {
    App::new().get("/", |headers: HeaderMap| async move {
        let mut response = response(StatusCode::NO_CONTENT, empty_body());
        if let Some(value) = headers.get("x-fuzz-set-cookie") {
            response.headers_mut().append(SET_COOKIE, value.clone());
        }
        response
    })
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_SET_COOKIE_BYTES {
        return;
    }
    let Ok(value) = std::str::from_utf8(data) else {
        return;
    };
    let client = TestApp::new(app()).with_cookie_jar();
    let Ok(request) = client.get("/") else {
        return;
    };
    let Ok(request) = request.header("x-fuzz-set-cookie", value) else {
        return;
    };

    let _ = runtime().block_on(request.send());
});
