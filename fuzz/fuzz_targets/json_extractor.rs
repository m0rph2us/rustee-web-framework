#![no_main]

use std::sync::OnceLock;

use http::{Request as HttpRequest, header::CONTENT_TYPE};
use libfuzzer_sys::fuzz_target;
use rustee_core::{FromRequest, Json, RouteParams, StateStore, full_body};
use tokio::runtime::Runtime;

const MAX_JSON_BYTES: usize = 64 * 1024;

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| Runtime::new().expect("create JSON fuzz runtime"))
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_JSON_BYTES {
        return;
    }
    let mut request = HttpRequest::builder()
        .header(CONTENT_TYPE, "application/json")
        .body(full_body(data.to_vec()))
        .expect("JSON fuzz request is valid");

    let _ = runtime().block_on(Json::<serde_json::Value>::from_request(
        &mut request,
        &RouteParams::default(),
        &StateStore::default(),
    ));
});
