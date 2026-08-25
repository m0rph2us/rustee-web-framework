#![no_main]

use std::{collections::BTreeMap, sync::OnceLock};

use http::Request as HttpRequest;
use libfuzzer_sys::fuzz_target;
use rustee_core::{FromRequest, Path, RouteParams, StateStore, empty_body};
use tokio::runtime::Runtime;

const MAX_PATH_PARAMETER_BYTES: usize = 16 * 1024;

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| Runtime::new().expect("create path fuzz runtime"))
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_PATH_PARAMETER_BYTES {
        return;
    }
    let Ok(value) = std::str::from_utf8(data) else {
        return;
    };
    let params = RouteParams::new(vec![("value".to_owned(), value.to_owned())]);
    let mut request = HttpRequest::builder()
        .uri("/")
        .body(empty_body())
        .expect("path fuzz request is valid");

    let _ = runtime().block_on(Path::<BTreeMap<String, String>>::from_request(
        &mut request,
        &params,
        &StateStore::default(),
    ));
});
