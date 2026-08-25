#![no_main]

use std::{collections::BTreeMap, sync::OnceLock};

use http::{Request as HttpRequest, Uri};
use libfuzzer_sys::fuzz_target;
use rustee_core::{FromRequest, Query, RouteParams, StateStore, empty_body};
use tokio::runtime::Runtime;

const MAX_QUERY_BYTES: usize = 16 * 1024;

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| Runtime::new().expect("create query fuzz runtime"))
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_QUERY_BYTES {
        return;
    }
    let Ok(query) = std::str::from_utf8(data) else {
        return;
    };
    let Ok(uri) = format!("/?{query}").parse::<Uri>() else {
        return;
    };
    let mut request = HttpRequest::builder()
        .uri(uri)
        .body(empty_body())
        .expect("query fuzz request is valid");

    let _ = runtime().block_on(Query::<BTreeMap<String, String>>::from_request(
        &mut request,
        &RouteParams::default(),
        &StateStore::default(),
    ));
});
