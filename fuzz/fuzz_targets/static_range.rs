#![no_main]

use std::{convert::Infallible, fs, path::PathBuf, sync::OnceLock};

use http::{HeaderValue, Method, Request as HttpRequest, StatusCode, header::RANGE};
use libfuzzer_sys::fuzz_target;
use rustee_core::{IntoResponse, empty_body};
use rustee_static::StaticFiles;
use tokio::runtime::Runtime;
use tower::{Layer, ServiceExt, service_fn, util::BoxCloneService};

fn fixture_root() -> &'static PathBuf {
    static ROOT: OnceLock<PathBuf> = OnceLock::new();

    ROOT.get_or_init(|| {
        let root =
            std::env::temp_dir().join(format!("rustee-static-range-fuzz-{}", std::process::id()));
        fs::create_dir_all(&root).expect("create static fuzz fixture root");
        fs::write(root.join("sequence.txt"), b"0123456789")
            .expect("write static fuzz fixture file");
        root
    })
}

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| Runtime::new().expect("create static fuzz runtime"))
}

fn fallback_service() -> BoxCloneService<rustee_core::Request, rustee_core::Response, Infallible> {
    BoxCloneService::new(service_fn(|_| async {
        Ok::<_, Infallible>((StatusCode::IM_A_TEAPOT, "fallback").into_response())
    }))
}

fuzz_target!(|data: &[u8]| {
    let Ok(range) = HeaderValue::from_bytes(data) else {
        return;
    };

    let service = StaticFiles::new(fixture_root())
        .expect("static fuzz fixture is a readable directory")
        .at("/assets")
        .expect("static fuzz fixture mount is valid")
        .layer()
        .layer(fallback_service());
    let mut request = HttpRequest::builder()
        .method(Method::GET)
        .uri("/assets/sequence.txt")
        .body(empty_body())
        .expect("static fuzz request is valid");
    request.headers_mut().insert(RANGE, range);

    let _ = runtime().block_on(service.oneshot(request));
});
