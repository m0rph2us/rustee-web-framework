use std::{
    error::Error as StdError,
    fs, io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use http::{
    HeaderValue, Method, Request as HttpRequest, StatusCode,
    header::{
        ACCEPT_ENCODING, ACCEPT_RANGES, CACHE_CONTROL, CONTENT_ENCODING, CONTENT_LENGTH,
        CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, IF_RANGE,
        LAST_MODIFIED, RANGE, VARY,
    },
};
use http_body_util::BodyExt;
use rustee_core::{IntoResponse, empty_body};
use tower::{Layer, ServiceExt, service_fn, util::BoxCloneService};

use super::{MAX_RANGE_MEMBERS, StaticFiles, StaticFilesError, delivery::STREAMING_CHUNK_BYTES};

static NEXT_TEMP_ROOT: AtomicU64 = AtomicU64::new(0);

struct TempRoot(PathBuf);

impl TempRoot {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let sequence = NEXT_TEMP_ROOT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "rustee-static-{}-{nonce}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn request(method: Method, uri: &str) -> rustee_core::Request {
    HttpRequest::builder()
        .method(method)
        .uri(uri)
        .body(empty_body())
        .unwrap()
}

fn fallback_service()
-> BoxCloneService<rustee_core::Request, rustee_core::Response, std::convert::Infallible> {
    BoxCloneService::new(service_fn(|_| async {
        Ok::<_, std::convert::Infallible>((StatusCode::IM_A_TEAPOT, "fallback").into_response())
    }))
}

mod delivery;
mod encoding;
mod range;
mod response;

#[test]
fn rejects_a_zero_streaming_threshold() {
    let root = TempRoot::new();
    assert!(matches!(
        StaticFiles::new(root.path())
            .unwrap()
            .with_streaming_threshold(0),
        Err(StaticFilesError::ZeroStreamingThreshold)
    ));
}

#[test]
fn configuration_diagnostics_redact_filesystem_details_and_preserve_sources() {
    let error = StaticFilesError::RootCanonicalization(io::Error::new(
        io::ErrorKind::NotFound,
        "private-static-root-path",
    ));

    assert!(!format!("{error:?}").contains("private-static-root-path"));
    assert!(!error.to_string().contains("private-static-root-path"));
    assert!(StdError::source(&error).is_some());
}

#[test]
fn configuration_debug_output_redacts_root_mount_and_header_values() {
    let root = TempRoot::new();
    let files = StaticFiles::new(root.path())
        .unwrap()
        .at("/private-assets")
        .unwrap()
        .with_cache_control(HeaderValue::from_static("private, max-age=17"));

    let debug = format!("{files:?}");
    assert!(!debug.contains(root.path().to_string_lossy().as_ref()));
    assert!(!debug.contains("/private-assets"));
    assert!(!debug.contains("private, max-age=17"));
    assert!(debug.contains("root_configured: true"));
    assert!(debug.contains("mount_path_configured: true"));
    assert!(debug.contains("cache_control_configured: true"));
}

fn precompressed_request(accept_encoding: &str) -> rustee_core::Request {
    HttpRequest::builder()
        .method(Method::GET)
        .uri("/assets/app.js")
        .header(ACCEPT_ENCODING, accept_encoding)
        .body(empty_body())
        .unwrap()
}

async fn collect_data_chunks(mut body: rustee_core::Body) -> (Vec<usize>, Vec<u8>) {
    let mut chunk_sizes = Vec::new();
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.unwrap();
        if let Ok(data) = frame.into_data() {
            chunk_sizes.push(data.len());
            bytes.extend_from_slice(data.as_ref());
        }
    }
    (chunk_sizes, bytes)
}
