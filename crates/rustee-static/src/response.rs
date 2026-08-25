//! Static HTTP response construction, cache validators, and multipart range bodies.

use std::{
    fs,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use http::{
    HeaderMap, HeaderValue, StatusCode,
    header::{
        ACCEPT_ENCODING, ACCEPT_RANGES, CACHE_CONTROL, CONTENT_ENCODING, CONTENT_LENGTH,
        CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED, VARY,
        X_CONTENT_TYPE_OPTIONS,
    },
};
use rustee_core::{Body, Response, empty_body, response};

use super::range::{content_range_header, unsatisfied_content_range_header};
use super::{StaticFiles, delivery::StaticRepresentation};
use super::{encoding::PrecompressedEncoding, range::ByteRange};

mod multipart;

pub(super) use multipart::{multipart_boundary, multipart_content_length, multipart_file_body};

#[derive(Clone, Debug)]
pub(super) struct CacheValidators {
    pub(super) etag: HeaderValue,
    pub(super) last_modified: HeaderValue,
    pub(super) modified: SystemTime,
}

pub(super) struct StaticResponseContext<'a> {
    pub(super) files: &'a StaticFiles,
    pub(super) representation: &'a StaticRepresentation,
    pub(super) validators: Option<&'a CacheValidators>,
    pub(super) content_type_target: &'a Path,
    pub(super) varies_by_encoding: bool,
}
pub(super) fn static_success_response(
    context: &StaticResponseContext<'_>,
    status: StatusCode,
    body: Body,
    length: u64,
    range: Option<ByteRange>,
) -> Response {
    let mut response = response(status, body);
    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, content_type(context.content_type_target));
    headers.insert(CONTENT_LENGTH, HeaderValue::from(length));
    headers.insert(CACHE_CONTROL, context.files.cache_control.clone());
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Some(range) = range {
        headers.insert(
            CONTENT_RANGE,
            content_range_header(range, context.representation.metadata.len()),
        );
    }
    if let Some(validators) = context.validators {
        insert_validators(headers, validators);
    }
    apply_representation_headers(
        headers,
        context.representation.content_encoding,
        context.varies_by_encoding,
    );
    response
}

pub(super) fn static_multipart_response(
    context: &StaticResponseContext<'_>,
    body: Body,
    length: u64,
    boundary: &str,
) -> Response {
    let mut response = response(StatusCode::PARTIAL_CONTENT, body);
    let headers = response.headers_mut();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str(&format!("multipart/byteranges; boundary={boundary}"))
            .expect("generated multipart content type is a valid header"),
    );
    headers.insert(CONTENT_LENGTH, HeaderValue::from(length));
    headers.insert(CACHE_CONTROL, context.files.cache_control.clone());
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Some(validators) = context.validators {
        insert_validators(headers, validators);
    }
    response
}

pub(super) fn cache_validators(metadata: &fs::Metadata) -> Option<CacheValidators> {
    let modified = metadata.modified().ok()?;
    let since_epoch = modified.duration_since(UNIX_EPOCH).ok()?;
    let etag = HeaderValue::from_str(&format!(
        "W/\"{:x}-{:x}-{:x}\"",
        metadata.len(),
        since_epoch.as_secs(),
        since_epoch.subsec_nanos()
    ))
    .ok()?;
    let last_modified = HeaderValue::from_str(&httpdate::fmt_http_date(modified)).ok()?;
    Some(CacheValidators {
        etag,
        last_modified,
        modified,
    })
}

pub(super) fn is_not_modified(headers: &HeaderMap, validators: Option<&CacheValidators>) -> bool {
    match if_none_match(headers, validators) {
        Some(matches) => matches,
        None => {
            validators.is_some_and(|validators| if_modified_since(headers, validators.modified))
        }
    }
}

fn if_none_match(headers: &HeaderMap, validators: Option<&CacheValidators>) -> Option<bool> {
    let values = headers.get_all(IF_NONE_MATCH);
    values.iter().next()?;
    let Some(validators) = validators else {
        return Some(
            values
                .iter()
                .any(|value| value.as_bytes().trim_ascii() == b"*"),
        );
    };
    let Ok(current) = validators.etag.to_str() else {
        return Some(false);
    };
    Some(values.iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value
                .split(',')
                .map(str::trim)
                .any(|candidate| candidate == "*" || weak_etag_matches(candidate, current))
        })
    }))
}

fn weak_etag_matches(candidate: &str, current: &str) -> bool {
    strip_weak_prefix(candidate) == strip_weak_prefix(current)
}

fn strip_weak_prefix(value: &str) -> &str {
    value.trim().strip_prefix("W/").unwrap_or(value.trim())
}

fn if_modified_since(headers: &HeaderMap, modified: SystemTime) -> bool {
    let values = headers.get_all(IF_MODIFIED_SINCE);
    let mut values = values.iter();
    let Some(value) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }
    let Ok(value) = value.to_str() else {
        return false;
    };
    let Ok(since) = httpdate::parse_http_date(value) else {
        return false;
    };
    truncate_to_seconds(modified).is_some_and(|modified| modified <= since)
}

pub(super) fn truncate_to_seconds(value: SystemTime) -> Option<SystemTime> {
    let duration = value.duration_since(UNIX_EPOCH).ok()?;
    UNIX_EPOCH.checked_add(Duration::from_secs(duration.as_secs()))
}

pub(super) fn static_not_modified(
    files: &StaticFiles,
    validators: Option<&CacheValidators>,
    content_encoding: Option<PrecompressedEncoding>,
    varies_by_encoding: bool,
) -> Response {
    let mut response = response(StatusCode::NOT_MODIFIED, empty_body());
    let headers = response.headers_mut();
    headers.insert(CACHE_CONTROL, files.cache_control.clone());
    headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Some(validators) = validators {
        insert_validators(headers, validators);
    }
    apply_representation_headers(headers, content_encoding, varies_by_encoding);
    response
}

pub(super) fn static_range_not_satisfiable(
    files: &StaticFiles,
    validators: Option<&CacheValidators>,
    length: u64,
) -> Response {
    let mut response = response(StatusCode::RANGE_NOT_SATISFIABLE, empty_body());
    let headers = response.headers_mut();
    headers.insert(CACHE_CONTROL, files.cache_control.clone());
    headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(CONTENT_RANGE, unsatisfied_content_range_header(length));
    if let Some(validators) = validators {
        insert_validators(headers, validators);
    }
    response
}

fn insert_validators(headers: &mut HeaderMap, validators: &CacheValidators) {
    headers.insert(ETAG, validators.etag.clone());
    headers.insert(LAST_MODIFIED, validators.last_modified.clone());
}

fn apply_representation_headers(
    headers: &mut HeaderMap,
    content_encoding: Option<PrecompressedEncoding>,
    varies_by_encoding: bool,
) {
    if let Some(content_encoding) = content_encoding {
        headers.insert(CONTENT_ENCODING, content_encoding.header_value());
    }
    if varies_by_encoding {
        add_vary_accept_encoding(headers);
    }
}

fn add_vary_accept_encoding(headers: &mut HeaderMap) {
    let already_varies = headers.get_all(VARY).iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value
                .split(',')
                .map(str::trim)
                .any(|field| field == "*" || field.eq_ignore_ascii_case(ACCEPT_ENCODING.as_str()))
        })
    });
    if !already_varies {
        headers.append(VARY, HeaderValue::from_static("Accept-Encoding"));
    }
}
pub(super) fn content_type(path: &Path) -> HeaderValue {
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase);
    match extension.as_deref() {
        Some("css") => HeaderValue::from_static("text/css; charset=utf-8"),
        Some("gif") => HeaderValue::from_static("image/gif"),
        Some("htm" | "html") => HeaderValue::from_static("text/html; charset=utf-8"),
        Some("ico") => HeaderValue::from_static("image/x-icon"),
        Some("jpeg" | "jpg") => HeaderValue::from_static("image/jpeg"),
        Some("js" | "mjs") => HeaderValue::from_static("text/javascript; charset=utf-8"),
        Some("json" | "map") => HeaderValue::from_static("application/json; charset=utf-8"),
        Some("png") => HeaderValue::from_static("image/png"),
        Some("svg") => HeaderValue::from_static("image/svg+xml"),
        Some("txt") => HeaderValue::from_static("text/plain; charset=utf-8"),
        Some("wasm") => HeaderValue::from_static("application/wasm"),
        Some("webp") => HeaderValue::from_static("image/webp"),
        _ => HeaderValue::from_static("application/octet-stream"),
    }
}
