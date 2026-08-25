//! HTTP content-coding negotiation and response eligibility policy.

use http::{
    HeaderMap, HeaderValue, StatusCode,
    header::{
        ACCEPT_ENCODING, CACHE_CONTROL, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE,
        CONTENT_TYPE, VARY,
    },
};
use rustee_core::Response;

use super::ContentCoding;

pub(super) fn select_coding(
    brotli_enabled: bool,
    gzip_enabled: bool,
    headers: &HeaderMap,
) -> Option<ContentCoding> {
    let brotli = brotli_enabled.then(|| quality_for(headers, ContentCoding::Brotli));
    let gzip = gzip_enabled.then(|| quality_for(headers, ContentCoding::Gzip));

    match (brotli.flatten(), gzip.flatten()) {
        (Some(brotli), Some(gzip)) if brotli >= gzip => Some(ContentCoding::Brotli),
        (Some(_) | None, Some(_)) => Some(ContentCoding::Gzip),
        (Some(_), None) => Some(ContentCoding::Brotli),
        (None, None) => None,
    }
}

pub(super) fn is_compressible_response(response: &Response) -> bool {
    let status = response.status();
    if status.is_informational()
        || matches!(status, StatusCode::NO_CONTENT | StatusCode::NOT_MODIFIED)
        || response.headers().contains_key(CONTENT_ENCODING)
        || response.headers().contains_key(CONTENT_RANGE)
        || cache_control_no_transform(response.headers())
    {
        return false;
    }

    if response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim() == "0")
    {
        return false;
    }

    response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(is_compressible_content_type)
}

pub(super) fn add_vary_accept_encoding(headers: &mut HeaderMap) {
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

fn cache_control_no_transform(headers: &HeaderMap) -> bool {
    headers.get_all(CACHE_CONTROL).iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value
                .split(',')
                .map(str::trim)
                .any(|directive| directive.eq_ignore_ascii_case("no-transform"))
        })
    })
}

fn is_compressible_content_type(value: &str) -> bool {
    let media_type = value
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    media_type.starts_with("text/")
        || matches!(
            media_type.as_str(),
            "application/javascript"
                | "application/json"
                | "application/ld+json"
                | "application/manifest+json"
                | "application/xhtml+xml"
                | "application/xml"
                | "application/rss+xml"
                | "application/atom+xml"
                | "image/svg+xml"
        )
        || media_type.ends_with("+json")
        || media_type.ends_with("+xml")
}

fn quality_for(headers: &HeaderMap, coding: ContentCoding) -> Option<u16> {
    let mut explicit = None;
    let mut wildcard = None;
    let mut found = false;

    for value in &headers.get_all(ACCEPT_ENCODING) {
        let Ok(value) = value.to_str() else {
            continue;
        };
        found = true;
        for item in value.split(',') {
            let mut parameters = item.split(';');
            let token = parameters.next().unwrap_or_default().trim();
            let quality = parameters
                .find_map(|parameter| {
                    let (name, value) = parameter.trim().split_once('=')?;
                    name.trim()
                        .eq_ignore_ascii_case("q")
                        .then_some(value.trim())
                })
                .map_or(Some(1_000), parse_quality)
                .unwrap_or(0);

            if token.eq_ignore_ascii_case(coding.token()) {
                explicit = Some(explicit.unwrap_or(0).max(quality));
            } else if token == "*" {
                wildcard = Some(wildcard.unwrap_or(0).max(quality));
            }
        }
    }

    if !found {
        return None;
    }
    explicit.or(wildcard).filter(|quality| *quality > 0)
}

fn parse_quality(value: &str) -> Option<u16> {
    if value == "0" || value == "0." {
        return Some(0);
    }
    if value == "1" || value == "1." {
        return Some(1_000);
    }
    let (whole, fraction) = value.split_once('.')?;
    if fraction.len() > 3 || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    match whole {
        "0" => {
            let parsed = fraction.parse::<u16>().ok()?;
            let scale = match fraction.len() {
                1 => 100,
                2 => 10,
                3 => 1,
                _ => return None,
            };
            Some(parsed * scale)
        }
        "1" if fraction.bytes().all(|byte| byte == b'0') => Some(1_000),
        _ => None,
    }
}
