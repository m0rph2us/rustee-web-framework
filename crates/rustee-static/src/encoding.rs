//! Precompressed representation selection and `Accept-Encoding` negotiation.

use std::path::{Path, PathBuf};

use http::{HeaderMap, HeaderValue, header::ACCEPT_ENCODING};

use super::{StaticFiles, delivery::StaticRepresentation};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PrecompressedEncoding {
    Brotli,
    Gzip,
}

impl PrecompressedEncoding {
    pub(super) const fn token(self) -> &'static str {
        match self {
            Self::Brotli => "br",
            Self::Gzip => "gzip",
        }
    }

    const fn suffix(self) -> &'static str {
        match self {
            Self::Brotli => ".br",
            Self::Gzip => ".gz",
        }
    }

    pub(super) const fn header_value(self) -> HeaderValue {
        match self {
            Self::Brotli => HeaderValue::from_static("br"),
            Self::Gzip => HeaderValue::from_static("gzip"),
        }
    }
}

pub(super) async fn select_precompressed_variant(
    files: &StaticFiles,
    target: &Path,
    request_headers: &HeaderMap,
) -> Option<StaticRepresentation> {
    for encoding in preferred_precompressed_encodings(request_headers) {
        let candidate = variant_path(target, encoding);
        let Ok(target) = tokio::fs::canonicalize(candidate).await else {
            continue;
        };
        if !target.starts_with(files.root.as_ref()) {
            continue;
        }
        let Ok(metadata) = tokio::fs::metadata(&target).await else {
            continue;
        };
        if metadata.is_file() && metadata.len() <= files.max_file_bytes {
            return Some(StaticRepresentation {
                target,
                metadata,
                content_encoding: Some(encoding),
            });
        }
    }
    None
}

fn preferred_precompressed_encodings(headers: &HeaderMap) -> Vec<PrecompressedEncoding> {
    let brotli = encoding_quality(headers, PrecompressedEncoding::Brotli);
    let gzip = encoding_quality(headers, PrecompressedEncoding::Gzip);
    match (brotli, gzip) {
        (Some(brotli), Some(gzip)) if brotli >= gzip => {
            vec![PrecompressedEncoding::Brotli, PrecompressedEncoding::Gzip]
        }
        (Some(_), Some(_)) => vec![PrecompressedEncoding::Gzip, PrecompressedEncoding::Brotli],
        (Some(_), None) => vec![PrecompressedEncoding::Brotli],
        (None, Some(_)) => vec![PrecompressedEncoding::Gzip],
        (None, None) => Vec::new(),
    }
}

fn encoding_quality(headers: &HeaderMap, encoding: PrecompressedEncoding) -> Option<u16> {
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

            if token.eq_ignore_ascii_case(encoding.token()) {
                explicit = Some(explicit.unwrap_or(0).max(quality));
            } else if token == "*" {
                wildcard = Some(wildcard.unwrap_or(0).max(quality));
            }
        }
    }

    found
        .then_some(explicit.or(wildcard).filter(|quality| *quality > 0))
        .flatten()
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

fn variant_path(target: &Path, encoding: PrecompressedEncoding) -> PathBuf {
    let mut path = target.as_os_str().to_os_string();
    path.push(encoding.suffix());
    PathBuf::from(path)
}
